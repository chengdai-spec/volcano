/*
Copyright 2021 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/util"
)

type PredicateHelper interface {
	PredicateNodes(task *api.TaskInfo, nodes []*api.NodeInfo, fn api.PredicateFn, enableErrorCache bool, nodesInShard sets.Set[string]) ([]*api.NodeInfo, *api.FitErrors)
}

type predicateHelper struct {
	taskPredicateErrorCache map[string]map[string]error
}

// PredicateNodes 对候选节点列表执行谓词过滤，返回满足 Task 调度要求的节点子集
//
// 步骤 1：初始化过滤结果容器与错误缓存
//   - 创建 FitErrors 收集所有节点的谓词失败原因
//   - 若 Task 的 TaskRole 为空，禁用错误缓存（防止不同 Pod 因相同空 TaskRole 互相影响）
//
// 步骤 2：计算需要找到的可行节点数量
//   - 调用 CalculateNumOfFeasibleNodesToFind 根据集群规模自适应计算
//   - 节点数较少或百分比 >= 100 时检查全部节点，否则按百分比截断以优化性能
//
// 步骤 3：初始化错误缓存与轮询起始位置
//   - taskGroupID = "JobUID/TaskRole"，同一角色规格的 Task 共享缓存
//   - lastProcessedNodeIndex 记录上一轮调度周期最后处理的节点位置
//   - 本轮从该位置开始轮询，确保所有节点在跨 Pod 调度中有均等的被检查机会
//
// 步骤 4：创建带取消功能的 Context
//   - 当已找到的可行节点数达到 numNodesToFind 时，触发 cancel() 提前终止并行检查
//
// 步骤 5：定义单节点检查函数 checkNode（由 workqueue 并行执行）
//
//	步骤 5.1：轮询偏移取节点
//	  - 从 startIndex 开始轮询，(startIndex+index)%allNodes 保证环形遍历
//
//	步骤 5.2：错误缓存快速跳过
//	  - 若该 TaskRole 组之前有过谓词失败记录，且当前节点已在缓存中，
//	    直接将缓存错误写入 FitErrors 并返回，跳过实际谓词执行
//
//	步骤 5.3：硬分片模式过滤
//	  - 硬分片模式下，不属于当前调度器分片的节点直接标记失败
//
//	步骤 5.4：执行实际谓词检查
//	  - 调用 fn(task, node) 执行完整的谓词链（资源、亲和性、污点等）
//	  - 失败时将错误写入节点缓存与 FitErrors
//
//	步骤 5.5：记录可行节点
//	  - 原子递增 numFoundNodes，若超过 numNodesToFind 则触发 cancel 并回退计数
//	  - 否则将节点写入 predicateNodes 数组对应位置
//
// 步骤 6：并行执行所有节点的 checkNode
//   - workqueue.ParallelizeUntil 最多 16 个 goroutine 并发
//   - 记录谓词阶段耗时指标
//
// 步骤 7：更新轮询起始位置并裁剪结果
//   - 将本轮处理过的节点数累加到 lastProcessedNodeIndex，供下一轮使用
//   - 将 predicateNodes 切片裁剪至实际找到的数量
func (ph *predicateHelper) PredicateNodes(task *api.TaskInfo, nodes []*api.NodeInfo, fn api.PredicateFn, enableErrorCache bool, nodesInShard sets.Set[string]) ([]*api.NodeInfo, *api.FitErrors) {
	// ── 步骤 1：初始化 ─────────────────────────────────────────────────────
	var errorLock sync.RWMutex
	fe := api.NewFitErrors()

	// 若 TaskRole 为空则禁用缓存：不同 Pod 的 TaskRole 都为空时会共享同一缓存 key，
	// 一个 Pod 谓词失败会导致所有其他 Pod 也被误判失败（参见 issue #3527）
	if len(task.TaskRole) == 0 {
		enableErrorCache = false
	}

	// ── 步骤 2：计算需要找到的可行节点数量 ────────────────────────────────────
	allNodes := len(nodes)
	if allNodes == 0 {
		return make([]*api.NodeInfo, 0), fe
	}
	// 根据集群节点数和配置百分比，计算找到多少个可行节点即可停止搜索
	// 大规模集群无需检查全部节点，找到足够多的候选即可进入打分阶段
	numNodesToFind := CalculateNumOfFeasibleNodesToFind(int32(allNodes))

	// 预分配固定大小数组，避免运行时扩容
	// 使用数组+原子计数器而非切片，保证并发写入的安全性
	predicateNodes := make([]*api.NodeInfo, numNodesToFind)

	numFoundNodes := int32(0)  // 已找到的可行节点数（原子操作）
	processedNodes := int32(0) // 已处理的节点总数（用于更新下轮起始位置）

	// ── 步骤 3：初始化错误缓存与轮询起始位置 ─────────────────────────────────
	// taskGroupID 格式为 "JobUID/TaskRole"，同一 Job 中相同角色的 Task 共享缓存
	// 设计意图：同一角色规格的 Pod 对节点的谓词结果通常一致，可复用历史失败记录
	taskGroupid := taskGroupID(task)
	nodeErrorCache, taskFailedBefore := ph.taskPredicateErrorCache[taskGroupid]
	if nodeErrorCache == nil {
		nodeErrorCache = map[string]error{}
	}

	// lastProcessedNodeIndex 是全局原子变量，跨调度周期持久化
	// 本轮从上一轮结束的位置开始，实现轮询式公平检查
	startIndex := int(lastProcessedNodeIndex.Load())

	// ── 步骤 4：创建带取消功能的 Context ─────────────────────────────────────
	// 当找到的可行节点数达到 numNodesToFind 时，通过 cancel() 提前终止剩余节点的检查
	ctx, cancel := context.WithCancel(context.Background())

	// ── 步骤 5：定义单节点检查函数 ───────────────────────────────────────────
	// 由 workqueue.ParallelizeUntil 并发调用，index 为当前处理的节点下标(0 ~ allNodes-1)
	checkNode := func(index int) {
		// 步骤 5.1：轮询偏移取节点
		// 从 startIndex 开始环形遍历，确保跨调度周期所有节点都有均等机会被检查
		node := nodes[(startIndex+index)%allNodes]
		atomic.AddInt32(&processedNodes, 1)
		klog.V(4).Infof("Considering Task <%v/%v> on node <%v>: <%v> vs. <%v>",
			task.Namespace, task.Name, node.Name, task.Resreq, node.Idle)

		// 步骤 5.2：错误缓存快速跳过
		// 若该 TaskRole 组历史上有过谓词失败，且当前节点已在缓存中，
		// 直接复用历史错误，跳过实际谓词执行(性能优化)
		if enableErrorCache && taskFailedBefore {
			errorLock.RLock()
			errC, ok := nodeErrorCache[node.Name]
			errorLock.RUnlock()

			if ok {
				// 将缓存的错误写入 FitErrors，供上层记录失败原因
				errorLock.Lock()
				fe.SetNodeError(node.Name, errC)
				errorLock.Unlock()
				return
			}
		}

		// 步骤 5.3：硬分片模式过滤
		// 硬分片模式下，调度器只负责一部分节点；不属于本分片的节点直接跳过
		if options.ServerOpts.ShardingMode == util.HardShardingMode && !nodesInShard.Has(node.Name) {
			klog.V(3).Infof("Predicates failed: node %s is not in scheduler shard", node.Name)
			err := fmt.Errorf("node isn't in scheduler node shard")
			errorLock.Lock()
			nodeErrorCache[node.Name] = err
			ph.taskPredicateErrorCache[taskGroupid] = nodeErrorCache
			fe.SetNodeError(node.Name, err)
			errorLock.Unlock()
			return
		}

		// 步骤 5.4：执行实际谓词检查
		// fn 是上层传入的谓词函数，内部会依次执行：
		//   - FilterOutUnschedulableAndUnresolvableNodes(历史失败缓存过滤)
		//   - 各插件的 PredicateFn(资源、亲和性、污点、PDB 等)
		// TODO (k82cn): 启用 eCache 进一步提升性能
		if err := fn(task, node); err != nil {
			klog.V(3).Infof("Predicates failed: %v", err)
			errorLock.Lock()
			// 将失败原因写入缓存，供同角色后续 Task 复用
			nodeErrorCache[node.Name] = err
			ph.taskPredicateErrorCache[taskGroupid] = nodeErrorCache
			fe.SetNodeError(node.Name, err)
			errorLock.Unlock()
			return
		}

		// 步骤 5.5：记录可行节点
		// 原子递增已找到节点数，判断是否已达到目标数量
		length := atomic.AddInt32(&numFoundNodes, 1)
		if length > numNodesToFind {
			// 已达到目标数量，触发 cancel 通知其他 goroutine 提前退出
			// 同时回退计数，因为当前节点不会被计入结果
			cancel()
			atomic.AddInt32(&numFoundNodes, -1)
		} else {
			// 将可行节点写入预分配数组的对应位置（length-1 为 0-based 索引）
			// 数组预分配避免了切片 append 的并发竞争问题
			predicateNodes[length-1] = node
		}
	}

	// ── 步骤 6：并行执行谓词检查 ────────────────────────────────────────────
	// 最多 16 个 goroutine 并发检查 allNodes 个节点
	// 当 ctx 被 cancel 时（已找到足够节点），剩余未开始的检查会被跳过
	predicateStart := time.Now()
	workqueue.ParallelizeUntil(ctx, 16, allNodes, checkNode)
	// 记录谓词阶段耗时，用于性能监控
	metrics.UpdateSchedulingStageDuration(metrics.SchedulingStagePredicate, time.Since(predicateStart))

	// ── 步骤 7：更新轮询起始位置并裁剪结果 ───────────────────────────────────
	// 将本轮处理过的节点数累加到全局索引，下一轮从该位置继续
	// 取模保证索引在 [0, allNodes) 范围内循环
	newIndex := int64((startIndex + int(processedNodes)) % allNodes)
	lastProcessedNodeIndex.Store(newIndex)

	// 将预分配数组裁剪至实际找到的节点数，去除未使用的空位
	predicateNodes = predicateNodes[:numFoundNodes]
	return predicateNodes, fe
}

func taskGroupID(task *api.TaskInfo) string {
	return fmt.Sprintf("%s/%s", task.Job, task.TaskRole)
}

func NewPredicateHelper() PredicateHelper {
	return &predicateHelper{taskPredicateErrorCache: map[string]map[string]error{}}
}

// GetPredicatedNodeByShard return predicateNodes by shard
func GetPredicatedNodeByShard(predicateNodes []*api.NodeInfo, nodesInShard sets.Set[string]) [2][]*api.NodeInfo {
	var candidateNodes [2][]*api.NodeInfo
	var candidateNodesInShard []*api.NodeInfo
	var candidateNodesInOtherShards []*api.NodeInfo
	shardingMode := options.ServerOpts.ShardingMode
	for _, node := range predicateNodes {
		if shardingMode == util.SoftShardingMode && nodesInShard != nil && !nodesInShard.Has(node.Name) {
			candidateNodesInOtherShards = append(candidateNodesInOtherShards, node)
		} else {
			candidateNodesInShard = append(candidateNodesInShard, node)
		}
	}
	candidateNodes[0] = candidateNodesInShard
	candidateNodes[1] = candidateNodesInOtherShards
	return candidateNodes
}
