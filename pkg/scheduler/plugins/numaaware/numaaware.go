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

// Package numaaware 实现了 Volcano 调度器中的 NUMA 感知（numa-aware）插件。
//
// 【NUMA 架构背景】
// NUMA（Non-Uniform Memory Access，非统一内存访问）是现代多路服务器的常见架构。
// 在 NUMA 架构下，每个 CPU 插槽（Socket）有自己的本地内存，访问本地内存速度快，
// 访问远端（其他 Socket 的）内存速度慢。因此，将 Pod 的 CPU 和内存资源尽量分配到
// 同一个或相邻的 NUMA Node 上，可以显著提升性能。
//
// 【插件核心职责】
// numa-aware 插件在调度阶段模拟 kubelet 的 Topology Manager 行为，
// 在调度器侧预先计算 Pod 的 CPU/资源在 NUMA 拓扑上的最优分配方案，
// 从而选择 NUMA 拓扑最优的节点，并在 predicate 阶段过滤掉无法满足 NUMA 约束的节点。
//
// 【生效阶段】
// 该插件注册了以下扩展点：
//   - PredicateFn：在 predicate 阶段检查节点是否能满足 Pod 的 NUMA 拓扑约束
//   - BatchNodeOrderFn：在 score 阶段根据 NUMA 拓扑质量对节点打分排序
//   - EventHandler：在 allocate/deallocate 事件时更新节点资源 NUMA 集合
//
// 【整体流程】
//  1. Predicate 阶段：遍历 Pod 的每个容器，收集 HintProvider 的拓扑提示（TopologyHint），
//     根据节点的 NUMA 策略（none/best-effort/restricted/single-numa-node）判断是否可以准入。
//  2. Score 阶段：根据分配给 Pod 的 CPU 所跨越的 NUMA Node 数量打分，
//     跨越的 NUMA Node 越少，分数越高（拓扑局部性越好）。
//  3. Session 结束时：将最终的资源分配结果写回调度器缓存，供后续调度周期使用。
package numaaware

import (
	"context"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/utils/cpuset"

	nodeinfov1alpha1 "volcano.sh/apis/pkg/apis/nodeinfo/v1alpha1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/numaaware/policy"
	"volcano.sh/volcano/pkg/scheduler/plugins/numaaware/provider/cpumanager"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// PluginName 插件名称，用于在调度器配置中标识该插件
	PluginName = "numa-aware"
	// NumaTopoWeight 插件权重配置项的键名，用于控制该插件在打分阶段的权重
	NumaTopoWeight = "weight"
)

// numaPlugin 是 NUMA 感知插件的主结构体，保存插件运行所需的全部状态
type numaPlugin struct {
	sync.Mutex
	// pluginArguments 存储插件的配置参数，从调度器配置中传入
	pluginArguments framework.Arguments
	// hintProviders 拓扑提示提供者列表，目前只有 cpuManager 一个实现
	// 每个 HintProvider 负责为特定资源类型（如 CPU）生成 NUMA 拓扑提示
	hintProviders []policy.HintProvider
	// assignRes 记录每个 Task 在每个节点上的资源 NUMA 分配方案
	// 结构：map[taskUID]map[nodename]map[resourceName]cpuset.CPUSet
	// 在 predicate 阶段计算并缓存，在 allocate 事件中使用，在 session 结束时写回
	assignRes map[api.TaskID]map[string]api.ResNumaSets
	// nodeResSets 记录每个节点当前可用的 NUMA 资源集合
	// 结构：map[nodename]map[resourceName]cpuset.CPUSet
	// 在 session 开始时从集群状态初始化，随 allocate/deallocate 事件动态更新
	nodeResSets map[string]api.ResNumaSets
	// taskBindNodeMap 记录已绑定节点的 Task 映射
	// 结构：map[taskUID]nodename
	// 在 allocate 事件中记录，在 deallocate 事件中移除，在 session 结束时用于写回分配结果
	taskBindNodeMap map[api.TaskID]string
}

// New 创建并返回一个新的 numa-aware 插件实例
// 初始化时注册 cpuManager 作为唯一的 HintProvider
func New(arguments framework.Arguments) framework.Plugin {
	plugin := &numaPlugin{
		pluginArguments: arguments,
		assignRes:       make(map[api.TaskID]map[string]api.ResNumaSets),
		taskBindNodeMap: make(map[api.TaskID]string),
	}

	// 注册 CPU 管理器作为拓扑提示提供者
	// 目前只支持 CPU 资源的 NUMA 感知分配
	plugin.hintProviders = append(plugin.hintProviders, cpumanager.NewProvider())
	return plugin
}

// Name 返回插件名称
func (pp *numaPlugin) Name() string {
	return PluginName
}

// calculateWeight 从插件参数中解析权重值，默认为 1
func calculateWeight(args framework.Arguments) int {
	weight := 1
	args.GetInt(&weight, NumaTopoWeight)
	return weight
}

// OnSessionOpen 在调度会话开始时调用，注册所有扩展点回调
func (pp *numaPlugin) OnSessionOpen(ssn *framework.Session) {
	// 解析插件权重，用于 score 阶段的加权计算
	weight := calculateWeight(pp.pluginArguments)
	// 从所有节点信息中提取 NUMA Node 列表
	// 结构：map[nodename][]numaNodeID
	numaNodes := api.GenerateNumaNodes(ssn.Nodes)
	// 从所有节点信息中提取可用资源的 NUMA 集合（主要是 CPU 集合）
	pp.nodeResSets = api.GenerateNodeResNumaSets(ssn.Nodes)

	// 【注册事件处理器】
	// 当 Task 被 allocate 或 deallocate 时，动态更新节点的 NUMA 资源集合
	ssn.AddEventHandler(&framework.EventHandler{
		// AllocateFunc 在 Task 成功分配到节点后调用
		// 从节点可用资源中扣除该 Task 已分配的 NUMA 资源
		AllocateFunc: func(event *framework.Event) {
			node := pp.nodeResSets[event.Task.NodeName]
			// 如果该 Task 没有预计算的分配方案，直接返回
			if _, ok := pp.assignRes[event.Task.UID]; !ok {
				return
			}

			resNumaSets, ok := pp.assignRes[event.Task.UID][event.Task.NodeName]
			if !ok {
				return
			}

			// 从节点可用资源中扣除已分配的资源
			node.Allocate(resNumaSets)
			// 记录 Task 绑定的节点
			pp.taskBindNodeMap[event.Task.UID] = event.Task.NodeName
		},
		// DeallocateFunc 在 Task 从节点释放时调用
		// 将该 Task 占用的 NUMA 资源归还给节点
		DeallocateFunc: func(event *framework.Event) {
			node := pp.nodeResSets[event.Task.NodeName]
			if _, ok := pp.assignRes[event.Task.UID]; !ok {
				return
			}

			resNumaSets, ok := pp.assignRes[event.Task.UID][event.Task.NodeName]
			if !ok {
				return
			}

			// 移除 Task 绑定记录
			delete(pp.taskBindNodeMap, event.Task.UID)
			// 将资源归还给节点可用集合
			node.Release(resNumaSets)
		},
	})

	// 【注册 Predicate 扩展点】
	// 在 predicate 阶段检查节点是否满足 Task 的 NUMA 拓扑约束
	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		predicateStatus := make([]*api.Status, 0)
		numaStatus := &api.Status{}

		// 【前置检查1】只对 Guaranteed QoS 的 Pod 做 NUMA 感知
		// Burstable/BestEffort 的 Pod 不做 NUMA 约束检查，直接放行
		if v1qos.GetPodQOS(task.Pod) != v1.PodQOSGuaranteed {
			klog.V(3).Infof("task %s isn't Guaranteed pod", task.Name)
			return nil
		}

		// 【前置检查2】根据 Task 和节点的 NUMA 策略进行过滤
		// 检查节点是否具备 NUMA 信息、策略是否匹配等
		if fit, err := filterNodeByPolicy(task, node, pp.nodeResSets); !fit {
			if err != nil {
				return api.NewFitError(task, node, err.Error())
			}
			return nil
		}

		// 克隆节点当前的可用 NUMA 资源集合，避免修改原始数据
		resNumaSets := pp.nodeResSets[node.Name].Clone()

		// 获取该节点对应的 NUMA 拓扑策略（none/best-effort/restricted/single-numa-node）
		taskPolicy := policy.GetPolicy(node, numaNodes[node.Name])
		// allResAssignMap 记录该 Task 在此节点上所有容器的资源分配方案
		allResAssignMap := make(map[string]cpuset.CPUSet)

		// 【逐容器检查】遍历 Pod 的每个容器，分别做 NUMA 拓扑检查
		for _, container := range task.Pod.Spec.Containers {
			// 收集所有 HintProvider 对该容器的拓扑提示
			// 每个 provider 返回 map[resourceName][]TopologyHint
			providersHints := policy.AccumulateProvidersHints(&container, node.NumaSchedulerInfo, resNumaSets, pp.hintProviders)

			// 根据节点的 NUMA 策略，合并所有提示并选出最优的拓扑亲和方案（bestHit）
			// hit: 最优的 TopologyHint（包含 NUMA Node 亲和性和是否 preferred）
			// admit: 是否允许准入（由策略决定）
			hit, admit := taskPolicy.Predicate(providersHints)
			if !admit {
				// 如果策略不允许准入（如 restricted 策略下无法找到 preferred 方案），返回不可调度
				numaStatus.Code = api.UnschedulableAndUnresolvable
				numaStatus.Reason = fmt.Sprintf("container %s cannot be assigned by numa", container.Name)
				numaStatus.Plugin = PluginName
				predicateStatus = append(predicateStatus, numaStatus)
				return api.NewFitErrWithStatus(task, node, predicateStatus...)
			}

			klog.V(4).Infof("[numaaware] hits for task %s container '%v': %v on node %s, besthit: %v",
				task.Name, container.Name, providersHints, node.Name, hit)

			// 根据最优拓扑方案，为容器分配具体的 CPU 集合
			// resAssignMap: map[resourceName]cpuset.CPUSet，记录了分配给该容器的具体 CPU ID
			resAssignMap := policy.Allocate(&container, &hit, node.NumaSchedulerInfo, resNumaSets, pp.hintProviders)

			// 累加所有容器的资源分配，并从可用资源中扣除已分配的部分
			for resName, assign := range resAssignMap {
				allResAssignMap[resName] = allResAssignMap[resName].Union(assign)
				resNumaSets[resName] = resNumaSets[resName].Difference(assign)
			}
		}

		// 将该 Task 在此节点上的资源分配方案缓存到 assignRes 中
		// 后续 allocate 事件和 score 阶段会用到
		pp.Lock()
		defer pp.Unlock()
		if _, ok := pp.assignRes[task.UID]; !ok {
			pp.assignRes[task.UID] = make(map[string]api.ResNumaSets)
		}

		pp.assignRes[task.UID][node.Name] = allResAssignMap

		klog.V(4).Infof(" task %s's on node<%s> resAssignMap: %v",
			task.Name, node.Name, pp.assignRes[task.UID][node.Name])

		return nil
	}

	ssn.AddPredicateFn(pp.Name(), predicateFn)

	// 【注册 BatchNodeOrder 扩展点】
	// 在 score 阶段，根据 NUMA 拓扑质量对所有节点批量打分
	// 核心思想：分配给 Task 的 CPU 跨越的 NUMA Node 越少，节点得分越高
	batchNodeOrderFn := func(task *api.TaskInfo, nodeInfo []*api.NodeInfo) (map[string]float64, error) {
		// 跳过没有预计算分配方案、或没有 NUMA 策略、或策略为 none 的 Task
		if _, found := pp.assignRes[task.UID]; !found || task.NumaInfo == nil || task.NumaInfo.Policy == "" || task.NumaInfo.Policy == "none" {
			return nil, nil
		}

		nodeScores := make(map[string]float64, len(nodeInfo))
		// 计算每个节点的 NUMA 质量分数（跨越的 NUMA Node 数量）
		scoreList := getNodeNumaNumForTask(nodeInfo, pp.assignRes[task.UID])
		// 归一化分数到 [0, DefaultMaxNodeScore] 范围
		util.NormalizeScore(api.DefaultMaxNodeScore, true, scoreList)

		// 乘以插件权重，得到最终分数
		for idx, scoreNode := range scoreList {
			scoreNode.Score *= int64(weight)
			nodeName := nodeInfo[idx].Name
			nodeScores[nodeName] = float64(scoreNode.Score)
		}

		klog.V(4).Infof("numa-aware plugin Score for task %s/%s is: %v",
			task.Namespace, task.Name, nodeScores)
		return nodeScores, nil
	}

	ssn.AddBatchNodeOrderFn(pp.Name(), batchNodeOrderFn)
}

// filterNodeByPolicy 根据 Task 和节点的 NUMA 策略进行过滤
// 返回值：
//   - fit=true, err=nil：节点满足策略要求，继续进行 NUMA 拓扑检查
//   - fit=false, err!=nil：节点不满足硬性条件（如缺少 NUMA 信息），返回错误
//   - fit=false, err=nil：节点不适合该 Task（如策略不匹配），但不算错误
func filterNodeByPolicy(task *api.TaskInfo, node *api.NodeInfo, nodeResSets map[string]api.ResNumaSets) (fit bool, err error) {
	// 情况一：Task 设置了非 none 的 NUMA 策略（如 best-effort/restricted/single-numa-node）
	if !(task.NumaInfo == nil || task.NumaInfo.Policy == "" || task.NumaInfo.Policy == "none") {
		// 检查节点是否有 NUMA 调度信息
		if node.NumaSchedulerInfo == nil {
			return false, fmt.Errorf("numa info is empty")
		}

		// 节点的 CPU Manager 策略必须是 static，否则无法做 CPU 独占分配
		if node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.CPUManagerPolicy] != "static" {
			return false, fmt.Errorf("cpu manager policy isn't static")
		}

		// Task 的拓扑策略必须与节点的 Topology Manager 策略一致
		if task.NumaInfo.Policy != node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy] {
			return false, fmt.Errorf("task topology policy[%s] is different with node[%s]",
				task.NumaInfo.Policy, node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy])
		}

		// 检查节点是否有拓扑资源信息
		if _, ok := nodeResSets[node.Name]; !ok {
			return false, fmt.Errorf("no topo information")
		}

		// 检查节点的 CPU 可分配集合是否为空
		if nodeResSets[node.Name][string(v1.ResourceCPU)].Size() == 0 {
			return false, fmt.Errorf("cpu allocatable map is empty")
		}
	} else {
		// 情况二：Task 没有设置 NUMA 策略（或策略为 none）
		// 此时需要检查节点是否有 NUMA 能力，如果有则跳过（让有 NUMA 能力的节点留给有 NUMA 需求的 Task）
		if node.NumaSchedulerInfo == nil {
			return false, nil
		}

		if node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.CPUManagerPolicy] != "static" {
			return false, nil
		}

		// 如果节点的 Topology Manager 策略为 none 或未设置，跳过该节点
		if (node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy] == "none") ||
			(node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy] == "") {
			return false, nil
		}
	}

	return true, nil
}

// getNodeNumaNumForTask 计算每个节点对于 Task 的 NUMA 质量分数
// 核心逻辑：分配给 Task 的 CPU 跨越的 NUMA Node 数量越少，说明拓扑局部性越好
// 使用并行计算加速多节点的评分过程
func getNodeNumaNumForTask(nodeInfo []*api.NodeInfo, resAssignMap map[string]api.ResNumaSets) []api.ScoredNode {
	nodeNumaCnts := make([]api.ScoredNode, len(nodeInfo))
	// 使用 workqueue 并行处理，最多 16 个并发
	workqueue.ParallelizeUntil(context.TODO(), 16, len(nodeInfo), func(index int) {
		node := nodeInfo[index]
		// 获取该 Task 在此节点上分配的 CPU 集合
		assignCpus := resAssignMap[node.Name][string(v1.ResourceCPU)]
		nodeNumaCnts[index] = api.ScoredNode{
			NodeName: node.Name,
			// 计算这些 CPU 跨越了多少个 NUMA Node
			Score: int64(getNumaNodeCntForCPUID(assignCpus, node.NumaSchedulerInfo.CPUDetail)),
		}
	})

	return nodeNumaCnts
}

// getNumaNodeCntForCPUID 计算给定 CPU 集合跨越了多少个不同的 NUMA Node
// 原理：遍历每个 CPU ID，查找其所属的 NUMA Node，用 bitmask 去重后统计数量
// 返回值越小，说明 CPU 分配越集中，NUMA 局部性越好
func getNumaNodeCntForCPUID(cpus cpuset.CPUSet, cpuDetails topology.CPUDetails) int {
	mask, _ := bitmask.NewBitMask()
	s := cpus.List()

	for _, cpuID := range s {
		// 将每个 CPU 所属 NUMA Node ID 加入 bitmask（自动去重）
		mask.Add(cpuDetails[cpuID].NUMANodeID)
	}

	// 返回 bitmask 中设置的位数，即跨越的 NUMA Node 数量
	return mask.Count()
}

// OnSessionClose 在调度会话结束时调用
// 将所有已绑定节点的 Task 的资源分配结果写回调度器缓存
// 这样下一个调度周期就能感知到这些资源已被占用
func (pp *numaPlugin) OnSessionClose(ssn *framework.Session) {
	if len(pp.taskBindNodeMap) == 0 {
		return
	}

	// allocatedResSet 按节点聚合所有已分配 Task 的资源 NUMA 集合
	allocatedResSet := make(map[string]api.ResNumaSets)
	for taskID, nodeName := range pp.taskBindNodeMap {
		if _, existed := pp.assignRes[taskID]; !existed {
			continue
		}

		if _, existed := pp.assignRes[taskID][nodeName]; !existed {
			continue
		}

		if _, existed := allocatedResSet[nodeName]; !existed {
			allocatedResSet[nodeName] = make(api.ResNumaSets)
		}

		resSet := pp.assignRes[taskID][nodeName]
		for resName, set := range resSet {
			if _, existed := allocatedResSet[nodeName][resName]; !existed {
				allocatedResSet[nodeName][resName] = cpuset.New()
			}

			// 将同一节点上多个 Task 的资源集合取并集
			allocatedResSet[nodeName][resName] = allocatedResSet[nodeName][resName].Union(set)
		}
	}

	klog.V(4).Infof("[numaPlugin]allocatedResSet: %v", allocatedResSet)
	// 将聚合后的资源分配结果更新到调度器缓存
	ssn.UpdateSchedulerNumaInfo(allocatedResSet)
}
