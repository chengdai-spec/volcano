/*
Copyright 2025 The Volcano Authors.

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

package cache

import (
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	agentapi "volcano.sh/volcano/pkg/agentscheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/util"
)

// ShardCoordinator 是分片协调器，负责管理多 Worker 调度场景下的节点分片。
//
// 核心职责：
//   - 跟踪当前调度器（scheduler）被分配的节点分片（shard）
//   - 协调多 Worker 与 NodeShard CR（自定义资源）之间的版本同步
//   - 确保在所有 Worker 都使用最新分片配置后，才将当前使用的节点集合更新到 NodeShard CR 的 status
//
// 版本同步机制（revision-based）：
//
//	latestRevision:        每当分片配置变更时递增，代表当前最新的分片版本
//	lastSyncedRevision:    上一次成功同步到 NodeShard CR 的版本号
//	revisionInScheduling:  每个 Worker 在调度周期开始时快照的 revision，代表该 Worker 正在使用的版本
//
// 更新流程：
//  1. NodeShard CR 变更 → RefreshNodeShards → checkAndUpdateShards（递增 latestRevision）
//  2. 检查所有 Worker 是否已切换到新版本 → tryUpdateNodeShardStatus
//  3. 如果所有 Worker 都使用 >= latest 的版本 → 发送通知到 updateChan
//  4. processUpdates 后台协程接收通知 → performUpdate → 调用 cache.UpdateNodeShardStatus 写入 CR
type ShardCoordinator struct {
	// schedulerShardName: 当前调度器所属的分片名称，用于从所有分片中定位自己的分片配置
	schedulerShardName string
	// workerStates: 每个 Worker 的分片使用状态，按 Worker 索引排列
	workerStates []*workerNodeShardState
	// nodeShardInfos: 所有分片的节点信息缓存（来自 NodeShard CR），key 为分片名称
	nodeShardInfos map[string]*api.NodeShardInfo
	// schedulerNodeShardInfo: 当前调度器自身分片的节点信息
	schedulerNodeShardInfo *api.NodeShardInfo
	// nodeToUse: 当前调度器应该使用的节点集合（= 期望节点 - 其他分片正在使用的节点）
	nodeToUse sets.Set[string]
	// mutex: 保护 nodeShardInfos、schedulerNodeShardInfo、nodeToUse 的读写锁
	mutex sync.RWMutex
	// shardingEnabled: 分片功能是否启用（hard 或 soft 模式时启用）
	shardingEnabled bool
	// lastSyncedRevision: 上一次成功同步到 NodeShard CR 的分片版本号（原子操作访问）
	lastSyncedRevision int64
	// latestRevision: 当前最新的分片版本号，每次分片配置变更时递增（原子操作访问）
	latestRevision int64
	// cache: 调度器缓存接口，用于调用 UpdateNodeShardStatus 将节点使用状态写入 NodeShard CR
	cache Cache
	// updateChan: 更新通知通道，用于异步触发 NodeShard CR 的状态更新
	updateChan chan struct{}
}

// workerNodeShardState 记录单个 Worker 当前正在使用的分片版本号
type workerNodeShardState struct {
	// revisionInScheduling: Worker 在调度周期中快照的分片版本号
	// 值 > 0 表示该 Worker 正在使用对应版本的节点进行调度
	// 值 = 0 表示该 Worker 当前不在调度周期中（空闲状态）
	revisionInScheduling int64
}

// NewShardCoordinator 创建并初始化分片协调器
//
// 参数：
//   - cache: 调度器缓存接口
//   - workerCount: Worker 数量，决定 workerStates 数组的大小
//   - shardName: 当前调度器的分片名称
//   - shardingMode: 分片模式（"hard" 或 "soft" 时启用分片功能）
func NewShardCoordinator(cache Cache, workerCount int, shardName string, shardingMode string) *ShardCoordinator {
	klog.V(3).Infof("Shard Coordinator is initialized")
	// 为每个 Worker 初始化分片状态（初始 revisionInScheduling = 0，表示空闲）
	workerStates := make([]*workerNodeShardState, workerCount)
	for i := range workerCount {
		workerStates[i] = &workerNodeShardState{}
	}

	sc := &ShardCoordinator{
		schedulerShardName: shardName,
		workerStates:       workerStates,
		shardingEnabled:    shardingMode == util.HardShardingMode || shardingMode == util.SoftShardingMode,
		cache:              cache,
		updateChan:         make(chan struct{}, 100), // 带缓冲的通道，避免通知发送阻塞
	}
	return sc
}

// Run 启动后台更新处理协程，监听 updateChan 中的更新通知并执行 NodeShard 状态写入
func (sc *ShardCoordinator) Run(stopCh <-chan struct{}) {
	go sc.processUpdates(stopCh)
}

// getNodesForScheduling 获取指定 Worker 在调度周期中应使用的节点集合
//
// 调用时机：Worker 开始调度周期时，由 OnWorkerStartSchedulingCycle 调用
//
// 核心逻辑：
//  1. 读取当前 latestRevision 并快照到该 Worker 的 revisionInScheduling
//  2. 返回当前可用节点集合 nodeToUse
//
// 这一步的快照操作非常关键——它"冻结"了该 Worker 使用的分片版本，
// 后续 tryUpdateNodeShardStatus 会据此判断是否有 Worker 还在使用旧版本节点
func (sc *ShardCoordinator) getNodesForScheduling(workerIdx int) sets.Set[string] {
	if workerIdx >= len(sc.workerStates) {
		klog.Errorf("Worker %d does not exist, no nodes are returned", workerIdx)
		return sets.Set[string]{}
	}
	state := sc.workerStates[workerIdx]
	if state == nil {
		klog.Errorf("Worker %d state is not inited, no nodes are returned", workerIdx)
		return sets.Set[string]{}
	}
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	// 快照当前最新版本号到该 Worker 的调度状态中
	latest := atomic.LoadInt64(&sc.latestRevision)
	atomic.StoreInt64(&state.revisionInScheduling, latest)
	klog.V(5).Infof("Worker %d will schedule with nodes%v", workerIdx, sc.nodeToUse.UnsortedList())
	return sc.nodeToUse
}

// RefreshNodeShards 在 NodeShard CR 发生变化时刷新协调器中的分片缓存
//
// 调用时机：event_handlers 监听到 NodeShard CR 变更时调用
//
// 流程：
//  1. 如果分片功能未启用则直接返回
//  2. checkAndUpdateShards 更新内部分片数据并判断是否需要触发状态同步
//  3. 如果需要更新，调用 tryUpdateNodeShardStatus 尝试将新状态写入 NodeShard CR
func (sc *ShardCoordinator) RefreshNodeShards(nodeShards map[string]*api.NodeShardInfo) {
	if !sc.shardingEnabled {
		return
	}
	if sc.checkAndUpdateShards(nodeShards) {
		klog.V(3).Infof("Try to update nodeshard status after nodeshard refresh")
		sc.tryUpdateNodeShardStatus()
	}
}

// checkAndUpdateShards 更新协调器内部缓存的分片和节点数据
//
// 返回值：是否需要触发 NodeShard CR 的状态更新
//
// 核心逻辑：
//  1. 更新 nodeShardInfos 缓存（所有分片的完整信息）
//  2. 从所有分片中找到自己的分片（通过 schedulerShardName 匹配）
//  3. 检查自己的分片期望节点集（NodeDesired）是否发生变化
//  4. 计算实际可用节点（= 期望节点 - 其他分片正在使用的节点）
//  5. 如果可用节点集变化了，递增 latestRevision 并更新 nodeToUse
func (sc *ShardCoordinator) checkAndUpdateShards(nodeShards map[string]*api.NodeShardInfo) bool {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.nodeShardInfos = nodeShards
	shardForSchedulerFound := false
	desiredNodesChanged := false

	// 从所有分片中定位到当前调度器所属的分片
	for shardName, shard := range nodeShards {
		if shardName == sc.schedulerShardName {
			// 检查期望节点集合是否发生变化（决定是否需要更新 nodeToUse）
			if sc.schedulerNodeShardInfo == nil || !shard.NodeDesired.Equal(sc.schedulerNodeShardInfo.NodeDesired) {
				desiredNodesChanged = true
			}
			sc.schedulerNodeShardInfo = shard
			shardForSchedulerFound = true
			break
		}
	}

	needUpdate := false
	if !shardForSchedulerFound {
		// 分片功能开启但没有找到属于自己的分片配置，这是异常情况
		klog.Errorf("Sharding is enabled but no shard is defined for this scheduler!")
		sc.schedulerNodeShardInfo = nil
	} else if availableNodes := sc.getAvailableNodesFromShard(); desiredNodesChanged || !sc.nodeToUse.Equal(availableNodes) {
		// 可用节点发生了变化（期望节点变化 或 其他分片释放/占用了节点），递增版本号
		atomic.AddInt64(&sc.latestRevision, 1)
		sc.nodeToUse = availableNodes
		needUpdate = true
	}
	return needUpdate
}

// tryUpdateNodeShardStatus 尝试将当前节点使用状态同步到 NodeShard CR
//
// 核心安全约束：
//
//	只有当所有 Worker 都不再使用旧版本分片配置时，才能更新 CR 的 status。
//	这是为了防止在旧版本分片下调度出的结果与新版本 status 产生不一致。
//
// 判断逻辑：
//   - 遍历所有 Worker 的 revisionInScheduling
//   - 如果某个 Worker 的 revision > 0（正在调度中）且 < latestRevision（还在用旧版本），则阻塞更新
//   - 只有所有 Worker 要么不在调度中（revision = 0），要么已使用最新版本（revision >= latest），才允许更新
//
// 返回值：是否成功发送了更新通知
func (sc *ShardCoordinator) tryUpdateNodeShardStatus() bool {
	latest := atomic.LoadInt64(&sc.latestRevision)

	// 如果已经同步过最新版本，无需重复更新
	if atomic.LoadInt64(&sc.lastSyncedRevision) >= latest {
		klog.V(3).Info("last updated revision is new than revision in coordinator, skip nodeshard update")
		return false
	}

	update := true
	// 检查每个 Worker 是否仍在使用旧版本的分片配置
	for index, state := range sc.workerStates {
		// 跳过未初始化的 Worker 状态
		p := &state.revisionInScheduling
		if p == nil {
			continue
		}
		revision := atomic.LoadInt64(p)
		// revision > 0 表示 Worker 正在调度周期中（非空闲状态）
		// revision < latest 表示 Worker 还在使用旧版本的节点分片
		if state != nil && revision > 0 && revision < latest {
			klog.V(3).Infof("Worker %d is scheduling with old nodes, skip nodeshard update", index)
			update = false
			break
		}
	}

	if update {
		// 通过 channel 发送更新通知，避免在锁内直接执行 CR 更新操作
		// 使用非阻塞发送（select + default），防止 channel 已满时阻塞调用者
		select {
		case sc.updateChan <- struct{}{}:
			klog.V(3).Info("Sent update notification to channel")
		default:
			klog.Error("Update channel is full, skipping notification")
			update = false
		}
	}
	return update
}

// processUpdates 后台协程，持续监听 updateChan 中的更新通知并批量处理
//
// 批量处理机制：
//   - 从 channel 中读取一条通知后，尝试排空 channel 中积压的其他通知（最多 maxBatch=10 条）
//   - 将多条通知合并为一次 performUpdate 调用，减少 CR 更新频率
//
// 生命周期：通过 stopCh 控制退出
func (sc *ShardCoordinator) processUpdates(stopCh <-chan struct{}) {
	maxBatch := 10
	for {
		select {
		case <-sc.updateChan:
			// 排空 channel 中积压的更新通知（批量合并）
			updateCount := 1
			for len(sc.updateChan) > 0 && updateCount < maxBatch {
				<-sc.updateChan
				updateCount++
			}
			klog.V(3).Infof("Processing %d update nodeshard notifications", updateCount)

			// 执行实际的 CR 更新
			sc.performUpdate()

		case <-stopCh:
			klog.V(3).Infof("Stopping update nodeshard processing goroutine")
			return
		}
	}
}

// performUpdate 执行实际的 NodeShard CR 状态更新操作
//
// 流程：
//  1. 再次检查 lastSyncedRevision 是否已 >= latestRevision（双重检查，防止并发更新）
//  2. 调用 cache.UpdateNodeShardStatus 将当前节点使用集合写入 NodeShard CR 的 status
//  3. 更新成功后记录 lastSyncedRevision
func (sc *ShardCoordinator) performUpdate() {
	latest := atomic.LoadInt64(&sc.latestRevision)
	// 双重检查：确认更新仍然需要（可能在等待 channel 处理期间已被其他路径更新）
	if atomic.LoadInt64(&sc.lastSyncedRevision) >= latest {
		return
	}

	if err := sc.cache.UpdateNodeShardStatus(sc.schedulerShardName, sc.nodeToUse); err == nil {
		atomic.StoreInt64(&sc.lastSyncedRevision, latest)
		klog.V(3).Infof("Successfully updated NodeShard status")
	} else {
		klog.Errorf("Failed to update NodeShard status: %v", err)
	}
}

// OnWorkerStartSchedulingCycle Worker 调度周期开始时的回调钩子
//
// 调用时机：每个 Worker 进入新一轮调度周期前，由 cache 层统一调用
//
// 作用：将当前可用节点集合注入到调度上下文（SchedulingContext.NodesInShard）中，
// 供后续的调度 Action 使用，确保 Worker 只在自己分片的节点范围内进行调度
func (sc *ShardCoordinator) OnWorkerStartSchedulingCycle(index int, schedCtx *agentapi.SchedulingContext) {
	if schedCtx != nil {
		schedCtx.NodesInShard = sc.getNodesForScheduling(index)
	}
}

// OnWorkerEndSchedulingCycle Worker 调度周期结束时的回调钩子
//
// 调用时机：每个 Worker 完成一轮调度后，由 cache 层统一调用
//
// 核心逻辑：
//  1. 清除该 Worker 的 revisionInScheduling（置 0，标记为空闲）
//  2. 如果该 Worker 在调度周期中使用的是旧版本分片（revisionInSchedulingCycle != latest），
//     说明分片配置在此期间发生了变更，尝试触发 NodeShard CR 状态更新
//     （因为现在少了一个使用旧版本的 Worker，可能满足更新条件了）
func (sc *ShardCoordinator) OnWorkerEndSchedulingCycle(index int) {
	if index >= len(sc.workerStates) {
		klog.Errorf("Worker %d does not exist", index)
		return
	}
	latest := atomic.LoadInt64(&sc.latestRevision)
	state := sc.workerStates[index]
	if state == nil {
		klog.Errorf("Worker %d state should not be nil when end scheduling cycle", index)
		return
	}
	// 保存该 Worker 在调度周期中使用的版本号，然后清除（标记空闲）
	revisionInSchedulingCycle := state.revisionInScheduling
	atomic.StoreInt64(&state.revisionInScheduling, 0)

	// 如果该 Worker 使用的是最新版本的分片配置，则无需触发额外更新
	// （因为它的调度结果与最新 status 一致，不会造成不一致问题）
	if revisionInSchedulingCycle == latest {
		return
	}

	// Worker 使用了旧版本分片完成调度，现在它已释放旧版本引用，
	// 尝试更新 NodeShard CR 状态（下一个调度周期它一定会使用新版本）
	klog.V(3).Infof("Try to update nodeshard status after worker %d end scheduling cycle", index)
	sc.tryUpdateNodeShardStatus()
}

// getAvailableNodesFromShard 计算当前调度器分片的实际可用节点集合
//
// 计算逻辑：
//
//	可用节点 = 期望节点（NodeDesired）- 其他分片正在使用的节点（NodeInUse）
//
// 这意味着即使某些节点被分配到当前分片的期望集合中，如果其他分片实际还在使用这些节点，
// 当前调度器也不应该使用它们，避免跨分片冲突
//
// 调用者需持有 mutex 读锁或写锁
func (sc *ShardCoordinator) getAvailableNodesFromShard() sets.Set[string] {
	nodes := sc.schedulerNodeShardInfo.NodeDesired
	// 排除其他分片正在使用的节点（差集操作）
	for shardName, nodeShard := range sc.nodeShardInfos {
		if shardName != sc.schedulerShardName {
			nodes = nodes.Difference(nodeShard.NodeInUse)
		}
	}
	return nodes
}
