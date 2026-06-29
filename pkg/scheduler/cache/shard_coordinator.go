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
	"context"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	nodeshardv1alpha1 "volcano.sh/apis/pkg/apis/shard/v1alpha1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/util"
)

// ShardUpdateCoordinator 用来协调 NodeShard 状态更新与调度 session 之间的关系。
// 主要解决的问题是：
// - 当调度 session 正在运行时，不要立即更新 NodeShard 状态
// - 先把更新请求挂起，等 session 结束后再继续更新
type ShardUpdateCoordinator struct {
	// IsSessionRunning 表示当前是否有调度 session 正在运行
	IsSessionRunning *atomic.Bool

	// ShardUpdatePending 表示是否已经有一个 NodeShard 状态更新请求在等待
	// 用于避免重复挂起多个相同的更新请求
	ShardUpdatePending *atomic.Bool

	// SessionEndCh 用于在 session 结束时通知等待中的 goroutine
	// 这里使用无缓冲 channel，表示一个同步通知信号
	SessionEndCh chan struct{}
}

// NewShardUpdateCoordinator 创建一个新的分片更新协调器
func NewShardUpdateCoordinator() *ShardUpdateCoordinator {
	return &ShardUpdateCoordinator{
		IsSessionRunning:   new(atomic.Bool),
		ShardUpdatePending: new(atomic.Bool),
		SessionEndCh:       make(chan struct{}), // 无缓冲 channel，用于 session 结束通知
	}
}

// RefreshNodeShards 刷新当前 scheduler 对应的 NodeShard 信息。
// 主要逻辑：
// 1. 如果没有开启分片模式，直接返回
// 2. 找到当前 scheduler 对应的 shard
// 3. 重新计算该 shard 当前可用节点
// 4. 更新缓存中的 InUseNodesInShard
// 5. 尝试更新 NodeShard 的 status
func (sc *SchedulerCache) RefreshNodeShards() {
	// 只有在 HardShardingMode 或 SoftShardingMode 下才处理
	if options.ServerOpts.ShardingMode != util.HardShardingMode && options.ServerOpts.ShardingMode != util.SoftShardingMode {
		return
	}

	var nodeShardInfo *api.NodeShardInfo

	// 在缓存的所有 NodeShards 中找到当前 scheduler 对应的 shard
	for shardName, shard := range sc.NodeShards {
		if shardName == options.ServerOpts.ShardName {
			nodeShardInfo = shard
			break
		}
	}

	// 如果开启了分片模式，但没有找到当前 shard，说明配置或状态有问题
	if nodeShardInfo == nil {
		klog.Errorf("Sharding is enabled but no shard is defined for this scheduler!")
		return
	}

	// 重新计算当前 shard 实际可用的节点：
	// desiredNodes - nodesInUseByOtherShards
	sc.InUseNodesInShard = sc.getAvailableNodesFromShard(nodeShardInfo)

	klog.V(3).Infof("Try to update NodeShard status after NodeShard refresh")

	// 异步尝试更新 NodeShard 状态
	go sc.tryUpdateNodeShardStatus(nodeShardInfo.Name)
}

// tryUpdateNodeShardStatus 尝试更新 NodeShard 的 status。
// 如果当前 session 正在运行：
// - 先标记更新为 pending
// - 等待 session 结束
// - session 结束后再更新
//
// 如果当前没有 session 运行，则直接更新。
func (sc *SchedulerCache) tryUpdateNodeShardStatus(nodeShardName string) {
	coordinator := sc.shardUpdateCoordinator
	if coordinator != nil {
		// 如果当前调度 session 正在运行，则不能立即更新状态
		if coordinator.IsSessionRunning.Load() {
			// 原子地把 pending 从 false 改成 true
			// 如果已经是 true，说明已经有一个更新在等待，不需要重复挂起
			if !coordinator.ShardUpdatePending.CompareAndSwap(false, true) {
				klog.V(3).Infof("Update status of NodeShard is already pending, skip this request")
				return
			}

			klog.V(3).Infof("Update status of NodeShard is pending because session is running")

			// 等待 session 结束通知
			<-coordinator.SessionEndCh
			klog.V(3).Infof("Update status of NodeShard is resumed")

			// session 结束后再次确认，确保当前确实已经没有 session 在运行
			if !coordinator.IsSessionRunning.Load() {
				sc.UpdateNodeShardStatus(nodeShardName)
			}

			// 重置 pending 标记
			coordinator.ShardUpdatePending.Store(false)
			return
		}

		// 如果当前没有 session 运行，则直接更新
		sc.UpdateNodeShardStatus(nodeShardName)
	}
}

// UpdateNodeShardStatus 负责真正调用 API 更新 NodeShard 的 status。
func (sc *SchedulerCache) UpdateNodeShardStatus(nodeShardName string) error {
	// 根据当前缓存生成一个带新 status 的 NodeShard 副本
	if nodeShard := sc.generateNodeShardWithStatus(nodeShardName); nodeShard != nil {
		klog.V(3).Infof("Update NodeShard %s status...", nodeShardName)

		// 调用 status updater 去更新 Kubernetes API 中的 NodeShard status
		_, err := sc.StatusUpdater.UpdateNodeShardStatus(nodeShard)
		if err != nil {
			klog.Errorf("Failed to update NodeShard %s status %v", nodeShard.Name, err)
			return err
		}

		klog.V(3).Infof("Updated NodeShard %s status", nodeShard.Name)
	}
	return nil
}

// generateNodeShardWithStatus 根据当前缓存信息生成一个新的 NodeShard 对象（带更新后的 status）。
// 如果状态没有变化，则返回 nil，表示无需更新。
func (sc *SchedulerCache) generateNodeShardWithStatus(nodeShardName string) *nodeshardv1alpha1.NodeShard {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	// 从缓存中取出对应的 NodeShard
	nodeShard, exist := sc.NodeShards[nodeShardName]
	if !exist {
		klog.Warningf("NodeShard %s does not exist in cache, skip status generation", nodeShardName)
		return nil
	}

	// 旧状态：从当前 NodeShard.Status 中读取
	oldNodesInUse := sets.New(nodeShard.NodeShard.Status.NodesInUse...)
	oldNodesToRemove := sets.New(nodeShard.NodeShard.Status.NodesToRemove...)
	oldNodesToAdd := sets.New(nodeShard.NodeShard.Status.NodesToAdd...)

	// 新状态的 NodesInUse 来自调度器缓存中最新计算得到的 InUseNodesInShard
	nodesInUse := sc.InUseNodesInShard

	// 深拷贝一份，避免直接修改缓存中的对象
	nodeShardCopy := nodeShard.NodeShard.DeepCopy()

	// desiredNodes 是该 shard 希望拥有的节点集合
	desiredNodes := sets.New(nodeShardCopy.Spec.NodesDesired...)

	// NodesToRemove：
	// 当前在用，但已经不属于 desiredNodes 的节点
	nodesToRemove := nodesInUse.Difference(desiredNodes)

	// NodesToAdd：
	// desiredNodes 中想要，但当前还不在 use 中的节点
	nodesToAdd := desiredNodes.Difference(nodesInUse)

	// 如果新旧状态完全一致，说明没有必要更新 status
	if nodesInUse.Equal(oldNodesInUse) && nodesToRemove.Equal(oldNodesToRemove) && nodesToAdd.Equal(oldNodesToAdd) {
		klog.V(3).Infof("No change for status of nodeshard %s status", nodeShard.Name)
		return nil
	}

	// 填充新的 status
	nodeShardCopy.Status.NodesInUse = nodesInUse.UnsortedList()
	nodeShardCopy.Status.NodesToRemove = nodesToRemove.UnsortedList()
	nodeShardCopy.Status.NodesToAdd = nodesToAdd.UnsortedList()

	return nodeShardCopy
}

// getAvailableNodesFromShard 根据当前 shard 的 desired nodes，计算真正可用的节点集合。
// 规则是：
// 当前 shard 想要的节点 - 其他 shard 正在使用的节点
//
// 举例：
// 当前 shard desired = {node1,node2,node3}
// 其他 shard 正在使用 {node3}
// 则本 shard 可用节点 = {node1,node2}
func (sc *SchedulerCache) getAvailableNodesFromShard(nodeShardInfo *api.NodeShardInfo) sets.Set[string] {
	// 从当前 shard 期望节点开始
	nodes := nodeShardInfo.NodeDesired

	// 遍历其他 shard，把它们正在使用的节点减掉
	for shardName, nodeShard := range sc.NodeShards {
		if shardName != nodeShardInfo.Name {
			nodes = nodes.Difference(nodeShard.NodeInUse)
		}
	}

	return nodes
}

// notifySessionEnd 用于在 session 结束时通知等待中的 shard status 更新逻辑。
// 如果有 goroutine 正在等 SessionEndCh，就发一个通知。
// 如果没有 goroutine 在等，也没关系，直接跳过。
func (sc *SchedulerCache) notifySessionEnd() {
	// 通知 shardUpdateCoordinator：session 已结束
	select {
	case sc.shardUpdateCoordinator.SessionEndCh <- struct{}{}:
	default:
		// 没有等待中的 goroutine，这种情况是正常的
	}
}

// UpdateNodeShardStatus 是 defaultStatusUpdater 对 NodeShard status 的更新实现。
// 本质上就是调用 Volcano CRD client 去更新 status 子资源。
func (su *defaultStatusUpdater) UpdateNodeShardStatus(nodeshard *nodeshardv1alpha1.NodeShard) (*nodeshardv1alpha1.NodeShard, error) {
	return su.vcclient.ShardV1alpha1().NodeShards().UpdateStatus(context.Background(), nodeshard, metav1.UpdateOptions{})
}
