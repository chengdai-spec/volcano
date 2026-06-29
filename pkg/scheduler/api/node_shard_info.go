package api

import (
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	nodeshardv1alpha1 "volcano.sh/apis/pkg/apis/shard/v1alpha1"
)

// ShardID 是基于 UID 的类型，用作每个 shard 的唯一标识
type ShardID types.UID

// NodeShardInfo 保存一个节点分片（node shard）的完整信息
type NodeShardInfo struct {
	// 分片名称
	Name string

	// 该 shard 期望拥有的节点集合（来自 spec.nodesDesired）
	NodeDesired sets.Set[string]

	// 该 shard 当前正在使用的节点集合（来自 status.nodesInUse）
	NodeInUse sets.Set[string]

	// 需要从该 shard 中移除的节点集合
	NodeToRemove sets.Set[string]

	// 需要加入该 shard 的节点集合
	NodeToAdd sets.Set[string]

	// 原始的 NodeShard CRD 对象
	NodeShard *nodeshardv1alpha1.NodeShard
}

// NewNodeShardInfo 用于创建一个新的 NodeShardInfo 对象
func NewNodeShardInfo(shard *nodeshardv1alpha1.NodeShard) *NodeShardInfo {
	shardInfo := &NodeShardInfo{
		Name:         shard.Name,
		NodeDesired:  sets.New(shard.Spec.NodesDesired...),
		NodeInUse:    sets.New(shard.Status.NodesInUse...),
		NodeToRemove: sets.New(shard.Status.NodesToRemove...),
		NodeToAdd:    sets.New(shard.Status.NodesToAdd...),
		NodeShard:    shard,
	}

	// status 中的 NodesToRemove 和 NodesToAdd 可能存在延迟，
	// 例如 scheduler 可能是基于旧的 NodesDesired 去更新它们，
	// 所以这里重新根据 NodesDesired 和 NodesInUse 计算一次
	shardInfo.NodeToRemove = shardInfo.NodeInUse.Difference(shardInfo.NodeDesired)
	shardInfo.NodeToAdd = shardInfo.NodeDesired.Difference(shardInfo.NodeInUse)

	return shardInfo
}

// Clone 用于复制一个 NodeShardInfo 对象
func (ns *NodeShardInfo) Clone() *NodeShardInfo {
	return &NodeShardInfo{
		Name:         ns.Name,
		NodeDesired:  ns.NodeDesired,
		NodeInUse:    ns.NodeInUse,
		NodeToRemove: ns.NodeToRemove,
		NodeToAdd:    ns.NodeToAdd,
	}
}
