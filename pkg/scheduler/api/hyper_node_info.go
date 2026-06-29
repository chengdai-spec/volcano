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

package api

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
)

// HyperNodesInfo 存储并管理所有 HyperNode 的层次结构信息。
//
// HyperNode 是 Volcano 网络拓扑感知调度中的核心抽象，它把物理节点按
// 机架、集群、区域等拓扑层级组织成树形结构。调度器可以利用这些信息
// 在拓扑近邻的节点上放置任务，降低通信开销。
type HyperNodesInfo struct {
	sync.Mutex

	// hyperNodes 存储所有 HyperNode 的详细信息，key 为 HyperNode 名称。
	// 每个 HyperNodeInfo 描述一个 HyperNode 的原始 CRD 对象、层级、父子关系等。
	hyperNodes HyperNodeInfoMap
	// hyperNodesSetByTier 按 tier（层级）对 HyperNode 名称进行分组。
	// tier 数值越小层级越高（如 root=0），便于按层级快速遍历拓扑树。
	hyperNodesSetByTier map[int]sets.Set[string]
	// realNodesSet 缓存每个 HyperNode 最终展开得到的真实节点集合。
	//
	// 例如 HyperNode s0 包含 node0、node1；s1 包含 node2、node3；
	// s4 的成员是 s0 和 s1，则 s4 的 realNodesSet 为 {node0, node1, node2, node3}。
	// 该缓存在 HyperNode 变化时增量重建，避免调度时实时递归计算。
	realNodesSet map[string]sets.Set[string]
	// nodeLister 用于列出 Kubernetes 集群中的 Node 对象，
	// 支持根据 HyperNode 的 Selector 做正则或标签匹配。
	nodeLister listerv1.NodeLister
	// builtErrHyperNode 记录上一次构建缓存时发生错误的 HyperNode 名称，
	// 用于在相关父节点更新后重新尝试构建。
	builtErrHyperNode string

	// ready 原子标志，表示 HyperNode 缓存是否已构建完成且没有错误。
	// 调度器只有在此标志为 true 时才能基于 HyperNode 拓扑进行调度。
	ready *atomic.Bool
}

// HyperNodeInfoMap 是 HyperNode 名称到 HyperNodeInfo 的映射类型别名。
type HyperNodeInfoMap map[string]*HyperNodeInfo

// HyperNodeTierNameMap 是层级名称到层级数值的映射类型别名。
// 例如 {"region": 0, "rack": 1, "node": 2}。
type HyperNodeTierNameMap map[string]int

// NewHyperNodesInfo 创建并初始化一个新的 HyperNodesInfo 实例。
//
// 参数 lister 用于获取集群节点列表，在根据 HyperNode 的 Selector 匹配成员时需要。
// 初始状态 ready=true，表示缓存尚未构建但结构已可用。
func NewHyperNodesInfo(lister listerv1.NodeLister) *HyperNodesInfo {
	ready := new(atomic.Bool)
	ready.Store(true)
	return &HyperNodesInfo{
		hyperNodes:          make(map[string]*HyperNodeInfo),
		hyperNodesSetByTier: make(map[int]sets.Set[string]),
		realNodesSet:        make(map[string]sets.Set[string]),
		nodeLister:          lister,
		ready:               ready,
	}
}

// NewHyperNodesInfoWithCache 使用已有的缓存数据创建 HyperNodesInfo 实例。
// 仅用于单元测试（ut）场景，方便构造测试数据。
// TODO: 抽象一个接口用于单元测试 mock，而不是直接暴露内部结构。
func NewHyperNodesInfoWithCache(hyperNodesMap map[string]*HyperNodeInfo, hyperNodesSetByTier map[int]sets.Set[string], realNodesSet map[string]sets.Set[string], ready *atomic.Bool) *HyperNodesInfo {
	return &HyperNodesInfo{
		hyperNodes:          hyperNodesMap,
		hyperNodesSetByTier: hyperNodesSetByTier,
		realNodesSet:        realNodesSet,
		ready:               ready,
	}
}

// HyperNodeInfo 描述单个 HyperNode 在调度器内部的缓存信息。
//
// 一个 HyperNode 对应一个 topology CRD 对象，它可以包含真实节点（Node）
// 作为成员，也可以包含其他 HyperNode 作为成员，从而形成树形拓扑结构。
type HyperNodeInfo struct {
	// Name 为 HyperNode 名称，与 CRD 对象名称一致。
	Name string
	// HyperNode 为原始 CRD 对象引用，包含成员定义、层级等原始配置。
	HyperNode *topologyv1alpha1.HyperNode

	// Parent 指向父 HyperNode 的名称。根节点该字段为空。
	Parent string
	// Children 保存直接子 HyperNode 的名称集合。
	// 该集合通过 BuildHyperNodeCache 递归构建并维护。
	Children sets.Set[string]

	// tier 表示 HyperNode 所在的层级数值，数值越小层级越高。
	// 例如 root 层级为 0，rack 层级为 1，node 层级为 2。
	tier int
	// tierName 为层级名称，如 "region"、"rack"、"node"。
	tierName string
	// isDeleting 标记该 HyperNode 是否正在被删除。
	// 删除过程中需要避免继续基于该节点构建缓存。
	isDeleting bool
}

// HyperNodeInfoOption 是用于配置 HyperNodeInfo 的函数选项类型。
// 采用函数选项模式（Functional Options）便于在测试等场景灵活构造对象。
type HyperNodeInfoOption func(*HyperNodeInfo)

// TierOpt 返回一个设置 HyperNodeInfo.tier 的选项函数。
func TierOpt(tier int) HyperNodeInfoOption {
	return func(hni *HyperNodeInfo) {
		hni.tier = tier
	}
}

// TierNameOpt 返回一个设置 HyperNodeInfo.tierName 的选项函数。
func TierNameOpt(tierName string) HyperNodeInfoOption {
	return func(hni *HyperNodeInfo) {
		hni.tierName = tierName
	}
}

// ParentOpt 返回一个设置 HyperNodeInfo.Parent 的选项函数。
func ParentOpt(parent string) HyperNodeInfoOption {
	return func(hni *HyperNodeInfo) {
		hni.Parent = parent
	}
}

// IsDeletingOpt 返回一个设置 HyperNodeInfo.isDeleting 的选项函数。
func IsDeletingOpt(isDeleting bool) HyperNodeInfoOption {
	return func(hni *HyperNodeInfo) {
		hni.isDeleting = isDeleting
	}
}

// NewHyperNodeInfo 根据 HyperNode CRD 对象创建 HyperNodeInfo。
//
// 参数 hn 为原始 CRD 对象；opts 为可选配置函数，可用于覆盖 tier、
// tierName、Parent、isDeleting 等字段，主要用于测试构造。
func NewHyperNodeInfo(hn *topologyv1alpha1.HyperNode, opts ...HyperNodeInfoOption) *HyperNodeInfo {
	hni := &HyperNodeInfo{
		Name:      hn.Name,
		HyperNode: hn,
		tier:      hn.Spec.Tier,
		tierName:  hn.Spec.TierName,
		Children:  sets.New[string](),
	}

	// 依次应用所有选项函数。
	for _, opt := range opts {
		opt(hni)
	}

	return hni
}

// String 返回 HyperNodeInfo 的可读字符串，用于日志输出。
func (hni *HyperNodeInfo) String() string {
	return strings.Join([]string{
		fmt.Sprintf("Name: %s", hni.Name),
		fmt.Sprintf(" Tier: %d", hni.tier),
		fmt.Sprintf(" TierName: %s", hni.tierName),
		fmt.Sprintf(" Parent: %s", hni.Parent)},
		",")
}

// Tier 返回 HyperNode 的层级数值。
func (hni *HyperNodeInfo) Tier() int {
	return hni.tier
}

// DeepCopy 对 HyperNodeInfo 进行深拷贝。
//
// 深拷贝包括 HyperNode CRD 对象以及 Children 集合，确保副本与原件互不干扰。
func (hni *HyperNodeInfo) DeepCopy() *HyperNodeInfo {
	if hni == nil {
		return nil
	}

	copiedHyperNodeInfo := &HyperNodeInfo{
		Name:       hni.Name,
		tier:       hni.tier,
		tierName:   hni.tierName,
		HyperNode:  hni.HyperNode.DeepCopy(),
		isDeleting: hni.isDeleting,
		Parent:     hni.Parent,
		Children:   hni.Children.Clone(),
	}

	return copiedHyperNodeInfo
}

// HyperNodes 返回所有 HyperNode 信息的深拷贝映射。
//
// 返回深拷贝是为了防止调用方意外修改内部缓存数据，保证并发安全。
func (hni *HyperNodesInfo) HyperNodes() HyperNodeInfoMap {
	copiedHyperNodes := make(map[string]*HyperNodeInfo, len(hni.hyperNodes))
	for hn, info := range hni.hyperNodes {
		copiedHyperNodes[hn] = info.DeepCopy()
	}

	return copiedHyperNodes
}

// HyperNodeTierNameMap 返回 tierName 到 tier 数值的映射。
//
// 如果不同 HyperNode 的同名 tierName 对应不同的 tier，会输出警告并使用先遇到的值。
func (hni *HyperNodesInfo) HyperNodeTierNameMap() HyperNodeTierNameMap {
	hyperNodeTierNameMap := make(map[string]int, len(hni.hyperNodes))
	for _, info := range hni.hyperNodes {
		if info.tierName != "" {
			if existingTier, ok := hyperNodeTierNameMap[info.tierName]; ok && existingTier != info.tier {
				klog.Warningf("Conflicting tiers for tierName %s: existing %d, new %d. Using %d.", info.tierName, existingTier, info.tier, info.tier)
			}
			hyperNodeTierNameMap[info.tierName] = info.tier
		}
	}

	return hyperNodeTierNameMap
}

// HyperNode 根据名称返回对应的原始 HyperNode CRD 对象。
//
// 如果指定名称的 HyperNode 不存在，返回 nil。
func (hni *HyperNodesInfo) HyperNode(name string) *topologyv1alpha1.HyperNode {
	hn := hni.hyperNodes[name]
	if hn == nil {
		return nil
	}
	return hn.HyperNode
}

// HyperNodesSetByTier 返回按 tier 分组的 HyperNode 名称集合的深拷贝。
//
// 返回深拷贝避免外部修改影响内部缓存。
func (hni *HyperNodesInfo) HyperNodesSetByTier() map[int]sets.Set[string] {
	copiedHyperNodesSetByTier := make(map[int]sets.Set[string], len(hni.hyperNodesSetByTier))
	for tier, row := range hni.hyperNodesSetByTier {
		copiedHyperNodesSetByTier[tier] = row.Clone()
	}

	return copiedHyperNodesSetByTier
}

// RealNodesSet 返回每个 HyperNode 展开后的真实节点集合的深拷贝。
//
// 真实节点集合是经过递归展开后得到的最终节点集合，例如包含子 HyperNode 的
// HyperNode 会合并所有后代 HyperNode 的真实节点。
func (hni *HyperNodesInfo) RealNodesSet() map[string]sets.Set[string] {
	copiedRealNodesSet := make(map[string]sets.Set[string], len(hni.realNodesSet))
	for name, value := range hni.realNodesSet {
		// 对集合也进行深拷贝，确保副本与原件隔离。
		copiedRealNodesSet[name] = value.Clone()
	}

	return copiedRealNodesSet
}

// DeleteHyperNode 从缓存中删除指定 HyperNode，并同步更新拓扑树。
//
// 删除流程：
//  1. 将该 HyperNode 标记为删除中，避免继续基于它构建缓存；
//  2. 重建其所有祖先的缓存，使子节点脱离被删除节点；
//  3. 移除该节点与其子节点的父子关系；
//  4. 从缓存中真正删除该节点。
func (hni *HyperNodesInfo) DeleteHyperNode(name string) error {
	hni.markHyperNodeIsDeleting(name)
	if err := hni.updateAncestors(name); err != nil {
		return err
	}
	hni.removeParent(name)

	// 在祖先缓存更新完成后，可以安全地从缓存中删除该 HyperNode。
	hni.deleteHyperNode(name)
	return nil
}

// UpdateHyperNode 在 HyperNode CRD 发生变化时更新内部缓存。
//
// 处理流程：
//  1. 根据新的成员列表更新父节点关系（移除不再存在的子节点引用）；
//  2. 更新按 tier 分组的集合；
//  3. 更新或新建 HyperNodeInfo；
//  4. 重建该节点所有祖先的缓存，使 realNodesSet 等数据保持最新。
func (hni *HyperNodesInfo) UpdateHyperNode(hn *topologyv1alpha1.HyperNode) error {
	hni.updateParent(hn)
	hni.updateHyperNodesSetByTier(hn)

	name := hn.Name
	old, exists := hni.hyperNodes[name]
	if exists {
		old.HyperNode = hn
		old.tier = hn.Spec.Tier
		old.tierName = hn.Spec.TierName
	} else {
		hni.hyperNodes[name] = NewHyperNodeInfo(hn)
	}

	return hni.updateAncestors(name)
}

// BuildHyperNodeCache 递归构建指定 HyperNode 的真实节点集合缓存。
//
// 参数说明：
//   - hn: 当前需要构建缓存的 HyperNode。
//   - processed: 已经处理过的 HyperNode 集合，用于避免重复处理和无限递归。
//   - ancestorsChain: 当前递归路径上的祖先链，用于检测循环依赖。
//   - ancestors: 需要更新缓存的目标祖先集合，只有在此集合中的节点才会被处理。
//   - nodes: 当前集群中的所有真实节点列表，用于按 Selector 匹配成员。
//
// 算法流程：
//  1. 环检测：若当前节点已在 ancestorsChain 中，说明存在循环依赖，返回错误；
//  2. 去重：若当前节点已被处理，直接返回；
//  3. 祖先过滤：若当前节点不在 ancestors 集合中，直接返回；
//  4. 删除过滤：若当前节点正在删除，跳过处理；
//  5. 遍历成员：
//     - Node 类型成员：根据 Selector（精确/正则/标签）匹配真实节点，加入 realNodesSet；
//     - HyperNode 类型成员：建立父子关系，递归构建子节点缓存，然后合并子节点的 realNodesSet；
//  6. 标记当前节点已处理。
func (hni *HyperNodesInfo) BuildHyperNodeCache(hn *HyperNodeInfo, processed sets.Set[string], ancestorsChain sets.Set[string], ancestors sets.Set[string], nodes []*corev1.Node) error {
	// 1. 环检测：若当前节点已在祖先链中，说明 HyperNode 层次结构存在循环引用。
	if ancestorsChain.Has(hn.Name) {
		return fmt.Errorf("cyclic dependency detected: HyperNode %s is already in the ancestor chain %v", hn.Name, sets.List(ancestorsChain))
	}
	// 2. 去重：已处理过的节点直接跳过，避免重复计算。
	if processed.Has(hn.Name) {
		return nil
	}
	// 3. 只处理需要更新的祖先节点，其他节点无需重建。
	if !ancestors.Has(hn.Name) {
		return nil
	}

	// 4. 将当前节点加入祖先链，函数返回时移除。
	ancestorsChain.Insert(hn.Name)
	defer ancestorsChain.Delete(hn.Name)

	// 5. 若当前节点正在删除，则跳过缓存构建。
	if hni.hyperNodeIsDeleting(hn.Name) {
		klog.InfoS("HyperNode is being deleted", "name", hn.Name)
		return nil
	}

	// 6. 遍历当前 HyperNode 的所有成员。
	for _, member := range hn.HyperNode.Spec.Members {
		switch member.Type {
		case topologyv1alpha1.MemberTypeNode:
			// 真实节点成员：按 Selector 匹配后合并到当前 HyperNode 的 realNodesSet。
			if _, ok := hni.realNodesSet[hn.Name]; !ok {
				hni.realNodesSet[hn.Name] = sets.New[string]()
			}
			members := GetMembers(member.Selector, nodes)
			klog.V(5).InfoS("Get members of hyperNode", "name", hn.Name, "members", members)
			hni.realNodesSet[hn.Name] = hni.realNodesSet[hn.Name].Union(members)

		case topologyv1alpha1.MemberTypeHyperNode:
			// HyperNode 成员：只支持精确匹配，不支持正则或标签匹配。
			memberName := hni.exactMatchMember(member.Selector)
			if memberName == "" {
				continue
			}

			// 建立当前节点与子 HyperNode 的父子关系。
			if err := hni.addChild(hn.Name, memberName); err != nil {
				return err
			}

			// 如果子 HyperNode 还未被缓存，可能是创建顺序问题，先跳过。
			memberHn, ok := hni.hyperNodes[memberName]
			if !ok {
				klog.InfoS("HyperNode not exists in cache, maybe not created or not be watched", "name", memberName, "parent", hn.Name)
				continue
			}

			// 递归构建子 HyperNode 的缓存。
			if err := hni.BuildHyperNodeCache(memberHn, processed, ancestorsChain, ancestors, nodes); err != nil {
				return err
			}

			// 将子 HyperNode 的真实节点集合合并到当前节点。
			if _, ok := hni.realNodesSet[hn.Name]; !ok {
				hni.realNodesSet[hn.Name] = sets.New[string]()
			}
			hni.realNodesSet[hn.Name] = hni.realNodesSet[hn.Name].Union(hni.realNodesSet[memberName])

		default:
			klog.ErrorS(nil, "Unknown member type", "type", member.Type)
		}
	}

	// 7. 标记当前节点已处理完成。
	processed.Insert(hn.Name)
	klog.V(3).InfoS("Successfully built RealNodesSet with members", "name", hn.Name, "nodeSets", hni.realNodesSet[hn.Name])
	return nil
}

// Ready 返回 HyperNodesInfo 是否已准备就绪。
//
// 只有当缓存构建完成且没有错误时，调度器才会基于 HyperNode 拓扑进行调度决策。
func (hni *HyperNodesInfo) Ready() bool {
	return hni.ready.Load()
}

// setReady 设置 HyperNodesInfo 的就绪标志。
//
// 在缓存开始重建时设置为 false，重建成功且无错误时设置为 true。
func (hni *HyperNodesInfo) setReady(ready bool) {
	hni.ready.Store(ready)
}

// updateParent 在 HyperNode 成员变化时更新父节点关系。
//
// 当某个 HyperNode 的成员列表中移除了某个子 HyperNode 时，
// 需要清除该子节点的 Parent 字段，避免旧引用导致拓扑关系混乱。
func (hni *HyperNodesInfo) updateParent(hn *topologyv1alpha1.HyperNode) {
	// 获取当前缓存中该 HyperNode 已有的直接子节点集合。
	oldMembers := hni.getChildren(hn.Name)
	// 从新的 CRD 对象中解析出当前的直接子节点集合。
	newMembers := sets.New[string]()
	for _, member := range hn.Spec.Members {
		if member.Type == topologyv1alpha1.MemberTypeHyperNode && member.Selector.ExactMatch != nil {
			newMembers.Insert(member.Selector.ExactMatch.Name)
		}
	}

	// 计算被移除的子节点，并清除它们的父节点引用。
	removedMembers := oldMembers.Difference(newMembers)
	if removedMembers.Len() == 0 {
		return
	}
	for member := range removedMembers {
		hni.resetParent(member)
	}
}

// GetDescendants 返回指定 HyperNode 的所有后代节点集合（包含自身）。
//
// 使用广度优先搜索（BFS）遍历 HyperNode 层次树，收集所有后代 HyperNode 名称。
// 该集合可用于在删除或重建缓存时确定受影响范围。
func (hni *HyperNodesInfo) GetDescendants(hyperNodeName string) sets.Set[string] {
	queue := []string{hyperNodeName}
	descendants := sets.New[string]()

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		descendants.Insert(current)
		if hn, ok := hni.hyperNodes[current]; ok {
			for _, member := range hn.HyperNode.Spec.Members {
				if member.Type == topologyv1alpha1.MemberTypeHyperNode && member.Selector.ExactMatch != nil {
					childName := member.Selector.ExactMatch.Name
					// 避免重复入队。
					if !descendants.Has(childName) {
						queue = append(queue, childName)
					}
				}
			}
		}
	}
	return descendants
}

// getChildren 返回指定 HyperNode 的直接子 HyperNode 集合。
//
// 只包含成员类型为 HyperNode 且使用精确匹配的成员。
func (hni *HyperNodesInfo) getChildren(hyperNodeName string) sets.Set[string] {
	children := sets.New[string]()
	hn, ok := hni.hyperNodes[hyperNodeName]
	if !ok {
		return children
	}

	for _, member := range hn.HyperNode.Spec.Members {
		if member.Type == topologyv1alpha1.MemberTypeHyperNode && member.Selector.ExactMatch != nil {
			childName := member.Selector.ExactMatch.Name
			children.Insert(childName)
		}
	}
	return children
}

// GetLeafNodes 返回指定 HyperNode 下的所有叶子 HyperNode 集合。
//
// 叶子 HyperNode 指不再包含 HyperNode 类型成员的节点，即拓扑树的最底层节点。
// 该方法会递归遍历并检测循环依赖。
func (hni *HyperNodesInfo) GetLeafNodes(hyperNodeName string) sets.Set[string] {
	leafNodes := sets.New[string]()
	ancestorsChain := sets.New[string]()
	hni.findLeafNodesWithCycleCheck(hyperNodeName, leafNodes, ancestorsChain)
	return leafNodes
}

// findLeafNodesWithCycleCheck 递归查找叶子 HyperNode，同时检测循环依赖。
//
// 参数：
//   - hyperNodeName: 当前正在处理的 HyperNode 名称。
//   - leafNodes: 收集到的叶子节点集合（引用传递，结果累积）。
//   - ancestorsChain: 当前递归路径上的祖先链，用于环检测。
func (hni *HyperNodesInfo) findLeafNodesWithCycleCheck(hyperNodeName string, leafNodes sets.Set[string], ancestorsChain sets.Set[string]) {
	// 若当前节点已在祖先链中，说明存在循环依赖，输出错误并终止该分支。
	if ancestorsChain.Has(hyperNodeName) {
		klog.ErrorS(nil, "Cycle detected in HyperNode hierarchy", "hyperNode", hyperNodeName)
		return
	}

	hn, ok := hni.hyperNodes[hyperNodeName]
	if !ok {
		return
	}

	// 将当前节点加入祖先链，退出当前递归时移除。
	ancestorsChain.Insert(hyperNodeName)
	defer ancestorsChain.Delete(hyperNodeName)

	// 默认当前节点为叶子节点；若发现 HyperNode 类型成员，则不是叶子。
	isLeaf := true
	for _, member := range hn.HyperNode.Spec.Members {
		if member.Type == topologyv1alpha1.MemberTypeHyperNode {
			isLeaf = false
			hni.findLeafNodesWithCycleCheck(member.Selector.ExactMatch.Name, leafNodes, ancestorsChain)
		}
	}

	// 若没有 HyperNode 类型成员，则将当前节点加入叶子节点集合。
	if isLeaf {
		leafNodes.Insert(hyperNodeName)
	}
}

// GetRegexOrLabelMatchLeafHyperNodes 返回使用正则或标签匹配成员的叶子 HyperNode 集合。
//
// 叶子节点指只包含真实节点成员、不包含 HyperNode 类型成员的节点。
// 该函数用于识别需要通过动态选择器（regex/label）与真实节点保持同步的叶子节点，
// 这类叶子节点的成员关系可能随集群节点变化而变化。
func (hni *HyperNodesInfo) GetRegexOrLabelMatchLeafHyperNodes() sets.Set[string] {
	leaf := sets.New[string]()
	for name, hnInfo := range hni.hyperNodes {
		if hnInfo == nil || hnInfo.HyperNode == nil {
			continue
		}

		isLeaf := true
		hasMatch := false
		for _, member := range hnInfo.HyperNode.Spec.Members {
			// 只要包含 HyperNode 类型成员，就不是叶子节点。
			if member.Type == topologyv1alpha1.MemberTypeHyperNode {
				isLeaf = false
				break
			}
			// 检查是否使用了正则或标签匹配。
			if member.Selector.RegexMatch != nil || member.Selector.LabelMatch != nil {
				hasMatch = true
			}
		}
		if isLeaf && hasMatch {
			leaf.Insert(name)
		}
	}
	return leaf
}

// addChild 将子 HyperNode 添加到父 HyperNode，并设置子节点的 Parent 字段。
//
// 处理逻辑：
//  1. 检查父节点是否存在于缓存中；
//  2. 若子节点不存在，则先创建一个占位 HyperNodeInfo（可能后续 CRD 事件会补全）；
//  3. 检查子节点是否已有其他父节点，HyperNode 不允许有多个父节点；
//  4. 建立父子关系。
func (hni *HyperNodesInfo) addChild(parent, member string) error {
	parentHn, ok := hni.hyperNodes[parent]
	if !ok {
		hni.builtErrHyperNode = parent
		return fmt.Errorf("parent HyperNode %s not exists in cache", parent)
	}

	childHn, ok := hni.hyperNodes[member]
	if !ok {
		// 子节点可能尚未被缓存（例如创建顺序导致父节点事件先于子节点事件到达），
		// 先创建一个占位对象并设置父节点，后续子节点事件到达时会补全信息。
		klog.InfoS("HyperNode not exists in cache, maybe not created or not be watched, will set parent first", "name", member, "parent", parent)
		childHn = NewHyperNodeInfo(&topologyv1alpha1.HyperNode{ObjectMeta: metav1.ObjectMeta{
			Name: member,
		}})
		hni.hyperNodes[member] = childHn
	}

	// HyperNode 拓扑树中，一个子节点只能有一个父节点。
	if childHn.Parent != "" && childHn.Parent != parent {
		hni.builtErrHyperNode = parent
		return fmt.Errorf("HyperNode %s already has a parent %s, and cannot set another parent %s", member, childHn.Parent, parent)
	}

	childHn.Parent = parent
	parentHn.Children.Insert(member)

	return nil
}

// GetMembers 根据 MemberSelector 从节点列表中筛选匹配的真实节点。
//
// 支持三种匹配方式（可组合使用）：
//   - ExactMatch：精确匹配节点名称；
//   - RegexMatch：正则表达式匹配节点名称；
//   - LabelMatch：Kubernetes 标签选择器匹配节点标签。
//
// 注意：如果 ExactMatch 的名称为空，或 LabelMatch 未设置任何条件，会提前返回空集合。
func GetMembers(selector topologyv1alpha1.MemberSelector, nodes []*corev1.Node) sets.Set[string] {
	members := sets.New[string]()

	// 1. 精确匹配：直接按名称选择节点。
	if selector.ExactMatch != nil {
		if selector.ExactMatch.Name == "" {
			return members
		}
		members.Insert(selector.ExactMatch.Name)
	}

	// 2. 正则匹配：按正则表达式过滤所有节点名称。
	if selector.RegexMatch != nil {
		pattern := selector.RegexMatch.Pattern
		reg, err := regexp.Compile(pattern)
		if err != nil {
			klog.ErrorS(err, "Failed to compile regular expression", "pattern", pattern)
			return sets.Set[string]{}
		}
		for _, node := range nodes {
			if reg.MatchString(node.Name) {
				members.Insert(node.Name)
			}
		}
	}

	// 3. 标签匹配：按标签选择器过滤所有节点。
	if selector.LabelMatch != nil {
		if len(selector.LabelMatch.MatchLabels) == 0 && len(selector.LabelMatch.MatchExpressions) == 0 {
			return members
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector.LabelMatch)
		if err != nil {
			klog.ErrorS(err, "Failed to convert labelMatch to labelSelector", "LabelMatch", selector.LabelMatch)
			return sets.Set[string]{}
		}
		for _, node := range nodes {
			nodeLabels := labels.Set(node.Labels)
			if labelSelector.Matches(nodeLabels) {
				members.Insert(node.Name)
			}
		}
	}

	return members
}

// exactMatchMember 从 MemberSelector 中获取精确匹配的成员名称。
//
// 若 Selector 不是精确匹配，或 ExactMatch 为空，则返回空字符串。
func (hni *HyperNodesInfo) exactMatchMember(selector topologyv1alpha1.MemberSelector) string {
	if selector.ExactMatch != nil {
		return selector.ExactMatch.Name
	}
	return ""
}

// updateAncestors 在 HyperNode 发生变化后重建其所有祖先的缓存。
//
// 处理流程：
//  1. 调用 rebuildCache 重建指定节点及其所有祖先的缓存；
//  2. 如果上一次构建错误恰好是当前节点，说明其父节点关系可能已修复，
//     需要额外重建该节点下所有叶子节点的缓存；
//  3. 清空错误记录并设置 ready=true。
func (hni *HyperNodesInfo) updateAncestors(name string) error {
	if err := hni.rebuildCache(name); err != nil {
		hni.setReady(false)
		return err
	}
	// 若上次构建缓存时因“某个 HyperNode 存在多个父节点”而失败，
	// 当相关父节点更新后，需要找到该父节点下的叶子节点重新构建正确缓存。
	if hni.builtErrHyperNode == name {
		klog.InfoS("Rebuilt parent hyperNode", "name", hni.builtErrHyperNode)
		leafHyperNodes := hni.GetLeafNodes(name)
		for leaf := range leafHyperNodes {
			if err := hni.rebuildCache(leaf); err != nil {
				return err
			}
		}
	}
	hni.builtErrHyperNode = ""
	hni.setReady(true)
	return nil
}

// rebuildCache 重建指定 HyperNode 及其所有祖先的缓存。
//
// 实现步骤：
//  1. 获取该节点的所有祖先（包括自身）；
//  2. 清空这些祖先的 realNodesSet、Parent、Children，避免旧数据污染；
//  3. 列出集群中所有节点；
//  4. 对每个祖先调用 BuildHyperNodeCache 重新构建真实节点集合和父子关系。
func (hni *HyperNodesInfo) rebuildCache(name string) error {
	// 获取需要重建缓存的所有祖先节点。
	ancestors := hni.hyperNodes.GetAncestors(name)

	// 在重建前清空祖先节点的旧缓存数据和父子关系。
	for _, ancestor := range ancestors {
		delete(hni.realNodesSet, ancestor)
		hni.resetParent(ancestor)
		hni.resetChildren(ancestor)
	}

	processed := sets.New[string]()
	ancestorsChain := sets.New[string]()

	// 获取当前集群所有节点，用于成员选择器匹配。
	nodes, err := hni.nodeLister.List(labels.Everything())
	if err != nil {
		klog.ErrorS(err, "Failed to list nodes", "hyperNodeName", name)
		return err
	}

	// 重新构建每个祖先节点的缓存。
	for _, ancestor := range ancestors {
		if hn, ok := hni.hyperNodes[ancestor]; ok {
			if err := hni.BuildHyperNodeCache(hn, processed, ancestorsChain, sets.New[string](ancestors...), nodes); err != nil {
				return err
			}
		}
	}

	return nil
}

// removeFromTierSet 将指定 HyperNode 从对应 tier 的集合中移除。
//
// 若该 tier 下再无其他 HyperNode，则删除整个 tier 分组。
func (hni *HyperNodesInfo) removeFromTierSet(hyperNodeName string, tier int) {
	set, ok := hni.hyperNodesSetByTier[tier]
	if !ok {
		return
	}

	set.Delete(hyperNodeName)
	if set.Len() == 0 {
		delete(hni.hyperNodesSetByTier, tier)
	}
}

// updateHyperNodesSetByTier 在 HyperNode 的 tier 变化时更新按 tier 分组的集合。
//
// 若 HyperNode 的 tier 从旧值变为新值，先从旧 tier 集合中移除，再加入新 tier 集合。
func (hni *HyperNodesInfo) updateHyperNodesSetByTier(hyperNode *topologyv1alpha1.HyperNode) {
	tier := hyperNode.Spec.Tier
	old, exists := hni.hyperNodes[hyperNode.Name]
	if exists {
		if old.tier != hyperNode.Spec.Tier {
			hni.removeFromTierSet(hyperNode.Name, old.tier)
		}
	}
	if _, ok := hni.hyperNodesSetByTier[tier]; !ok {
		hni.hyperNodesSetByTier[tier] = sets.New[string]()
	}
	hni.hyperNodesSetByTier[tier].Insert(hyperNode.Name)
}

// removeParent 移除指定 HyperNode 的所有子节点的父节点引用。
//
// 在删除 HyperNode 前调用，确保子节点不再指向即将被删除的父节点。
func (hni *HyperNodesInfo) removeParent(name string) {
	children := hni.getChildren(name)
	for child := range children {
		hni.resetParent(child)
	}
}

// resetParent 将指定 HyperNode 的 Parent 字段清空。
func (hni *HyperNodesInfo) resetParent(name string) {
	if hn, ok := hni.hyperNodes[name]; ok {
		hn.Parent = ""
	}
}

// resetChildren 清空指定 HyperNode 的 Children 集合。
func (hni *HyperNodesInfo) resetChildren(name string) {
	if hn, ok := hni.hyperNodes[name]; ok {
		hn.Children.Clear()
	}
}

// deleteHyperNode 从缓存中彻底删除指定 HyperNode。
//
// 同时会将其从对应 tier 的集合中移除。
func (hni *HyperNodesInfo) deleteHyperNode(name string) {
	hn, ok := hni.hyperNodes[name]
	if !ok {
		klog.ErrorS(nil, "HyperNode not exists in cache", "name", name)
		return
	}
	delete(hni.hyperNodes, name)
	hni.removeFromTierSet(name, hn.tier)
}

// markHyperNodeIsDeleting 将指定 HyperNode 标记为删除中。
func (hni *HyperNodesInfo) markHyperNodeIsDeleting(name string) {
	hn, ok := hni.hyperNodes[name]
	if !ok {
		klog.ErrorS(nil, "HyperNode not exists in cache", "name", name)
		return
	}
	hn.isDeleting = true
}

// hyperNodeIsDeleting 检查指定 HyperNode 是否处于删除中状态。
func (hni *HyperNodesInfo) hyperNodeIsDeleting(name string) bool {
	hn, ok := hni.hyperNodes[name]
	if !ok {
		klog.ErrorS(nil, "HyperNode not exists in cache", "name", name)
		return false
	}
	return hn.isDeleting
}

// HyperNodesInfo 返回所有 HyperNode 的可读信息映射，便于日志或调试输出。
func (hni *HyperNodesInfo) HyperNodesInfo() map[string]string {
	actualHyperNodes := make(map[string]string)
	for name, hn := range hni.hyperNodes {
		actualHyperNodes[name] = hn.String()
	}
	return actualHyperNodes
}

// NodeRegexOrLabelMatchLeafHyperNode 检查给定真实节点是否匹配某个叶子 HyperNode 的成员选择器。
//
// 该方法只检查成员类型为 Node 的选择器（正则或标签匹配），用于判断节点是否属于该叶子 HyperNode。
func (hni *HyperNodesInfo) NodeRegexOrLabelMatchLeafHyperNode(nodeName string, hyperNodeName string) (bool, error) {
	hn, ok := hni.hyperNodes[hyperNodeName]
	if !ok {
		return false, fmt.Errorf("HyperNode %s not found in cache", hyperNodeName)
	}

	for _, member := range hn.HyperNode.Spec.Members {
		if member.Type == topologyv1alpha1.MemberTypeNode {
			if hni.nodeMatchSelector(nodeName, member.Selector) {
				return true, nil
			}
		}
	}
	return false, nil
}

// nodeMatchSelector 检查给定节点名称是否匹配 MemberSelector。
//
// 只支持 RegexMatch 和 LabelMatch 两种动态匹配方式；若两者都未设置，直接返回 false。
func (hni *HyperNodesInfo) nodeMatchSelector(nodeName string, selector topologyv1alpha1.MemberSelector) bool {
	// 只处理动态匹配，ExactMatch 不在此函数处理。
	if selector.RegexMatch == nil && selector.LabelMatch == nil {
		return false
	}

	// 正则表达式匹配节点名称。
	if selector.RegexMatch != nil {
		reg, err := regexp.Compile(selector.RegexMatch.Pattern)
		if err != nil {
			klog.ErrorS(err, "Failed to compile regex pattern", "pattern", selector.RegexMatch.Pattern)
			return false
		}
		return reg.MatchString(nodeName)
	}

	// 标签选择器匹配节点标签。
	if selector.LabelMatch != nil {
		if len(selector.LabelMatch.MatchLabels) == 0 && len(selector.LabelMatch.MatchExpressions) == 0 {
			return false
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector.LabelMatch)
		if err != nil {
			klog.ErrorS(err, "Failed to convert labelMatch to labelSelector", "LabelMatch", selector.LabelMatch)
			return false
		}
		node, err := hni.nodeLister.Get(nodeName)

		if err != nil {
			klog.ErrorS(err, "Failed to get node", "nodeName", nodeName)
			return false
		}
		nodeLabels := labels.Set(node.Labels)
		if labelSelector.Matches(nodeLabels) {
			return true
		}
	}
	return false
}

// GetAncestors 返回指定 HyperNode 的所有祖先节点名称（包括自身）。
//
// 使用 BFS 向上遍历拓扑树。优先使用已缓存的 Parent 字段；
// 若 Parent 为空（例如新节点加入但缓存尚未构建），则通过 getParent 在全局映射中查找父节点。
func (hnim HyperNodeInfoMap) GetAncestors(name string) []string {
	ancestors := []string{name}
	queue := []string{name}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		parent := ""
		hn, ok := hnim[current]
		if ok && hn.Parent != "" {
			parent = hn.Parent
		} else {
			parent = hnim.getParent(current)
		}
		if parent != "" && !slices.Contains(ancestors, parent) {
			ancestors = append(ancestors, parent)
			queue = append(queue, parent)
		}
	}
	return ancestors
}

// getParent 在全局 HyperNode 映射中查找指定节点的父节点。
//
// 该方法通常在新增 HyperNode 且 BuildHyperNodeCache 尚未构建其缓存时使用。
// 查找逻辑：遍历所有 tier 比当前节点高的 HyperNode，看其精确匹配成员中是否包含当前节点。
func (hnim HyperNodeInfoMap) getParent(name string) string {
	tier := -1
	if hn, ok := hnim[name]; ok {
		tier = hn.tier
	}
	for _, hn := range hnim {
		// 父节点的 tier 必须比子节点高（数值更小）。
		if hn.tier <= tier {
			continue
		}
		for _, member := range hn.HyperNode.Spec.Members {
			// HyperNode 的父节点也必须是 HyperNode，且只支持精确匹配。
			if member.Type == topologyv1alpha1.MemberTypeNode || member.Selector.ExactMatch == nil {
				continue
			}
			memberName := member.Selector.ExactMatch.Name
			if memberName == name {
				// 找到一个父节点即可返回。
				return hn.Name
			}
		}
	}
	return ""
}

// GetLCAHyperNode 返回两个 HyperNode 的最近公共祖先(Least Common Ancestor)。
//
// 参数：
//   - hypernode：待分配任务的目标 HyperNode。
//   - jobHyperNode：作业已分配资源所在的 HyperNode。
//
// 返回值是这两个 HyperNode 在拓扑树中最近的公共祖先节点名称。
// 若任一参数为空，则直接返回另一个非空参数。
// 该值用于评估任务分配位置与作业已有资源之间的拓扑距离。
func (hnim HyperNodeInfoMap) GetLCAHyperNode(hypernode, jobHyperNode string) string {
	if hypernode == "" {
		return jobHyperNode
	}
	if jobHyperNode == "" {
		return hypernode
	}

	// 分别获取两个节点的所有祖先（包含自身）。
	hyperNodeAncestors := hnim.GetAncestors(hypernode)
	jobHyperNodeAncestors := hnim.GetAncestors(jobHyperNode)

	// 将第一个节点的祖先放入 map，便于 O(1) 查询。
	hyperNodeAncestorsMap := make(map[string]bool)
	for _, ancestor := range hyperNodeAncestors {
		hyperNodeAncestorsMap[ancestor] = true
	}

	// 遍历第二个节点的祖先，第一个同时存在于 map 中的即为最近公共祖先。
	for _, ancestor := range jobHyperNodeAncestors {
		if hyperNodeAncestorsMap[ancestor] {
			return ancestor
		}
	}
	return ""
}
