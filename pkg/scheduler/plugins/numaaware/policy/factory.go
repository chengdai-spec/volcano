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

// Package policy 实现了 NUMA 拓扑感知的策略（Policy）框架。
//
// 【核心概念】
//   - TopologyHint：拓扑提示，描述了一组资源分配方案对 NUMA Node 的亲和性偏好
//   - Policy：拓扑策略接口，定义了如何从多个 HintProvider 的提示中选出最优方案
//   - HintProvider：拓扑提示提供者接口，为特定资源类型（如 CPU）生成 NUMA 拓扑提示
//
// 【四种策略】
//   - none：不做 NUMA 约束，直接放行
//   - best-effort：尽量选择最优 NUMA 拓扑，但不强制，无法优化时也允许准入
//   - restricted：必须找到 preferred 的拓扑方案，否则拒绝准入
//   - single-numa-node：必须将所有资源分配到单个 NUMA Node 上，否则拒绝准入
package policy

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/utils/cpuset"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	nodeinfov1alpha1 "volcano.sh/apis/pkg/apis/nodeinfo/v1alpha1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// TopologyHint 拓扑提示结构体，描述资源分配对 NUMA Node 的亲和性
type TopologyHint struct {
	// NUMANodeAffinity 表示资源分配偏好的 NUMA Node 集合（用 bitmask 表示）
	// 例如：bitmask 中只设置了 bit 0，表示偏好 NUMA Node 0
	// 如果为 nil，表示没有特别的 NUMA 亲和偏好（任意 NUMA Node 都可以）
	NUMANodeAffinity bitmask.BitMask
	// Preferred 表示该亲和方案是否为"最优"（preferred）方案
	// true：表示这是一个局部性最优的分配方案（跨越的 NUMA Node 最少）
	// false：表示这是一个可行但非最优的方案
	Preferred bool
}

// Policy 拓扑策略接口
// 不同的策略实现决定了如何从多个 HintProvider 的提示中选出最优的拓扑方案
type Policy interface {
	/*
		Predicate 从所有 HintProvider 的提示中选出最优拓扑方案
		参数 providersHints：每个 HintProvider 返回的提示集合
		结构：[]map[resourceName][]TopologyHint
		外层切片：每个 HintProvider 一个元素
		map：资源名 -> 该资源的候选拓扑提示列表
		返回值：
		TopologyHint：选出的最优拓扑方案
		bool：是否允许准入（由具体策略决定）
	*/
	Predicate(providersHints []map[string][]TopologyHint) (TopologyHint, bool)
}

// HintProvider 拓扑提示提供者接口
// 每种资源类型（如 CPU）实现一个 HintProvider，负责：
//  1. 生成该资源的 NUMA 拓扑提示（GetTopologyHints）
//  2. 根据最优拓扑方案执行具体资源分配（Allocate）
type HintProvider interface {
	// Name 返回提供者名称，用于注册和日志
	Name() string
	// GetTopologyHints 返回该资源的 NUMA 拓扑提示
	// 参数：
	//   container：待调度的容器
	//   topoInfo：节点的 NUMA 拓扑信息（CPU 详情、保留资源等）
	//   resNumaSets：节点当前各资源的可用 NUMA 集合
	// 返回：map[resourceName][]TopologyHint
	GetTopologyHints(container *v1.Container, topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets) map[string][]TopologyHint
	// Allocate 根据最优拓扑方案执行具体的资源分配
	// 参数：
	//   container：待调度的容器
	//   bestHit：选出的最优拓扑方案
	//   topoInfo：节点的 NUMA 拓扑信息
	//   resNumaSets：节点当前各资源的可用 NUMA 集合
	// 返回：map[resourceName]cpuset.CPUSet，即分配给该容器的具体 CPU 集合
	Allocate(container *v1.Container, bestHit *TopologyHint, topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets) map[string]cpuset.CPUSet
}

// GetPolicy 根据节点的 Topology Manager 策略返回对应的 Policy 实现
// 参数：
//
//	node：节点信息，包含节点的 NUMA 策略配置
//	numaNodes：该节点上的 NUMA Node ID 列表
//
// 策略映射：
//   - "none"              -> policyNone（无约束）
//   - "best-effort"       -> policyBestEffort（尽力优化）
//   - "restricted"        -> policyRestricted（严格约束）
//   - "single-numa-node"  -> policySingleNumaNode（单 NUMA Node 约束）
func GetPolicy(node *api.NodeInfo, numaNodes []int) Policy {
	switch batch.NumaPolicy(node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy]) {
	case batch.None:
		return NewPolicyNone(numaNodes)
	case batch.BestEffort:
		return NewPolicyBestEffort(numaNodes)
	case batch.Restricted:
		return NewPolicyRestricted(numaNodes)
	case batch.SingleNumaNode:
		return NewPolicySingleNumaNode(numaNodes)
	}

	// 默认返回 none 策略
	return &policyNone{}
}

// AccumulateProvidersHints 收集所有 HintProvider 对指定容器的拓扑提示
// 参数：
//
//	container：待调度的容器
//	topoInfo：节点的 NUMA 拓扑信息
//	resNumaSets：节点当前各资源的可用 NUMA 集合
//	hintProviders：所有注册的 HintProvider 列表
//
// 返回：[]map[resourceName][]TopologyHint
//
//	外层切片：每个 HintProvider 一个元素
//	内层 map：该 provider 为每种资源生成的候选拓扑提示
func AccumulateProvidersHints(
	container *v1.Container,
	topoInfo *api.NumatopoInfo,
	resNumaSets api.ResNumaSets,
	hintProviders []HintProvider) (providersHints []map[string][]TopologyHint) {

	for _, provider := range hintProviders {
		hints := provider.GetTopologyHints(container, topoInfo, resNumaSets)
		providersHints = append(providersHints, hints)
	}

	return providersHints
}

// Allocate 根据最优拓扑方案，调用所有 HintProvider 执行具体的资源分配
// 参数：
//
//	container：待调度的容器
//	bestHit：选出的最优拓扑方案
//	topoInfo：节点的 NUMA 拓扑信息
//	resNumaSets：节点当前各资源的可用 NUMA 集合
//	hintProviders：所有注册的 HintProvider 列表
//
// 返回：map[resourceName]cpuset.CPUSet，即分配给该容器的各资源的具体 CPU 集合
func Allocate(container *v1.Container, bestHit *TopologyHint,
	topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets, hintProviders []HintProvider) map[string]cpuset.CPUSet {
	allResAlloc := make(map[string]cpuset.CPUSet)
	for _, provider := range hintProviders {
		resAlloc := provider.Allocate(container, bestHit, topoInfo, resNumaSets)
		for resName, assign := range resAlloc {
			allResAlloc[resName] = assign
		}
	}

	return allResAlloc
}
