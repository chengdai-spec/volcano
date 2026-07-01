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

package policy

import "k8s.io/klog/v2"

// policyRestricted 实现了 "restricted" 拓扑策略
//
// 【策略特点】
// restricted 策略要求必须找到 preferred 的 NUMA 拓扑方案，否则拒绝 Pod 准入。
// 与 best-effort 策略的区别在于：
//   - best-effort：尽力优化，找不到 preferred 也放行
//   - restricted：严格要求，找不到 preferred 就拒绝
//
// 【适用场景】
// 适用于对 NUMA 局部性有较高要求的性能敏感型工作负载，
// 如数据库、高性能计算等，不允许跨 NUMA Node 访问内存带来的性能损失。
type policyRestricted struct {
	numaNodes []int
}

// NewPolicyRestricted 创建并返回一个 restricted 策略实例
func NewPolicyRestricted(numaNodes []int) Policy {
	return &policyRestricted{numaNodes: numaNodes}
}

// canAdmitPodResult restricted 策略下只有 preferred 的方案才允许准入
// 如果找不到 preferred 的拓扑方案，返回 false，Pod 将被拒绝
func (p *policyRestricted) canAdmitPodResult(hint *TopologyHint) bool {
	return hint.Preferred
}

// Predicate restricted 策略的准入判断
// 流程：
//  1. 过滤所有 HintProvider 的提示
//  2. 合并提示，选出最优拓扑方案（bestHint）
//  3. 只有 bestHint.Preferred == true 时才允许准入
//
// 返回值：
//   TopologyHint：最优拓扑方案
//   bool：仅当方案为 preferred 时为 true
func (p *policyRestricted) Predicate(providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredHints := filterProvidersHints(providersHints)
	bestHint := mergeFilteredHints(p.numaNodes, filteredHints)
	admit := p.canAdmitPodResult(&bestHint)

	klog.V(4).Infof("bestHint: %v admit %v\n", bestHint, admit)
	return bestHint, admit
}
