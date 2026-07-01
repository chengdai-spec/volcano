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

// policyBestEffort 实现了 "best-effort" 拓扑策略
//
// 【策略特点】
// best-effort 策略会尝试寻找最优的 NUMA 拓扑方案（preferred），
// 但如果找不到也不会拒绝 Pod，仍然允许准入。
// 这是一种"尽力而为"的策略：能优化就优化，不能优化也不阻塞调度。
//
// 【与 restricted 的区别】
//   - best-effort：canAdmitPodResult 始终返回 true，无论 hint 是否 preferred
//   - restricted：canAdmitPodResult 只在 hint.Preferred == true 时返回 true
type policyBestEffort struct {
	numaNodes []int
}

// NewPolicyBestEffort 创建并返回一个 best-effort 策略实例
func NewPolicyBestEffort(numaNodes []int) Policy {
	return &policyBestEffort{numaNodes: numaNodes}
}

// canAdmitPodResult best-effort 策略下始终返回 true，允许准入
// 即使找不到 preferred 的拓扑方案，也不会拒绝 Pod
func (p *policyBestEffort) canAdmitPodResult(hint *TopologyHint) bool {
	return true
}

// Predicate best-effort 策略的准入判断
// 流程：
//  1. 过滤所有 HintProvider 的提示
//  2. 合并提示，选出最优拓扑方案（bestHint）
//  3. 无论 bestHint 是否 preferred，都允许准入
//
// 返回值：
//   TopologyHint：最优拓扑方案（可能是 preferred，也可能不是）
//   bool：始终为 true
func (p *policyBestEffort) Predicate(providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredProvidersHints := filterProvidersHints(providersHints)
	bestHint := mergeFilteredHints(p.numaNodes, filteredProvidersHints)
	admit := p.canAdmitPodResult(&bestHint)

	klog.V(4).Infof("bestHint: %v admit %v\n", bestHint, admit)
	return bestHint, admit
}
