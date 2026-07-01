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

// policyNone 实现了 "none" 拓扑策略
//
// 【策略特点】
// none 策略是最宽松的策略，对 NUMA 拓扑不做任何约束。
// 无论 HintProvider 返回什么提示，都直接允许 Pod 准入。
// 适用于对 NUMA 局部性没有要求的普通工作负载。
type policyNone struct {
	numaNodes []int
}

// NewPolicyNone 创建并返回一个 none 策略实例
func NewPolicyNone(numaNodes []int) Policy {
	return &policyNone{numaNodes: numaNodes}
}

// canAdmitPodResult none 策略下始终返回 true，允许任何拓扑方案准入
func (policy *policyNone) canAdmitPodResult(hint *TopologyHint) bool {
	return true
}

// Predicate none 策略的准入判断：直接返回空提示 + 允许准入
// 不做任何拓扑提示的收集和合并，直接放行
func (policy *policyNone) Predicate(providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	return TopologyHint{}, policy.canAdmitPodResult(nil)
}
