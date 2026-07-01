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

// policySingleNumaNode 实现了 "single-numa-node" 拓扑策略
//
// 【策略特点】
// single-numa-node 是最严格的 NUMA 拓扑策略，要求 Pod 的所有资源必须分配到
// 单个 NUMA Node 上。这是性能最优的拓扑方案，因为所有 CPU 和内存访问都在
// 同一个 NUMA Node 内，完全没有跨 NUMA 的开销。
//
// 【与 restricted 的区别】
//   - restricted：允许跨越多个 NUMA Node，只要方案是 preferred
//   - single-numa-node：强制要求只跨越 1 个 NUMA Node
//
// 【实现原理】
// 在合并提示之前，先过滤掉所有跨越多个 NUMA Node 的提示，
// 只保留 NUMANodeAffinity 为 nil（任意）或 Count() == 1（单个 NUMA Node）的提示。
type policySingleNumaNode struct {
	numaNodes []int
}

// NewPolicySingleNumaNode 创建并返回一个 single-numa-node 策略实例
func NewPolicySingleNumaNode(numaNodes []int) Policy {
	return &policySingleNumaNode{numaNodes: numaNodes}
}

// canAdmitPodResult single-numa-node 策略下只有 preferred 的方案才允许准入
func (policy *policySingleNumaNode) canAdmitPodResult(hint *TopologyHint) bool {
	return hint.Preferred
}

// filterSingleNumaHints 过滤提示，只保留单 NUMA Node 的候选方案
//
// 【过滤规则】
// 对于每种资源的提示列表：
//   - {nil, true}：保留（表示任意 NUMA Node，可以是单个）
//   - {affinity, true} 且 affinity.Count() == 1：保留（明确指定单个 NUMA Node）
//   - {affinity, true} 且 affinity.Count() > 1：丢弃（跨越多个 NUMA Node）
//   - {affinity, false}：丢弃（非 preferred 方案）
//
// 参数：allResourcesHints 各资源的候选提示列表
// 返回：过滤后的各资源提示列表
func filterSingleNumaHints(allResourcesHints [][]TopologyHint) [][]TopologyHint {
	var filteredResourcesHints [][]TopologyHint
	for _, oneResourceHints := range allResourcesHints {
		var filtered []TopologyHint
		for _, hint := range oneResourceHints {
			// 保留 nil 亲和性的 preferred 提示（表示任意 NUMA Node 都可以）
			if hint.NUMANodeAffinity == nil && hint.Preferred {
				filtered = append(filtered, hint)
			}
			// 保留只跨越 1 个 NUMA Node 的 preferred 提示
			if hint.NUMANodeAffinity != nil && hint.NUMANodeAffinity.Count() == 1 && hint.Preferred {
				filtered = append(filtered, hint)
			}
		}
		filteredResourcesHints = append(filteredResourcesHints, filtered)
	}
	return filteredResourcesHints
}

// Predicate single-numa-node 策略的准入判断
// 流程：
//  1. 过滤所有 HintProvider 的提示
//  2. 进一步过滤，只保留单 NUMA Node 的提示
//  3. 合并提示，选出最优拓扑方案
//  4. 只有 preferred 的方案才允许准入
//
// 返回值：
//   TopologyHint：最优拓扑方案（只跨越 1 个 NUMA Node）
//   bool：仅当方案为 preferred 时为 true
func (policy *policySingleNumaNode) Predicate(providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredHints := filterProvidersHints(providersHints)
	// 关键步骤：过滤掉多 NUMA Node 的提示，只保留单 NUMA Node 的候选
	singleNumaHints := filterSingleNumaHints(filteredHints)
	bestHint := mergeFilteredHints(policy.numaNodes, singleNumaHints)
	klog.V(4).Infof("bestHint: %v\n", bestHint)
	admit := policy.canAdmitPodResult(&bestHint)
	return bestHint, admit
}
