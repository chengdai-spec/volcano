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

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
)

// filterProvidersHints 过滤并整理所有 HintProvider 返回的拓扑提示
//
// 【输入结构】
// providersHints: []map[resourceName][]TopologyHint
//   - 外层切片：每个 HintProvider 一个元素
//   - 内层 map：资源名 -> 该资源的候选拓扑提示列表
//
// 【输出结构】
// [][]TopologyHint
//   - 外层切片：每种资源一个元素
//   - 内层切片：该资源的所有候选拓扑提示
//
// 【处理逻辑】
//   - 如果某个 provider 对某种资源没有提示（nil 或空），插入一个默认的"任意 NUMA"提示
//   - 如果提示列表为空（len==0），插入一个非 preferred 的默认提示（表示无法满足）
func filterProvidersHints(providersHints []map[string][]TopologyHint) [][]TopologyHint {
	var allProviderHints [][]TopologyHint
	for _, hints := range providersHints {
		// 如果 provider 没有返回任何提示，说明该 provider 对 NUMA 亲和没有偏好
		// 插入一个 {nil, true} 的默认提示：nil 表示任意 NUMA Node，true 表示 preferred
		if len(hints) == 0 {
			klog.Infof("[numatopo] Hint Provider has no preference for NUMA affinity with any resource")
			allProviderHints = append(allProviderHints, []TopologyHint{{nil, true}})
			continue
		}

		// 遍历该 provider 返回的每种资源的提示
		for resource := range hints {
			if hints[resource] == nil {
				// 该资源没有提示，插入默认 preferred 提示
				klog.Infof("[numatopo] Hint Provider has no preference for NUMA affinity with resource '%s'", resource)
				allProviderHints = append(allProviderHints, []TopologyHint{{nil, true}})
				continue
			}

			if len(hints[resource]) == 0 {
				// 该资源有提示但列表为空，说明无法满足，插入非 preferred 提示
				klog.Infof("[numatopo] Hint Provider has no possible NUMA affinities for resource '%s'", resource)
				allProviderHints = append(allProviderHints, []TopologyHint{{nil, false}})
				continue
			}

			// 正常情况：直接使用该资源的候选提示列表
			allProviderHints = append(allProviderHints, hints[resource])
		}
	}
	return allProviderHints
}

// mergeFilteredHints 合并所有资源的拓扑提示，选出全局最优的拓扑方案
//
// 【算法核心】
//  1. 遍历所有 HintProvider 提示的排列组合（笛卡尔积）
//  2. 对每种排列，将各提示的 NUMA Node 亲和性做位与（bitwise-AND）合并
//  3. 从所有合并结果中选出"最优"的 hint 作为 bestHint
//
// 【最优选择规则】
//   - preferred > non-preferred：优先选择 preferred 的方案
//   - 同等 preferred 下，更窄（narrower）的 NUMA 亲和性更优
//     （跨越更少的 NUMA Node = 更好的局部性）
//
// 参数：
//   numaNodes：节点上所有 NUMA Node ID 列表
//   filteredHints：过滤后的各资源候选提示
//
// 返回：合并后的最优 TopologyHint
func mergeFilteredHints(numaNodes []int, filteredHints [][]TopologyHint) TopologyHint {
	// 默认亲和性：包含所有 NUMA Node（即不做任何限制）
	defaultAffinity, _ := bitmask.NewBitMask(numaNodes...)

	// 初始化 bestHint 为默认值：全 NUMA Node 亲和、非 preferred
	bestHint := TopologyHint{defaultAffinity, false}

	// 遍历所有提示的排列组合
	iterateAllProviderTopologyHints(filteredHints, func(permutation []TopologyHint) {
		// 将当前排列的所有提示合并为一个
		mergedHint := mergePermutation(numaNodes, permutation)

		// 如果合并后的亲和性为空（没有有效的 NUMA Node），跳过
		if mergedHint.NUMANodeAffinity.Count() == 0 {
			return
		}

		// 选择规则1：如果当前 bestHint 非 preferred，而 mergedHint 是 preferred，
		// 则选择 mergedHint（preferred 优先）
		if mergedHint.Preferred && !bestHint.Preferred {
			bestHint = mergedHint
			return
		}

		// 选择规则2：如果当前 bestHint 是 preferred，而 mergedHint 非 preferred，
		// 则保持 bestHint 不变（preferred 永远优于 non-preferred）
		if !mergedHint.Preferred && bestHint.Preferred {
			return
		}

		// 选择规则3：如果两者 preferred 状态相同，选择更窄（narrower）的亲和性
		// 更窄 = 跨越更少的 NUMA Node = 更好的 NUMA 局部性
		if !mergedHint.NUMANodeAffinity.IsNarrowerThan(bestHint.NUMANodeAffinity) {
			return
		}

		// 其他情况：更新 bestHint 为当前 mergedHint
		bestHint = mergedHint
	})

	return bestHint
}

// iterateAllProviderTopologyHints 遍历所有 HintProvider 提示的排列组合
//
// 【实现原理】
// 使用递归实现笛卡尔积遍历，等价于多层嵌套循环：
//
//	for i := 0; i < len(providerHints[0]); i++
//	    for j := 0; j < len(providerHints[1]); j++
//	        for k := 0; k < len(providerHints[2]); k++
//	            ...
//	            permutation := []TopologyHint{
//	                providerHints[0][i],
//	                providerHints[1][j],
//	                providerHints[2][k],
//	                ...
//	            }
//	            callback(permutation)
//
// 参数：
//   allProviderHints：每种资源的候选提示列表
//   callback：对每种排列执行的回调函数
func iterateAllProviderTopologyHints(allProviderHints [][]TopologyHint, callback func([]TopologyHint)) {
	// 递归辅助函数
	var iterate func(i int, accum []TopologyHint)
	iterate = func(i int, accum []TopologyHint) {
		// 递归终止条件：已遍历完所有 provider
		if i == len(allProviderHints) {
			callback(accum)
			return
		}

		// 遍历当前 provider 的所有提示，递归处理下一个 provider
		for j := range allProviderHints[i] {
			iterate(i+1, append(accum, allProviderHints[i][j]))
		}
	}
	iterate(0, []TopologyHint{})
}

// mergePermutation 将一个提示排列合并为单个 TopologyHint
//
// 【合并逻辑】
//  1. 收集排列中所有提示的 NUMA Node 亲和性 bitmask
//  2. 对所有 bitmask 做位与（bitwise-AND）操作，得到合并后的亲和性
//  3. 如果排列中所有提示都是 preferred，合并结果也是 preferred；否则为 non-preferred
//
// 参数：
//   numaNodes：节点上所有 NUMA Node ID 列表
//   permutation：一个提示排列（每个元素来自不同的 HintProvider）
//
// 返回：合并后的 TopologyHint
func mergePermutation(numaNodes []int, permutation []TopologyHint) TopologyHint {
	// 默认亲和性：包含所有 NUMA Node
	defaultAffinity, _ := bitmask.NewBitMask(numaNodes...)

	// 假设初始为 preferred，只有所有提示都是 preferred 时才保持 true
	preferred := true
	var numaAffinities []bitmask.BitMask

	for _, hint := range permutation {
		if hint.NUMANodeAffinity == nil {
			// nil 亲和性表示"任意 NUMA Node"，使用默认全量亲和性
			numaAffinities = append(numaAffinities, defaultAffinity)
		} else {
			numaAffinities = append(numaAffinities, hint.NUMANodeAffinity)
		}

		// 只要有一个提示不是 preferred，合并结果就不是 preferred
		if !hint.Preferred {
			preferred = false
		}
	}

	// 对所有亲和性做位与操作，得到交集
	mergedAffinity := bitmask.And(defaultAffinity, numaAffinities...)

	// 构建合并后的提示
	return TopologyHint{mergedAffinity, preferred}
}
