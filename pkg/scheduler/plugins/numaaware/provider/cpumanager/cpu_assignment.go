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

// Package cpumanager 实现了拓扑感知的 CPU 分配算法。
//
// 【核心算法】takeByTopology
// 该算法按照"先整 Socket、再整 Core、最后单线程"的顺序分配 CPU，
// 尽量保持 CPU 分配的拓扑局部性，减少跨 Socket/Core 的开销。
//
// 分配优先级：
//  1. 整个 Socket（所有 CPU 都在同一个 Socket 上）
//  2. 整个 Core（所有 CPU 都在同一个 Core 上）
//  3. 单个线程（超线程场景下，优先填充已部分分配的 Core）
package cpumanager

import (
	"fmt"
	"sort"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/utils/cpuset"
)

// cpuAccumulator CPU 分配累加器
// 用于跟踪 CPU 分配过程中的状态，包括：
//   - 还需要多少 CPU
//   - 已分配的 CPU 集合
//   - 剩余的 CPU 详情
type cpuAccumulator struct {
	// topo CPU 拓扑结构
	topo *topology.CPUTopology
	// details 当前剩余的 CPU 详情（随分配过程动态减少）
	details topology.CPUDetails
	// numCPUsNeeded 还需要的 CPU 数量
	numCPUsNeeded int
	// result 已分配的 CPU 集合
	result cpuset.CPUSet
}

// newCPUAccumulator 创建一个新的 CPU 分配累加器
// 参数：
//
//	topo：CPU 拓扑结构
//	availableCPUs：当前可用的 CPU 集合
//	numCPUs：需要分配的 CPU 数量
func newCPUAccumulator(topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int) *cpuAccumulator {
	return &cpuAccumulator{
		topo:          topo,
		details:       topo.CPUDetails.KeepOnly(availableCPUs),
		numCPUsNeeded: numCPUs,
		result:        cpuset.New(),
	}
}

// take 从累加器中取走指定的 CPU 集合
// 更新 result 和 details，并减少 numCPUsNeeded
/*
Socket 0:
  Core 0: CPU 0, CPU 1
  Core 1: CPU 2, CPU 3
Socket 1:
  Core 2: CPU 4, CPU 5
  Core 3: CPU 6, CPU 7

# 为什么需要同时维护 `result` 和 `details`？

| 字段             | 作用                         | 谁在读它                                                              |
|-----------------|-----------------------------|----------------------------------------------------------------------|
| `result`        | 记录“已拿走哪些 CPU”           | 最终返回分配结果                                                        |
| `details`       | 记录“还剩哪些 CPU 可用及其拓扑”  | `freeSockets()`、`freeCores()`、`freeCPUs()` 依赖它来判断下一步能分配什么  |
| `numCPUsNeeded` | 记录“还差多少”                 | `isSatisfied()`、`needs()` 做终止判断                                  |

## 核心思想

`take` 每调用一次，就从“可用池”(`details`) 中移除已分配的 CPU，同时往“结果池”(`result`) 中添加。

这样后续的 `freeSockets()` / `freeCores()` / `freeCPUs()` 看到的永远是“扣除已分配部分后的真实剩余拓扑”，保证不会重复分配。
*/
func (a *cpuAccumulator) take(cpus cpuset.CPUSet) {
	a.result = a.result.Union(cpus)
	// 从 details 中移除已分配的 CPU
	a.details = a.details.KeepOnly(a.details.CPUs().Difference(a.result))
	a.numCPUsNeeded -= cpus.Size()
}

// freeSockets 返回完全空闲的 Socket ID 列表
// 排序规则：Socket ID 升序
//
// 一个 Socket 被认为是"空闲"的，当它的所有 CPU 都还在 details 中（未被分配）
func (a *cpuAccumulator) freeSockets() []int {
	// 取 Socket 的 CPU 集合与剩余 CPU 集合的交集，如果相等则该 Socket 完全空闲
	return a.details.Sockets().Intersection(a.details.CPUs()).List()
}

// freeCores 返回完全空闲的 Core ID 列表
//
// 【排序规则】(多级排序)
//  1. 该 Core 所在 Socket 上的空闲 Core 数量（升序）：优先从空闲 Core 少的 Socket 取
//  2. Socket ID（升序）
//  3. Core ID（升序）
//
// 这种排序策略的目的是：优先填满已经部分使用的 Socket，减少 Socket 碎片
func (a *cpuAccumulator) freeCores() []int {
	socketIDs := a.details.Sockets().UnsortedList()
	// 按 Socket 上的空闲 Core 数量和 Socket ID 排序
	sort.Slice(socketIDs,
		func(i, j int) bool {
			iCores := a.details.CoresInSockets(socketIDs[i]).Intersection(a.details.CPUs())
			jCores := a.details.CoresInSockets(socketIDs[j]).Intersection(a.details.CPUs())
			return iCores.Size() < jCores.Size() || socketIDs[i] < socketIDs[j]
		})

	coreIDs := []int{}
	for _, s := range socketIDs {
		coreIDs = append(coreIDs, a.details.CoresInSockets(s).Intersection(a.details.CPUs()).List()...)
	}
	return coreIDs
}

// freeCPUs 返回可用的单个 CPU ID 列表
//
// 【排序规则】（多级排序，用于超线程场景）
//  1. 与已分配 CPU 在同一 Socket 上的 Core 优先（socketColoScore 降序）
//  2. 同一 Socket 上可用 CPU 少的 Core 优先（socketFreeScore 升序）
//  3. 同一 Core 上可用 CPU 少的优先（coreFreeScore 升序）
//  4. Socket ID（升序）
//  5. Core ID（升序）
//
// 这种排序策略的目的是：
//   - 优先填充已部分分配的 Socket（提高 Socket 利用率）
//   - 优先填充已部分分配的 Core（减少超线程干扰）
func (a *cpuAccumulator) freeCPUs() []int {
	result := []int{}
	cores := a.details.Cores().List()

	sort.Slice(
		cores,
		func(i, j int) bool {
			iCore := cores[i]
			jCore := cores[j]

			iCPUs := a.topo.CPUDetails.CPUsInCores(iCore).List()
			jCPUs := a.topo.CPUDetails.CPUsInCores(jCore).List()

			iSocket := a.topo.CPUDetails[iCPUs[0]].SocketID
			jSocket := a.topo.CPUDetails[jCPUs[0]].SocketID

			// 计算与已分配 CPU 在同一 Socket 上的数量（colocation score）
			iSocketColoScore := a.topo.CPUDetails.CPUsInSockets(iSocket).Intersection(a.result).Size()
			jSocketColoScore := a.topo.CPUDetails.CPUsInSockets(jSocket).Intersection(a.result).Size()

			// 计算同一 Socket 上可用的 CPU 数量
			iSocketFreeScore := a.details.CPUsInSockets(iSocket).Size()
			jSocketFreeScore := a.details.CPUsInSockets(jSocket).Size()

			// 计算同一 Core 上可用的 CPU 数量
			iCoreFreeScore := a.details.CPUsInCores(iCore).Size()
			jCoreFreeScore := a.details.CPUsInCores(jCore).Size()

			return iSocketColoScore > jSocketColoScore ||
				iSocketFreeScore < jSocketFreeScore ||
				iCoreFreeScore < jCoreFreeScore ||
				iSocket < jSocket ||
				iCore < jCore
		})

	// 按排序后的顺序追加 CPU ID
	for _, core := range cores {
		result = append(result, a.details.CPUsInCores(core).List()...)
	}
	return result
}

// needs 判断是否还需要至少 n 个 CPU
func (a *cpuAccumulator) needs(n int) bool {
	return a.numCPUsNeeded >= n
}

// isSatisfied 判断是否已满足 CPU 需求
func (a *cpuAccumulator) isSatisfied() bool {
	return a.numCPUsNeeded < 1
}

// isFailed 判断是否无法满足需求（剩余 CPU 不足）
func (a *cpuAccumulator) isFailed() bool {
	return a.numCPUsNeeded > a.details.CPUs().Size()
}

// takeByTopology 使用拓扑感知算法分配指定数量的 CPU
//
// 【算法流程】（三级递进）
//  1. 先尝试分配整个 Socket：如果需要且有空闲 Socket，整个 Socket 分配
//  2. 再尝试分配整个 Core：如果需要且有空闲 Core，整个 Core 分配
//  3. 最后分配单线程：按排序规则逐个分配 CPU
//
// 这种"先大后小"的策略确保了：
//   - 尽量保持 CPU 在同一个 Socket/Core 上，减少跨 NUMA 访问
//   - 避免 Socket/Core 碎片化
//
// 参数：
//
//	topo：CPU 拓扑结构
//	availableCPUs：当前可用的 CPU 集合
//	numCPUs：需要分配的 CPU 数量
//
// 返回：分配结果 cpuset.CPUSet，或错误
func takeByTopology(topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	acc := newCPUAccumulator(topo, availableCPUs, numCPUs)
	if acc.isSatisfied() {
		return acc.result, nil
	}
	if acc.isFailed() {
		return cpuset.New(), fmt.Errorf("not enough cpus available to satisfy request")
	}

	// 第一步：分配整个 Socket
	// 如果需要的 CPU 数量 >= 一个 Socket 的 CPU 数量，尝试分配整个 Socket
	if acc.needs(acc.topo.CPUsPerSocket()) {
		for _, s := range acc.freeSockets() {
			klog.V(4).Infof("[cpumanager] takeByTopology: claiming socket [%d]", s)
			acc.take(acc.details.CPUsInSockets(s))
			if acc.isSatisfied() {
				return acc.result, nil
			}
			// 如果剩余需求不足一个 Socket，停止 Socket 分配
			if !acc.needs(acc.topo.CPUsPerSocket()) {
				break
			}
		}
	}

	// 第二步：分配整个 Core
	// 如果需要的 CPU 数量 >= 一个 Core 的 CPU 数量，尝试分配整个 Core
	if acc.needs(acc.topo.CPUsPerCore()) {
		for _, c := range acc.freeCores() {
			klog.V(4).Infof("[cpumanager] takeByTopology: claiming core [%d]", c)
			acc.take(acc.details.CPUsInCores(c))
			if acc.isSatisfied() {
				return acc.result, nil
			}
			// 如果剩余需求不足一个 Core，停止 Core 分配
			if !acc.needs(acc.topo.CPUsPerCore()) {
				break
			}
		}
	}

	// 第三步：分配单个 CPU 线程
	// 优先填充已部分分配的 Core（减少超线程干扰）
	for _, c := range acc.freeCPUs() {
		klog.V(4).Infof("[cpumanager] takeByTopology: claiming CPU [%d]", c)
		if acc.needs(1) {
			acc.take(cpuset.New(c))
		}
		if acc.isSatisfied() {
			return acc.result, nil
		}
	}

	return cpuset.New(), fmt.Errorf("failed to allocate cpus")
}
