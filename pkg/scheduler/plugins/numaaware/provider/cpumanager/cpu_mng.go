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

// Package cpumanager 实现了 CPU 资源的 NUMA 拓扑感知提供者（HintProvider）。
//
// 【核心职责】
//  1. 根据容器的 CPU 请求和节点 NUMA 拓扑，生成 CPU 的 NUMA 亲和性提示
//  2. 根据最优拓扑方案，为容器分配具体的 CPU ID 集合
//
// 【与 kubelet CPU Manager 的关系】
// 本包在调度器侧模拟了 kubelet 的 static CPU Manager 行为：
//   - 只对 Guaranteed QoS 的 Pod（整数 CPU 请求）生效
//   - 使用拓扑感知的 best-fit 算法分配 CPU，优先填满整个 Socket/Core
//   - 分配结果会写入 Pod 的 annotation，kubelet 据此设置 cpuset
package cpumanager

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/utils/cpuset"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/numaaware/policy"
)

// cpuMng 是 CPU 资源的 HintProvider 实现
// 负责为 CPU 资源生成 NUMA 拓扑提示，并执行具体的 CPU 分配
type cpuMng struct {
}

// NewProvider 创建并返回一个新的 CPU HintProvider
func NewProvider() policy.HintProvider {
	return &cpuMng{}
}

// Name 返回提供者名称
func (mng *cpuMng) Name() string {
	return "cpuMng"
}

// guaranteedCPUs 获取容器的整数 CPU 请求数
//
// 【重要约束】
// 只有 Guaranteed QoS 的 Pod（CPU request == limit，且为整数）才能做 NUMA 感知的 CPU 独占分配。
// 如果 CPU 请求不是整数（如 500m），返回 0，表示该容器不参与 NUMA 拓扑分配。
//
// 参数：container 待调度的容器
// 返回：整数 CPU 数量，0 表示非整数请求
func guaranteedCPUs(container *v1.Container) int {
	cpuQuantity := container.Resources.Requests[v1.ResourceCPU]
	// 检查 CPU 请求是否为整数：MilliValue() 是毫核值，Value()*1000 也是毫核值
	// 如果两者不等，说明有毫核部分（如 500m），不是整数 CPU
	if cpuQuantity.Value()*1000 != cpuQuantity.MilliValue() {
		return 0
	}

	return int(cpuQuantity.Value())
}

// generateCPUTopologyHints 根据可用 CPU 和请求数量，生成 CPU 的 NUMA 拓扑提示
//
// 【算法流程】
//  1. 遍历所有可能的 NUMA Node 组合（bitmask 迭代）
//  2. 对每个组合，检查可用 CPU 中有多少落在该 NUMA Node 组合内
//  3. 如果数量满足请求，生成一个候选提示
//  4. 找出满足请求的最小 NUMA Node 数量（minAffinitySize）
//  5. 将 NUMA Node 数量等于 minAffinitySize 的提示标记为 preferred
//
// 参数：
//
//	availableCPUs：当前节点可用的 CPU ID 集合
//	CPUDetails：节点的 CPU 拓扑详情（每个 CPU 属于哪个 NUMA Node/Socket/Core）
//	request：容器请求的整数 CPU 数量
//
// 返回：[]TopologyHint，所有可行的 NUMA 亲和方案
func generateCPUTopologyHints(availableCPUs cpuset.CPUSet, CPUDetails topology.CPUDetails, request int) []policy.TopologyHint {
	// minAffinitySize 记录满足请求的最小 NUMA Node 数量
	minAffinitySize := CPUDetails.NUMANodes().Size()
	hints := []policy.TopologyHint{}

	// 遍历所有可能的 NUMA Node 组合
	bitmask.IterateBitMasks(CPUDetails.NUMANodes().List(), func(mask bitmask.BitMask) {
		// 第一步：更新当前请求大小下的 minAffinitySize
		// 检查当前 NUMA Node 组合中的 CPU 总数是否满足请求
		cpusInMask := CPUDetails.CPUsInNUMANodes(mask.GetBits()...).Size()
		if cpusInMask >= request && mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// 第二步：检查当前 NUMA Node 组合中有多少可用 CPU
		numMatching := 0
		for _, c := range availableCPUs.List() {
			if mask.IsSet(CPUDetails[c].NUMANodeID) {
				numMatching++
			}
		}

		// 如果可用 CPU 数量不足，跳过该组合
		if numMatching < request {
			return
		}

		// 生成候选提示，初始 preferred 设为 false
		hints = append(hints, policy.TopologyHint{
			NUMANodeAffinity: mask,
			Preferred:        false,
		})
	})

	// 第三步：根据 minAffinitySize 标记 preferred
	// NUMA Node 数量等于 minAffinitySize 的提示为 preferred（最优局部性）
	for i := range hints {
		if hints[i].NUMANodeAffinity.Count() == minAffinitySize {
			hints[i].Preferred = true
		}
	}

	return hints
}

// getPhysicalCoresNum 计算节点上的物理核心数
//
// 【注意】
// resource-exporter 上报的 CoreID 只在每个 Socket 内唯一，
// 需要使用 SocketID + CoreID 的组合作为全局唯一标识来获取物理核心数。
//
// 参数：CPUDetails 节点的 CPU 拓扑详情
// 返回：物理核心总数
func getPhysicalCoresNum(CPUDetails topology.CPUDetails) int {
	uniques := make(map[string]struct{})
	for _, v := range CPUDetails {
		// 使用 "SocketID/CoreID" 作为唯一键
		key := fmt.Sprintf("%d/%d", v.SocketID, v.CoreID)
		uniques[key] = struct{}{}
	}
	return len(uniques)
}

// GetTopologyHints 为容器生成 CPU 的 NUMA 拓扑提示
//
// 【流程】
//  1. 检查容器是否有 CPU 请求，且为整数（Guaranteed QoS）
//  2. 构建 CPU 拓扑结构
//  3. 扣除系统保留的 CPU
//  4. 根据可用 CPU 和请求数量生成拓扑提示
//
// 参数：
//
//	container：待调度的容器
//	topoInfo：节点的 NUMA 拓扑信息
//	resNumaSets：节点当前各资源的可用 NUMA 集合
//
// 返回：map["cpu"][]TopologyHint，CPU 的 NUMA 亲和方案列表
func (mng *cpuMng) GetTopologyHints(container *v1.Container,
	topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets) map[string][]policy.TopologyHint {
	// 检查容器是否有 CPU 请求
	if _, ok := container.Resources.Requests[v1.ResourceCPU]; !ok {
		klog.Warningf("container %s has no cpu request", container.Name)
		return nil
	}

	// 获取整数 CPU 请求数
	requestNum := guaranteedCPUs(container)
	if requestNum == 0 {
		klog.Warningf(" the cpu request isn't  integer in container %s", container.Name)
		return nil
	}

	// 构建 CPU 拓扑结构，用于后续的拓扑感知分配
	cputopo := &topology.CPUTopology{
		NumCPUs:    topoInfo.CPUDetail.CPUs().Size(),
		NumCores:   getPhysicalCoresNum(topoInfo.CPUDetail),
		NumSockets: topoInfo.CPUDetail.Sockets().Size(),
		CPUDetails: topoInfo.CPUDetail,
	}

	// 扣除系统保留的 CPU（如 kubelet/system reserved）
	reserved := cpuset.New()
	reservedCPUs, ok := topoInfo.ResReserved[v1.ResourceCPU]
	if ok {
		// 向上取整，因为不能独占分配分数 CPU
		reservedCPUsFloat := float64(reservedCPUs.MilliValue()) / 1000
		numReservedCPUs := int(math.Ceil(reservedCPUsFloat))
		reserved, _ = takeByTopology(cputopo, cputopo.CPUDetails.CPUs(), numReservedCPUs)
		klog.V(4).Infof("[cpumanager] reserve cpuset :%v", reserved)
	}

	// 获取可用的 CPU 集合
	availableCPUSet, ok := resNumaSets[string(v1.ResourceCPU)]
	if !ok {
		klog.Warningf("no cpu resource")
		return nil
	}

	// 扣除系统保留 CPU 后的真正可用 CPU
	availableCPUSet = availableCPUSet.Difference(reserved)
	klog.V(4).Infof("requested: %d, availableCPUSet: %v", requestNum, availableCPUSet)

	return map[string][]policy.TopologyHint{
		string(v1.ResourceCPU): generateCPUTopologyHints(availableCPUSet, topoInfo.CPUDetail, requestNum),
	}
}

// Allocate 根据最优拓扑方案，为容器分配具体的 CPU ID 集合
//
// 【分配策略】
//  1. 先尝试分配与 bestHit NUMA 亲和性对齐的 CPU（alignedCPUs）
//  2. 如果对齐的 CPU 不够，再从剩余可用 CPU 中补充
//
// 参数：
//
//	container：待调度的容器
//	bestHit：选出的最优拓扑方案
//	topoInfo：节点的 NUMA 拓扑信息
//	resNumaSets：节点当前各资源的可用 NUMA 集合
//
// 返回：map["cpu"]cpuset.CPUSet，分配给该容器的具体 CPU ID 集合
func (mng *cpuMng) Allocate(container *v1.Container, bestHit *policy.TopologyHint,
	topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets) map[string]cpuset.CPUSet {
	// 构建 CPU 拓扑结构
	cputopo := &topology.CPUTopology{
		NumCPUs:    topoInfo.CPUDetail.CPUs().Size(),
		NumCores:   getPhysicalCoresNum(topoInfo.CPUDetail),
		NumSockets: topoInfo.CPUDetail.Sockets().Size(),
		CPUDetails: topoInfo.CPUDetail,
	}

	// 扣除系统保留的 CPU
	reserved := cpuset.New()
	reservedCPUs, ok := topoInfo.ResReserved[v1.ResourceCPU]
	if ok {
		reservedCPUsFloat := float64(reservedCPUs.MilliValue()) / 1000
		numReservedCPUs := int(math.Ceil(reservedCPUsFloat))
		reserved, _ = takeByTopology(cputopo, cputopo.CPUDetails.CPUs(), numReservedCPUs)
		klog.V(3).Infof("[cpumanager] reserve cpuset :%v", reserved)
	}

	// 获取容器请求的整数 CPU 数量
	requestNum := guaranteedCPUs(container)
	availableCPUSet := resNumaSets[string(v1.ResourceCPU)]
	// 扣除系统保留 CPU
	availableCPUSet = availableCPUSet.Difference(reserved)

	klog.V(4).Infof("alignedCPUs: %v requestNum: %v bestHit %v", availableCPUSet, requestNum, bestHit)

	result := cpuset.New()

	// 第一步：尝试分配与 bestHit NUMA 亲和性对齐的 CPU
	if bestHit.NUMANodeAffinity != nil {
		alignedCPUs := cpuset.New()
		// 遍历 bestHit 中指定的每个 NUMA Node
		for _, numaNodeID := range bestHit.NUMANodeAffinity.GetBits() {
			// 获取该 NUMA Node 上且可用的 CPU 集合
			alignedCPUs = alignedCPUs.Union(availableCPUSet.Intersection(cputopo.CPUDetails.CPUsInNUMANodes(numaNodeID)))
		}

		// 计算实际要对齐的 CPU 数量（不超过请求数）
		numAlignedToAlloc := alignedCPUs.Size()
		if requestNum < numAlignedToAlloc {
			numAlignedToAlloc = requestNum
		}

		// 使用拓扑感知算法从对齐的 CPU 中选择
		alignedCPUs, err := takeByTopology(cputopo, alignedCPUs, numAlignedToAlloc)
		if err != nil {
			return map[string]cpuset.CPUSet{
				string(v1.ResourceCPU): cpuset.New(),
			}
		}

		result = result.Union(alignedCPUs)
	}

	// 第二步：如果对齐的 CPU 不够，从剩余可用 CPU 中补充
	remainingCPUs, err := takeByTopology(cputopo, availableCPUSet.Difference(result), requestNum-result.Size())
	if err != nil {
		return map[string]cpuset.CPUSet{
			string(v1.ResourceCPU): cpuset.New(),
		}
	}

	result = result.Union(remainingCPUs)

	return map[string]cpuset.CPUSet{
		string(v1.ResourceCPU): result,
	}
}
