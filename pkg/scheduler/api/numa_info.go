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

// Package api 中的本文件负责描述与维护节点级 NUMA（非统一内存访问）拓扑信息，
// 是 NUMA 感知调度插件（numaaware）进行 CPU/资源按 NUMA 精细分配的数据基础。
package api

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/utils/cpuset"

	nodeinfov1alpha1 "volcano.sh/apis/pkg/apis/nodeinfo/v1alpha1"
)

// NumaChgFlag 用位标记表示节点 NUMA 信息的变更状态，
// 底层由 NumatopoInfo.Compare 的比较结果推导而来。
type NumaChgFlag int

const (
	// NumaInfoResetFlag 表示重置操作（二进制 0b00）
	NumaInfoResetFlag NumaChgFlag = 0b00
	// NumaInfoMoreFlag 表示收到的可分配资源正在变多或不变（二进制 0b11）
	NumaInfoMoreFlag NumaChgFlag = 0b11
	// NumaInfoLessFlag 表示收到的可分配资源正在减少（二进制 0b10）
	NumaInfoLessFlag NumaChgFlag = 0b10
	// DefaultMaxNodeScore 表示节点打分的默认满分值
	DefaultMaxNodeScore = 100
)

// PodResourceDecision 是调度器确定的资源分配方案，
// 通过 Pod 注解（volcano.sh/topology-decision）下发给 kubelet 落地执行。
type PodResourceDecision struct {
	// NUMAResources 是按 NUMA ID 索引的资源列表，记录每个 NUMA 节点应分配的资源
	NUMAResources map[int]v1.ResourceList `json:"numa,omitempty"`
}

// ResourceInfo 描述某一类资源在 NUMA 维度上的可分配与使用情况
type ResourceInfo struct {
	Allocatable        cpuset.CPUSet   // 该资源当前空闲、可分配的 CPU 集合
	Capacity           int             // 该资源的 CPU 总容量
	AllocatablePerNuma map[int]float64 // key: NUMA ID，各 NUMA 上的可分配量
	UsedPerNuma        map[int]float64 // key: NUMA ID，各 NUMA 上已使用量
}

// NumatopoInfo 是单个节点上拓扑管理器（Topology Manager）上报的完整 NUMA 拓扑快照
type NumatopoInfo struct {
	Namespace   string                                 // 对应 Numatopo CR 的命名空间
	Name        string                                 // 对应 Numatopo CR 的名称（通常即节点名）
	Policies    map[nodeinfov1alpha1.PolicyName]string // 各策略类型的生效配置（如 cpu-manager-policy）
	NumaResMap  map[string]*ResourceInfo               // key: 资源名，记录每种资源的 NUMA 分配信息
	CPUDetail   topology.CPUDetails                    // key: CPU ID，每个 CPU 的拓扑明细（socket/core/NUMA 归属）
	ResReserved v1.ResourceList                        // 系统预留资源
}

// DeepCopy 深拷贝 NumatopoInfo，逐字段复制所有 map/slice，
// 返回一个与原对象完全独立的新副本，避免调度模拟过程中的状态污染。
// 步骤：
//  1. 构造新对象骨架，预分配各 map 容器
//  2. 复制 Policies 策略映射
//  3. 复制 NumaResMap，其中每个 ResourceInfo 也逐项克隆（含 CPUSet.Clone 与两个 per-NUMA map）
//  4. 复制 CPUDetail 的每个 CPU 明细
//  5. 复制 ResReserved 预留资源
func (info *NumatopoInfo) DeepCopy() *NumatopoInfo {
	numaInfo := &NumatopoInfo{
		Namespace:   info.Namespace,
		Name:        info.Name,
		Policies:    make(map[nodeinfov1alpha1.PolicyName]string),
		NumaResMap:  make(map[string]*ResourceInfo),
		CPUDetail:   topology.CPUDetails{},
		ResReserved: make(v1.ResourceList),
	}

	// 步骤 2：复制调度策略
	policies := info.Policies
	for name, policy := range policies {
		numaInfo.Policies[name] = policy
	}

	// 步骤 3：逐资源克隆 NUMA 分配信息
	for resName, resInfo := range info.NumaResMap {
		tmpInfo := &ResourceInfo{
			AllocatablePerNuma: make(map[int]float64),
			UsedPerNuma:        make(map[int]float64),
		}
		tmpInfo.Capacity = resInfo.Capacity
		tmpInfo.Allocatable = resInfo.Allocatable.Clone()

		for numaID, data := range resInfo.AllocatablePerNuma {
			tmpInfo.AllocatablePerNuma[numaID] = data
		}

		for numaID, data := range resInfo.UsedPerNuma {
			tmpInfo.UsedPerNuma[numaID] = data
		}

		numaInfo.NumaResMap[resName] = tmpInfo
	}

	// 步骤 4：复制 CPU 拓扑明细
	cpuDetail := info.CPUDetail
	for cpuID, detail := range cpuDetail {
		numaInfo.CPUDetail[cpuID] = detail
	}

	// 步骤 5：复制预留资源
	resReserved := info.ResReserved
	for resName, res := range resReserved {
		numaInfo.ResReserved[resName] = res
	}

	return numaInfo
}

// Compare 用于判断 kubelet 上报的资源相对当前快照的变化方向
// 返回 val:
//   - true  : kubelet 上的可分配资源变多或持平（可安全覆盖）
//   - false : kubelet 上的可分配资源在减少
//
// 步骤：
//  1. 遍历当前已有的每种资源，比较其 Allocatable CPUSet 大小
//  2. 只要存在一种资源旧值 <= 新值，即判定为"变多或持平"，返回 true
//  3. 遍历结束均未满足上述条件，说明资源在减少，返回 false
func (info *NumatopoInfo) Compare(newInfo *NumatopoInfo) bool {
	for resName := range info.NumaResMap {
		oldSize := info.NumaResMap[resName].Allocatable.Size()
		newSize := newInfo.NumaResMap[resName].Allocatable.Size()
		if oldSize <= newSize {
			return true
		}
	}

	return false
}

// Allocate 从 NUMA 拓扑快照中扣除已被分配出去的资源
// 步骤：
//  1. 遍历待分配的每种资源
//  2. 对该资源的可分配 CPUSet 执行 Difference（差集），移除已分配部分
func (info *NumatopoInfo) Allocate(resSets ResNumaSets) {
	for resName := range resSets {
		info.NumaResMap[resName].Allocatable = info.NumaResMap[resName].Allocatable.Difference(resSets[resName])
	}
}

// Release 将已释放的资源重新回收进 NUMA 拓扑快照
// 步骤：
//  1. 遍历待回收的每种资源
//  2. 对该资源的可分配 CPUSet 执行 Union（并集），补回释放部分
func (info *NumatopoInfo) Release(resSets ResNumaSets) {
	for resName := range resSets {
		info.NumaResMap[resName].Allocatable = info.NumaResMap[resName].Allocatable.Union(resSets[resName])
	}
}

// GetPodResourceNumaInfo 获取指定 Task（Pod）应当使用的、按 NUMA ID 索引的资源分配方案
// 步骤：
//  1. 优先返回 Task 缓存中已有的 NUMA 分配信息（Ti.NumaInfo）
//  2. 若缓存为空且 Pod 未携带拓扑决策注解，返回 nil
//  3. 否则解析 Pod 注解中的拓扑决策 JSON，反序列化后返回其中的 NUMAResources
func GetPodResourceNumaInfo(ti *TaskInfo) map[int]v1.ResourceList {
	// 步骤 1：缓存命中直接返回
	if ti.NumaInfo != nil && len(ti.NumaInfo.ResMap) > 0 {
		return ti.NumaInfo.ResMap
	}

	// 步骤 2：无注解则无法确定 NUMA 分配
	if _, ok := ti.Pod.Annotations[topologyDecisionAnnotation]; !ok {
		return nil
	}

	// 步骤 3：解析注解中记录的调度决策
	decision := PodResourceDecision{}
	err := json.Unmarshal([]byte(ti.Pod.Annotations[topologyDecisionAnnotation]), &decision)
	if err != nil {
		return nil
	}

	return decision.NUMAResources
}

// AddTask 在 Task 被调度到节点后，累加更新各 NUMA 上对应资源的已用量
// 步骤：
//  1. 获取该 Task 的 NUMA 资源分配方案，为空则跳过
//  2. 遍历每个 NUMA、每种资源，将其用量累加进 UsedPerNuma
func (info *NumatopoInfo) AddTask(ti *TaskInfo) {
	numaInfo := GetPodResourceNumaInfo(ti)
	if numaInfo == nil {
		return
	}

	// 步骤 2：按 NUMA/资源维度累加已用量
	for numaID, resList := range numaInfo {
		for resName, quantity := range resList {
			info.NumaResMap[string(resName)].UsedPerNuma[numaID] += ResQuantity2Float64(resName, quantity)
		}
	}
}

// RemoveTask 在 Task 从节点移除后，回退扣减各 NUMA 上对应资源的已用量
// 步骤：
//  1. 获取该 Task 的 NUMA 资源分配方案，为空则跳过
//  2. 遍历 Task 缓存中记录的每个 NUMA、每种资源，将其用量从 UsedPerNuma 中扣减
func (info *NumatopoInfo) RemoveTask(ti *TaskInfo) {
	decision := GetPodResourceNumaInfo(ti)
	if decision == nil {
		return
	}

	// 步骤 2：按 NUMA/资源维度回退已用量
	for numaID, resList := range ti.NumaInfo.ResMap {
		for resName, quantity := range resList {
			info.NumaResMap[string(resName)].UsedPerNuma[numaID] -= ResQuantity2Float64(resName, quantity)
		}
	}
}

// GenerateNodeResNumaSets 汇总所有节点，返回每个节点当前的空闲资源 CPUSet 集合
// 返回结构：map[节点名]ResNumaSets，其中 ResNumaSets 为 map[资源名]CPUSet
// 步骤：
//  1. 遍历全部节点
//  2. 跳过未上报 NUMA 拓扑的节点（NumaSchedulerInfo 为 nil）
//  3. 对每种资源克隆其 Allocatable CPUSet，组成该节点的 Resset
//  4. 以节点名为 key 存入结果映射
/*
	map[string]ResNumaSets{
    "node-a": {
        "cpu":          cpuset.New(0, 1, 2, 3, 8, 9, 10, 11), // 空闲 CPU
        "memory":       cpuset.New(0, 1),  // 有空闲内存的 NUMA 关联 CPU
        "hugepage-1Gi": cpuset.New(0),
    },
    "node-b": {
        "cpu":    cpuset.New(4, 5, 6, 7),
        "memory": cpuset.New(0, 1, 2, 3),
    },
}
*/
func GenerateNodeResNumaSets(nodes map[string]*NodeInfo) map[string]ResNumaSets {
	nodeSlice := make(map[string]ResNumaSets)
	for _, node := range nodes {
		// 步骤 2：无 NUMA 信息的节点直接跳过
		if node.NumaSchedulerInfo == nil {
			continue
		}

		// 步骤 3：克隆各资源的可分配 CPUSet（避免污染原始快照）
		resMaps := make(ResNumaSets)
		for resName, resMap := range node.NumaSchedulerInfo.NumaResMap {
			resMaps[resName] = resMap.Allocatable.Clone()
		}

		// 步骤 4：以节点名归档
		nodeSlice[node.Name] = resMaps
	}

	return nodeSlice
}

// GenerateNumaNodes 汇总所有节点，返回每个节点包含的 NUMA 节点 ID 列表
// 返回结构：map[节点名][]NUMA_ID
// 步骤：
//  1. 遍历全部节点
//  2. 跳过未上报 NUMA 拓扑的节点（NumaSchedulerInfo 为 nil）
//  3. 从 CPU 拓扑明细中提取该节点去重后的 NUMA 节点 ID 有序列表
func GenerateNumaNodes(nodes map[string]*NodeInfo) map[string][]int {
	nodeNumaMap := make(map[string][]int)

	for _, node := range nodes {
		// 步骤 2：无 NUMA 信息的节点跳过
		if node.NumaSchedulerInfo == nil {
			continue
		}
		// 步骤 3：CPUDetail.NUMANodes() 得到 CPUSet，.List() 转为 []int
		nodeNumaMap[node.Name] = node.NumaSchedulerInfo.CPUDetail.NUMANodes().List()
	}
	return nodeNumaMap
}

// ResNumaSets 是"资源名 -> CPUSet"的映射，表示某节点上一类资源在各 NUMA 上可用的 CPU 集合
type ResNumaSets map[string]cpuset.CPUSet

// Allocate 从节点可用资源集中扣除某个 Task 已占用的资源
// 步骤：
//  1. 遍历 Task 待分配的每种资源
//  2. 若节点资源集中不存在该资源则跳过
//  3. 对该资源的 CPUSet 执行 Difference，移除被占用部分
func (resSets ResNumaSets) Allocate(taskSets ResNumaSets) {
	for resName := range taskSets {
		if _, ok := resSets[resName]; !ok {
			continue
		}
		resSets[resName] = resSets[resName].Difference(taskSets[resName])
	}
}

// Release 将某个 Task 释放的资源重新并入节点可用资源集
// 步骤：
//  1. 遍历 Task 释放的每种资源
//  2. 若节点资源集中不存在该资源则跳过
//  3. 对该资源的 CPUSet 执行 Union，补回释放部分
func (resSets ResNumaSets) Release(taskSets ResNumaSets) {
	for resName := range taskSets {
		if _, ok := resSets[resName]; !ok {
			continue
		}
		resSets[resName] = resSets[resName].Union(taskSets[resName])
	}
}

// Clone 深拷贝资源集合映射，返回各 CPUSet 独立的新副本
// 步骤：
//  1. 新建空映射
//  2. 逐个克隆每种资源的 CPUSet
func (resSets ResNumaSets) Clone() ResNumaSets {
	newSets := make(ResNumaSets)
	for resName := range resSets {
		newSets[resName] = resSets[resName].Clone()
	}

	return newSets
}

// ScoredNode 在节点打分阶段封装"节点名 + 得分"，用于排序与择优
type ScoredNode struct {
	NodeName string // 节点名称
	Score    int64  // 该节点获得的调度分数
}
