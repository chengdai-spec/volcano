/*
Copyright 2023 The Volcano Authors.

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

package gpushare

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// getDevicesIdleGPUMemory 计算每块 GPU 当前的空闲显存。
//
// 计算方式：对于每块 GPU，空闲显存 = 该卡总显存 - 该卡已被 Pod 占用的显存。
// 已占用显存由 getUsedGPUMemory 汇总 PodMap 中非终态 Pod 的 gpu-memory 请求得到。
//
// 实际案例：
// 节点有 2 块 GPU，每块 8192MiB。
// ID=0 上运行了请求 1024MiB 的 Pod，ID=1 空闲。
// 则返回 map[int]uint{0: 7168, 1: 8192}。
func getDevicesIdleGPUMemory(gs *GPUDevices) map[int]uint {
	devicesAllGPUMemory := getDevicesAllGPUMemory(gs)
	devicesUsedGPUMemory := getDevicesUsedGPUMemory(gs)
	res := map[int]uint{}
	for id, allMemory := range devicesAllGPUMemory {
		if usedMemory, found := devicesUsedGPUMemory[id]; found {
			res[id] = allMemory - usedMemory
		} else {
			res[id] = allMemory
		}
	}
	return res
}

// getDevicesUsedGPUMemory 汇总每块 GPU 上已被占用的显存。
func getDevicesUsedGPUMemory(gs *GPUDevices) map[int]uint {
	res := map[int]uint{}
	for _, device := range gs.Device {
		res[device.ID] = device.getUsedGPUMemory()
	}
	return res
}

// getDevicesAllGPUMemory 返回每块 GPU 的总显存。
func getDevicesAllGPUMemory(gs *GPUDevices) map[int]uint {
	res := map[int]uint{}
	for _, device := range gs.Device {
		res[device.ID] = device.Memory
	}
	return res
}

// getDevicesIdleGPUs 返回当前没有任何 Pod 占用的 GPU ID 列表。
//
// 注意：这里的“空闲”是整卡维度，只要 PodMap 为空即认为空闲，
// 与按显存共享的“部分空闲”概念不同。
func getDevicesIdleGPUs(gs *GPUDevices) []int {
	res := []int{}
	for _, device := range gs.Device {
		if device.isIdleGPU() {
			res = append(res, device.ID)
		}
	}
	return res
}

// getUnhealthyGPUs 从节点注解中解析故障 GPU ID 列表。
//
// 注解 volcano.sh/gpu-unhealthy-ids 的值为逗号分隔的整数，
// 例如 "1,3"。解析失败的 ID 会打印警告并跳过。
func getUnhealthyGPUs(gs *GPUDevices, node *v1.Node) (unhealthyGPUs []int) {
	unhealthyGPUs = []int{}
	devicesStr, ok := node.Annotations[UnhealthyGPUIDs]

	if !ok {
		return
	}

	idsStr := strings.Split(devicesStr, ",")
	for _, sid := range idsStr {
		id, err := strconv.Atoi(sid)
		if err != nil {
			klog.Warningf("Failed to parse unhealthy gpu id %s due to %v", sid, err)
		} else {
			unhealthyGPUs = append(unhealthyGPUs, id)
		}
	}
	return
}

// GetGPUIndex 从 Pod 注解中解析出分配的 GPU ID 列表。
//
// 注解 volcano.sh/gpu-index 格式为逗号分隔整数，例如 "0,2"。
// 若 Pod 没有注解或格式非法，返回 nil。
func GetGPUIndex(pod *v1.Pod) []int {
	if len(pod.Annotations) == 0 {
		return nil
	}

	value, found := pod.Annotations[GPUIndex]
	if !found {
		return nil
	}

	ids := strings.Split(value, ",")
	if len(ids) == 0 {
		klog.Errorf("invalid gpu index annotation %s=%s", GPUIndex, value)
		return nil
	}

	idSlice := make([]int, len(ids))
	for idx, id := range ids {
		j, err := strconv.Atoi(id)
		if err != nil {
			klog.Errorf("invalid %s=%s", GPUIndex, value)
			return nil
		}
		idSlice[idx] = j
	}
	return idSlice
}

// checkNodeGPUSharingPredicate 检查请求 gpu-memory 的 Pod 是否能放入节点。
//
// 若 Pod 未请求 gpu-memory，直接返回 true。
// 否则调用 predicateGPUbyMemory，只要存在任意一块 GPU 的空闲显存 >= 请求量，
// 即认为节点满足条件。
func checkNodeGPUSharingPredicate(pod *v1.Pod, gs *GPUDevices) (bool, error) {
	// no gpu sharing request
	if getGPUMemoryOfPod(pod) <= 0 {
		return true, nil
	}
	ids := predicateGPUbyMemory(pod, gs)
	if len(ids) == 0 {
		return false, fmt.Errorf("no enough gpu memory on node %s", gs.Name)
	}
	return true, nil
}

// checkNodeGPUNumberPredicate 检查请求 gpu-number 的 Pod 是否能放入节点。
//
// 若 Pod 未请求 gpu-number，直接返回 true。
// 否则调用 predicateGPUbyNumber，要求节点上空闲整卡数量 >= 请求数量。
func checkNodeGPUNumberPredicate(pod *v1.Pod, gs *GPUDevices) (bool, error) {
	//no gpu number request
	if getGPUNumberOfPod(pod) <= 0 {
		return true, nil
	}
	ids := predicateGPUbyNumber(pod, gs)
	if len(ids) == 0 {
		return false, fmt.Errorf("no enough gpu number on node %s", gs.Name)
	}
	return true, nil
}

// predicateGPUbyMemory 筛选出空闲显存足以容纳该 Pod 的 GPU ID 列表，并按 ID 升序排列。
//
// 实际案例：
// 节点 2 块 GPU，空闲显存分别为 {0: 1024, 1: 8192}，Pod 请求 2048MiB。
// 则只有 ID=1 满足条件，返回 [1]。
func predicateGPUbyMemory(pod *v1.Pod, gs *GPUDevices) []int {
	gpuRequest := getGPUMemoryOfPod(pod)
	allocatableGPUs := getDevicesIdleGPUMemory(gs)

	var devIDs []int

	for devID := range allocatableGPUs {
		if availableGPU, ok := allocatableGPUs[devID]; ok && availableGPU >= gpuRequest {
			devIDs = append(devIDs, devID)
		}
	}
	sort.Ints(devIDs)
	return devIDs
}

// predicateGPUbyNumber 返回能够满足 Pod 整卡数量请求的空闲 GPU ID 列表。
//
// 若空闲 GPU 数量不足，返回 nil；否则返回前 gpuRequest 个空闲 GPU ID（已按 ID 升序）。
//
// 实际案例：
// 节点有 4 块 GPU，ID=0、2 被占用，空闲为 [1,3]，Pod 请求 1 张卡，返回 [1]。
// 若 Pod 请求 3 张卡，因空闲只有 2 张，返回 nil。
func predicateGPUbyNumber(pod *v1.Pod, gs *GPUDevices) []int {
	gpuRequest := getGPUNumberOfPod(pod)
	allocatableGPUs := getDevicesIdleGPUs(gs)

	if len(allocatableGPUs) < gpuRequest {
		klog.Errorf("Not enough gpu cards")
		return nil
	}

	return allocatableGPUs[:gpuRequest]
}

// escapeJSONPointer 对字符串进行 RFC 6901 JSON Pointer 转义。
//
// JSON Patch 中的路径若包含 / 或 ~ 需要转义：
//   - ~ 替换为 ~0
//   - / 替换为 ~1
//
// 例如注解 key "volcano.sh/gpu-index" 中的 / 需要转义，
// 否则生成的 Patch 路径不合法。
func escapeJSONPointer(p string) string {
	// Escaping reference name using https://tools.ietf.org/html/rfc6901
	p = strings.Replace(p, "~", "~0", -1)
	p = strings.Replace(p, "/", "~1", -1)
	return p
}

// AddGPUIndexPatch 构造一个 JSON Patch，用于向 Pod 写入 GPU 分配结果。
//
// Patch 包含两个 add 操作：
//   1. 在 /metadata/annotations/volcano.sh~1predicate-time 写入当前时间戳（UnixNano）。
//   2. 在 /metadata/annotations/volcano.sh~1gpu-index 写入分配的 GPU ID 列表字符串。
//
// 例如 ids=[0,2]，生成的 value 为 "0,2"。
func AddGPUIndexPatch(ids []int) string {
	idsstring := strings.Trim(strings.Replace(fmt.Sprint(ids), " ", ",", -1), "[]")
	return fmt.Sprintf(`[{"op": "add", "path": "/metadata/annotations/%s", "value":"%d"},`+
		`{"op": "add", "path": "/metadata/annotations/%s", "value": "%s"}]`,
		escapeJSONPointer(PredicateTime), time.Now().UnixNano(),
		escapeJSONPointer(GPUIndex), idsstring)
}

// RemoveGPUIndexPatch 构造一个 JSON Patch，用于清除 Pod 上的 GPU 分配注解。
//
// 主要用于抢占、回滚或释放资源时，将 gpu-index 和 predicate-time 移除。
func RemoveGPUIndexPatch() string {
	return fmt.Sprintf(`[{"op": "remove", "path": "/metadata/annotations/%s"},`+
		`{"op": "remove", "path": "/metadata/annotations/%s"}]`, escapeJSONPointer(PredicateTime), escapeJSONPointer(GPUIndex))
}

// getUsedGPUMemory 计算该 GPU 上已被占用的显存总量。
//
// 遍历 PodMap，跳过状态为 Succeeded 或 Failed 的终态 Pod，
// 将其 gpu-memory 请求累加，得到当前已被占用的显存。
//
// 为什么跳过终态 Pod：
// 终态 Pod 不再实际占用 GPU 资源，但在调度缓存中可能仍短暂存在，
// 跳过可避免重复计算、错误地认为显存已被占满。
func (g *GPUDevice) getUsedGPUMemory() uint {
	res := uint(0)
	for _, pod := range g.PodMap {
		if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			continue
		} else {
			gpuRequest := getGPUMemoryOfPod(pod)
			res += gpuRequest
		}
	}
	return res
}

// isIdleGPU 判断该 GPU 当前是否没有任何 Pod 占用。
func (g *GPUDevice) isIdleGPU() bool {
	return len(g.PodMap) == 0
}

// getGPUMemoryOfPod 计算 Pod 对 GPU 显存的总需求。
//
// 计算规则：
//   - 对于普通容器（Containers），显存需求为所有容器 gpu-memory 请求之和。
//   - 对于 Init 容器，由于其按顺序执行，只需取最大单个 Init 容器的 gpu-memory 请求。
//   - 最终取两者中的较大值。
//
// 实际案例：
// Pod 有两个容器分别请求 1Gi 和 3Gi，两个 Init 容器分别请求 1Gi 和 2Gi。
// 则普通容器总和为 4Gi，Init 容器最大为 2Gi，最终返回 4Gi。
func getGPUMemoryOfPod(pod *v1.Pod) uint {
	var initMem uint
	for _, container := range pod.Spec.InitContainers {
		res := getGPUMemoryOfContainer(container.Resources)
		if initMem < res {
			initMem = res
		}
	}

	var mem uint
	for _, container := range pod.Spec.Containers {
		mem += getGPUMemoryOfContainer(container.Resources)
	}

	if mem > initMem {
		return mem
	}
	return initMem
}

// getGPUMemoryOfContainer 从容器的 Resources.Limits 中提取 gpu-memory 请求量。
//
// 使用 resource.Quantity.Value() 获取整数值（单位 MiB）。
// 若容器未设置 volcano.sh/gpu-memory limit，返回 0。
func getGPUMemoryOfContainer(resources v1.ResourceRequirements) uint {
	var mem uint
	if val, ok := resources.Limits[VolcanoGPUResource]; ok {
		mem = uint(val.Value())
	}
	return mem
}

// getGPUNumberOfPod 计算 Pod 请求的整卡 GPU 数量。
//
// 计算规则与 getGPUMemoryOfPod 相同：
//   - 普通容器 gpu-number 求和。
//   - Init 容器取最大值。
//   - 返回两者较大值。
func getGPUNumberOfPod(pod *v1.Pod) int {
	var gpus int
	for _, container := range pod.Spec.Containers {
		gpus += getGPUNumberOfContainer(container.Resources)
	}

	var initGPUs int
	for _, container := range pod.Spec.InitContainers {
		res := getGPUNumberOfContainer(container.Resources)
		if initGPUs < res {
			initGPUs = res
		}
	}

	if gpus > initGPUs {
		return gpus
	}
	return initGPUs
}

// getGPUNumberOfContainer 从容器的 Resources.Limits 中提取 gpu-number 请求量。
//
// 若容器未设置 volcano.sh/gpu-number limit，返回 0。
func getGPUNumberOfContainer(resources v1.ResourceRequirements) int {
	var gpus int
	if val, ok := resources.Limits[VolcanoGPUNumber]; ok {
		gpus = int(val.Value())
	}
	return gpus
}
