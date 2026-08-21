/*
Copyright 2025 The Volcano Authors.

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

package vnpu310p

import (
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	"volcano.sh/volcano/third_party/mindcluster/common/util"
)

// ScoreBatchNodes 对一批候选节点进行批量打分，用于 Ascend310P 动态 vNPU 调度场景下的节点选择。
//
// 打分策略的核心思想是"紧凑放置 + 降级感知"：
//   - 紧凑放置：优先将 Pod 调度到空闲 AI Core 最少的节点，提高单节点资源利用率；
//   - 降级感知：如果 Pod 在某些节点上曾触发过 AI CPU 降级（DowngradeCache），
//     则尽量避开这些节点，优先选择一个未降级过的节点并给予更高分。
//
// 参数：
//   - pod:              待调度的 Pod；
//   - schedulePolicy:   调度策略名称（当前未使用，预留扩展）；
//   - device:           触发打分的设备（当前未使用，由 neighbours 提供实际数据）；
//   - neighbours:       所有候选节点的设备信息列表（每个元素为 *vnpu.NPUDevices）。
//
// 返回：与 neighbours 等长的分数数组，scores[i] 对应 neighbours[i] 的得分。
//
// 步骤：
//  1. 遍历所有候选节点，提取 NodeInf 并收集 Pod 的降级记录；
//  2. 初始化所有节点分数为 0，按空闲 AI Core 升序排列节点；
//  3. 根据是否存在降级记录，分两种情况给节点加分；
//  4. 将 scoreMap 转换为与 neighbours 顺序对齐的分数数组并返回。
func ScoreBatchNodes(pod *v1.Pod, schedulePolicy string, device api.Devices, neighbours []api.Devices) []float64 {
	// 步骤 1：遍历所有候选节点，提取 NodeInf 并收集 Pod 的降级记录。
	//
	// neighbourNodeInfs: 所有候选节点的 NodeInf 指针列表，用于后续排序和打分。
	// podDowngradeCache: 记录哪些节点上曾经对该 Pod 执行过 AI CPU 降级。
	//   如果某节点的 DowngradeCache 中包含当前 Pod 名称，说明该节点之前尝试调度
	//   这个 Pod 时因资源不足触发了降级（AI CPU 从 2 降到 1 等），需要记录在案。
	var neighbourNodeInfs []*vnpu.NodeInf
	var podDowngradeCache []string
	for _, neighbour := range neighbours {
		if npuNeighbour, ok := neighbour.(*vnpu.NPUDevices); ok {
			// 提取节点资源视图信息，用于后续按空闲资源排序
			neighbourNodeInfs = append(neighbourNodeInfs, &npuNeighbour.NodeInf)
			// 检查该节点的降级缓存中是否记录了这个 Pod
			// DowngradeCache 的 key 是 Pod 名称，表示该 Pod 在此节点上曾触发降级
			_, ok := npuNeighbour.DowngradeCache[pod.Name]
			if ok {
				podDowngradeCache = append(podDowngradeCache, npuNeighbour.NodeInf.Name)
			}
		}
	}

	// 步骤 2：初始化打分表并按空闲资源排序

	// 初始化所有节点的分数为 0.0，key 为节点名称
	scoreMap := initScoreMap(neighbourNodeInfs)

	// 按节点空闲 AI Core 数量升序排列（空闲越少的节点排在越前面）
	// 排序目的：优先选择空闲资源最紧凑的节点，实现资源紧凑放置
	nodesSorted := orderVNodesByFreeResource(neighbourNodeInfs)
	if len(nodesSorted) == 0 {
		klog.V(util.LogErrorLev).Infof("dynamic vnpu task<%s> ScoreBestNPUNodes err: sorted nodes len 0", pod.Name)
		// 无有效节点，返回与 neighbours 等长的全零数组。
		return make([]float64, len(neighbours))
	}

	// 步骤 3：根据降级记录给节点加分。
	//
	// 加分规则（两种情况）：
	//
	// 情况 A（无降级记录）：
	//   给空闲资源最少的节点（nodesSorted[0]）加 8 分。
	//   效果：该 Pod 会被优先调度到资源最紧凑的节点，提高利用率。
	//
	// 情况 B（有降级记录）：
	//   遍历排序后的节点列表：
	//   - 如果节点在 podDowngradeCache 中（曾对此 Pod 降级过）：加 8 分（较低分）；
	//   - 如果节点不在 podDowngradeCache 中（未降级过）：加 16 分（8×2，较高分），然后 break。
	//   效果：优先选择一个从未对此 Pod 降级过的节点，给予更高分以引导调度。
	//         对于已经降级过的节点，虽然也给分但分数更低，排在后面。
	if len(podDowngradeCache) == 0 {
		// 情况 A：没有任何节点对此 Pod 触发过降级，
		// 直接给空闲资源最少的节点加 NPUIndex8(8) 分。
		_, sOK := scoreMap[nodesSorted[0].Name]
		if !sOK {
			scoreMap[nodesSorted[0].Name] = 0.0
		}
		scoreMap[nodesSorted[0].Name] += util.NPUIndex8
	} else {
		// 情况 B：存在曾对此 Pod 降级的节点，需要区分对待。
		for _, node := range nodesSorted {
			// 检查当前节点是否在降级记录中。
			downgradeFlag := false
			for _, dNode := range podDowngradeCache {
				if node.Name == dNode {
					downgradeFlag = true
					break
				}
			}
			if !downgradeFlag {
				// 找到一个未降级过的节点，给予更高分 NPUIndex8*NPUIndex2(16)，
				// 然后立即 break，只给第一个符合条件的节点加分。
				scoreMap[node.Name] += util.NPUIndex8 * util.NPUIndex2
				break
			}
			// 当前节点曾触发过降级，给予较低分 NPUIndex8(8) 作为保底。
			scoreMap[node.Name] += util.NPUIndex8
		}
	}

	// 步骤 4：将 scoreMap（按节点名称索引）转换为与 neighbours 顺序对齐的分数数组。
	// 非 NPUDevices 类型的 neighbour 对应位置保持 0 分。
	scores := make([]float64, len(neighbours))
	for i, neighbour := range neighbours {
		if npuNeighbour, ok := neighbour.(*vnpu.NPUDevices); ok {
			if score, exists := scoreMap[npuNeighbour.NodeInf.Name]; exists {
				scores[i] = score
			}
		}
	}

	return scores
}

// initScoreMap 初始化节点打分表，将所有有效节点的分数设为 0.0。
//
// 遍历传入的节点列表，跳过 nil 节点，为每个有效节点创建一个
// key=节点名称、value=0.0 的条目。返回的 map 在 ScoreBatchNodes
// 中用于累加各轮打分结果。
func initScoreMap(nodes []*vnpu.NodeInf) map[string]float64 {
	scoreMap := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		scoreMap[node.Name] = 0.0
	}
	return scoreMap
}
