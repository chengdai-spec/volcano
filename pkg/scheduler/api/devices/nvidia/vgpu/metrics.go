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

package vgpu

// ──────────────────────────────────────────────────────────────────────────────
// metrics.go  —— vgpu 的 Prometheus 监控指标定义与更新
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 与 gpushare 的核心区别：                                                  ║
// ║                                                                          ║
// ║  gpushare 没有监控指标，无法通过 Prometheus 观察 GPU 共享状态。          ║
// ║  vgpu 定义了 6 个 Prometheus 指标，实时跟踪每块 GPU 和每个 Pod 的        ║
// ║  共享数/已分配显存/已分配核心/总显存等状态。                           ║
// ║                                                                          ║
// ║  这些指标在 AddPodMetrics/SubPodMetrics/ResetDeviceMetrics 中更新，    ║
// ║  由 AddResource/SubResource 在资源变更时自动调用。                   ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
// ──────────────────────────────────────────────────────────────────────────────

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto" // auto-registry collectors in default registry
)

const (
	// VolcanoSubSystemName 是 Prometheus 指标的子系统名称。
	// 所有 vgpu 指标均以 "volcano_" 为前缀，例如 "volcano_vgpu_device_shared_number"
	VolcanoSubSystemName = "volcano"

	// OnSessionOpen 标签值，表示 Session 开始事件（预留）
	OnSessionOpen = "OnSessionOpen"

	// OnSessionClose 标签值，表示 Session 结束事件（预留）
	OnSessionClose = "OnSessionClose"
)

var (
	// VGPUDevicesSharedNumber 指标：每块 GPU 当前被多少个 vgpu Pod 共享。
	// 标签：devID（GPU UUID）、NodeName（节点名）
	VGPUDevicesSharedNumber = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "vgpu_device_shared_number",
			Help:      "The number of vgpu tasks sharing this card",
		},
		[]string{"devID", "NodeName"},
	)
	// VGPUDevicesAllocatedMemory 指标：每块 GPU 当前已分配的显存量（MiB）。
	VGPUDevicesAllocatedMemory = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "vgpu_device_allocated_memory",
			Help:      "The number of vgpu memory allocated in this card",
		},
		[]string{"devID", "NodeName"},
	)
	// VGPUDevicesAllocatedCores 指标：每块 GPU 当前已分配的核心数百分比（0-100）。
	VGPUDevicesAllocatedCores = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "vgpu_device_allocated_cores",
			Help:      "The percentage of gpu compute cores allocated in this card",
		},
		[]string{"devID", "NodeName"},
	)
	// VGPUDevicesMemoryTotal 指标：每块 GPU 的物理显存总量（MiB）。
	VGPUDevicesMemoryTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "vgpu_device_memory_limit",
			Help:      "The number of total device memory in this card",
		},
		[]string{"devID", "NodeName"},
	)
	// VGPUPodMemoryAllocated 指标：每个 Pod 在指定 GPU 上分配的显存量。
	// 标签：devID、NodeName、podName
	VGPUPodMemoryAllocated = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "vgpu_device_memory_allocation_for_a_certain_pod",
			Help:      "The vgpu device memory allocated for a certain pod",
		},
		[]string{"devID", "NodeName", "podName"},
	)
	// VGPUPodCoreAllocated 指标：每个 Pod 在指定 GPU 上分配的核心数百分比。
	VGPUPodCoreAllocated = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "vgpu_device_core_allocation_for_a_certain_pod",
			Help:      "The vgpu device core allocated for a certain pod",
		},
		[]string{"devID", "NodeName", "podName"},
	)
)

// GetStatus 返回设备状态字符串，vgpu 模式暂无实现，返回空串。
// 与 gpushare.GetStatus 相同，均返回空字符串。
func (gs *GPUDevices) GetStatus() string {
	return ""
}

// ResetDeviceMetrics 重置指定 GPU 的所有监控指标。
//
// 调用时机：NewGPUDevices 构建节点 GPU 视图时，对每块 GPU 调用。
// 重置内容：总显存设为物理容量，共享数/已分配显存/已分配核心均归零，
// 并删除该 GPU 上的 Pod 级别指标。
func ResetDeviceMetrics(UUID string, nodeName string, memory float64) {
	VGPUDevicesMemoryTotal.WithLabelValues(UUID, nodeName).Set(memory)
	VGPUDevicesSharedNumber.WithLabelValues(UUID, nodeName).Set(0)
	VGPUDevicesAllocatedCores.WithLabelValues(UUID, nodeName).Set(0)
	VGPUDevicesAllocatedMemory.WithLabelValues(UUID, nodeName).Set(0)

	VGPUPodMemoryAllocated.DeletePartialMatch(prometheus.Labels{"devID": UUID})
	VGPUPodCoreAllocated.DeletePartialMatch(prometheus.Labels{"devID": UUID})
}
// AddPodMetrics 在 Pod 被加入 GPU 后更新监控指标。
//
// 调用时机：AddResource 中 Sharing.AddPod 成功后调用。
// 更新内容：
//   - Pod 级别：该 Pod 在该 GPU 上的已分配显存和核心
//   - GPU 级别：该 GPU 的共享数 +1、已分配显存和核心更新
func (gs *GPUDevices) AddPodMetrics(index int, podUID, podName string) {
	UUID := gs.Device[index].UUID
	NodeName := gs.Device[index].Node
	usage := gs.Device[index].PodMap[podUID]
	if usage == nil {
		VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedNum))
		VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
		VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
		return
	}
	VGPUPodMemoryAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedMem))
	VGPUPodCoreAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedCore))
	VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Inc()
	VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
	VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
}

// SubPodMetrics 在 Pod 被从 GPU 移除后更新监控指标。
//
// 调用时机：SubResource 中 Sharing.SubPod 成功后调用。
// 更新内容：
//   - 若 Pod 仍在使用资源，更新 Pod 级别指标
//   - 若 Pod 已无资源使用（UsedMem=0），删除 Pod 级别指标并从 PodMap 中移除
//   - GPU 级别：共享数 -1、已分配显存和核心更新
func (gs *GPUDevices) SubPodMetrics(index int, podUID, podName string) {
	UUID := gs.Device[index].UUID
	NodeName := gs.Device[index].Node
	usage := gs.Device[index].PodMap[podUID]
	if usage == nil {
		VGPUPodMemoryAllocated.DeleteLabelValues(UUID, NodeName, podName)
		VGPUPodCoreAllocated.DeleteLabelValues(UUID, NodeName, podName)
		VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedNum))
		VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
		VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
		return
	}
	VGPUPodMemoryAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedMem))
	VGPUPodCoreAllocated.WithLabelValues(UUID, NodeName, podName).Set(float64(usage.UsedCore))
	if usage.UsedMem == 0 {
		delete(gs.Device[index].PodMap, podUID)
		VGPUPodMemoryAllocated.DeleteLabelValues(UUID, NodeName, podName)
		VGPUPodCoreAllocated.DeleteLabelValues(UUID, NodeName, podName)
	}
	VGPUDevicesSharedNumber.WithLabelValues(UUID, NodeName).Dec()
	VGPUDevicesAllocatedCores.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedCore))
	VGPUDevicesAllocatedMemory.WithLabelValues(UUID, NodeName).Set(float64(gs.Device[index].UsedMem))
}
