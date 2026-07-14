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

const (
	// DeviceName 是 vgpu 设备在 Volcano 调度器内部的注册名称。
	// 注意该名称历史原因写作 "hamivgpu"，表示与 HAMi 设备插件配合工作。
	DeviceName = "hamivgpu"

	// GPUInUse 是 Pod 注解 key，用于白名单方式指定可使用的 GPU 型号。
	// 例如：nvidia.com/use-gputype: "V100,A100"
	GPUInUse = "nvidia.com/use-gputype"
	// GPUNoUse 是 Pod 注解 key，用于黑名单方式排除特定 GPU 型号。
	// 例如：nvidia.com/nouse-gputype: "T4"
	GPUNoUse = "nvidia.com/nouse-gputype"

	// AssignedTimeAnnotations 记录 vGPU 分配时间戳。
	AssignedTimeAnnotations = "volcano.sh/vgpu-time"
	// AssignedIDsAnnotations 记录实际分配给 Pod 的 vGPU 设备信息。
	// 格式详见 utils.go 中的 encodePodDevices / DecodePodDevices。
	AssignedIDsAnnotations = "volcano.sh/vgpu-ids-new"
	// AssignedIDsToAllocateAnnotations 与 AssignedIDsAnnotations 内容相同，
	// 用于 device plugin 在绑核前识别需要挂载的设备。
	AssignedIDsToAllocateAnnotations = "volcano.sh/devices-to-allocate"
	// AssignedNodeAnnotations 记录 Pod 被分配到的节点名，用于防止重复分配。
	AssignedNodeAnnotations = "volcano.sh/vgpu-node"
	// BindTimeAnnotations 记录设备绑定时间戳。
	BindTimeAnnotations = "volcano.sh/bind-time"
	// DeviceBindPhase 记录设备绑定阶段，例如 "allocating"。
	DeviceBindPhase = "volcano.sh/bind-phase"

	// NvidiaGPUDevice 表示 NVIDIA GPU 设备类型。
	NvidiaGPUDevice = "NVIDIA"

	// PredicateTime 用于记录 predicate 时间戳。
	PredicateTime = "volcano.sh/predicate-time"
	// GPUIndex 用于记录 GPU 设备索引。
	GPUIndex = "volcano.sh/gpu-index"

	// UnhealthyGPUIDs 是节点注解 key，用于声明故障 GPU ID。
	UnhealthyGPUIDs = "volcano.sh/gpu-unhealthy-ids"

	// binpackPolicy 是紧凑调度策略：优先把任务放到已用显存多的 GPU 上，
	// 目标是让少数 GPU 尽量满载，从而留下整块空闲 GPU。
	binpackPolicy = "binpack"
	// spreadPolicy 是分散调度策略：优先把任务放到已用槽位少的 GPU 上，
	// 目标是让任务分散到不同 GPU，减少资源竞争。
	spreadPolicy = "spread"
	// DefaultMemPercentage = 101 表示没有显式设置显存百分比请求。

	DefaultMemPercentage = 101
	binpackMultiplier    = 100
	spreadMultiplier     = 100

	// GPUModeAnnotation 是 Pod 注解 key，用于指定该 Pod 期望的 GPU 共享模式。
	// 可选值：hami-core、mig、mps。
	GPUModeAnnotation = "volcano.sh/vgpu-mode"
	// VGPUPodGroupPolicyAnnotation 是 PodGroup 分散策略注解 key。
	VGPUPodGroupPolicyAnnotation = "volcano.sh/vgpu-podgroup-policy"
	// VGPUPodGroupPolicySpreadValue 表示开启 PodGroup 分散策略。
	VGPUPodGroupPolicySpreadValue = "spread"
	// vGPUControllerHAMICore 表示 HAMi-core 时间切片共享模式。
	vGPUControllerHAMICore = "hami-core"
	// vGPUControllerMIG 表示 NVIDIA MIG 硬件级切分模式。
	vGPUControllerMIG = "mig"
	// vGPUControllerMPS 表示 NVIDIA MPS 多进程服务共享模式。
	vGPUControllerMPS = "mps"
)

var (
	// VGPUEnable 是 vGPU 调度的全局开关，由 deviceshare 插件初始化。
	VGPUEnable bool
	// NodeLockEnable 控制 Allocate 阶段是否对节点加分布式锁。
	NodeLockEnable bool
	// SchedulePolicy 是默认调度策略（binpack 或 spread）。
	SchedulePolicy string
)

// ContainerDevice 描述一个容器实际分配到的 vGPU 设备单元。
type ContainerDevice struct {
	// UUID 是 GPU 或 MIG 实例的唯一标识。
	UUID string
	// Type 是设备类型，例如 NVIDIA、MLU。
	Type string
	// Usedmem 是该容器实际分配的显存量（单位 MiB）。
	Usedmem uint
	// Usedcores 是该容器实际分配的核心数百分比（0-100）。
	Usedcores uint
}

// ContainerDevices 是一个容器对应的设备分配列表。
// 当容器请求多张卡或多个 MIG 实例时，列表中会有多个元素。
type ContainerDevices []ContainerDevice
