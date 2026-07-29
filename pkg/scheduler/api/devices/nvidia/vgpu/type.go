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
// vgpu —— HAMi 生态 GPU 虚拟化方案（支持 hami-core / MIG / MPS）
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 与 gpushare 方案的核心区别：                                                 ║
// ║                                                                           ║
// ║  vgpu（本包）                  gpushare（兄弟包）                            ║
// ║  ─────────────────────         ─────────────────────────                  ║
// ║  注册名: "hamivgpu"           注册名: "GpuShare"                            ║
// ║  设备来源: HAMi 插件上报       设备来源: 节点 Capacity 推算                     ║
// ║  GPU 感知: 物理真实            GPU 感知: 逻辑抽象(无UUID)                      ║
// ║  共享模式: 三种                共享模式: 单一(显存/卡数)                        ║
// ║  调度策略: binpack/spread     调度策略: 无(固定最小ID)                         ║
// ║  打分能力: 有                   打分能力: 无                                  ║
// ║  运行时: CUDA Hook 隔离       运行时: 无隔离                                  ║
// ║  资源维度: 3维(显存+核心+槽)  资源维度: 1维(显存或卡数)                           ║
// ║  Pod追踪: GPUUsage 结构体     Pod追踪: *v1.Pod 指针                           ║
// ║                                                                           ║
// ║ 适用场景: 生产级 GPU 共享，需要精细资源控制和运行时隔离                            ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
//
// 三种共享模式：
//   hami-core: 软件时间切片，通过 UsedNum 限制最大共享 Pod 数
//   MIG:       NVIDIA 硬件级切分（A100/H100），每个实例有独立显存/核心/缓存
//   MPS:       多进程服务共享（预留，尚未完整实现）
//
// 调用链路：
//   deviceshare.OnSessionOpen
//     → NewGPUDevices(name, node)           // 从 HAMi 上报的注解构建物理 GPU 视图
//     → AddResource(pod) / SubResource(pod) // 通过 Sharing 工厂恢复/释放 Pod
//     → FilterNode(pod, policy)             // 模拟分配 + 打分 + 缓存
//     → ScoreNode(pod, policy)              // 返回缓存的打分
//     → Allocate(kubeClient, pod)           // 分配 GPU + Patch 6 个注解
//
// 注解协议：
//   写入 Pod: volcano.sh/vgpu-node           // 分配到的节点名
//   写入 Pod: volcano.sh/vgpu-time           // 分配时间戳
//   写入 Pod: volcano.sh/vgpu-ids-new        // UUID,NVIDIA,mem,cores 编码
//   写入 Pod: volcano.sh/devices-to-allocate // 同上，供 device plugin 识别
//   写入 Pod: volcano.sh/bind-phase          // 绑定阶段
//   写入 Pod: volcano.sh/bind-time           // 绑定时间戳
//   读取 Node: volcano.sh/vgpu-register      // HAMi 上报的物理 GPU 信息
//   读取 Pod:  volcano.sh/vgpu-mode          // 期望的共享模式
//   读取 Pod:  nvidia.com/use-gputype        // GPU 型号白名单
//   读取 Pod:  nvidia.com/nouse-gputype      // GPU 型号黑名单
// ──────────────────────────────────────────────────────────────────────────────

const (
	// DeviceName 是 vgpu 设备在 Volcano 调度器内部的注册名称。
	// 注意该名称历史原因写作 "hamivgpu"，表示与 HAMi 设备插件配合工作。
	//
	// 与 gpushare 对比：
	//   vgpu:     DeviceName = "hamivgpu"
	//   gpushare: DeviceName = "GpuShare"
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
	//
	// 与 gpushare 对比：gpushare 无策略概念，始终按 ID 升序选择第一个满足条件的 GPU
	binpackPolicy = "binpack"
	// spreadPolicy 是分散调度策略：优先把任务放到已用槽位少的 GPU 上，
	// 目标是让任务分散到不同 GPU，减少资源竞争。
	spreadPolicy = "spread"
	// DefaultMemPercentage = 101 表示没有显式设置显存百分比请求。

	DefaultMemPercentage = 101
	binpackMultiplier    = 100
	spreadMultiplier     = 100

	// GPUModeAnnotation 是 Pod 注解 key，用于指定该 Pod 期望的 GPU 共享模式
	// 可选值：hami-core、mig、mps。
	GPUModeAnnotation = "volcano.sh/vgpu-mode"
	// VGPUPodGroupPolicyAnnotation 是 PodGroup 分散策略注解 key
	VGPUPodGroupPolicyAnnotation = "volcano.sh/vgpu-podgroup-policy"
	// VGPUPodGroupPolicySpreadValue 表示开启 PodGroup 分散策略
	VGPUPodGroupPolicySpreadValue = "spread"
	// vGPUControllerHAMICore 表示 HAMi-core 时间切片共享模式
	vGPUControllerHAMICore = "hami-core"
	// vGPUControllerMIG 表示 NVIDIA MIG 硬件级切分模式
	vGPUControllerMIG = "mig"
	// vGPUControllerMPS 表示 NVIDIA MPS 多进程服务共享模式
	vGPUControllerMPS = "mps"
)

var (
	// VGPUEnable 是 vGPU 调度的全局开关，由 deviceshare 插件初始化。
	//
	// 与 gpushare 对比：
	//   vgpu 只有一个总开关 VGPUEnable
	//   gpushare 有两个独立开关 GpuSharingEnable / GpuNumberEnable
	VGPUEnable bool
	// NodeLockEnable 控制 Allocate 阶段是否对节点加分布式锁
	NodeLockEnable bool
	// SchedulePolicy 是默认调度策略(binpack 或 spread)
	SchedulePolicy string
)

// ContainerDevice 描述一个容器实际分配到的 vGPU 设备单元
//
// 与 gpushare 对比：
//   - gpushare 没有等价结构，仅通过 Pod 注解 gpu-index 记录分配的 GPU ID
//   - vgpu 通过 ContainerDevice 记录 UUID + Type + Usedmem + Usedcores
//     并编码到 Pod 注解 vgpu-ids-new 中，供 HAMi device plugin 识别
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
