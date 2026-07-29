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

// ──────────────────────────────────────────────────────────────────────────────
// gpushare —— Volcano 原生轻量级 GPU 共享方案
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 与 vgpu 方案的核心区别：                                                  ║
// ║                                                                          ║
// ║  gpushare（本包）              vgpu（兄弟包）                              ║
// ║  ─────────────────────         ─────────────────────────                 ║
// ║  注册名: "GpuShare"            注册名: "hamivgpu"                        ║
// ║  设备来源: 节点 Capacity 推算   设备来源: HAMi 设备插件上报               ║
// ║  GPU 感知: 逻辑抽象(无UUID)     GPU 感知: 物理真实(UUID/型号/健康)        ║
// ║  共享模式: 单一(显存/卡数)      共享模式: hami-core / MIG / MPS          ║
// ║  调度策略: 无(固定最小ID)       调度策略: binpack / spread               ║
// ║  打分能力: 固定返回 0           打分能力: FilterNode 阶段计算             ║
// ║  运行时隔离: 无                 运行时隔离: HAMi CUDA Hook               ║
// ║  资源维度: 显存 或 卡数         资源维度: 显存 + 核心% + 槽位数          ║
// ║  Pod追踪: map[string]*Pod      Pod追踪: map[string]*GPUUsage            ║
// ║                                                                          ║
// ║ 适用场景: 不依赖外部设备插件的简单 GPU 共享/独占场景                       ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
//
// 调用链路：
//   deviceshare.OnSessionOpen
//     → NewGPUDevices(name, node)           // 从节点 Capacity 构建逻辑 GPU 视图
//     → AddResource(pod) / SubResource(pod) // 恢复/释放 Pod 占用
//     → FilterNode(pod, policy)             // predicate 阶段检查显存/卡数
//     → Allocate(kubeClient, pod)           // 分配 GPU + Patch 注解
//
// 注解协议：
//   写入 Pod: volcano.sh/gpu-index="0,2"    // 分配的 GPU ID 列表
//   写入 Pod: volcano.sh/predicate-time     // 分配时间戳
//   读取 Node: volcano.sh/gpu-unhealthy-ids // 故障 GPU ID 列表
// ──────────────────────────────────────────────────────────────────────────────

// 全局开关变量，由 deviceshare 插件根据调度器配置初始化。
//
// 与 vgpu 方案的对比：
//   - gpushare 有三个独立开关（GpuSharingEnable / GpuNumberEnable / NodeLockEnable）
//   - vgpu 只有一个总开关 VGPUEnable + NodeLockEnable
//   - gpushare 的“共享”和“整卡”是两种独立模式，分别由不同开关控制
//   - vgpu 统一通过 vgpu-number + vgpu-memory + vgpu-cores 三维资源请求来描述
var GpuSharingEnable bool  // 是否启用按显存共享 GPU（volcano.sh/gpu-memory）
var NodeLockEnable bool    // 分配 GPU 时是否对节点加分布式锁，防止并发分配冲突
var GpuNumberEnable bool   // 是否启用按卡数分配 GPU（volcano.sh/gpu-number）

const (
	// DeviceName 是 gpushare 设备在 Volcano 调度器内部的注册名称。
	//
	// 与 vgpu 对比：
	//   gpushare: DeviceName = "GpuShare"
	//   vgpu:     DeviceName = "hamivgpu"（历史原因，表示与 HAMi 设备插件配合）
	//
	// 在 deviceshare 插件中通过 api.RegisterDevice(DeviceName) 注册。
	DeviceName = "GpuShare"

	// VolcanoGPUResource 是扩展资源名，表示 Pod 请求的 GPU 显存（单位：MiB）。
	//
	// 与 vgpu 对比：
	//   gpushare 使用 "volcano.sh/gpu-memory"  —— 仅按显存分配
	//   vgpu 使用配置化的 ResourceMemoryName（如 "volcano.sh/vgpu-memory"）
	//         并额外支持 vgpu-cores（算力百分比）和 vgpu-number（设备数量）
	//
	// 使用方式示例：
	//   limits:
	//     volcano.sh/gpu-memory: "2048"
	// 调度器会据此判断节点上是否有足够的空闲显存。
	VolcanoGPUResource = "volcano.sh/gpu-memory"

	// VolcanoGPUNumber 是扩展资源名，表示 Pod 请求的 GPU 卡数量。
	//
	// 与 vgpu 对比：
	//   gpushare: gpu-number 分配的是完全空闲的整卡（PodMap 为空的 GPU）
	//   vgpu:     vgpu-number 分配的是"槽位"，一张卡可被多个 Pod 共享
	//
	// 使用方式示例：
	//   limits:
	//     volcano.sh/gpu-number: "2"
	// 调度器会分配 2 张空闲的完整 GPU 卡给该 Pod。
	VolcanoGPUNumber = "volcano.sh/gpu-number"

	// PredicateTime 用于在 Pod 注解中记录本次 GPU 分配 predicate 的时间戳（UnixNano）。
	// 主要配合 GPUIndex 一起写入，便于排查分配时序问题。
	//
	// 注意：vgpu 方案也有同名常量，但注解值格式不同：
	//   gpushare: UnixNano 精度
	//   vgpu:     Unix 秒精度
	PredicateTime = "volcano.sh/predicate-time"

	// GPUIndex 用于在 Pod 注解中记录最终分配的 GPU 设备索引列表。
	// 格式为逗号分隔的整数，例如 "0,2" 表示分配到该节点的第 0 块和第 2 块 GPU。
	// 该注解由调度器在 Allocate 阶段通过 JSON Patch 写入 Pod。
	//
	// 与 vgpu 注解对比：
	//   gpushare: gpu-index = "0,2"           （仅记录 ID）
	//   vgpu:     vgpu-ids-new = "UUID,NVIDIA,2048,50:..." （记录 UUID + 类型 + 显存 + 核心）
	GPUIndex = "volcano.sh/gpu-index"

	// UnhealthyGPUIDs 是节点注解的 key，用于声明该节点上哪些 GPU 设备不可用。
	// 值为逗号分隔的 GPU ID，例如 "1,3"。
	// NewGPUDevices 构建节点 GPU 视图时会将这些设备剔除，避免调度到故障卡上。
	//
	// 与 vgpu 对比：
	//   gpushare: 通过节点注解手动维护故障 GPU 列表
	//   vgpu:     从 HAMi 设备插件上报的 health 字段自动获取每块 GPU 的健康状态
	UnhealthyGPUIDs = "volcano.sh/gpu-unhealthy-ids"
)
