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

// 全局开关变量，由 deviceshare 插件根据调度器配置初始化。
// 这三个开关决定 gpushare 模块的工作模式：
//   - GpuSharingEnable：是否启用按显存共享 GPU（volcano.sh/gpu-memory）
//   - GpuNumberEnable：是否启用按卡数分配 GPU（volcano.sh/gpu-number）
//   - NodeLockEnable：分配 GPU 时是否对节点加分布式锁，防止并发分配冲突
var GpuSharingEnable bool
var NodeLockEnable bool
var GpuNumberEnable bool

const (
	// DeviceName 是 gpushare 设备在 Volcano 调度器内部的注册名称。
	// 在 deviceShare 插件中通过 api.RegisterDevice(DeviceName) 注册。
	DeviceName = "GpuShare"

	// VolcanoGPUResource 是扩展资源名，表示 Pod 请求的 GPU 显存（单位：MiB）。
	// 使用方式示例：
	//   limits:
	//     volcano.sh/gpu-memory: "2048"
	// 调度器会据此判断节点上是否有足够的空闲显存。
	VolcanoGPUResource = "volcano.sh/gpu-memory"

	// VolcanoGPUNumber 是扩展资源名，表示 Pod 请求的 GPU 卡数量。
	// 使用方式示例：
	//   limits:
	//     volcano.sh/gpu-number: "2"
	// 调度器会分配 2 张空闲的完整 GPU 卡给该 Pod。
	VolcanoGPUNumber = "volcano.sh/gpu-number"

	// PredicateTime 用于在 Pod 注解中记录本次 GPU 分配 predicate 的时间戳（UnixNano）。
	// 主要配合 GPUIndex 一起写入，便于排查分配时序问题。
	PredicateTime = "volcano.sh/predicate-time"

	// GPUIndex 用于在 Pod 注解中记录最终分配的 GPU 设备索引列表。
	// 格式为逗号分隔的整数，例如 "0,2" 表示分配到该节点的第 0 块和第 2 块 GPU。
	// 该注解由调度器在 Allocate 阶段通过 JSON Patch 写入 Pod。
	GPUIndex = "volcano.sh/gpu-index"

	// UnhealthyGPUIDs 是节点注解的 key，用于声明该节点上哪些 GPU 设备不可用。
	// 值为逗号分隔的 GPU ID，例如 "1,3"。
	// NewGPUDevices 构建节点 GPU 视图时会将这些设备剔除，避免调度到故障卡上。
	UnhealthyGPUIDs = "volcano.sh/gpu-unhealthy-ids"
)
