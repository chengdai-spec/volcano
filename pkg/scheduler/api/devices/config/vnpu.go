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

package config

// Template 定义了一种 VNPU（虚拟 NPU）的切分模板
//
// 每种 Template 代表将一个物理 NPU 芯片虚拟化为不同规格子设备的方案，
// 用户可以在 Pod 中通过指定模板名称来申请对应规格的虚拟 NPU 资源。
//
// 例如：
//   - vir05_1c_16g：5 个 AI Core、1 个 AI CPU、16GB 显存
//   - vir10_3c_32g：10 个 AI Core、3 个 AI CPU、32GB 显存
type Template struct {
	// Name 模板名称，用于在 Pod annotation/resource 中引用此虚拟规格
	// 例如 "vir01"、"vir04"、"vir05_1c_16g" 等
	Name string `yaml:"name"`

	// Memory 该模板分配的显存大小，单位为 MiB
	Memory int64 `yaml:"memory"`

	// AICore 该模板分配的 AI Core 数量（昇腾芯片的算力核心）
	// omitempty 表示如果模板未指定则使用物理芯片的默认值
	AICore int32 `yaml:"aiCore,omitempty"`

	// AICPU 该模板分配的 AI CPU 数量（昇腾芯片的通用计算核心）
	// omitempty 表示如果模板未指定则使用物理芯片的默认值
	AICPU int32 `yaml:"aiCPU,omitempty"`
}

// VNPUConfig 定义了一种昇腾 NPU 芯片型号的完整调度配置
//
// 每种芯片型号(如 Ascend910B3、Ascend310P)对应一个 VNPUConfig 条目，
// 由 ConfigMap 中的 vnpus 列表加载，供 HAMi VNPU 设备插件在注册和调度时使用。
//
// 配置示例(YAML)：
//
//	vnpus:
//	- chipName: 910B3
//	  commonWord: Ascend910B3
//	  resourceName: huawei.com/Ascend910B3
//	  resourceMemoryName: huawei.com/Ascend910B3-memory
//	  memoryAllocatable: 65536
//	  memoryCapacity: 65536
//	  aiCore: 20
//	  aiCPU: 7
//	  templates:
//	    - name: vir05_1c_16g
//	      memory: 16384
//	      aiCore: 5
//	      aiCPU: 1
type VNPUConfig struct {
	// CommonWord 设备通用名称，用于在 Volcano 全局设备列表中注册和标识该芯片型号
	// 例如 "Ascend910B3"、"Ascend310P"
	// 该值会作为 api.RegisterDevice() 的参数，也是节点 Others map 中的 key
	CommonWord string `yaml:"commonWord"`

	// ChipName 芯片型号名称，用于与节点上报的设备信息进行匹配
	// 例如 "910B3"、"310P3"
	ChipName string `yaml:"chipName"`

	// ResourceName 该设备在 Kubernetes 扩展资源中的名称
	// 例如 "huawei.com/Ascend910B3"，Pod 通过该名称申请 NPU 设备
	ResourceName string `yaml:"resourceName"`

	// ResourceMemoryName 该设备显存在 Kubernetes 扩展资源中的名称
	// 例如 "huawei.com/Ascend910B3-memory"，用于申请指定大小的 NPU 显存
	ResourceMemoryName string `yaml:"resourceMemoryName"`

	// MemoryAllocatable 该芯片可分配的显存总量，单位为 MiB
	// 即可用于 Pod 调度的显存上限（扣除系统保留后）
	MemoryAllocatable int64 `yaml:"memoryAllocatable"`

	// MemoryCapacity 该芯片的物理显存总量，单位为 MiB
	MemoryCapacity int64 `yaml:"memoryCapacity"`

	// AICore 该芯片的 AI Core 总数(昇腾芯片专用的矩阵计算核心)
	AICore int32 `yaml:"aiCore"`

	// AICPU 该芯片的 AI CPU 总数(昇腾芯片的通用计算核心)
	AICPU int32 `yaml:"aiCPU"`

	// Templates 该芯片支持的所有虚拟切分模板列表
	// 每个模板定义了一种虚拟 NPU 规格，用户可以按需选择
	Templates []Template `yaml:"templates"`
}
