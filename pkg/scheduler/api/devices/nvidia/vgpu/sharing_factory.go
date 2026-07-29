/*
Copyright 2025 Volcano Authors.

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
// sharing_factory.go  —— vGPU 共享策略的工厂接口与注册表
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 与 gpushare 的核心区别：                                                  ║
// ║                                                                          ║
// ║  gpushare 没有策略抽象，所有逻辑直接写在 share.go 中：                   ║
// ║    - predicateGPUbyMemory: 找空闲显存 >= 请求的 GPU                    ║
// ║    - predicateGPUbyNumber: 找完全空闲的整卡                              ║
// ║    - 分配/释放直接操作 PodMap                                           ║
// ║                                                                          ║
// ║  vgpu 通过 SharingFactory 接口解耦三种共享模式：                       ║
// ║    - HAMICoreFactory: 软件时间切片（TryAddPod/AddPod/SubPod）         ║
// ║    - MIGFactory:      硬件级切分（管理 MigTemplate + MigUsage）      ║
// ║    - MPSFactory:      多进程服务（预留）                              ║
// ║                                                                          ║
// ║  这种设计使得新增共享模式时只需实现接口 + 注册，无需修改核心调度逻辑  ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
// ──────────────────────────────────────────────────────────────────────────────

// SharingFactory 是 vGPU 共享策略的抽象接口。
//
// 与 gpushare 对比：
//   - gpushare 没有策略抽象，分配/释放逻辑直接硬编码在 share.go 中
//   - vgpu 通过该接口解耦，使得不同共享模式有独立的资源管理逻辑：
//     TryAddPod: predicate 阶段模拟分配（不修改 PodMap）
//     AddPod:    真正将 Pod 加入设备，更新 UsedXxx 和 PodMap
//     SubPod:    将 Pod 从设备中移除，回收 UsedXxx 和 PodMap
type SharingFactory interface {
	// TryAddPod 在 predicate 阶段尝试将 Pod 加入设备，但不真正修改 PodMap。
	// 返回是否可容纳，以及分配到的设备 UUID（MIG 模式下可能是 MIG 实例 ID）。
	//
	// 与 gpushare 对比：gpushare 的 predicateGPUbyMemory 只做显存检查，
	// 而 TryAddPod 可能还包含 MIG 几何模板匹配等复杂逻辑
	TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string)
	// AddPod 真正将 Pod 加入设备，修改 UsedNum、UsedMem、UsedCore 以及 PodMap。
	//
	// 与 gpushare 对比：gpushare 的 AddResource 仅将 *v1.Pod 放入 PodMap，
	// 而 AddPod 还会更新 UsedNum/UsedMem/UsedCore 等实时计数器
	AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error
	// SubPod 将 Pod 从设备中移除，回收资源。
	//
	// 与 gpushare 对比：gpushare 的 SubResource 仅从 PodMap 中 delete，
	// 而 SubPod 还会回收 UsedNum/UsedMem/UsedCore 并更新 metrics
	SubPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error
}

// sharingRegistry 是共享策略的注册表。
// key 为模式名称（hami-core、mig、mps），value 为对应的工厂实现。
var sharingRegistry = make(map[string]SharingFactory)

// RegisterFactory 注册一种 vGPU 共享策略。
//
// 由各个共享模式包的 init 函数调用，例如 hamicore.go 和 mig.go 中的 init。
func RegisterFactory(mode string, factory SharingFactory) {
	sharingRegistry[mode] = factory
}

// GetSharingHandler 根据模式名称获取对应的共享策略实现。
//
// 若该模式未注册，返回 false。
func GetSharingHandler(sharingMode string) (SharingFactory, bool) {
	s, ok := sharingRegistry[sharingMode]
	return s, ok
}
