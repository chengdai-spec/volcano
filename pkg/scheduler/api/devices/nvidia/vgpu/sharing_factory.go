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

// SharingFactory 是 vGPU 共享策略的抽象接口。
//
// vGPU 支持多种共享模式：hami-core（时间切片）、mig（硬件切分）、mps（多进程服务）。
// 不同模式对“能否将一个新 Pod 加入某块 GPU”以及“如何维护 PodMap”有不同的实现，
// 因此通过该工厂接口进行解耦。
type SharingFactory interface {
	// TryAddPod 在 predicate 阶段尝试将 Pod 加入设备，但不真正修改 PodMap。
	// 返回是否可容纳，以及分配到的设备 UUID（MIG 模式下可能是 MIG 实例 ID）。
	TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string)
	// AddPod 真正将 Pod 加入设备，修改 UsedNum、UsedMem、UsedCore 以及 PodMap。
	AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error
	// SubPod 将 Pod 从设备中移除，回收资源。
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
