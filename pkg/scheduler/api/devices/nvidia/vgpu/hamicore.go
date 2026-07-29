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

package vgpu

// ──────────────────────────────────────────────────────────────────────────────
// hamicore.go  —— HAMi-core 软件时间切片共享模式的实现
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ HAMi-core 模式说明：                                                      ║
// ║                                                                          ║
// ║  一块物理 GPU 可被多个 Pod 同时挂载，通过软件时间切片实现共享。        ║
// ║  Volcano 调度器只负责决定 Pod 放到哪块 GPU，                         ║
// ║  实际的显存隔离、算力限制由 HAMi device plugin 通过 CUDA Hook enforce。║
// ║                                                                          ║
// ║  与 gpushare 的对比：                                                      ║
// ║  ─────────────────────────────────────────────────────────────────────────╢
// ║  gpushare 的“显存共享”：                                              ║
// ║    - 仅检查空闲显存 >= 请求量，不限制槽位数和核心数                 ║
// ║    - 分配时将 Pod 放入 PodMap，无 UsedNum/UsedCore 概念            ║
// ║    - 无运行时隔离，多个 Pod 共享 GPU 时无安全边界                  ║
// ║                                                                          ║
// ║  HAMi-core 的“软件切片”：                                              ║
// ║    - 检查槽位数(UsedNum < Number)、显存、核心数三重约束              ║
// ║    - 分配时更新 UsedNum/UsedMem/UsedCore 实时计数器                  ║
// ║    - 运行时由 HAMi 通过 CUDA Hook 强制隔离显存和算力                ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
// ──────────────────────────────────────────────────────────────────────────────

import (
	"fmt"

	"k8s.io/klog/v2"
)

// HAMICoreFactory 实现 hami-core 模式的 SharingFactory。
//
// hami-core 是一种软件级时间切片共享模式：
//   - 一块物理 GPU 可以被多个 Pod 同时挂载
//   - 通过 UsedNum 限制最大共享 Pod 数量（Number）
//   - 通过 UsedMem 和 UsedCore 限制显存与算力总使用
//
// 与 gpushare 对比：
//   - gpushare 没有槽位概念，只要空闲显存足够就可以放入
//   - hami-core 额外要求 UsedNum < Number（槽位数），以及 UsedCore + coreReq <= 100
//   - gpushare 无运行时隔离
//   - hami-core 由 HAMi device plugin 通过 CUDA Hook 在运行时 enforce 显存/算力限制
//
// 该模式下 Volcano 调度器只负责决定 Pod 放到哪块 GPU；
// 实际的显存隔离、算力限制由 HAMi device plugin 通过 CUDA Hook 等技术在运行时 enforce。
type HAMICoreFactory struct{}

func init() {
	RegisterFactory(vGPUControllerHAMICore, HAMICoreFactory{})
}

// TryAddPod 在 predicate 阶段尝试将 Pod 加入该 GPU。
//
// hami-core 模式下，调用方（checkNodeGPUSharingPredicateAndScore）已经检查过
// Number、Memory、Core 等约束，因此这里直接累加 UsedNum/UsedMem/UsedCore 并返回成功。
// 返回的 uuid 是物理 GPU 的 UUID。
//
// 与 gpushare 对比：
//   - gpushare 的 predicateGPUbyMemory 仅检查空闲显存 >= 请求量
//   - hami-core 的 TryAddPod 在调用前已检查了槽位 + 核心 + 型号 + PodGroup 等多个约束
func (f HAMICoreFactory) TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string) {
	gd.UsedNum++
	gd.UsedMem += mem
	gd.UsedCore += core

	return true, gd.UUID
}

// AddPod 真正将 Pod 加入该 GPU。
//
// 与 gpushare.AddResource 对比：
//   - gpushare: 仅将 *v1.Pod 指针放入 PodMap[uid]
//   - hami-core: 创建 GPUUsage 结构体记录 UsedMem/UsedCore，
//     并更新设备的 UsedNum/UsedMem/UsedCore 实时计数器
//
// 更新设备的 UsedNum、UsedMem、UsedCore，并在 PodMap 中记录该 Pod 的资源使用。
// 如果 Pod 已经在 PodMap 中（重复调用），直接返回 nil。
func (f HAMICoreFactory) AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	if _, ok := gd.PodMap[podUID]; ok {
		return nil
	}
	gd.PodMap[podUID] = &GPUUsage{
		UsedMem:  0,
		UsedCore: 0,
	}
	gd.UsedNum++
	gd.UsedMem += mem
	gd.UsedCore += core

	gd.PodMap[podUID].UsedMem += mem
	gd.PodMap[podUID].UsedCore += core

	klog.V(4).Infoln("add Pod: ", podUID, mem, gd.PodMap[podUID].UsedMem, gd.PodMap[podUID].UsedCore)
	return nil
}

// SubPod 将 Pod 从该 GPU 中移除。
//
// 与 gpushare.SubResource 对比：
//   - gpushare: 仅从 PodMap 中 delete Pod UID
//   - hami-core: 额外回收 UsedNum/UsedMem/UsedCore 计数器
//
// 回收 UsedNum、UsedMem、UsedCore，并从 PodMap 中删除该 Pod。
// 如果 Pod 不在 PodMap 中，返回错误。
func (f HAMICoreFactory) SubPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	_, ok := gd.PodMap[podUID]
	if !ok {
		return fmt.Errorf("pod not exist in GPU pod map")
	}

	gd.UsedNum--
	gd.UsedMem -= mem
	gd.UsedCore -= core
	klog.V(4).Infoln("sub Pod: ", podUID, mem)
	delete(gd.PodMap, podUID)
	return nil
}
