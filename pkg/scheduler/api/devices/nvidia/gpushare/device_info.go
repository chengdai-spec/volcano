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
// device_info.go  —— gpushare 的 GPU 设备数据结构与 api.Devices 接口实现
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 核心设计思想：                                                            ║
// ║                                                                          ║
// ║  gpushare 不依赖任何外部设备插件，纯从节点 Capacity 推算出“逻辑 GPU”。   ║
// ║  它将节点上报的 gpu-memory 总量和 gpu-number 总量平均分配，             ║
// ║  构建出若干等显存的逻辑 GPUDevice，每个仅含 ID + Memory + PodMap。  ║
// ║                                                                          ║
// ║  与 vgpu 对比：                                                            ║
// ║  ─────────────────────────────────────────────────────────────────────────╢
// ║  gpushare.GPUDevice          vgpu.GPUDevice                              ║
// ║  ─────────────────         ────────────────────                        ║
// ║  ID (int)                     ID (int)                                 ║
// ║  Memory (uint)                UUID (string)   ← vgpu 独有               ║
// ║  PodMap map[string]*Pod       Node (string)   ← vgpu 独有               ║
// ║                                 Memory (uint)                          ║
// ║                                 Number (uint) ← 槽位数，vgpu 独有       ║
// ║                                 Type (string) ← 型号，vgpu 独有         ║
// ║                                 Health (bool) ← 健康，vgpu 独有         ║
// ║                                 UsedNum/UsedMem/UsedCore ← vgpu 独有   ║
// ║                                 PodMap map[string]*GPUUsage            ║
// ║                                 MigTemplate/MigUsage ← MIG 独有       ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
// ──────────────────────────────────────────────────────────────────────────────

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock"
)

// GPUDevice 描述节点上的单块 GPU 设备。
//
// 在 gpushare 模式下，Volcano 并不感知真实物理 GPU 的 UUID，
// 而是根据节点 Capacity 中上报的 gpu-memory 总量和 gpu-number 总量，
// 按 ID(0,1,2...)抽象出若干等显存的“逻辑 GPU”。
//
// 与 vgpu.GPUDevice 对比：
//   - gpushare 只有 3 个字段（ID / Memory / PodMap）
//   - vgpu 有 13 个字段（增加 UUID / Node / Number / Type / Health / UsedXxx / MigXxx）
//   - gpushare 的 PodMap 存 *v1.Pod 指针，通过 Pod.Spec 反查资源请求
//   - vgpu 的 PodMap 存 *GPUUsage 结构体，直接记录 UsedMem/UsedCore
type GPUDevice struct {
	// ID 是 GPU 在该节点内的逻辑索引，从 0 开始。
	ID int
	// PodMap 记录当前共享/占用这块 GPU 的所有 Pod。
	// key 为 Pod UID，value 为 Pod 对象指针。
	PodMap map[string]*v1.Pod
	// Memory 是这块逻辑 GPU 的显存容量（单位 MiB）。
	// 由 NewGPUDevices 中 totalMemory/gpuNumber 计算得出。
	Memory uint
}

// GPUDevices 描述一个节点上的所有 gpushare GPU 设备集合。
//
// 与 vgpu.GPUDevices 对比：
//   - gpushare 仅含 Name + Device 两个字段
//   - vgpu 额外含 Mode（共享模式）、Score（缓存打分）、Sharing（策略工厂）
type GPUDevices struct {
	// Name 为节点名称。
	Name string

	// Device 是 ID -> GPUDevice 的映射。
	Device map[int]*GPUDevice
}

// NewGPUDevice 创建一块逻辑 GPU 设备对象。
//
// 参数：
//   - id: GPU 逻辑索引
//   - mem: 该 GPU 的显存容量（MiB）
//
// 注意：创建的 GPU 初始状态为空闲（PodMap 为空）。
func NewGPUDevice(id int, mem uint) *GPUDevice {
	return &GPUDevice{
		ID:     id,
		Memory: mem,
		PodMap: map[string]*v1.Pod{},
	}
}

// NewGPUDevices 根据节点资源 Capacity 构建该节点的 gpushare GPU 视图。
//
// 与 vgpu.NewGPUDevices 对比：
//   - gpushare: 从节点 Capacity 的 gpu-memory / gpu-number 推算逻辑 GPU
//   - vgpu:     从节点注解 volcano.sh/vgpu-register 解析 HAMi 上报的真实物理 GPU
//     并检查 Allocatable 中的 vgpu-number / vgpu-cores / vgpu-memory 是否存在
//   - gpushare 不需要任何外部设备插件，仅依赖 kubelet 上报的 Capacity
//   - vgpu 必须安装 HAMi device plugin 才能工作
//
// 构建逻辑：
//  1. 读取节点 Capacity 中的 volcano.sh/gpu-memory 总量
//  2. 读取节点 Capacity 中的 volcano.sh/gpu-number 总卡数
//  3. 用 totalMemory / gpuNumber 得到每块逻辑 GPU 的显存
//  4. 按 gpuNumber 创建对应数量的 GPUDevice（ID 从 0 递增）
//  5. 读取节点注解 volcano.sh/gpu-unhealthy-ids，将故障 GPU 从映射中删除。
//
// 实际案例：
// 某节点 Capacity 为 gpu-memory=32768、gpu-number=4，
// 则生成 4 块逻辑 GPU，每块 Memory = 8192 MiB。
// 若注解 UnhealthyGPUIDs="1"，则最终 Device 中只保留 ID 0、2、3。
func NewGPUDevices(name string, node *v1.Node) *GPUDevices {
	if node == nil {
		return nil
	}
	memory, ok := node.Status.Capacity[VolcanoGPUResource]
	if !ok {
		return nil
	}
	totalMemory := memory.Value()

	res, ok := node.Status.Capacity[VolcanoGPUNumber]
	if !ok {
		return nil
	}
	gpuNumber := res.Value()
	if gpuNumber == 0 {
		klog.Warningf("invalid %s=%s", VolcanoGPUNumber, res.String())
		return nil
	}

	memoryPerCard := uint(totalMemory / gpuNumber)
	gpudevices := GPUDevices{}
	gpudevices.Device = make(map[int]*GPUDevice)
	gpudevices.Name = name
	for i := 0; i < int(gpuNumber); i++ {
		gpudevices.Device[i] = NewGPUDevice(i, memoryPerCard)
	}
	unhealthyGPUs := getUnhealthyGPUs(&gpudevices, node)
	for i := range unhealthyGPUs {
		klog.V(4).Infof("delete unhealthy gpu id %d from GPUDevices", unhealthyGPUs[i])
		delete(gpudevices.Device, unhealthyGPUs[i])
	}
	return &gpudevices
}

// GetIgnoredDevices 返回希望调度器忽略的设备名称列表。
// gpushare 模式没有需要特别忽略的设备名，因此返回空字符串占位。
func (gs *GPUDevices) GetIgnoredDevices() []string {
	return []string{""}
}

// AddResource 在调度器初始化节点缓存时，将已分配到该节点的 Pod 加入 GPU 占用视图。
//
// 与 vgpu.AddResource 对比：
//   - gpushare: 从 Pod 注解 gpu-index 获取 ID，将 *v1.Pod 放入 PodMap
//   - vgpu:     从 Pod 注解 vgpu-ids-new 解码 UUID+显存+核心，调用 Sharing.AddPod 更新 UsedXxx
//   - gpushare 不跟踪 UsedMem/UsedCore，每次过滤时通过遍历 PodMap 重新计算
//   - vgpu 实时维护 UsedMem/UsedCore/UsedNum，过滤时直接读取
//
// 触发场景：
//   - 调度器启动时同步现有 Pod
//   - 其他调度周期已经分配 GPU 的 Pod 被 watch 到
//
// 实现说明：
// 通过读取 Pod 注解 volcano.sh/gpu-index 获取分配的 GPU ID 列表，
// 并将 Pod 指针放入对应 GPUDevice.PodMap 中，从而恢复“哪些 Pod 占用了哪些 GPU”。
func (gs *GPUDevices) AddResource(pod *v1.Pod) {
	gpuRes := getGPUMemoryOfPod(pod)
	gpuNumRes := getGPUNumberOfPod(pod)
	if gpuRes > 0 || gpuNumRes > 0 {
		ids := GetGPUIndex(pod)
		for _, id := range ids {
			if dev := gs.Device[id]; dev != nil {
				dev.PodMap[string(pod.UID)] = pod
			}
		}
	}
}

// SubResource 将 Pod 从 GPU 占用视图中移除。
//
// 触发场景：
//   - Pod 完成（Succeeded/Failed）或删除时，调度器更新缓存
//   - 抢占/回滚时释放已占用的 GPU
func (gs *GPUDevices) SubResource(pod *v1.Pod) {
	gpuRes := getGPUMemoryOfPod(pod)
	gpuNumRes := getGPUNumberOfPod(pod)
	if gpuRes > 0 || gpuNumRes > 0 {
		ids := GetGPUIndex(pod)
		for _, id := range ids {
			if dev := gs.Device[id]; dev != nil {
				delete(dev.PodMap, string(pod.UID))
			}
		}
	}
}

// HasDeviceRequest 判断 Pod 是否请求了 gpushare 类型的 GPU 资源。
//
// 当且仅当对应开关开启且 Pod 显式请求了 gpu-memory 或 gpu-number 时返回 true。
// 这是 deviceShare 插件决定是否将 Pod 交给 gpushare 设备处理的入口。
func (gs *GPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	if GpuSharingEnable && getGPUMemoryOfPod(pod) > 0 ||
		GpuNumberEnable && getGPUNumberOfPod(pod) > 0 {
		return true
	}
	return false
}

// AddQueueResource 返回队列维度需要统计的资源量。
// gpushare 模式暂不向队列层上报额外资源，返回空 map。
func (gs *GPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	return map[string]float64{}
}

// Release 在 Pod 被回滚（例如 UnPipeline、抢占失败）时释放 GPU 资源，
// 并通过 JSON Patch 清除 Pod 上的 gpu-index 和 predicate-time 注解。
//
// 与 vgpu.Release 对比：
//   - gpushare: 先 Patch 移除注解，再清理本地 PodMap
//   - vgpu:     直接调用 SubResource，由 Sharing.SubPod 清理资源 + 更新 metrics
//   - gpushare: Release 需要 kubeClient 来 Patch
//   - vgpu:     Release 不需要 kubeClient，仅操作内存状态
func (gs *GPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	ids := GetGPUIndex(pod)
	patch := RemoveGPUIndexPatch()
	_, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return errors.Errorf("patch pod %s failed with patch %s: %v", pod.Name, patch, err)
	}

	for _, id := range ids {
		if dev, ok := gs.Device[id]; ok {
			delete(dev.PodMap, string(pod.UID))
		}
	}

	klog.V(4).Infof("predicates with gpu sharing, update pod %s/%s deallocate from node [%s]", pod.Namespace, pod.Name, gs.Name)
	return nil
}

// FilterNode 在 predicate 阶段检查 Pod 是否能放入该节点的 GPU 资源。
//
// 与 vgpu.FilterNode 对比：
//   - gpushare: 分别检查显存和卡数，无打分逻辑，成功时返回 devices.Success
//   - vgpu:     调用 checkNodeGPUSharingPredicateAndScore 进行完整的模拟分配，
//     同时计算打分并缓存到 gs.Score，支持 binpack/spread 策略、型号过滤、PodGroup Spread
//   - gpushare 的过滤是简单的“是否有足够资源”判断
//   - vgpu 的过滤是复杂的“最优分配 + 打分”过程
//
// 如果开启了 GpuSharingEnable，则检查显存是否足够；
// 如果开启了 GpuNumberEnable，则检查是否有足够数量的空闲整卡。
// 任一检查失败返回 devices.Unschedulable，成功返回 devices.Success。
func (gs *GPUDevices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	klog.V(4).Infoln("DeviceSharing:Into FitInPod", pod.Name)
	if GpuSharingEnable {
		fit, err := checkNodeGPUSharingPredicate(pod, gs)
		if err != nil || !fit {
			klog.Errorln("deviceSharing err=", err.Error())
			return devices.Unschedulable, fmt.Sprintf("GpuShare %s", err.Error()), err
		}
	}
	if GpuNumberEnable {
		fit, err := checkNodeGPUNumberPredicate(pod, gs)
		if err != nil || !fit {
			klog.Errorln("deviceSharing err=", err.Error())
			return devices.Unschedulable, fmt.Sprintf("GpuNumber %s", err.Error()), err
		}
	}
	klog.V(4).Infoln("DeviceSharing:FitInPod successed")
	return devices.Success, "", nil
}

// GetStatus 返回设备状态字符串，gpushare 模式暂无实现，返回空串。
func (gs *GPUDevices) GetStatus() string {
	return ""
}

// DeepCopy 返回 GPUDevices 的深拷贝，用于 dry-run 模拟调度。
//
// 与 vgpu.DeepCopy 对比：
//   - gpushare: 拷贝 GPUDevice.ID/Memory/PodMap，PodMap 中 Pod 指针不做深拷贝
//   - vgpu:     额外拷贝 UUID/Node/Type/Health/UsedXxx/MigTemplate/MigUsage
//     且 PodMap 中的 GPUUsage 会做值拷贝（u := *usage）
//
// 注意：PodMap 中存储的 Pod 指针没有做深拷贝（仅拷贝 map 结构），
// 因为调度模拟阶段通常只读 Pod 信息，不修改其内容。
func (gs *GPUDevices) DeepCopy() interface{} {
	if gs == nil {
		return nil
	}
	cp := &GPUDevices{
		Name:   gs.Name,
		Device: make(map[int]*GPUDevice, len(gs.Device)),
	}
	for id, dev := range gs.Device {
		newDev := &GPUDevice{
			ID:     dev.ID,
			Memory: dev.Memory,
			PodMap: make(map[string]*v1.Pod, len(dev.PodMap)),
		}
		for uid, pod := range dev.PodMap {
			newDev.PodMap[uid] = pod
		}
		cp.Device[id] = newDev
	}
	return cp
}

// ScoreNode 返回节点 GPU 打分，gpushare 模式无打分逻辑，固定返回 0。
//
// 与 vgpu.ScoreNode 对比：
//   - gpushare: 始终返回 0，所有节点在 GPU 维度得分相同
//   - vgpu:     返回 FilterNode 阶段缓存的 gs.Score，不同节点得分不同
//     （binpack 模式下已用显存多的节点得分高，spread 模式下空闲 GPU 多的得分高）
func (gs *GPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	return 0
}

// Allocate 在调度决策后，为 Pod 实际分配 GPU 并通过 Patch 写入注解。
//
// 与 vgpu.Allocate 对比：
//   - gpushare: 写入 2 个注解（gpu-index + predicate-time），使用 JSON Patch
//   - vgpu:     写入 6 个注解（vgpu-node + vgpu-time + vgpu-ids-new
//   - devices-to-allocate + bind-phase + bind-time），使用 StrategicMergePatch
//   - gpushare: 无防重复分配检查
//   - vgpu:     先检查 alreadyAssignedOnNode，避免同 Pod 重复分配
//   - gpushare: 分配后才更新 PodMap
//   - vgpu:     先调 addToPodMap 更新内存，再 Patch，防止 apiserver watch 延迟
//
// 分配逻辑：
//  1. 若 Pod 请求 gpu-memory：
//     - 若 NodeLockEnable 为 true，先对节点加锁，防止并发分配同一块卡。
//     - 调用 predicateGPUbyMemory 筛选空闲显存足够的 GPU，取第一个（ID 最小）。
//     - 通过 JSON Patch 将 gpu-index 和 predicate-time 写入 Pod 注解。
//     - 在本地 GPUDevice.PodMap 中记录该 Pod。
//  2. 若 Pod 请求 gpu-number：
//     - 调用 predicateGPUbyNumber 获取所需数量的空闲 GPU ID。
//     - 同样通过 Patch 写入 gpu-index，并在 PodMap 中记录。
//
// 实际案例：
// 节点有 4 块 8192MiB GPU，当前只有 ID=0 被占用 1024MiB。
// Pod A 请求 gpu-memory=2048，predicateGPUbyMemory 返回 [0,1,2,3]（0 剩余 7168 也足够），
// 取 ID=0，Patch 注解 gpu-index="0"，Pod A 与已占用 Pod 共享 ID=0 这块卡。
func (gs *GPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	klog.V(4).Infoln("DeviceSharing:Into AllocateToPod", pod.Name)
	if getGPUMemoryOfPod(pod) > 0 {
		if NodeLockEnable {
			nodelock.UseClient(kubeClient)
			err := nodelock.LockNode(gs.Name, "gpu")
			if err != nil {
				return errors.Errorf("node %s locked for lockname gpushare %s", gs.Name, err.Error())
			}
		}
		ids := predicateGPUbyMemory(pod, gs)
		if len(ids) == 0 {
			return errors.Errorf("the node %s can't place the pod %s in ns %s", pod.Spec.NodeName, pod.Name, pod.Namespace)
		}
		id := ids[0]
		patch := AddGPUIndexPatch([]int{id})
		pod, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
		if err != nil {
			return errors.Errorf("patch pod %s failed with patch %s: %v", pod.Name, patch, err)
		}
		dev, ok := gs.Device[id]
		if !ok {
			return errors.Errorf("failed to get GPU %d from node %s", id, gs.Name)
		}
		dev.PodMap[string(pod.UID)] = pod
		klog.V(4).Infof("predicates with gpu sharing, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, gs.Name)
	}
	if getGPUNumberOfPod(pod) > 0 {
		ids := predicateGPUbyNumber(pod, gs)
		if len(ids) == 0 {
			return errors.Errorf("the node %s can't place the pod %s in ns %s", pod.Spec.NodeName, pod.Name, pod.Namespace)
		}
		patch := AddGPUIndexPatch(ids)
		pod, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
		if err != nil {
			return errors.Errorf("patch pod %s failed with patch %s: %v", pod.Name, patch, err)
		}
		for _, id := range ids {
			dev, ok := gs.Device[id]
			if !ok {
				return errors.Errorf("failed to get GPU %d from node %s", id, gs.Name)
			}
			dev.PodMap[string(pod.UID)] = pod
		}
		klog.V(4).Infof("predicates with gpu number, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, gs.Name)
	}
	return nil
}
