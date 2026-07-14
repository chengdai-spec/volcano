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
// 在 gpushare 模式下，Volcano 并不感知真实物理 GPU 的 UUID，
// 而是根据节点 Capacity 中上报的 gpu-memory 总量和 gpu-number 总量，
// 按 ID（0,1,2...）抽象出若干等显存的“逻辑 GPU”。
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
// 构建逻辑：
//  1. 读取节点 Capacity 中的 volcano.sh/gpu-memory 总量。
//  2. 读取节点 Capacity 中的 volcano.sh/gpu-number 总卡数。
//  3. 用 totalMemory / gpuNumber 得到每块逻辑 GPU 的显存。
//  4. 按 gpuNumber 创建对应数量的 GPUDevice（ID 从 0 递增）。
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

// ScoreNode 返回节点 GPU 打分，gpushare 模式暂无特殊打分逻辑，固定返回 0。
func (gs *GPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	return 0
}

// Allocate 在调度决策后，为 Pod 实际分配 GPU 并通过 Patch 写入注解。
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
