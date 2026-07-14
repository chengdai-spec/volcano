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

import (
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices"
	deviceconfig "volcano.sh/volcano/pkg/scheduler/api/devices/config"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock"
)

// GPUUsage 描述一个 Pod 在单块 GPU 上的资源使用情况。
type GPUUsage struct {
	// UsedMem 是该 Pod 在该 GPU 上占用的显存（MiB）。
	UsedMem uint
	// UsedCore 是该 Pod 在该 GPU 上占用的核心数百分比。
	UsedCore uint
	// PodGroupKey 是 Pod 所属 PodGroup 的标识（namespace/name），
	// 用于实现 PodGroup Spread 策略；无 PodGroup 时为空串。
	PodGroupKey string
}

// GPUDevice 描述 vGPU 模式下的一块物理 GPU 及其共享状态。
//
// 与 gpushare 不同，vgpu 的 GPUDevice 来自设备插件（如 HAMi）上报的真实物理设备信息，
// 包含 UUID、型号、健康状态、最大共享槽位数等字段。
type GPUDevice struct {
	// ID 是 GPU 在该节点内的索引。
	ID int
	// Node 是 GPU 所在节点名。
	Node string
	// UUID 是 GPU 的全局唯一标识。
	UUID string
	// PodMap 记录当前占用该 GPU 的 Pod 及其资源使用。
	PodMap map[string]*GPUUsage
	// Memory 是 GPU 总显存（MiB）。
	Memory uint
	// Number 是该 GPU 最大可共享的虚拟槽位数。
	// 例如 HAMi 将一块 GPU 划分为 10 份，则 Number=10。
	Number uint
	// Type 是 GPU 型号，例如 "Tesla-A100-SXM4-40GB"。
	Type string
	// Health 表示 GPU 是否健康。
	Health bool
	// UsedNum 是当前已占用的虚拟槽位数。
	UsedNum uint
	// UsedMem 是当前已占用的显存总量。
	UsedMem uint
	// UsedCore 是当前已占用的核心数百分比总和。
	UsedCore uint
	// MigTemplate 是 MIG 模式下该 GPU 支持的几何切分模板。
	MigTemplate []deviceconfig.Geometry
	// MigUsage 是 MIG 模式下各实例的占用情况。
	MigUsage deviceconfig.MigInUse
}

// GPUDevices 描述一个节点上的所有 vGPU 设备集合。
type GPUDevices struct {
	// Name 是节点名。
	Name string
	// Mode 是该节点 GPU 的共享模式（hami-core / mig / mps）。
	Mode string
	// Score 在 FilterNode 阶段缓存节点打分，避免 ScoreNode 阶段重复计算。
	Score float64

	// Device 是索引 -> GPUDevice 的映射。
	Device map[int]*GPUDevice
	// Sharing 是具体的共享策略实现（HAMICoreFactory 或 MIGFactory）。
	Sharing SharingFactory
}

// NewGPUDevice 创建一块 vGPU 物理设备对象。
func NewGPUDevice(id int, mem uint) *GPUDevice {
	return &GPUDevice{
		ID:       id,
		Memory:   mem,
		PodMap:   make(map[string]*GPUUsage),
		UsedNum:  0,
		UsedMem:  0,
		UsedCore: 0,
	}
}

// NewGPUDevices 根据节点注解和 Allocatable 资源构建该节点的 vGPU 视图。
//
// 构建逻辑：
//  1. 读取节点注解 volcano.sh/vgpu-register，获取由设备插件上报的物理 GPU 信息。
//  2. 检查节点 Allocatable 中是否存在 volcano.sh/vgpu-number、volcano.sh/vgpu-cores、
//     volcano.sh/vgpu-memory 资源且值大于 0；若不存在则认为该节点未启用 vGPU。
//  3. 调用 decodeNodeDevices 解析注解字符串，得到每块 GPU 的 UUID、显存、槽位数、型号、
//     健康状态和共享模式。
//  4. 根据共享模式获取对应的 SharingFactory（hami-core 或 MIG）。
//  5. 重置该节点 GPU 的监控指标。
//
// 实际案例：
// 某节点由 HAMi device plugin 上报注解：
//   volcano.sh/vgpu-register: "GPU-xxx1,10,40960,A100-SXM4-40GB,true,hami-core:GPU-xxx2,..."
// 并且节点 Allocatable 包含 vgpu-number=20、vgpu-memory=819200、vgpu-cores=2000，
// 则本函数会构建出两块物理 GPU，每块 Number=10、Memory=40960，模式为 hami-core。
func NewGPUDevices(name string, node *v1.Node) *GPUDevices {
	if node == nil {
		return nil
	}
	annos, ok := node.Annotations[deviceconfig.VolcanoVGPURegister]
	if !ok {
		return nil
	}

	if node.Status.Allocatable != nil {
		gpuNumberRes, gpuNumberExists := node.Status.Allocatable[v1.ResourceName(deviceconfig.VolcanoVGPUNumber)]
		if !gpuNumberExists || gpuNumberRes.Value() == 0 {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0, returning nil", node.Name, deviceconfig.VolcanoVGPUNumber)
			return nil
		}

		vgpuCoresRes, vgpuCoresExists := node.Status.Allocatable[v1.ResourceName(deviceconfig.VolcanoVGPUCores)]
		if !vgpuCoresExists || vgpuCoresRes.Value() == 0 {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0, returning nil", node.Name, deviceconfig.VolcanoVGPUCores)
			return nil
		}

		vgpuMemoryRes, vgpuMemoryExists := node.Status.Allocatable[v1.ResourceName(deviceconfig.VolcanoVGPUMemory)]
		if !vgpuMemoryExists || vgpuMemoryRes.Value() == 0 {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0, returning nil", node.Name, deviceconfig.VolcanoVGPUMemory)
			return nil
		}
	} else {
		klog.V(3).Infof("Node %s does not have allocatable resources information, returning nil", node.Name)
		return nil
	}

	nodedevices, sharingMode := decodeNodeDevices(name, annos)
	if (nodedevices == nil) || len(nodedevices.Device) == 0 {
		return nil
	}

	sharingHandler, _ := GetSharingHandler(sharingMode)
	klog.V(3).Infoln("GPU sharing mode: ", sharingMode)
	for _, val := range nodedevices.Device {
		klog.V(3).InfoS("Nvidia Device registered name", "name", nodedevices.Name, "val", *val)
		ResetDeviceMetrics(val.UUID, node.Name, float64(val.Memory))
	}

	nodedevices.Sharing = sharingHandler
	return nodedevices
}

// ScoreNode 返回该节点在 vGPU 维度上的打分。
//
// 为兼容抢占场景，分数在 FilterNode 阶段已经计算并缓存到 gs.Score 中，
// 此处直接返回缓存值，避免重复遍历设备。
func (gs *GPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	/* TODO: we need a base score to be campatable with preemption, it means a node without evicting a task has
	   a higher score than those needs to evict a task */

	// Use cached stored in filter state in order to avoid recalculating.
	return gs.Score
}

// GetIgnoredDevices 返回希望调度器忽略的设备名称列表。
// vGPU 模式暂无需要忽略的设备，返回空列表。
func (gs *GPUDevices) GetIgnoredDevices() []string {
	return []string{}
}

// AddQueueResource 根据 Pod 已分配的设备，计算队列维度需要统计的 vGPU 资源量。
//
// 读取 Pod 注解 volcano.sh/vgpu-ids-new，将其中的 Usedmem、Usedcores 转换为
// 队列资源计数（乘以 1000 以支持浮点精度）。
func (gs *GPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	if gs == nil {
		return map[string]float64{}
	}
	klog.V(5).InfoS("AddQueueResource", "Name", pod.Name)
	res := map[string]float64{}
	ids, ok := pod.Annotations[AssignedIDsAnnotations]
	if !ok {
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name)
		return res
	}
	podDev := DecodePodDevices(ids)
	for _, val := range podDev {
		for _, deviceused := range val {
			for _, gsdevice := range gs.Device {
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					res[getConfig().ResourceMemoryName] += float64(deviceused.Usedmem * 1000)
					res[getConfig().ResourceCoreName] += float64(deviceused.Usedcores * 1000)
				}
			}
		}
	}
	klog.V(4).InfoS("AddQueueResource", "Name=", pod.Name, "res=", res)
	return res
}

// AddResource 在调度器初始化节点缓存时，将已分配到该节点的 Pod 加入 vGPU 占用视图。
//
// 它通过读取 Pod 注解 volcano.sh/vgpu-ids-new，找到对应 UUID 的 GPU，
// 然后调用 Sharing.AddPod 更新设备的 UsedNum、UsedMem、UsedCore 以及 PodMap。
func (gs *GPUDevices) AddResource(pod *v1.Pod) {
	if gs == nil {
		return
	}

	gs.addResource(pod.Annotations, pod)
}

func (gs *GPUDevices) addResource(annotations map[string]string, pod *v1.Pod) {
	ids, ok := annotations[AssignedIDsAnnotations]
	if !ok {
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name)
		return
	}
	podDev := DecodePodDevices(ids)
	for _, val := range podDev {
		for _, deviceused := range val {
			for index, gsdevice := range gs.Device {
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					err := gs.Sharing.AddPod(gsdevice, deviceused.Usedmem, deviceused.Usedcores, string(pod.UID), deviceused.UUID)
					if err == nil {
						if u := gsdevice.PodMap[string(pod.UID)]; u != nil {
							u.PodGroupKey = getPodGroupKey(pod)
						}
						gs.AddPodMetrics(index, string(pod.UID), pod.Name)
					} else {
						klog.ErrorS(err, "add resource failed")
					}
					break
				}
			}
		}
	}
}

// addToPodMap 直接将分配信息映射到内存中的 PodMap，不经过 Sharing 策略。
//
// 主要用于 Allocate 阶段：在 Patch Pod 注解之前先更新本地状态，
// 避免 apiserver watch 延迟导致后续回滚路径看不到已分配的 ID。
func (gs *GPUDevices) addToPodMap(annotations map[string]string, pod *v1.Pod) {
	ids, ok := annotations[AssignedIDsAnnotations]
	if !ok {
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name)
		return
	}
	podDev := DecodePodDevices(ids)
	for _, val := range podDev {
		for _, deviceused := range val {
			for _, gsdevice := range gs.Device {
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					podUID := string(pod.UID)
					_, ok := gsdevice.PodMap[podUID]
					if !ok {
						gsdevice.PodMap[podUID] = &GPUUsage{
							UsedMem:     0,
							UsedCore:    0,
							PodGroupKey: getPodGroupKey(pod),
						}
					}

					gsdevice.PodMap[podUID].UsedMem += deviceused.Usedmem
					gsdevice.PodMap[podUID].UsedCore += deviceused.Usedcores
					gsdevice.PodMap[podUID].PodGroupKey = getPodGroupKey(pod)
				}
			}
		}
	}
}

// SubResource 将 Pod 从 vGPU 占用视图中释放。
//
// 根据 Pod 注解找到对应 UUID 的 GPU，调用 Sharing.SubPod 回收资源，
// 并同步更新 Prometheus 指标。
func (gs *GPUDevices) SubResource(pod *v1.Pod) {
	if gs == nil {
		return
	}
	ids, ok := pod.Annotations[AssignedIDsAnnotations]
	if !ok {
		return
	}
	podDev := DecodePodDevices(ids)
	for _, val := range podDev {
		for _, deviceused := range val {
			for index, gsdevice := range gs.Device {
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					err := gs.Sharing.SubPod(gsdevice, uint(deviceused.Usedmem), uint(deviceused.Usedcores), string(pod.UID), deviceused.UUID)
					if err != nil {
						klog.ErrorS(err, "sub resource failed")
					} else {
						gs.SubPodMetrics(index, string(pod.UID), pod.Name)
					}
					break
				}
			}
		}
	}
}

// HasDeviceRequest 判断 Pod 是否请求了 vGPU 资源。
//
// 当 VGPUEnable 为 true 且 Pod 的 Limits 中设置了 vgpu-memory 或 vgpu-number 时返回 true。
func (gs *GPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	if VGPUEnable && checkVGPUResourcesInPod(pod) {
		return true
	}
	return false
}

// Release 在回滚路径中释放 Pod 占用的 vGPU 资源。
//
// 对于 Pipelined 任务，NodeInfo 不会调用 SubResource，因此 Release 需要主动调用 SubResource
// 以确保 GPU 占用状态被正确回收。
func (gs *GPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// Release is required for rollback paths (e.g. UnPipeline) where NodeInfo
	// does not invoke subResource for Pipelined tasks.
	gs.SubResource(pod)
	return nil
}

// FilterNode 在 predicate 阶段检查 Pod 是否能放入该节点的 vGPU 资源。
//
// 调用 checkNodeGPUSharingPredicateAndScore 进行模拟分配（replicate=true），
// 若成功则将计算出的 score 缓存到 gs.Score，供 ScoreNode 直接使用。
func (gs *GPUDevices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	if VGPUEnable {
		klog.V(4).Infoln("hami-vgpu DeviceSharing starts filtering pods", pod.Name)
		fit, _, score, err := checkNodeGPUSharingPredicateAndScore(pod, gs, true, schedulePolicy)
		if err != nil || !fit {
			klog.ErrorS(err, "Failed to fitler node to vgpu task", "pod", pod.Name)
			return devices.Unschedulable, "hami-vgpuDeviceSharing error", err
		}
		gs.Score = score
		klog.V(4).Infoln("hami-vgpu DeviceSharing successfully filters pods")
	}
	return devices.Success, "", nil
}

// Allocate 在调度决策后，为 Pod 实际分配 vGPU 并通过 Patch 写入注解。
//
// 分配流程：
//  1. 检查 Pod 是否已经在该节点上分配过（通过 AssignedNodeAnnotations），避免重复分配。
//  2. 调用 checkNodeGPUSharingPredicateAndScore 进行真实分配（replicate=false）。
//  3. 若 NodeLockEnable 为 true，对节点加锁。
//  4. 构造分配注解：vgpu-node、vgpu-time、vgpu-ids-new、devices-to-allocate、bind-phase、bind-time。
//  5. 先更新本地 PodMap，再 Patch Pod 注解，最后记录 metrics。
//
// 实际案例：
// 某 Pod 请求 volcano.sh/vgpu-number=1、volcano.sh/vgpu-memory=2048、volcano.sh/vgpu-cores=50。
// 节点有一块 A100（Memory=40960，Number=10，当前 UsedNum=2，UsedMem=4096，UsedCore=50）。
// checkNodeGPUSharingPredicateAndScore 判断该 GPU 还有 8 个槽位、36864 MiB 显存、50% 核心，
// 可以容纳该 Pod，返回分配结果 [{UUID=GPU-xxx,Type=NVIDIA,Usedmem=2048,Usedcores=50}]。
// 最终 Patch 注解 vgpu-ids-new="GPU-xxx,NVIDIA,2048,50"。
func (gs *GPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	if VGPUEnable {
		klog.V(4).Infoln("hami-vgpu DeviceSharing:Into AllocateToPod", pod.Name)
		if alreadyAssignedOnNode(pod, gs.Name) {
			klog.V(4).InfoS("hami-vgpu DeviceSharing: skip duplicate AllocateToPod",
				"pod", pod.Name, "namespace", pod.Namespace, "node", gs.Name)
			return nil
		}
		fit, device, _, err := checkNodeGPUSharingPredicateAndScore(pod, gs, false, SchedulePolicy)
		if err != nil || !fit {
			klog.ErrorS(err, "Failed to allocate vgpu task", "pod", pod.Name)
			return err
		}
		if NodeLockEnable {
			nodelock.UseClient(kubeClient)
			err = nodelock.LockNode(gs.Name, DeviceName)
			if err != nil {
				return errors.Errorf("node %s locked for %s hamivgpu lockname %s", gs.Name, pod.Name, err.Error())
			}
		}

		annotations := make(map[string]string)
		annotations[AssignedNodeAnnotations] = gs.Name
		annotations[AssignedTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		annotations[AssignedIDsAnnotations] = encodePodDevices(device)
		annotations[AssignedIDsToAllocateAnnotations] = annotations[AssignedIDsAnnotations]

		annotations[DeviceBindPhase] = "allocating"
		annotations[BindTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		// Keep in-memory pod object in sync so rollback paths (UnPipeline ->
		// Deallocate -> Release) can see allocated IDs before apiserver watch
		// catches up.
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		for k, v := range annotations {
			pod.Annotations[k] = v
		}
		// To avoid that the pod allocated info updating latency, add it first
		gs.addToPodMap(annotations, pod)
		err = patchPodAnnotations(kubeClient, pod, annotations)
		if err != nil {
			return err
		}

		klog.V(3).Infoln("DeviceSharing:Allocate Success")
	}
	return nil
}

// DeepCopy 返回 GPUDevices 的深拷贝，用于 dry-run 模拟调度。
//
// 注意：Sharing 字段是接口，拷贝后仍指向同一个工厂对象；
// 这在只读场景下是安全的。PodMap、MigUsage、MigTemplate 都会做深拷贝。
func (gs *GPUDevices) DeepCopy() interface{} {
	if gs == nil {
		return nil
	}
	cp := &GPUDevices{
		Name:    gs.Name,
		Mode:    gs.Mode,
		Score:   gs.Score,
		Sharing: gs.Sharing,
		Device:  make(map[int]*GPUDevice, len(gs.Device)),
	}
	for id, dev := range gs.Device {
		newDev := &GPUDevice{
			ID:       dev.ID,
			Node:     dev.Node,
			UUID:     dev.UUID,
			Memory:   dev.Memory,
			Number:   dev.Number,
			Type:     dev.Type,
			Health:   dev.Health,
			UsedNum:  dev.UsedNum,
			UsedMem:  dev.UsedMem,
			UsedCore: dev.UsedCore,
			MigUsage: deviceconfig.MigInUse{
				Index:     dev.MigUsage.Index,
				UsageList: make(deviceconfig.MIGS, len(dev.MigUsage.UsageList)),
			},
			PodMap: make(map[string]*GPUUsage, len(dev.PodMap)),
		}
		copy(newDev.MigUsage.UsageList, dev.MigUsage.UsageList)
		if len(dev.MigTemplate) > 0 {
			newDev.MigTemplate = make([]deviceconfig.Geometry, len(dev.MigTemplate))
			for i, g := range dev.MigTemplate {
				ng := deviceconfig.Geometry{
					Group:     g.Group,
					Instances: make([]deviceconfig.MigTemplate, len(g.Instances)),
				}
				copy(ng.Instances, g.Instances)
				newDev.MigTemplate[i] = ng
			}
		}
		for uid, usage := range dev.PodMap {
			u := *usage
			newDev.PodMap[uid] = &u
		}
		cp.Device[id] = newDev
	}
	return cp
}

// alreadyAssignedOnNode 判断 Pod 是否已经在指定节点上完成 vGPU 分配。
//
// 通过比对 AssignedNodeAnnotations 和 AssignedIDsAnnotations 实现，
// 用于避免同一个 Pod 在同一个 session 中被重复分配。
func alreadyAssignedOnNode(pod *v1.Pod, nodeName string) bool {
	if pod == nil || pod.Annotations == nil || nodeName == "" {
		return false
	}

	return pod.Annotations[AssignedNodeAnnotations] == nodeName &&
		pod.Annotations[AssignedIDsAnnotations] != ""
}
