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

// Package vnpu 实现了 Volcano 对华为 MindCluster 动态 vNPU 的调度支持。
//
// 与 hami 包（通用 Ascend vNPU）相比，mindcluster 包更贴近华为 MindCluster 生态：
//  - 使用 label "ring-controller.atlas=ascend-310P" 识别 Ascend310P 推理任务；
//  - 使用资源名 "huawei.com/npu-core" 表达 AI Core 数量需求；
//  - 通过 vNPU 模板（vir01/vir02/vir04 等）动态切分物理芯片；
//  - 支持 AI CPU 降级、DVPP 开关、模板隔离、整卡与切分混合调度。
//
// 实际案例：
//  某在线推理服务部署在 Ascend310P 集群，Pod 声明：
//    labels:
//      ring-controller.atlas: ascend-310P
//      vnpu-level: low
//      vnpu-dvpp: "null"
//    resources.limits:
//      huawei.com/npu-core: "2"
//  调度器会：
//    (1) HasDeviceRequest 识别出这是 MindCluster vNPU 任务；
//    (2) GetPodResource 把 2 核 + low + null 映射为 vir02_1c 模板资源；
//    (3) CheckNodeNPUByDyPod 检查节点是否有芯片满足 Aicore/Aicpu/DVPP/vGroup 约束；
//    (4) 若资源不足但可降级，把任务 AI CPU 从 2 降到 1；
//    (5) SelectChipFromNode 选择剩余资源最少的芯片，通过 JSON Patch 把
//        "huawei.com/npu-core" 注解写成 "0-vir02_1c"，设备插件据此创建 vNPU。
package vnpu

import (
	"strings"
	"sync"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices"
)

// NPUDevices 是 MindCluster vNPU 调度器在 Volcano 中的封装。
//
// 它聚合了：
//  - Name:      节点名；
//  - NodeInf:   节点资源视图（Capability/Allocate/Idle 等）；
//  - NPUDevice: 昇腾 NPU 物理芯片与 vNPU 模板信息；
//  - FrameAttr: Volcano 框架注入的客户端、informer、配置参数。
//
// NPUDevices 实现了 deviceShare 插件要求的设备接口：AddResource、SubResource、
// FilterNode、ScoreNode、Allocate、DeepCopy 等。
type NPUDevices struct { //schedulerHandler, including all the scheduler cache
	Name string

	NodeInf

	NPUDevice

	FrameAttr VolcanoFrame
}

// NewNPUDevices 构造一个空的 NPUDevices。
//
// 初始化时会：
//  - 构造空的 NodeInf（Capability/Allocate/Idle 等 map）；
//  - 预置 Ascend310P 的 vNPU 模板表；
//  - 初始化物理芯片 map、不健康芯片集合、降级缓存、并发任务缓存；
//  - 初始化 VolcanoFrame 中的静态参数（OnceInit、IsFirstSession 等）。
//
// 注意：这里只是构造空壳，真实芯片信息通常由上层在节点同步时填充。
func NewNPUDevices(name string, node *v1.Node) *NPUDevices {
	return &NPUDevices{
		Name: name,
		NodeInf: NodeInf{
			Name:       name,
			Annotation: make(map[string]string),
			Label:      make(map[string]string),
			Capability: make(map[v1.ResourceName]float64),
			Allocate:   make(map[v1.ResourceName]float64),
			Idle:       make(map[v1.ResourceName]float64),
		},
		NPUDevice: NPUDevice{
			VT: VTemplate{
				Temp: Ascend310P,
				Data: map[string]VResource{
					VNPUTempVir01:        {Aicore: 1, Aicpu: 1, DVPP: AscendDVPPEnabledNull},
					VNPUTempVir02:        {Aicore: NPUIndex2, Aicpu: NPUIndex2, DVPP: AscendDVPPEnabledNull},
					VNPUTempVir02C1:      {Aicore: NPUIndex2, Aicpu: 1, DVPP: AscendDVPPEnabledNull},
					VNPUTempVir04:        {Aicore: NPUIndex4, Aicpu: NPUIndex4, DVPP: AscendDVPPEnabledNull},
					VNPUTempVir04C3:      {Aicore: NPUIndex4, Aicpu: NPUIndex3, DVPP: AscendDVPPEnabledNull},
					VNPUTempVir04C3NDVPP: {Aicore: NPUIndex4, Aicpu: NPUIndex3, DVPP: AscendDVPPEnabledOff},
					VNPUTempVir04C4cDVPP: {Aicore: NPUIndex4, Aicpu: NPUIndex4, DVPP: AscendDVPPEnabledOn},
				},
			},
			Chips:            make(map[int]*VChip),
			UnhealthyChipIds: make(map[int]struct{}),
			DowngradeCache:   make(map[string]struct{}, MapInitNum),
			ConCache:         make(map[string]map[types.UID]struct{}),
		},
		FrameAttr: VolcanoFrame{
			VJobTemplate: make(map[string]map[string]VResource),
			ConfigParameters: ConfigParameters{
				StaticParameters: StaticParameters{
					OnceInit:       &sync.Once{},
					IsFirstSession: PtrInit(true),
				},
			},
		},
	}
}

// AddResource 在 Pod 调度到节点后，把其占用的 vNPU 资源累加到本地缓存。
//
// 流程：
//  1. 通过 HasDeviceRequest 过滤非 vNPU Pod；
//  2. GetPodResource 解析 Pod 需要的 VResource；
//  3. 读取 Pod 注解 AscendNPUCore，格式为 "chipID" 或 "chipID-template"；
//  4. 若是切分任务，调用 UpdateNodeInfoSegmentWithAdd 更新芯片资源并设置 SegmentFlag；
//     若是整卡任务，调用 UpdateNodeInfoWholeWithAdd 扣除整张卡的资源；
//  5. 把 Pod UID 加入 ConCache，用于模板隔离。
func (ns *NPUDevices) AddResource(pod *v1.Pod) {
	if !ns.HasDeviceRequest(pod) {
		return
	}

	//Upper level mechanism has assured that the pod(task) has been scheduled to this node
	//Judge if the pod has resource request on this device
	podRes, err := ns.GetPodResource(pod)
	if err != nil {
		klog.V(LogErrorLev).Infof("%s require get task resource failed: %s",
			ns.NodeInf.Name, err)
	}

	coreAnnotation, ok := pod.Annotations[AscendNPUCore]
	if !ok {
		return
	}
	ascendNPUCoreSplit := strings.Split(coreAnnotation, "-")

	var allocChipID, chipVTemplate string

	if len(ascendNPUCoreSplit) == 2 {
		allocChipID, chipVTemplate = ascendNPUCoreSplit[0], ascendNPUCoreSplit[1]
		ns.UpdateNodeInfoSegmentWithAdd(allocChipID, podRes)
	} else {
		allocChipID = ascendNPUCoreSplit[0]
		ns.UpdateNodeInfoWholeWithAdd(allocChipID)
	}

	if addErr := ns.addTaskInConCache(pod, podRes, chipVTemplate); addErr != nil {
		klog.V(LogErrorLev).Infof("dynamic vnpu %s addResource addTaskInConCache:%s", pod.Name, addErr)
	}
}

// SubResource 在 Pod 删除或释放时，把其占用的 vNPU 资源从本地缓存释放。
//
// 逻辑与 AddResource 对称：根据 AscendNPUCore 注解判断是切分还是整卡，
// 分别调用 UpdateNodeInfoSegmentWithSub / UpdateNodeInfoWholeWithSub，
// 并从 ConCache 中移除 Pod UID。
func (ns *NPUDevices) SubResource(pod *v1.Pod) {
	if !ns.HasDeviceRequest(pod) {
		return
	}

	//Upper level mechanism has assured that the pod(task) has been scheduled to this node
	podRes, err := ns.GetPodResource(pod)
	if err != nil {
		klog.V(LogErrorLev).Infof("%s require get task resource failed: %s",
			ns.NodeInf.Name, err)
	}

	coreAnnotation, ok := pod.Annotations[AscendNPUCore]
	if !ok {
		return
	}

	ascendNPUCoreSplit := strings.Split(coreAnnotation, "-")

	var allocChipID, chipVTemplate string

	if len(ascendNPUCoreSplit) == 2 {
		allocChipID, chipVTemplate = ascendNPUCoreSplit[0], ascendNPUCoreSplit[1]
		ns.UpdateNodeInfoSegmentWithSub(allocChipID, podRes)
	} else {
		allocChipID = ascendNPUCoreSplit[0]
		ns.UpdateNodeInfoWholeWithSub(allocChipID)
	}

	if addErr := ns.releaseTaskInConCache(pod, podRes, chipVTemplate); addErr != nil {
		klog.V(LogErrorLev).Infof("dynamic vnpu %s addResource addTaskInConCache:%s", pod.Name, addErr)
	}
}

// AddQueueResource 返回队列资源增量，当前 MindCluster vNPU 未实现。
func (ns *NPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	return map[string]float64{}
}

// HasDeviceRequest 判断 Pod 是否请求了 MindCluster vNPU。
//
// 只有当 AscendMindClusterVNPUEnable 开启，且 Pod 同时满足：
//  - label "ring-controller.atlas" == "ascend-310P"；
//  - 容器 limits 中包含 "huawei.com/npu-core"；
// 才返回 true。
func (ns *NPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	if AscendMindClusterVNPUEnable && checkVNPUResourcesInPod(pod) {
		return true
	}
	return false
}

// FilterNode 是 deviceShare 插件的节点过滤入口。
//
// 执行两步检查：
//  1. preCheckNodePredicate：检查节点是否处于 PreSeparate 等不可用状态、节点芯片数是否足够；
//  2. CheckNodeNPUByPod：检查是否存在满足资源、DVPP、vGroup、模板隔离的芯片。
// 任意一步失败即返回 devices.Error。
func (ns *NPUDevices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	if err := ns.preCheckNodePredicate(pod); err != nil {
		return devices.Error, "preCheckNodePredicate failure", err
	}
	if err := ns.CheckNodeNPUByPod(pod); err != nil {
		// node doesn't have enough npu for the task
		klog.V(LogDebugLev).Infof("checkNPUByTask %s:%s ,cannot be selected.", ns.NodeInf.Name, SafePrint(err))
		return devices.Error, "", err
	}

	return devices.Success, "", nil
}

// ScoreNode 返回节点得分，当前 MindCluster vNPU 把排序策略交给 deviceShare 插件处理，
// 因此直接返回 0。
func (ns *NPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	// implement in deviceShare plugin score policy
	return 0
}

// Allocate 为 Pod 实际分配芯片并通过 JSON Patch 写回 Pod 注解。
//
// 流程：
//  1. 解析 Pod 资源需求 VResource；
//  2. 若该 Pod 在 DowngradeCache 中，调用 downgradeTaskAICPU 降级 AI CPU；
//  3. SelectChipFromNode 选择最佳芯片；
//  4. SetNPUTopologyToPodFn 把分配结果（"chipID" 或 "chipID-template"）通过 Patch 写入 Pod。
func (ns *NPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	klog.V(4).Infoln("DeviceSharing:Into AllocateToPod", pod.Name)
	if ns == nil {
		klog.V(LogDebugLev).Infof("UseAnnotation failed: %s", ArgumentError)
		return errors.New(ArgumentError)
	}

	podResReq, err := ns.GetPodResource(pod)
	if err != nil {
		klog.V(LogErrorLev).Infof("%s UseAnnotation get require task resource failed: %s", ns.Name, err)
		return err
	}

	_, ok := ns.DowngradeCache[pod.Name]
	if ok {
		podResReq = ns.downgradeTaskAICPU(podResReq)
	}

	allocChipID, err := ns.SelectChipFromNode(podResReq)
	if err != nil {
		klog.V(LogErrorLev).Infof("UseAnnotation dynamic %s on %s err: %s", pod.Name, ns.NodeInf.Name, err)
		return err
	}
	klog.V(LogDebugLev).Infof("dynamic vnpu UseAnnotation allocChipID:<%s>", allocChipID)
	ns.SetNPUTopologyToPodFn(kubeClient, pod, podResReq, allocChipID, ns.VT)
	return nil
}

// Release 预留接口，当前 MindCluster vNPU 未实现显式释放，资源扣减由 SubResource 处理。
func (ns *NPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	return nil
}

// GetStatus 返回设备状态字符串，当前未实现。
func (ns *NPUDevices) GetStatus() string {
	return ""
}

// GetIgnoredDevices return device names which wish vc-scheduler to ignore
func (ns *NPUDevices) GetIgnoredDevices() []string {
	return []string{""}
}

// DeepCopy 返回 NPUDevices 的深拷贝，用于 deviceShare 插件的 dry-run 模拟。
//
// 深拷贝会递归复制 NodeInf 中的各类 map、NPUDevice 中的芯片、模板、缓存等，
// 避免模拟调度污染真实缓存。
func (ns *NPUDevices) DeepCopy() interface{} {
	if ns == nil {
		return nil
	}
	cp := &NPUDevices{
		Name:      ns.Name,
		FrameAttr: ns.FrameAttr,
	}

	// Deep copy NodeInf maps
	cp.NodeInf = NodeInf{
		Name:              ns.NodeInf.Name,
		BaseDeviceInfo:    ns.NodeInf.BaseDeviceInfo,
		Address:           ns.NodeInf.Address,
		SuperPodID:        ns.NodeInf.SuperPodID,
		DevInfoUpdateTime: ns.NodeInf.DevInfoUpdateTime,
		Capability:        make(map[v1.ResourceName]float64, len(ns.NodeInf.Capability)),
		Allocate:          make(map[v1.ResourceName]float64, len(ns.NodeInf.Allocate)),
		Idle:              make(map[v1.ResourceName]float64, len(ns.NodeInf.Idle)),
		Annotation:        make(map[string]string, len(ns.NodeInf.Annotation)),
		Label:             make(map[string]string, len(ns.NodeInf.Label)),
	}
	for k, v := range ns.NodeInf.Capability {
		cp.NodeInf.Capability[k] = v
	}
	for k, v := range ns.NodeInf.Allocate {
		cp.NodeInf.Allocate[k] = v
	}
	for k, v := range ns.NodeInf.Idle {
		cp.NodeInf.Idle[k] = v
	}
	for k, v := range ns.NodeInf.Annotation {
		cp.NodeInf.Annotation[k] = v
	}
	for k, v := range ns.NodeInf.Label {
		cp.NodeInf.Label[k] = v
	}

	// Deep copy NPUDevice
	cp.NPUDevice = NPUDevice{
		VT: VTemplate{
			Temp: ns.NPUDevice.VT.Temp,
			Data: make(map[string]VResource, len(ns.NPUDevice.VT.Data)),
		},
		ChipKind:         ns.NPUDevice.ChipKind,
		ServerType:       ns.NPUDevice.ServerType,
		TotalChipNum:     ns.NPUDevice.TotalChipNum,
		AiCorePerChip:    ns.NPUDevice.AiCorePerChip,
		FreeChipNum:      ns.NPUDevice.FreeChipNum,
		TotalRes:         ns.NPUDevice.TotalRes,
		ValidVNode:       ns.NPUDevice.ValidVNode,
		ChipType:         ns.NPUDevice.ChipType,
		Chips:            make(map[int]*VChip, len(ns.NPUDevice.Chips)),
		UnhealthyChipIds: make(map[int]struct{}, len(ns.NPUDevice.UnhealthyChipIds)),
		DowngradeCache:   make(map[string]struct{}, len(ns.NPUDevice.DowngradeCache)),
		ConCache:         make(map[string]map[types.UID]struct{}, len(ns.NPUDevice.ConCache)),
	}
	for k, v := range ns.NPUDevice.VT.Data {
		cp.NPUDevice.VT.Data[k] = v
	}
	for k := range ns.NPUDevice.UnhealthyChipIds {
		cp.NPUDevice.UnhealthyChipIds[k] = struct{}{}
	}
	for k := range ns.NPUDevice.DowngradeCache {
		cp.NPUDevice.DowngradeCache[k] = struct{}{}
	}
	for k, inner := range ns.NPUDevice.ConCache {
		newInner := make(map[types.UID]struct{}, len(inner))
		for uid := range inner {
			newInner[uid] = struct{}{}
		}
		cp.NPUDevice.ConCache[k] = newInner
	}
	for id, chip := range ns.NPUDevice.Chips {
		newChip := &VChip{
			Name:        chip.Name,
			Kind:        chip.Kind,
			IsDual:      chip.IsDual,
			Unstable:    chip.Unstable,
			CoreNum:     chip.CoreNum,
			SegmentFlag: chip.SegmentFlag,
			TotalRes:    chip.TotalRes,
			UsedRes:     chip.UsedRes,
			FreeRes:     chip.FreeRes,
			ID:          make([]string, len(chip.ID)),
			PodMap:      make(map[string]*v1.Pod, len(chip.PodMap)),
		}
		copy(newChip.ID, chip.ID)
		for uid, pod := range chip.PodMap {
			newChip.PodMap[uid] = pod
		}
		cp.NPUDevice.Chips[id] = newChip
	}

	return cp
}
