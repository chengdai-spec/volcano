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

package vnpu

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// addTaskInConCache 把 Pod UID 记录到指定 vNPU 模板的并发缓存中。
//
// ConCache 的结构是 map[templateName]map[podUID]struct{}，用于实现模板隔离：
// 同一节点上同时只能运行一种 vNPU 模板的任务（详见 IsNodeHasDifferentUnFinishedTask）。
// 如果 Pod UID 已存在则直接返回，避免重复记录。
func (ns *NPUDevices) addTaskInConCache(pod *v1.Pod, taskResReq VResource, chipVTemplate string) error {
	if ns == nil {
		return fmt.Errorf("addTaskInConCache failed:%s", ArgumentError)
	}

	date := ns.ConCache
	if date == nil {
		date = make(map[string]map[types.UID]struct{})
	}

	temp, ok := date[chipVTemplate]
	if !ok {
		temp = make(map[types.UID]struct{}, MapInitNum)
	}
	_, ok = temp[pod.UID]
	if ok {
		return nil
	}
	temp[pod.UID] = struct{}{}
	date[chipVTemplate] = temp
	ns.ConCache = date
	klog.V(LogDebugLev).Infof("addTaskInConCache %s %s ConCache: %v", ns.NodeInf.Name, pod.Name, ns.ConCache)
	return nil
}

// releaseTaskInConCache 从指定 vNPU 模板的并发缓存中移除 Pod UID。
//
// 当某个模板下没有任何 Pod 时，会删除该模板条目，释放模板隔离锁。
func (ns *NPUDevices) releaseTaskInConCache(pod *v1.Pod, taskResReq VResource, chipVTemplate string) error {
	if ns == nil {
		return fmt.Errorf("releaseTaskInConCache failed:%s", ArgumentError)
	}

	temp := ns.ConCache
	if temp == nil {
		return fmt.Errorf("template %s not in %s ConCache", chipVTemplate, ns.NodeInf.Name)
	}
	tIDs, ok := temp[chipVTemplate]
	if !ok {
		return fmt.Errorf("template %s not in %s ConCache", chipVTemplate, ns.NodeInf.Name)
	}
	if _, ok := tIDs[pod.UID]; !ok {
		return fmt.Errorf("tID %s not in %s %s ConCache", pod.UID, chipVTemplate, ns.NodeInf.Name)
	}
	delete(tIDs, pod.UID)
	if len(tIDs) == 0 {
		delete(temp, chipVTemplate)
		ns.ConCache = temp
		return nil
	}
	temp[chipVTemplate] = tIDs
	ns.ConCache = temp
	return nil
}

// GetTemplateByResReq 根据资源请求在模板表中查找匹配的模板名。
//
// 匹配条件：Aicore、Aicpu、DVPP 三者完全一致。如果没有找到，返回错误。
// 例如请求 {Aicore:2, Aicpu:1, DVPP:"null"} 会匹配到 vir02_1c。
func (ns *NPUDevices) GetTemplateByResReq(taskResReq VResource, vt VTemplate) (string, error) {
	if ns == nil {
		return "", fmt.Errorf("getTemplateByResReq failed:%s", ArgumentError)
	}

	name := ""
	for tName, value := range vt.Data {
		if value.Aicore != taskResReq.Aicore {
			continue
		}
		if value.Aicpu != taskResReq.Aicpu {
			continue
		}
		if value.DVPP != taskResReq.DVPP {
			continue
		}
		name = tName
	}
	if name == "" {
		return "", fmt.Errorf("%#v not get template", taskResReq)
	}
	return name, nil
}

// UpdateNodeInfoSegmentWithAdd 在切分任务分配后，更新物理芯片的已用/空闲资源。
//
// 找到 allocChipID 对应的芯片后：
//  - UsedRes 加上 taskResReq；
//  - FreeRes 减去 taskResReq；
//  - 如果请求不是整卡，标记 SegmentFlag=true，表示该芯片已被切分；
//  - 调用 UpdateDVPP 更新 DVPP 状态。
func (ns *NPUDevices) UpdateNodeInfoSegmentWithAdd(allocChipID string, taskResReq VResource) {
	if ns == nil {
		klog.V(LogErrorLev).Infof("UpdateNodeInfoSegmentWithAdd error : %s", ArgumentError)
		return
	}

	for chipID, chip := range ns.Chips {
		if strconv.Itoa(chipID) != allocChipID {
			continue
		}
		chip.UsedRes.Add(taskResReq)
		chip.FreeRes.Sub(taskResReq)
		if !ns.IsResourceWholeCard(taskResReq.Aicore) {
			chip.SegmentFlag = true
		}
		chip.UpdateDVPP(taskResReq.DVPP)
	}
	klog.V(LogInfoLev).Infof("dynamic vnpu UpdateNodeInfo node <%s> chip resource updated", ns.NodeInf.Name)
}

// UpdateNodeInfoSegmentWithSub 在切分任务释放后，恢复物理芯片的已用/空闲资源。
//
// 与 UpdateNodeInfoSegmentWithAdd 对称：UsedRes 减、FreeRes 加、ResetDVPP。
// 注释掉的 SegmentFlag 设置说明释放时不重置切分标记，避免影响其他正在运行的 vNPU。
func (ns *NPUDevices) UpdateNodeInfoSegmentWithSub(allocChipID string, taskResReq VResource) {
	if ns == nil {
		klog.V(LogErrorLev).Infof("UpdateNodeInfoSegmentWithSub error : %s", ArgumentError)
		return
	}

	for chipID, chip := range ns.Chips {
		if strconv.Itoa(chipID) != allocChipID {
			continue
		}
		chip.UsedRes.Sub(taskResReq)
		chip.FreeRes.Add(taskResReq)
		//if !ns.IsResourceWholeCard(taskResReq.Aicore) {
		//	chip.SegmentFlag = true
		//}
		chip.ResetDVPP(taskResReq.DVPP)
	}
	klog.V(LogInfoLev).Infof("dynamic vnpu UpdateNodeInfo node <%s> chip resource updated", ns.NodeInf.Name)
}

// UpdateNodeInfoWholeWithAdd 在整卡任务分配后，扣除整张卡的资源。
//
// 计算单卡资源 chipRes：Aicore=AiCorePerChip，Aicpu=TotalRes.Aicpu/TotalChipNum，DVPP=null。
// 对 allocChipIDs 列表中的每张卡，UsedRes 加、FreeRes 减、UpdateDVPP。
func (ns *NPUDevices) UpdateNodeInfoWholeWithAdd(allocChipIDs string) {
	if ns == nil {
		klog.V(LogErrorLev).Infof("UpdateNodeInfoWholeWithAdd error : %s", ArgumentError)
		return
	}
	if ns.TotalChipNum == 0 {
		klog.V(LogErrorLev).Infof("UpdateNodeInfoWhole node <%s> total chip number equal zero", ns.NodeInf.Name)
		return
	}

	chipRes := VResource{
		Aicore: ns.AiCorePerChip,
		Aicpu:  ns.TotalRes.Aicpu / ns.TotalChipNum,
		DVPP:   AscendDVPPEnabledNull,
	}
	allocChipIDList := strings.Split(allocChipIDs, ",")
	for _, allocChipID := range allocChipIDList {
		for chipID, chip := range ns.Chips {
			if strconv.Itoa(chipID) != allocChipID {
				continue
			}
			chip.UsedRes.Add(chipRes)
			chip.FreeRes.Sub(chipRes)
			chip.UpdateDVPP(chipRes.DVPP)
		}
	}
}

// UpdateNodeInfoWholeWithSub 在整卡任务释放后，恢复整张卡的资源。
func (ns *NPUDevices) UpdateNodeInfoWholeWithSub(allocChipIDs string) {
	if ns == nil {
		klog.V(LogErrorLev).Infof("UpdateNodeInfoWholeWithSub error : %s", ArgumentError)
		return
	}
	if ns.TotalChipNum == 0 {
		klog.V(LogErrorLev).Infof("UpdateNodeInfoWhole node <%s> total chip number equal zero", ns.NodeInf.Name)
		return
	}

	chipRes := VResource{
		Aicore: ns.AiCorePerChip,
		Aicpu:  ns.TotalRes.Aicpu / ns.TotalChipNum,
		DVPP:   AscendDVPPEnabledNull,
	}
	allocChipIDList := strings.Split(allocChipIDs, ",")
	for _, allocChipID := range allocChipIDList {
		for chipID, chip := range ns.Chips {
			if strconv.Itoa(chipID) != allocChipID {
				continue
			}
			chip.UsedRes.Sub(chipRes)
			chip.FreeRes.Add(chipRes)
			chip.ResetDVPP(chipRes.DVPP)
		}
	}
}

// downgradeTaskAICPU 在资源不足时降低任务的 AI CPU 需求。
//
// 降级策略：
//  - 2 核 2 CPU -> 2 核 1 CPU；
//  - 4 核 4 CPU 且未开启 DVPP -> 4 核 3 CPU。
//
// 这种降级可以在不减少算力（Aicore 不变）的情况下，缓解 AI CPU 瓶颈，
// 提高调度成功率，但可能影响控制面性能。
func (ns *NPUDevices) downgradeTaskAICPU(podResReq VResource) VResource {
	if ns == nil {
		klog.V(LogErrorLev).Infof("downgradeTaskAICPU error : %s", ArgumentError)
		return podResReq
	}
	if podResReq.Aicore == NPUIndex2 {
		return VResource{
			Aicore: podResReq.Aicore,
			Aicpu:  NPUIndex1,
			DVPP:   podResReq.DVPP,
		}
	}
	if podResReq.Aicore == NPUIndex4 {
		return VResource{
			Aicore: podResReq.Aicore,
			Aicpu:  NPUIndex3,
			DVPP:   podResReq.DVPP,
		}
	}
	return podResReq
}

// GetPodResource 把 Pod 的资源声明转换为 MindCluster 内部使用的 VResource。
//
// 流程：
//  1. 从容器 limits 读取 huawei.com/npu-core 数量，得到 coreNum；
//  2. 如果 coreNum 是整卡倍数，按整卡计算 Aicpu 比例，DVPP="null"；
//  3. 否则读取 label vnpu-dvpp 与 vnpu-level，通过 getResTemplateFromTaskSetting
//     选择模板，再从 ns.VT.Data 中取出对应 VResource。
//
// 实际案例：
//  Pod 请求 2 核，label vnpu-level=low，vnpu-dvpp=null，
//  则选择 vir02_1c 模板，返回 {Aicore:2, Aicpu:1, DVPP:"null"}。
func (ns *NPUDevices) GetPodResource(pod *v1.Pod) (VResource, error) {
	if ns == nil || pod == nil {
		return VResource{}, fmt.Errorf("GetPodResource error : %s", ArgumentError)
	}

	coreNum, err := ns.getAiCoreNumFromPod(pod)
	if err != nil {
		return VResource{}, fmt.Errorf("task %s AscendNPUCore read failed", pod.Name)
	}
	tempCore := ns.TotalRes.Aicore
	if tempCore == 0 {
		klog.V(LogInfoLev).Infof("%s not inital for Aicore is 0", ns.NodeInf.Name)
		//return VResource{}, fmt.Errorf("%s not inital for Aicore is 0", ns.NodeInf.Name)
		return VResource{}, nil
	}
	if ns.IsResourceWholeCard(coreNum) {
		res := VResource{
			Aicore: coreNum,
			Aicpu:  coreNum * ns.TotalRes.Aicpu / tempCore,
			DVPP:   "null",
		}
		return res, nil
	}

	dvpp, err := ns.GetVTaskDVPP(pod)
	if err != nil {
		return VResource{}, err
	}

	cpuLevel := ns.GetVTaskLevel(pod)

	virTemplate := getResTemplateFromTaskSetting(coreNum, cpuLevel, dvpp)

	taskReqRes := ns.VT.Data[virTemplate]
	return taskReqRes, nil
}

// GetVTaskDVPP 从 Pod label 中读取 DVPP 开关。
//
// 合法值为 yes/no/null，缺失时使用默认值 null。
// 非法值会返回错误，阻止调度到该 Pod。
func (ns *NPUDevices) GetVTaskDVPP(pod *v1.Pod) (string, error) {
	if ns == nil || pod == nil {
		return "", fmt.Errorf("GetVTaskDVPP error : %s", ArgumentError)
	}

	dvpp, ok := pod.Labels[AscendVNPUDVPP]
	if !ok {
		klog.V(LogWarningLev).Infof("%s not set VNPU dvpp, use default null.", pod.Name)
		return AscendDVPPEnabledNull, nil
	}
	switch dvpp {
	case AscendDVPPEnabledOff, AscendDVPPEnabledNull, AscendDVPPEnabledOn:
		break
	default:
		klog.V(LogWarningLev).Infof("%s set wrong dvpp %s.", pod.Name, dvpp)
		return "", fmt.Errorf("err dvpp value:%s", dvpp)
	}
	return dvpp, nil
}

// GetVTaskLevel 从 Pod label 中读取 VNPU level。
//
// 合法值为 low/high，缺失时使用默认值 low。非法值会被修正为 low。
// level 影响 AI CPU 数量选择：low 会选择更少的 AI CPU，high 选择完整 AI CPU。
func (ns *NPUDevices) GetVTaskLevel(pod *v1.Pod) string {
	if ns == nil || pod == nil {
		klog.V(LogErrorLev).Infof("GetVTaskLevel error : %s", ArgumentError)
		return ""
	}

	cpuLevel, ok := pod.Labels[AscendVNPULevel]
	if !ok {
		klog.V(LogWarningLev).Infof("%s not set VNPU level, use default low.", pod.Name)
		return AscendVNPULevelLow
	}
	switch cpuLevel {
	case AscendVNPULevelLow, AscendVNPULevelHigh:
		break
	default:
		klog.V(LogWarningLev).Infof("%s set wrong VNPU level %s, use default low.", pod.Name, cpuLevel)
		cpuLevel = AscendVNPULevelLow
	}
	return cpuLevel
}

// IsResourceWholeCard 判断请求的 AI Core 数是否对应整卡。
//
// 通过 ServerType 中的核心数（如 Ascend310P-10-dual 中的 10）计算单卡核心数，
// 如果 coreNum 能被单卡核心数整除，则认为是整卡请求。
func (ns *NPUDevices) IsResourceWholeCard(aiCore int) bool {
	if ns == nil {
		klog.V(4).Infof("IsResourceWholeCard failed: %s", "invalid argument")
		return false
	}
	chipCoreNum, err := ns.getVChipCoreNum()
	if err != nil || chipCoreNum == 0 {
		klog.V(2).Infof("IsResourceWholeCard get chipCoreNum failed or zero number")
		return false
	}
	return aiCore%chipCoreNum == 0
}

// getVChipCoreNum 从 ServerType 字符串中解析单卡核心数。
//
// 例如 ServerType="Ascend310P-10-dual"，解析后得到 10。
func (ns *NPUDevices) getVChipCoreNum() (int, error) {
	if ns == nil {
		return 0, fmt.Errorf("getVChipCoreNum failed: %s", "invalid argument")
	}
	serverTypeSplit := strings.Split(ns.ServerType, "-")
	if len(serverTypeSplit) < 2 {
		return 0, fmt.Errorf("getVChipCoreNum serverType %s format error", ns.ServerType)
	}
	coreNum, err := strconv.Atoi(serverTypeSplit[1])
	if err != nil {
		return 0, fmt.Errorf("getVChipCoreNum serverType %s split error", ns.ServerType)
	}
	return coreNum, nil
}

// getAiCoreNumFromPod 从 Pod 的容器资源请求中读取 npu-core 数量。
//
// 读取的是 resources.requests["huawei.com/npu-core"] 或 limits 中的值。
// 如果找不到或值为 0，返回 0，不会报错（避免原始逻辑在 Pod 日志中产生大量无效错误）。
func (ns *NPUDevices) getAiCoreNumFromPod(pod *v1.Pod) (int, error) {
	if ns == nil {
		return 0, fmt.Errorf("getAiCoreNumFromPod failed: %s", "invalid argument")
	}

	for _, container := range pod.Spec.Containers {
		coreNum, ok := container.Resources.Requests["huawei.com/npu-core"]
		if !ok {
			continue
		}
		if coreNum.Value() == 0 {
			continue
		}
		return int(coreNum.Value()), nil
	}

	return 0, nil

	// original npu scheduling logic may lead to too much inval error in pod's log file
	//return 0, fmt.Errorf("getAiCoreNumFromTask get resource requests failed")
}

// preCheckNodePredicate 在节点过滤前做前置检查。
//
// 当前检查项：
//  - 如果节点被 nodeD 报告为 PreSeparate 状态，则不可调度；
//  - 检查节点芯片总数是否满足 Pod 请求。
func (ns *NPUDevices) preCheckNodePredicate(pod *v1.Pod) error {
	nodeHealthyStatusByNodeD := ns.Annotation[NodedNodeHealtyStatuskey]
	if nodeHealthyStatusByNodeD == PreSeparateFaultCode {
		klog.V(LogDebugLev).Infof("NodePredicate %s failed, cause node is %s.", ns.NodeInf.Name,
			nodeHealthyStatusByNodeD)
		return fmt.Errorf("node is %s, due to nodeD reported node status", nodeHealthyStatusByNodeD)
	}

	// vNPU job no need to check
	//if err := ns.checkNPUResourceStable(pod); err != nil {
	//	return err
	//}
	if err := ns.checkNodeNum(pod); err != nil {
		return err
	}
	return nil
}

// checkNodeNum 检查节点空闲芯片数量是否满足任务需求。
//
// ns.Idle[AscendNPUCore] 中保存的是 millicore 为单位的空闲 npu-core 数量，
// 除以 NPUHexKilo(1000) 后得到实际核数。若实际核数 < reqNPUNum，返回错误。
//
// 实际案例：
//  节点空闲 npu-core 为 4000（millicore），Pod 请求 2 核，
//  4000/1000=4 >= 2，通过；若请求 8 核则不通过。
func (ns *NPUDevices) checkNodeNum(pod *v1.Pod) error {
	if ns == nil {
		return errors.New(objectNilError)
	}

	nodeNPUNum, ok := ns.Idle[AscendNPUCore]
	klog.V(3).Infof("DEBUG: nodeNPUNum millicore from ns.Idle[huawei.com/npu-core]: %f", nodeNPUNum)
	klog.V(3).Infof("DEBUG: nodeNPUNum from ns.Idle[huawei.com/npu-core]: %d", int(nodeNPUNum/NPUHexKilo))
	reqNPUNum, err := ns.getAiCoreNumFromPod(pod)
	if err != nil {
		return fmt.Errorf("failed to get requested NPU number from pod: %v", err)
	}
	if !ok {
		return fmt.Errorf("not have %s", AscendNPUCore)
	}
	if int(nodeNPUNum/NPUHexKilo) < reqNPUNum {
		return fmt.Errorf("node not meet task request %s:%d", AscendNPUCore, reqNPUNum)
	}
	return nil
}

// CheckNodeNPUByPod 检查节点上是否存在满足任务需求的芯片。
//
// 内部调用 GetPodResource 得到 VResource，再交给 CheckNodeNPUByDyPod 做
// 资源、DVPP、vGroup、模板隔离等详细检查。
func (ns *NPUDevices) CheckNodeNPUByPod(pod *v1.Pod) error {
	if ns == nil || pod == nil {
		return errors.New(ArgumentError)
	}
	taskRes, err := ns.GetPodResource(pod)
	if err != nil {
		return err
	}
	return ns.CheckNodeNPUByDyPod(pod, taskRes)
}

// CheckNodeNPUByDyPod 是动态 vNPU 调度的核心过滤逻辑。
//
// 检查项：
//  1. 节点必须是有效 vNode（ValidVNode）；
//  2. 节点总资源与单芯片资源是否都足够（IsNodeNotMeetRes）；
//  3. 若节点资源不足但任务可被降级（taskAICPUCanBeDowngrade），则记录 DowngradeCache
//     并递归检查降级后的资源；
//  4. 检查模板隔离：同一节点上不能同时运行不同 vNPU 模板的任务
//     （IsNodeHasDifferentUnFinishedTask）。
//
// 实际案例：
//  节点已运行一个 vir04 模板任务，此时再来一个 vir02_1c 任务，
//  IsNodeHasDifferentUnFinishedTask 会返回错误，调度器会选择其他节点，
//  避免不同模板混跑导致设备插件配置冲突。
func (ns *NPUDevices) CheckNodeNPUByDyPod(pod *v1.Pod, taskResReq VResource) error {
	if ns == nil || pod == nil {
		klog.V(LogDebugLev).Infof("CheckNodeNPUByDyTask failed: %s", ArgumentError)
		return errors.New(ArgumentError)
	}
	klog.V(LogDebugLev).Infof("check dynamic vNPU %s on %s", pod.Name, ns.NodeInf.Name)
	if !ns.ValidVNode {
		klog.V(LogInfoLev).Infof("dynamic vNPU node<%s> not valid vNode", ns.NodeInf.Name)
		return errors.New("checkNodeNPUByDyTask invalid VNode")
	}
	if ns.IsNodeNotMeetRes(taskResReq) {
		// if node resource not enough, reduce task aiCPU
		if ns.taskAICPUCanBeDowngrade(taskResReq) {
			klog.V(LogInfoLev).Infof("dynamic vnpu task<%s> resource not enough, downgrade cpu", pod.Name)
			ns.DowngradeCache[pod.Name] = struct{}{}
			return ns.CheckNodeNPUByDyPod(pod, ns.downgradeTaskAICPU(taskResReq))
		}
	}
	if diffErr := ns.IsNodeHasDifferentUnFinishedTask(pod, taskResReq); diffErr != nil {
		return diffErr
	}
	klog.V(LogInfoLev).Infof("dynamic vnpu task<%s> CheckNodeNPUByDyTask node<%s> ok", pod.Name, ns.NodeInf.Name)
	return nil
}

// IsNodeNotMeetRes 判断节点是否不满足资源需求。
//
// 节点资源不足有两种可能：
//  - 节点总资源不足（isNodeTotalResEnough）：所有芯片 FreeRes 之和小于请求；
//  - 单芯片资源不足（isNodeChipResEnough）：没有任意一张芯片能单独容纳请求。
func (ns *NPUDevices) IsNodeNotMeetRes(podResReq VResource) bool {
	return !ns.isNodeTotalResEnough(podResReq) || !ns.isNodeChipResEnough(podResReq)
}

// isNodeTotalResEnough 判断节点所有稳定芯片的剩余资源之和是否满足请求。
//
// Unstable（不稳定）芯片会被跳过。
func (ns *NPUDevices) isNodeTotalResEnough(vRes VResource) bool {
	var nodeResFree VResource
	for _, chip := range ns.Chips {
		if chip.Unstable {
			klog.V(LogDebugLev).Infof("chip <%s> unstable, resource exempted", chip.Name)
		}
		nodeResFree.Add(chip.FreeRes)
	}
	return nodeResFree.BeGreater(vRes)
}

// isNodeChipResEnough 判断是否存在至少一张芯片能满足任务资源需求。
//
// 如果是整卡请求，调用 isNodeChipResEnoughWholeCard 检查空闲整卡数量；
// 否则逐张芯片检查 isChipMeetResReq。
func (ns *NPUDevices) isNodeChipResEnough(vRes VResource) bool {
	if ns.IsResourceWholeCard(vRes.Aicore) {
		return ns.isNodeChipResEnoughWholeCard(vRes)
	}
	for _, vChip := range ns.Chips {
		if !vChip.isChipMeetResReq(vRes) || vChip.Unstable {
			klog.V(LogDebugLev).Infof("vChip %s does not meet resource requirements", vChip.Name)
			continue
		}
		return true
	}
	return false
}

// isNodeChipResEnoughWholeCard 检查节点上是否有足够数量的空闲整卡。
//
// 空闲整卡的条件：SegmentFlag==false 且 FreeRes.Aicore>0。
// 需要空闲整卡数量 >= vRes.Aicore / AiCorePerChip。
func (ns *NPUDevices) isNodeChipResEnoughWholeCard(vRes VResource) bool {
	if ns.AiCorePerChip == 0 {
		return false
	}
	freeWholeCard := 0
	for _, vChip := range ns.Chips {
		if vChip.SegmentFlag || vChip.FreeRes.Aicore == 0 {
			continue
		}
		freeWholeCard += 1
	}
	return vRes.Aicore/ns.AiCorePerChip <= freeWholeCard
}

// IsNodeHasDifferentUnFinishedTask 实现模板隔离策略。
//
// 同一节点上同时只能运行一种 vNPU 模板，这是 MindCluster 设备插件的约束。
// 判断逻辑：
//  - 若 ConCache 为空，直接通过；
//  - 若 ConCache 中只有一项且就是当前任务模板，直接通过；
//  - 否则返回错误，拒绝调度。
//
// 该策略保证节点上的芯片不会在不同 vNPU 模板间反复切换，减少设备插件重置开销。
func (ns *NPUDevices) IsNodeHasDifferentUnFinishedTask(pod *v1.Pod, podResReq VResource) error {
	if ns == nil || pod == nil {
		klog.V(LogDebugLev).Infof("IsNodeHasDifferentUnFinishedTask failed :%s", ArgumentError)
		return errors.New(ArgumentError)
	}
	klog.V(LogDebugLev).Infof("%s IsNodeHasDifferentUnFinishedTask cache :%v", pod.Name, ns.ConCache)
	nodeTempMap := ns.ConCache
	if len(nodeTempMap) == 0 {
		klog.V(LogDebugLev).Infof("%s IsNodeHasDifferentUnFinishedTask cache no node %s, ok.",
			pod.Name, ns.NodeInf.Name)
		return nil
	}
	template, getErr := ns.GetTemplateByResReq(podResReq, ns.VT)
	if getErr != nil {
		klog.V(LogDebugLev).Infof("IsNodeHasDifferentUnFinishedTask %s", getErr)
		return getErr
	}
	if len(nodeTempMap) == 1 {
		_, tOK := nodeTempMap[template]
		if tOK {
			klog.V(LogDebugLev).Infof("%s IsNodeHasDifferentUnFinishedTask cache no template:%s, ok.",
				pod.Name, template)
			return nil
		}
	}

	return fmt.Errorf("%s is using %s, and not rewrite", pod.Name, ns.NodeInf.Name)
}

// taskAICPUCanBeDowngrade 判断任务是否可以进行 AI CPU 降级。
//
// 可降级场景：
//  - 2 核任务且当前 AI CPU 为 2；
//  - 4 核任务且当前 AI CPU 为 4 且未开启 DVPP。
func (ns *NPUDevices) taskAICPUCanBeDowngrade(podResReq VResource) bool {
	if podResReq.Aicore == NPUIndex2 && podResReq.Aicpu == NPUIndex2 {
		return true
	}
	if podResReq.Aicore == NPUIndex4 && podResReq.Aicpu == NPUIndex4 && podResReq.DVPP != AscendDVPPEnabledOn {
		return true
	}

	return false
}

// SetNPUTopologyToPodFn 把调度器选中的芯片信息通过 JSON Patch 写回 Pod 注解。
//
// 写入的注解：
//  - PodPredicateTime：当前时间戳（纳秒），供设备插件判断分配是否过期；
//  - AscendNPUCore：
//      整卡任务 -> "chipID"（如 "0"）；
//      切分任务 -> "chipID-template"（如 "0-vir02_1c"）。
//
// 设备插件在容器启动时读取 AscendNPUCore 注解，到对应芯片上创建/绑定 vNPU。
func (ns *NPUDevices) SetNPUTopologyToPodFn(kubeClient kubernetes.Interface, pod *v1.Pod, podResReq VResource, allocChipID string, chipVTemplate VTemplate) {
	if ns == nil || pod == nil {
		klog.V(LogDebugLev).Infof("SetNPUTopologyToPodFn failed: %s", ArgumentError)
		return
	}
	tmp := strconv.FormatInt(time.Now().UnixNano(), Base10)
	pod.Annotations[PodPredicateTime] = tmp
	// 1. whole card
	if ns.IsResourceWholeCard(podResReq.Aicore) {
		pod.Annotations[AscendNPUCore] = allocChipID

		patch := AddNPUAllocationPatch(allocChipID, "", tmp)
		_, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
		if err != nil {
			klog.V(LogErrorLev).Infof("patch pod %s failed: %v", pod.Name, err)
		}

		klog.V(LogInfoLev).Infof("dynamic vnpu setNPUTopologyToPod %s top:%s.", pod.Name, allocChipID)
		return
	}

	// 2. segment task: find matched template name and write "chipID-template"
	for curTemplate, jobVResource := range chipVTemplate.Data {
		if podResReq != jobVResource {
			continue
		}
		pod.Annotations[AscendNPUCore] = fmt.Sprintf("%s-%s", allocChipID, curTemplate)

		patch := AddNPUAllocationPatch(allocChipID, curTemplate, tmp)
		_, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
		if err != nil {
			klog.V(LogErrorLev).Infof("patch pod %s failed: %v", pod.Name, err)
		}

		klog.V(LogInfoLev).Infof("dynamic vnpu setNPUTopologyToPod %s top:%s.", pod.Name,
			pod.Annotations[AscendNPUCore])
		return
	}
}

// SelectChipFromNode 为任务选择满足需求的最佳芯片。
//
// 选择策略：
//  - 把所有芯片按剩余资源从少到多排序（vChipsList.Less）；
//  - 整卡任务调用 selectChipFromNodeWhole，依次选择空闲整卡，直到满足 reqCardNum；
//  - 切分任务调用 selectChipFromNodeSegment，选择第一张满足资源/DVPP/vGroup 的芯片。
//
// 返回的是芯片物理 ID 字符串，整卡场景可能是多个 ID 用逗号拼接（如 "0,1"）。
func (ns *NPUDevices) SelectChipFromNode(vRes VResource) (string, error) {
	if ns == nil {
		klog.V(LogDebugLev).Infof("SelectChipFromNode failed: %s", ArgumentError)
		return "", errors.New(ArgumentError)
	}
	var vChipSlice []*VChip
	for _, Chip := range ns.Chips {
		vChipSlice = append(vChipSlice, Chip)
	}

	tempVChips := vChipsList(vChipSlice)
	sort.Sort(tempVChips)
	if len(tempVChips) == 0 {
		return "", fmt.Errorf("selectChipFromNode sorted chips len 0")
	}

	if ns.IsResourceWholeCard(vRes.Aicore) {
		return ns.selectChipFromNodeWhole(tempVChips, vRes)
	}
	return ns.selectChipFromNodeSegment(tempVChips, vRes)
}

// selectChipFromNodeWhole 为整卡任务选择多张空闲整卡。
//
// 计算需要卡数 reqCardNum = vRes.Aicore / AiCorePerChip，
// 每张卡分配的资源为 vRes 的平均值，遍历排序后的芯片，挑选未被切分且资源充足的芯片，
// 直到凑够 reqCardNum，返回 "id0,id1,..." 字符串。
func (ns *NPUDevices) selectChipFromNodeWhole(vChips []*VChip, vRes VResource) (string, error) {
	if ns.AiCorePerChip == 0 {
		return "", errors.New("AiCorePerChip is zero, division by zero avoided")
	}
	reqCardNum := vRes.Aicore / ns.AiCorePerChip
	allocCardNum := 0
	if reqCardNum == 0 {
		klog.V(LogDebugLev).Infof("selectChipFromNodeWhole aiCore:%d perCard:%d", vRes.Aicore,
			ns.AiCorePerChip)
		return "", errors.New("task require card number 0")
	}
	vResChip := VResource{
		Aicore: vRes.Aicore / reqCardNum,
		Aicpu:  vRes.Aicpu / reqCardNum,
		DVPP:   AscendDVPPEnabledNull,
	}
	cardNames := make([]string, 0)
	for _, chip := range vChips {
		if !chip.isChipMeetResReq(vResChip) || chip.SegmentFlag {
			klog.V(LogDebugLev).Infof("chip %s does not meet whole card resource requirements", chip.Name)
			continue
		}
		chipID, err := getWholeCardIDFromAscendReal(chip.Name)
		if err != nil {
			return "", fmt.Errorf("selectChipFromNodeWhole chip name <%s> err: %s", chip.Name,
				SafePrint(err))
		}
		cardNames = append(cardNames, strconv.Itoa(chipID))
		allocCardNum += 1
		if allocCardNum == reqCardNum {
			return strings.Join(cardNames, ","), nil
		}
	}
	return "", fmt.Errorf("selectChipFromNodeWhole free whole chip <%d> not enough for req <%d>", allocCardNum,
		reqCardNum)
}

// selectChipFromNodeSegment 为切分任务选择一张最佳芯片。
//
// 在已排序的芯片列表中，选择第一张满足 isChipMeetResReq 且稳定的芯片，
// 返回其物理 ID。如果没有可用芯片，返回错误。
func (ns *NPUDevices) selectChipFromNodeSegment(vChip []*VChip, vRes VResource) (string, error) {
	sort.Sort(vChipsList(vChip))
	for _, chip := range vChip {
		if !chip.isChipMeetResReq(vRes) || chip.Unstable {
			klog.V(LogDebugLev).Infof("chip %s does not meet resource requirements", chip.Name)
			continue
		}
		chipID, err := getWholeCardIDFromAscendReal(chip.Name)
		if err != nil {
			return "", fmt.Errorf("selectChipFromNodeSegment chip name <%s> err: %s", chip.Name,
				SafePrint(err))
		}
		return strconv.Itoa(chipID), nil
	}

	return "", fmt.Errorf("selectChipFromNodeSegment available chip not found for req <%d>", vRes.Aicore)
}

// escapeJSONPointer 对 JSON Pointer 中的特殊字符进行转义。
//
// JSON Pointer 规范要求 "~" 替换为 "~0"，"/" 替换为 "~1"，
// 否则在 Patch 路径 "/metadata/annotations/<key>" 中可能出现解析错误。
func escapeJSONPointer(p string) string {
	p = strings.Replace(p, "~", "~0", -1)
	p = strings.Replace(p, "/", "~1", -1)
	return p
}

// AddNPUAllocationPatch 构造写入 Pod 注解的 JSON Patch 字符串。
//
// 生成的 Patch 包含两个 add 操作：
//  - /metadata/annotations/<PodPredicateTime> -> timestamp；
//  - /metadata/annotations/<AscendNPUCore>   -> "chipID" 或 "chipID-template"。
//
// 注解 key 会先经过 escapeJSONPointer 转义，适配 JSON Pointer 路径。
func AddNPUAllocationPatch(allocChipID string, template string, timestamp string) string {
	var allocValue string
	if template != "" {
		allocValue = fmt.Sprintf("%s-%s", allocChipID, template)
	} else {
		allocValue = allocChipID
	}

	return fmt.Sprintf(`[{"op": "add", "path": "/metadata/annotations/%s", "value":"%s"},`+
		`{"op": "add", "path": "/metadata/annotations/%s", "value": "%s"}]`,
		escapeJSONPointer(PodPredicateTime), timestamp,
		escapeJSONPointer(AscendNPUCore), allocValue)
}
