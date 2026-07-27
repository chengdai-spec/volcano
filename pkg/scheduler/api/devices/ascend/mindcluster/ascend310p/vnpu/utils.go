package vnpu

import (
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// checkVNPUResourcesInPod 判断 Pod 是否请求了 MindCluster 动态 vNPU 资源。
//
// 判断条件：
//  1. Pod 的 label "ring-controller.atlas" 必须等于 "ascend-310P"；
//  2. 任意一个容器的 Resources.Limits 中包含 "huawei.com/npu-core"。
//
// 这意味着 MindCluster vNPU 目前主要针对 Ascend310P 推理场景，
// 用户通过 npu-core 资源来表达需要多少 AI Core（如 1/2/4 核）。
func checkVNPUResourcesInPod(pod *v1.Pod) bool {
	if pod.Labels["ring-controller.atlas"] != "ascend-310P" {
		return false
	}
	for _, container := range pod.Spec.Containers {
		_, ok := container.Resources.Limits["huawei.com/npu-core"]
		if ok {
			return true
		}
	}
	return false
}

// getWholeCardIDFromAscendReal 从物理芯片名称中解析出芯片物理 ID。
//
// 例如 "Ascend910-0" -> 0；"Ascend310P-2c.1cpu-105-0_3" -> 0（取 "-" 分隔后的第二段）。
// 如果格式不符合要求，返回 ErrorInt 与 FormatIncorrectError 错误。
func getWholeCardIDFromAscendReal(cardNameStr string) (int, error) {
	idStr := strings.Split(cardNameStr, "-")
	if len(idStr) < NPUIndex2 {
		return ErrorInt, fmt.Errorf("getCardIDFromCardNameStr %s %s", cardNameStr, FormatIncorrectError)
	}
	id, err := strconv.Atoi(idStr[NPUIndex1])
	if err != nil {
		return ErrorInt, fmt.Errorf("getCardIDFromCardNameStr %s %v", cardNameStr, err)
	}
	return id, nil
}

// getResTemplateFromTaskSetting 根据请求的 AI Core 数、CPU 级别、DVPP 开关选择 vNPU 模板名。
//
// 模板映射规则（Ascend310P）：
//  - 1 核 -> vir01；
//  - 2 核且 level=low -> vir02_1c（AI CPU 降级为 1），否则 vir02；
//  - 4 核 -> 由 getVirTemplate 根据 DVPP 与 level 决定：
//      DVPP=yes -> vir04_4c_dvpp
//      DVPP=no  -> vir04_3c_ndvpp
//      DVPP=null 且 level=low -> vir04_3c
//      DVPP=null 且 level=high -> vir04
//
// 实际案例：
//  一个推理任务请求 2 核，label vnpu-level=low，则选择 vir02_1c 模板，
//  相比 vir02 少用 1 个 AI CPU，可在 CPU 紧张时提升调度成功率。
func getResTemplateFromTaskSetting(coreNum int, cpuLevel, dvpp string) string {
	var virTemplate string
	switch coreNum {
	case NPUIndex1:
		virTemplate = VNPUTempVir01
	case NPUIndex2:
		virTemplate = VNPUTempVir02
		if cpuLevel == AscendVNPULevelLow {
			virTemplate = virTemplate + "_1c"
		}
	case NPUIndex4:
		virTemplate = getVirTemplate(dvpp, cpuLevel)
	default:
		klog.V(LogErrorLev).Infof("wrong number %d", coreNum)
		return ""
	}
	return virTemplate
}

// getVirTemplate 是 4 核场景下模板选择的辅助函数。
func getVirTemplate(dvpp string, cpuLevel string) string {
	switch dvpp {
	case AscendDVPPEnabledOn:
		return VNPUTempVir04C4cDVPP
	case AscendDVPPEnabledOff:
		return VNPUTempVir04C3NDVPP
	default:
		virTemplate := VNPUTempVir04
		if cpuLevel == AscendVNPULevelLow {
			virTemplate = virTemplate + "_3c"
		}
		return virTemplate
	}
}

// Add 把 resource 的 Aicore 与 Aicpu 加到当前资源上。
// 注意：DVPP 是开关字符串，不参与加减。
func (vResource *VResource) Add(resource VResource) {
	vResource.Aicore += resource.Aicore
	vResource.Aicpu += resource.Aicpu
}

// Sub 把 resource 的 Aicore 与 Aicpu 从当前资源中扣除。
func (vResource *VResource) Sub(resource VResource) {
	vResource.Aicore -= resource.Aicore
	vResource.Aicpu -= resource.Aicpu
}

// BeGreater 判断当前资源是否大于等于目标资源（仅比较 Aicore 与 Aicpu）。
func (vResource *VResource) BeGreater(resource VResource) bool {
	return vResource.Aicore >= resource.Aicore && vResource.Aicpu >= resource.Aicpu
}

// UpdateDVPP 在分配资源后更新芯片的 DVPP 占用状态。
//
// 规则：
//  - 任务需要 DVPP（yes）-> 把 UsedRes.DVPP 设为 yes，FreeRes.DVPP 设为 no；
//  - 任务明确不需要 DVPP（no）且当前 UsedRes.DVPP 已经是 no -> 把 FreeRes.DVPP 释放为 yes；
//  - 任务对 DVPP 无要求（null）且当前未启用 DVPP -> 保持 null。
//
// 该机制保证多个 Pod 共享芯片时，DVPP 独占与释放不会冲突。
func (vChip *VChip) UpdateDVPP(podResDVPP string) {
	if vChip == nil {
		klog.V(LogDebugLev).Infof("UpdateDVPP failed: %s", ArgumentError)
		return
	}
	if podResDVPP == AscendDVPPEnabledOn {
		vChip.UsedRes.DVPP = AscendDVPPEnabledOn
		vChip.FreeRes.DVPP = AscendDVPPEnabledOff
	}
	if podResDVPP == AscendDVPPEnabledOff && vChip.UsedRes.DVPP == AscendDVPPEnabledOff {
		vChip.UsedRes.DVPP = AscendDVPPEnabledOff
		vChip.FreeRes.DVPP = AscendDVPPEnabledOn
	}
	if podResDVPP == AscendDVPPEnabledNull && vChip.UsedRes.DVPP != AscendDVPPEnabledOn {
		vChip.UsedRes.DVPP = AscendDVPPEnabledNull
		vChip.FreeRes.DVPP = AscendDVPPEnabledNull
	}
}

// ResetDVPP 在释放资源后重置芯片的 DVPP 占用状态。
//
// 仅处理 DVPP=yes 的情况：释放后把 UsedRes.DVPP 置为 no，FreeRes.DVPP 置为 yes。
func (vChip *VChip) ResetDVPP(podResDVPP string) {
	if vChip == nil {
		klog.V(LogDebugLev).Infof("UpdateDVPP failed: %s", ArgumentError)
		return
	}
	if podResDVPP == AscendDVPPEnabledOn {
		vChip.UsedRes.DVPP = AscendDVPPEnabledOff
		vChip.FreeRes.DVPP = AscendDVPPEnabledOn
	}
}

// isChipMeetResReq 判断单张芯片是否满足任务资源要求。
//
// 依次检查：
//  1. isChipResourceEnough：Aicore/Aicpu 是否足够；
//  2. isChipVGroupValid：vGroup 分组约束是否允许当前请求；
//  3. isChipDVPPValid：DVPP 开关是否兼容。
func (vChip *VChip) isChipMeetResReq(vRes VResource) bool {
	if vChip == nil {
		klog.V(LogDebugLev).Infof("isChipMeetResReq failed: %s", ArgumentError)
		return false
	}
	if !vChip.isChipResourceEnough(vRes) {
		klog.V(LogDebugLev).Infof("vChip %s resource <%#v> not enough", vChip.Name, vChip.FreeRes)
		return false
	}
	if !vChip.isChipVGroupValid(vRes) {
		klog.V(LogDebugLev).Infof("vChip %s vGroup not enough", vChip.Name)
		return false
	}
	if !vChip.isChipDVPPValid(vRes) {
		klog.V(LogDebugLev).Infof("vChip %s DVPP not enough", vChip.Name)
		return false
	}
	return true
}

// Len 返回芯片列表长度，实现 sort.Interface。
func (vChips vChipsList) Len() int {
	return len(vChips)
}

// Less 定义芯片排序规则：优先选择剩余资源更少的芯片（紧凑）。
//
// 返回 !vChips[i].FreeRes.BeGreater(vChips[j].FreeRes)，
// 即如果 i 的剩余资源不比 j 多，则认为 i 应该排在前面。
func (vChips vChipsList) Less(i, j int) bool {
	if i > vChips.Len() || j > vChips.Len() {
		return false
	}
	return !vChips[i].FreeRes.BeGreater(vChips[j].FreeRes)
}

// Swap 交换芯片列表中的两个元素。
func (vChips vChipsList) Swap(i, j int) {
	if i > vChips.Len() || j > vChips.Len() {
		return
	}
	vChips[i], vChips[j] = vChips[j], vChips[i]
}

// isChipResourceEnough 判断芯片剩余资源是否大于等于请求。
func (vChip *VChip) isChipResourceEnough(vRes VResource) bool {
	return vChip.FreeRes.BeGreater(vRes)
}

// isChipVGroupValid 检查 vGroup 约束是否允许当前请求。
//
// vGroup 是 Ascend310P 芯片内部用于隔离不同 vNPU 实例的分组机制。
// 规则：
//  - 非 Ascend310P 芯片不检查；
//  - 未切分的整卡不检查；
//  - 已切分的芯片根据已有 vGroup 数量限制新请求：
//      3 个 vGroup 时只支持 1/2 核请求；
//      4 个 vGroup 时只支持 1 核请求。
//
// 这是昇腾硬件虚拟化对片上资源分组数量的硬性限制。
func (vChip *VChip) isChipVGroupValid(vRes VResource) bool {
	if vChip.Kind != Ascend310P {
		klog.V(LogDebugLev).Infof("not %s task, no need to check vGroup", Ascend310P)
		return true
	}

	if !vChip.SegmentFlag {
		klog.V(LogDebugLev).Info("whole card, no need to check vGroup")
		return true
	}

	vGroups := vChip.getVGroups()

	if len(vGroups) == NPUIndex3 && vRes.Aicore >= NPUIndex4 {
		klog.V(LogDebugLev).Infof("%d vGroups, only support 1,2 core", len(vGroups))
		return false
	}

	if len(vGroups) == NPUIndex4 && vRes.Aicore >= NPUIndex2 {
		klog.V(LogDebugLev).Infof("%d vGroups, only support 1 core", len(vGroups))
		return false
	}

	return true
}

// isChipDVPPValid 判断芯片 DVPP 状态是否与任务需求兼容。
//
// 规则：
//  1. 任务需要 DVPP（yes）-> 芯片 FreeRes.DVPP 必须是 yes；
//  2. 任务无要求（null）-> 芯片 FreeRes.DVPP 不能是 no（no 表示已被显式关闭）；
//  3. 任务明确不需要（no）-> 任何状态都可以。
func (vChip *VChip) isChipDVPPValid(vRes VResource) bool {
	// 1. if task dvpp on, the node's free resource must support dvpp
	if vRes.DVPP == AscendDVPPEnabledOn && vChip.FreeRes.DVPP != AscendDVPPEnabledOn {
		return false
	}
	// 2. if task dvpp null, the node's free resource cannot be off
	if vRes.DVPP == AscendDVPPEnabledNull && vChip.FreeRes.DVPP == AscendDVPPEnabledOff {
		return false
	}
	// 3. if task dvpp no, the node's free resource can be any
	return true
}

// getVGroups 从芯片真实 ID 列表中提取 vGroup 编号集合。
//
// 真实 ID 格式示例：Ascend310P-2c.1cpu-105-0_3，其中下划线后的 "3" 为 vGroup 编号。
// 函数返回去重后的 vGroup 编号切片，供 isChipVGroupValid 判断分组数量。
//
// realChip like:Ascend310P-2c.1cpu-105-0_3.
func (vChip *VChip) getVGroups() []int {
	vGroups := make([]int, 0)
	for _, realChip := range vChip.ID {
		realChipSplit := strings.Split(realChip, "_")
		if len(realChipSplit) < NPUIndex2 {
			continue
		}
		vGroupStr := realChipSplit[len(realChipSplit)-1]
		vGroup, err := strconv.Atoi(vGroupStr)
		if err != nil {
			continue
		}
		var existFlag bool
		for _, v := range vGroups {
			if vGroup == v {
				existFlag = true
				break
			}
		}
		if !existFlag {
			vGroups = append(vGroups, vGroup)
		}
	}

	return vGroups
}

// SetIsDual 设置芯片是否为双槽卡。
func (vChip *VChip) SetIsDual(value bool) {
	vChip.IsDual = value
}

// IsPodResUnstable 判断 Pod 的资源注解是否缺失，缺失则视为不稳定。
func (vChip *VChip) IsPodResUnstable(pod *v1.Pod) bool {
	realStr, ok := pod.Annotations[AscendNPUPodRealUse]
	return !ok || realStr == ""
}

// AddRealCardID 向芯片 ID 列表追加真实芯片 ID。
func (vChip *VChip) AddRealCardID(id string) {
	if id == "" {
		return
	}
	vChip.ID = append(vChip.ID, id)
}

// AddPodToPodMap 把 Pod 记录到芯片的 PodMap 中。
func (vChip *VChip) AddPodToPodMap(pod *v1.Pod) {
	vChip.PodMap[string(pod.UID)] = pod
}

// SetSegmentFlag 设置芯片是否已被切分。
func (vChip *VChip) SetSegmentFlag(value bool) {
	vChip.SegmentFlag = value
}

// PtrInit 返回任意类型的指针，用于初始化 *bool 等配置字段。
func PtrInit[T any](v T) *T { return &v }

// SafePrint 把错误信息中的换行符替换为空格，避免日志格式被破坏。
func SafePrint(args ...interface{}) string {
	msg := fmt.Sprint(args...)
	trimMsg := strings.Replace(msg, "\r", " ", -1)
	trimMsg = strings.Replace(trimMsg, "\n", " ", -1)
	return trimMsg
}
