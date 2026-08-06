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

// ──────────────────────────────────────────────────────────────────────────────
// utils.go  —— vgpu 的核心调度函数、编解码器和辅助工具
//
// 本文件包含 vgpu 方案的核心调度逻辑，与 gpushare/share.go 对应但复杂得多。
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 核心函数对比：                                                            ║
// ║                                                                          ║
// ║  gpushare (share.go)              vgpu (utils.go)                         ║
// ║  ─────────────────────         ─────────────────────────                 ║
// ║  predicateGPUbyMemory        checkNodeGPUSharingPredicateAndScore      ║
// ║    简单遍历找空闲显存           完整的模拟分配+打分+回滚                   ║
// ║                                  检查 7 个约束条件                     ║
// ║                                  支持 binpack/spread 策略              ║
// ║  无                             sortedDeviceIndicesByPolicy             ║
// ║  无                             GPUScore                                ║
// ║  无                             getGPUDeviceSnapShot                    ║
// ║  无                             checkGPUtype (白/黑名单)                ║
// ║  无                             deviceHasPodFromSameGroup               ║
// ║  getGPUMemoryOfPod             resourcereqs (三维资源请求)              ║
// ║  AddGPUIndexPatch (JSON)       patchPodAnnotations (StrategicMerge)    ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
// ──────────────────────────────────────────────────────────────────────────────

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
)

// extractGeometryFromType 从 GPU 型号中提取 MIG 几何形状配置。
//
// MIG (Multi-Instance GPU) 允许将一块物理 GPU 切分为多个独立实例。
// 不同的 GPU 型号支持不同的切分几何形状。
//
// 参数：
//   - t: GPU 型号字符串（如 "Tesla-A100-SXM4-40GB"）
//
// 返回值：
//   - []config.Geometry: 支持的几何形状列表
//   - error: 如果找不到对应的几何配置则返回错误
//
// 工作流程：
// 1. 从全局配置中读取 NvidiaConfig.MigGeometriesList
// 2. 遍历所有支持的 GPU 型号，查找匹配的型号
// 3. 返回该型号支持的几何形状列表
func extractGeometryFromType(t string) ([]config.Geometry, error) {
	if config.GetConfig() != nil {
		for _, val := range config.GetConfig().NvidiaConfig.MigGeometriesList {
			found := false
			for _, migDevType := range val.Models {
				if strings.Contains(t, migDevType) {
					found = true
				}
			}
			if found {
				return val.Geometries, nil
			}
		}
	}
	return []config.Geometry{}, errors.New("mig type not found")
}

// decodeNodeDevices 从节点注解字符串中解码 GPU 设备信息。
//
// 与 gpushare 对比：
//   - gpushare 不需要解码注解，直接从节点 Capacity 推算
//   - vgpu 必须解析 HAMi 上报的注解字符串，提取每块 GPU 的完整物理信息
//
// 节点注解格式示例：
// "UUID0,count,memory,type,health,mode:UUID1,count,memory,type,health,mode:..."
//
// 参数：
//   - name: 节点名称
//   - str: 设备信息字符串（来自节点注解 volcano.sh/vgpu-register）
//
// 返回值：
//   - *GPUDevices: 解析后的 GPU 设备对象
//   - string: 共享模式（hami-core 或 MIG）
//
// 每个 GPU 设备的字段含义：
//   - items[0]: UUID（GPU 唯一标识）
//   - items[1]: count（总虚拟 GPU 数量，即 Number）
//   - items[2]: memory（显存大小，单位 MB）
//   - items[3]: type（GPU 型号）
//   - items[4]: health（健康状态，true/false）
//   - items[5]: mode（共享模式：hami-core 或 MIG）
//
// 实际案例：
// 注解："GPU-abc,10,40960,A100-SXM4-40GB,true,hami-core:GPU-def,10,40960,A100-SXM4-40GB,true,hami-core"
// 解析后得到 2 块 A100 GPU，每块 Number=10、Memory=40960、Mode=hami-core。
func decodeNodeDevices(name, str string) (*GPUDevices, string) {
	if !strings.Contains(str, ":") {
		return nil, ""
	}
	tmp := strings.Split(str, ":")
	retval := &GPUDevices{
		Name:   name,
		Device: make(map[int]*GPUDevice),
		Score:  float64(0),
	}

	// 默认使用 hami-core 作为分区模式
	sharingMode := vGPUControllerHAMICore

	for index, val := range tmp {
		if strings.Contains(val, ",") {
			items := strings.Split(val, ",")
			if len(items) < 6 {
				klog.Error("wrong Node GPU info: ", val)
				return nil, ""
			}

			count, _ := strconv.Atoi(items[1])
			devmem, _ := strconv.Atoi(items[2])
			health, _ := strconv.ParseBool(items[4])

			i := GPUDevice{
				ID:          index,
				Node:        name,
				UUID:        items[0],
				Number:      uint(count),                // 总虚拟 GPU 槽位数
				Memory:      uint(devmem),               // 显存总量
				Type:        items[3],                   // GPU 型号
				PodMap:      make(map[string]*GPUUsage), // Pod 使用情况映射
				Health:      health,                     // 健康状态
				MigTemplate: []config.Geometry{},        // MIG 模板（仅 MIG 模式使用）
				MigUsage: config.MigInUse{
					Index: -1}, // MIG 使用索引，-1 表示未使用 MIG
			}

			// 确定共享模式
			sharingMode = getSharingMode(items[5])
			if sharingMode == vGPUControllerMIG {
				var err error
				// 如果是 MIG 模式，需要提取几何形状配置
				i.MigTemplate, err = extractGeometryFromType(i.Type)
				if err != nil {
					// 如果提取失败，降级到 hami-core 模式
					sharingMode = vGPUControllerHAMICore
					klog.ErrorS(err, "extract mig geometry error and fall back to hamicore mode")
				}
			}
			retval.Device[index] = &i
		}
	}
	retval.Mode = sharingMode
	return retval, sharingMode
}

// encodeContainerDevices 将容器设备列表编码为字符串。
//
// 编码格式：
// "UUID,type,usedmem,usedcores:UUID,type,usedmem,usedcores:..."
//
// 用于将分配结果写入 Pod 注解，例如：
// vgpu-ids-new: "uuid1,NVIDIA,2048,50:uuid2,NVIDIA,1024,25"
func encodeContainerDevices(cd []ContainerDevice) string {
	tmp := ""
	for _, val := range cd {
		tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores)) + ":"
	}
	klog.V(4).Infoln("Encoded container Devices=", tmp)
	return tmp
}

// encodePodDevices 将整个 Pod 的设备分配编码为字符串。
//
// 格式：
// "container1_devices;container2_devices;..."
// 其中每个 container 的设备用 encodeContainerDevices 编码。
func encodePodDevices(pd []ContainerDevices) string {
	var ss []string
	for _, cd := range pd {
		ss = append(ss, encodeContainerDevices(cd))
	}
	return strings.Join(ss, ";")
}

// decodeContainerDevices 从容器的设备字符串中解码出设备列表。
//
// 参数：
//   - str: 设备字符串，格式为 "UUID,type,usedmem,usedcores:..."
//
// 返回值：
//   - ContainerDevices: 解析后的设备列表
func decodeContainerDevices(str string) ContainerDevices {
	if len(str) == 0 {
		return ContainerDevices{}
	}
	cd := strings.Split(str, ":")
	contdev := ContainerDevices{}
	tmpdev := ContainerDevice{}
	if len(str) == 0 {
		return contdev
	}
	for _, val := range cd {
		if strings.Contains(val, ",") {
			tmpstr := strings.Split(val, ",")
			tmpdev.UUID = tmpstr[0]
			tmpdev.Type = tmpstr[1]
			devmem, _ := strconv.ParseInt(tmpstr[2], 10, 32)
			tmpdev.Usedmem = uint(devmem)
			devcores, _ := strconv.ParseInt(tmpstr[3], 10, 32)
			tmpdev.Usedcores = uint(devcores)
			contdev = append(contdev, tmpdev)
		}
	}
	return contdev
}

// DecodePodDevices 解析 Pod 的 vgpu-ids-new 注解为每个容器的设备列表。
//
// 参数：
//   - str: Pod 注解字符串，格式为 "container1;container2;..."
//
// 返回值：
//   - []ContainerDevices: 每个容器的设备列表
//
// 典型用途：
// - 在调度周期开始时，从已调度 Pod 的注解中恢复设备分配状态
// - 用于构建 GPUDevice.PodMap，追踪哪些 Pod 使用了哪些 GPU
func DecodePodDevices(str string) []ContainerDevices {
	if len(str) == 0 {
		return []ContainerDevices{}
	}
	var pd []ContainerDevices
	for _, s := range strings.Split(str, ";") {
		cd := decodeContainerDevices(s)
		pd = append(pd, cd)
	}
	return pd
}

// getPodGroupKey 获取 Pod 所属的作业组唯一标识。
//
// 参数：
//   - pod: Pod 对象
//
// 返回值：
//   - string: 作业组键（namespace/groupName），如果没有组注解则返回空字符串
//
// 作业组用于实现 PodGroup Spread 策略：
// - 同一作业的多个 Pod 应该分散到不同的 GPU 上
// - 避免单点故障导致整个作业失败
//
// 优先级：
// 1. KubeGroupNameAnnotationKey（KubeVela 等使用的标准注解）
// 2. VolcanoGroupNameAnnotationKey（Volcano 专有注解）
func getPodGroupKey(pod *v1.Pod) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	groupName := pod.Annotations[v1beta1.KubeGroupNameAnnotationKey]
	if groupName == "" {
		groupName = pod.Annotations[v1beta1.VolcanoGroupNameAnnotationKey]
	}
	if groupName == "" {
		return ""
	}
	return pod.Namespace + "/" + groupName
}

// deviceHasPodFromSameGroup 检查设备是否已有来自同一 PodGroup 的 Pod。
//
// 这是 vgpu 独有的能力，gpushare 不支持 PodGroup 级别的分散调度。
// vgpu 通过此函数实现 PodGroup Spread 策略：
//
//	同一作业的多个 Pod 分散到不同 GPU，避免单点故障。
//
// 参数：
//   - gd: GPU 设备对象
//   - currentKey: 当前 Pod 的 PodGroup key
//
// 返回值：
//   - bool: 如果设备已有同组 Pod 且使用了资源，返回 true
//
// 用途：
// - 实现 PodGroup Spread 策略
// - 防止同一作业的多个 Pod 挤在同一块 GPU 上
// - 提高作业的容错性和性能隔离
func deviceHasPodFromSameGroup(gd *GPUDevice, currentKey string) bool {
	if gd == nil || currentKey == "" {
		return false
	}
	for _, usage := range gd.PodMap {
		if usage == nil {
			continue
		}
		// 只有当同组 Pod 实际使用了资源时才认为冲突
		if usage.PodGroupKey == currentKey && (usage.UsedMem > 0 || usage.UsedCore > 0) {
			return true
		}
	}
	return false
}

// checkVGPUResourcesInPod 检查 Pod 是否请求了 vGPU 资源。
//
// 参数：
//   - pod: Pod 对象
//
// 返回值：
//   - bool: 如果 Pod 请求了 GPU 内存或 GPU 数量，返回 true
//
// 用途：
// - 快速判断 Pod 是否需要 GPU 调度
// - 避免对不需要 GPU 的 Pod 执行不必要的检查
func checkVGPUResourcesInPod(pod *v1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		_, ok := container.Resources.Limits[v1.ResourceName(getConfig().ResourceMemoryName)]
		if ok {
			return true
		}
		_, ok = container.Resources.Limits[v1.ResourceName(getConfig().ResourceCountName)]
		if ok {
			return true
		}
	}
	return false
}

// resourcereqs 提取 Pod 的 GPU 资源请求。
//
// 参数：
//   - pod: Pod 对象
//
// 返回值：
//   - []devices.ContainerDeviceRequest: 每个容器的设备请求
//
// 这个函数是 devices.ExtractResourceRequest 的包装器，
// 自动从配置中读取资源名称（如 nvidia.com/gpu、nvidia.com/gpumem 等）。
//
// 实际案例：
// Pod 中容器请求 limits:
//
//	volcano.sh/vgpu-number: "1"
//	volcano.sh/vgpu-memory: "2048"
//	volcano.sh/vgpu-cores: "50"
//
// 则返回的 ContainerDeviceRequest 中 Nums=1、Memreq=2048、Coresreq=50。
func resourcereqs(pod *v1.Pod) []devices.ContainerDeviceRequest {
	countName := getConfig().ResourceCountName
	memoryName := getConfig().ResourceMemoryName
	percentageName := getConfig().ResourceMemoryPercentageName
	coreName := getConfig().ResourceCoreName
	return devices.ExtractResourceRequest(pod, "NVIDIA", countName, memoryName, percentageName, coreName)
}

// checkGPUtype 检查 GPU 型号是否符合 Pod 注解中的过滤条件。
//
// 与 gpushare 对比：
//   - gpushare 完全不知道 GPU 型号，无法进行型号过滤
//   - vgpu 支持白名单（nvidia.com/use-gputype）和黑名单（nvidia.com/nouse-gputype）
//     例如只允许 A100 或排除 T4
//
// 参数：
//   - annos: Pod 注解
//   - cardtype: GPU 卡型号（如 "Tesla-V100-SXM2-16GB"）
//
// 返回值：
//   - bool: 如果型号符合要求，返回 true
//
// 支持的注解：
// - nvidia.com/use-gputype: 指定要使用的 GPU 型号（白名单）
// - nvidia.com/nouse-gputype: 指定要排除的 GPU 型号（黑名单）
//
// 示例：
//
//	nvidia.com/use-gputype: "V100,A100"  # 只使用 V100 或 A100
//	nvidia.com/nouse-gputype: "T4"      # 不使用 T4
func checkGPUtype(annos map[string]string, cardtype string) bool {
	inuse, ok := annos[GPUInUse]
	if ok {
		// 白名单模式：只允许指定型号
		if !strings.Contains(inuse, ",") {
			// 单个型号
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(inuse)) {
				return true
			}
		} else {
			// 多个型号，用逗号分隔
			for _, val := range strings.Split(inuse, ",") {
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return true
				}
			}
		}
		return false
	}

	nouse, ok := annos[GPUNoUse]
	if ok {
		// 黑名单模式：排除指定型号
		if !strings.Contains(nouse, ",") {
			// 单个型号
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(nouse)) {
				return false
			}
		} else {
			// 多个型号
			for _, val := range strings.Split(nouse, ",") {
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return false
				}
			}
		}
		return true
	}
	return true
}

// checkType 检查设备类型是否与 Pod 请求匹配。
//
// 参数：
//   - annos: Pod 注解
//   - d: GPU 设备
//   - n: 容器设备请求
//
// 返回值：
//   - bool: 如果类型匹配，返回 true
//
// 检查逻辑：
// 1. 通用类型检查：NVIDIA->NVIDIA, MLU->MLU
// 2. 对于 NVIDIA GPU，进一步检查型号过滤（白名单/黑名单）
func checkType(annos map[string]string, d GPUDevice, n devices.ContainerDeviceRequest) bool {
	// General type check, NVIDIA->NVIDIA MLU->MLU
	if !strings.Contains(d.Type, n.Type) {
		return false
	}
	if n.Type == NvidiaGPUDevice {
		return checkGPUtype(annos, d.Type)
	}
	klog.Errorf("Unrecognized device %v", n.Type)
	return false
}

// getGPUDeviceSnapShot 创建 GPU 设备的快照（浅拷贝）。
//
// 与 gpushare 对比：
//   - gpushare 没有 dry-run 概念，predicate 和 allocate 是同一过程
//   - vgpu 通过快照实现“试探性分配”：
//     FilterNode 时用快照模拟分配，成功则缓存打分，失败则丢弃快照
//     Allocate 时才真正修改原始状态
//
// 参数：
//   - snap: 原始 GPUDevices 对象
//
// 返回值：
//   - *GPUDevices: 快照副本
//
// 注意：
// - 这不是严格的深拷贝，指针类型的字段（如 MigTemplate）仍指向原对象
// - PodMap 会被深拷贝，避免修改影响原始数据
// - 用于 dry-run 模拟分配，不影响真实状态
//
// 用途：
// - 在 Predicate 阶段尝试分配，看看是否可行
// - 如果失败可以丢弃快照，不影响原始状态
func getGPUDeviceSnapShot(snap *GPUDevices) *GPUDevices {
	ret := GPUDevices{
		Name:    snap.Name,
		Device:  make(map[int]*GPUDevice),
		Score:   float64(0),
		Sharing: snap.Sharing,
	}
	for index, val := range snap.Device {
		if val != nil {
			// 深拷贝 PodMap
			podMapCopy := make(map[string]*GPUUsage, len(val.PodMap))
			for uid, usage := range val.PodMap {
				if usage != nil {
					u := *usage
					podMapCopy[uid] = &u
				}
			}
			ret.Device[index] = &GPUDevice{
				ID:          val.ID,
				Node:        val.Node,
				UUID:        val.UUID,
				PodMap:      podMapCopy,
				Memory:      val.Memory,
				Number:      val.Number,
				Type:        val.Type,
				Health:      val.Health,
				UsedNum:     val.UsedNum,
				UsedMem:     val.UsedMem,
				UsedCore:    val.UsedCore,
				MigTemplate: val.MigTemplate,
				MigUsage:    val.MigUsage,
			}
			// 深拷贝 MIG 使用状态
			ret.Device[index].MigUsage = deepCopyMigInUse(val.MigUsage)
			klog.V(4).Infoln("getGPUDeviceSnapShot:", ret.Device[index].UsedMem, val.UsedMem, ret.Device[index].MigUsage, val.MigUsage)
		}
	}
	return &ret
}

// deepCopyMigInUse 深拷贝 MIG 使用状态。
//
// MIG (Multi-Instance GPU) 允许将 GPU 切分为多个实例。
// 这个函数复制 MIG 实例的使用情况，避免修改影响原始数据。
func deepCopyMigInUse(src config.MigInUse) config.MigInUse {
	dst := config.MigInUse{
		Index: src.Index,
	}

	dst.UsageList = make(config.MIGS, len(src.UsageList))
	for i, usage := range src.UsageList {
		dst.UsageList[i] = config.MigTemplateUsage{
			Name:      usage.Name,
			Memory:    usage.Memory,
			InUse:     usage.InUse,
			UsedIndex: make([]int, len(usage.UsedIndex)),
		}
		copy(dst.UsageList[i].UsedIndex, usage.UsedIndex)
	}

	return dst
}

// getSharingMode 根据模式字符串确定共享模式。
//
// 参数：
//   - mode: 模式字符串（如 "MIG" 或其他）
//
// 返回值：
//   - string: 确定的共享模式
//
// 默认情况下使用 hami-core 作为分区模式。
// 目前支持的模式：
// - vGPUControllerMIG: NVIDIA MIG 硬件级虚拟化
// - vGPUControllerHAMICore: HAMi 软件级时间切片
func getSharingMode(mode string) string {
	switch mode {
	case vGPUControllerMIG:
		return vGPUControllerMIG
	default:
		return vGPUControllerHAMICore
	}
}

// checkNodeGPUSharingPredicateAndScore 检查 Pod 是否可以调度到节点并计算分数
//
// 这是 vgpu 调度的核心函数，相当于 gpushare 中 predicateGPUbyMemory + predicateGPUbyNumber
// 的超集，但复杂度高得多。
//
// 与 gpushare 对比：
//   - gpushare 的 predicate 只做简单的“空闲 >= 请求”判断，无打分、无策略
//   - vgpu 的 checkNodeGPUSharingPredicateAndScore 实现了：
//     (1) 7 层约束检查（槽位/PodGroup/显存/核心/独占/零核/型号）
//     (2) dry-run 模拟分配（replicate=true 时用快照，失败可回滚）
//     (3) binpack/spread 调度策略排序
//     (4) GPUScore 打分累加
//     (5) SharingFactory.TryAddPod 最终确认
//
// 参数：
//   - pod: 要调度的 Pod
//   - gssnap: GPU 设备快照（replicate=true 时传入快照）
//   - replicate: 是否为干跑模式（true=模拟分配，false=真实分配）
//   - schedulePolicy: 调度策略（binpack 或 spread）
//
// 返回值：
//   - bool: 是否可以调度
//   - []ContainerDevices: 分配的设备列表
//   - float64: 节点分数
//   - error: 错误信息
//
// 工作流程：
// 1. 检查 Pod 是否请求了 vGPU 资源
// 2. 验证 Pod 要求的共享模式是否与节点一致
// 3. 提取 Pod 的资源请求
// 4. 遍历每个容器的请求，尝试分配到合适的 GPU
// 5. 如果所有请求都满足，返回成功和分配结果
//
// 分配检查（按顺序）：
//   - GPU 健康且还有空闲槽位（UsedNum < Number）
//   - PodGroup Spread：避免同组 Pod 在同一 GPU
//   - 显存足够（剩余显存 >= 请求显存，支持百分比请求）
//   - 核心数足够（UsedCore + Coresreq <= 100）
//   - 独占约束：Coresreq=100 时要求 UsedNum=0
//   - 不能将 core=0 的任务分配给已用满核心的 GPU
//   - GPU 型号匹配（白名单/黑名单）
//   - Sharing.TryAddPod 最终确认
func checkNodeGPUSharingPredicateAndScore(pod *v1.Pod, gssnap *GPUDevices, replicate bool, schedulePolicy string) (bool, []ContainerDevices, float64, error) {
	// ═══════════════════════════════════════════════════════════════
	// 阶段 0：初始化评分变量
	// ═══════════════════════════════════════════════════════════════
	// score 累计节点得分，用于调度器选择最优节点
	score := float64(0)

	// ═══════════════════════════════════════════════════════════════
	// 阶段 1：快速前置检查 —— Pod 是否请求了 vGPU 资源
	// ═══════════════════════════════════════════════════════════════
	// 如果 Pod 没有请求任何 vGPU 资源（如 nvidia.com/gpumem、nvidia.com/gpu），
	// 则直接返回成功（不阻塞调度），无需进行后续复杂的设备分配逻辑
	if !checkVGPUResourcesInPod(pod) {
		return true, []ContainerDevices{}, 0, nil
	}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 2：共享模式兼容性检查
	// ═══════════════════════════════════════════════════════════════
	// vGPU 支持两种共享模式：
	//   - hami-core: 软件级时间切片（默认）
	//   - MIG: NVIDIA 硬件级多实例 GPU
	// 如果 Pod 通过注解 volcano.sh/vgpu-mode 指定了特定模式，
	// 但节点的 GPU 不支持该模式，则判定为不兼容
	podSharingMode, ok := pod.Annotations[GPUModeAnnotation]
	if ok && podSharingMode != gssnap.Mode {
		return false, []ContainerDevices{}, 0, fmt.Errorf("pod required sharing mode %s is not the same as the node mode %s", podSharingMode, gssnap.Mode)
	}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 3：提取 Pod 的 GPU 资源请求
	// ═══════════════════════════════════════════════════════════════
	// resourcereqs 从 Pod 的所有容器中提取 GPU 资源请求，
	// 每个容器对应一个 ContainerDeviceRequest，包含：
	//   - Nums: 需要的 GPU 数量
	//   - Memreq: 需要的显存量（MB）
	//   - MemPercentagereq: 显存百分比（101 表示未设置）
	//   - Coresreq: 需要的算力核心百分比（0~100）
	//   - Type: 设备类型（如 "NVIDIA"）
	ctrReq := resourcereqs(pod)
	if len(ctrReq) == 0 {
		return true, []ContainerDevices{}, 0, nil
	}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 4：确定操作模式（干跑 vs 真实分配）
	// ═══════════════════════════════════════════════════════════════
	// replicate=true  → 干跑模式（Filter/Predicate 阶段）：
	//   创建快照进行模拟分配，不修改真实设备状态，
	//   用于预判节点是否可行并计算分数
	// replicate=false → 真实分配模式（Allocate 阶段）：
	//   直接操作原始 GPUDevices 对象，修改 UsedMem/UsedCore/UsedNum
	var gs *GPUDevices
	if replicate {
		// 干跑模式：创建深拷贝快照，所有修改不影响真实状态
		gs = getGPUDeviceSnapShot(gssnap)
	} else {
		// 真实分配：直接操作原对象，修改会持久化到调度器缓存
		gs = gssnap
	}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 5：PodGroup Spread 策略初始化
	// ═══════════════════════════════════════════════════════════════
	// 如果 Pod 设置了 volcano.sh/vgpu-podgroup-policy: "spread" 注解，
	// 则需要确保同一 PodGroup（同一作业）的多个 Pod 分散到不同 GPU 上，
	// 避免单点故障，提高容错性。
	// currentPodGroupKey 格式为 "namespace/groupName"
	var currentPodGroupKey string
	if pod.Annotations[VGPUPodGroupPolicyAnnotation] == VGPUPodGroupPolicySpreadValue {
		currentPodGroupKey = getPodGroupKey(pod)
	}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 6：定义临时分配记录与回滚机制
	// ═══════════════════════════════════════════════════════════════
	// 由于 Pod 可能包含多个容器，每个容器可能请求多块 GPU，
	// 如果中途某个容器的请求无法满足，需要回滚之前已做的所有分配。
	//
	// tentativeAlloc 记录一次临时分配的信息：
	//   - device: 被分配的 GPU 设备指针
	//   - mem: 该次分配占用的显存量
	//   - core: 该次分配占用的算力核心百分比
	type tentativeAlloc struct {
		device *GPUDevice
		mem    uint
		core   uint
	}

	// rollbackTentative 回滚所有临时分配，恢复设备状态。
	// 遍历所有已记录的临时分配，逆序扣减 UsedNum/UsedMem/UsedCore，
	// 使用 max(0, x) 防止下溢。
	rollbackTentative := func(allocs []tentativeAlloc) {
		for _, alloc := range allocs {
			if alloc.device == nil {
				continue
			}
			// 回滚占用数：UsedNum--（不低于 0）
			if alloc.device.UsedNum > 0 {
				alloc.device.UsedNum--
			}
			// 回滚显存：UsedMem -= mem（不低于 0）
			if alloc.device.UsedMem >= alloc.mem {
				alloc.device.UsedMem -= alloc.mem
			} else {
				alloc.device.UsedMem = 0
			}
			// 回滚核心：UsedCore -= core（不低于 0）
			if alloc.device.UsedCore >= alloc.core {
				alloc.device.UsedCore -= alloc.core
			} else {
				alloc.device.UsedCore = 0
			}
		}
	}

	// tentativeAllocs: 记录本次函数执行中所有成功的临时分配，用于失败回滚
	tentativeAllocs := []tentativeAlloc{}
	// ctrdevs: 最终分配结果，每个容器对应一组 ContainerDevices
	ctrdevs := []ContainerDevices{}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 7：逐容器分配 GPU 设备（核心循环）
	// ═══════════════════════════════════════════════════════════════
	// 遍历 Pod 中每个容器的 GPU 请求，为每个容器分配足够的 GPU
	for _, val := range ctrReq {
		// devs: 当前容器分配到的设备列表
		devs := []ContainerDevice{}

		// 前置检查：请求的 GPU 数量不能超过节点上的 GPU 总数
		if int(val.Nums) > len(gs.Device) {
			rollbackTentative(tentativeAllocs)
			return false, []ContainerDevices{}, 0, fmt.Errorf("no enough gpu cards on node %s", gs.Name)
		}
		klog.V(3).InfoS("Allocating device for container", "request", val)

		// ───────────────────────────────────────────────────────────
		// 按调度策略排序后遍历所有 GPU 设备
		// sortedDeviceIndicesByPolicy 根据策略返回排序后的设备索引：
		//   - binpack: 已用显存多的优先（集中填满少数卡）
		//   - spread: 已用槽位少的优先（分散到多个卡）
		//   - default: 按索引逆序
		// ───────────────────────────────────────────────────────────
		for _, i := range sortedDeviceIndicesByPolicy(gs, schedulePolicy) {
			klog.V(3).InfoS("Scoring pod request", "memReq", val.Memreq, "memPercentageReq", val.MemPercentagereq, "coresReq", val.Coresreq, "Nums", val.Nums, "Index", i, "ID", gs.Device[i].ID)
			klog.V(3).InfoS("Current Device", "Index", i, "TotalMemory", gs.Device[i].Memory, "UsedMemory", gs.Device[i].UsedMem, "UsedCores", gs.Device[i].UsedCore, "UsedNum", gs.Device[i].UsedNum, "Number", gs.Device[i].Number, "replicate", replicate)

			// ─── 约束检查 1：GPU 槽位容量 ───────────────────────────
			// Number 表示该 GPU 最大可共享的 Pod 数（虚拟 GPU 槽位总数），
			// UsedNum 表示已分配的 Pod 数。如果已用数 >= 总数，无空闲槽位
			if gs.Device[i].Number <= uint(gs.Device[i].UsedNum) {
				continue
			}

			// ─── 约束检查 2：PodGroup Spread 策略 ─────────────────────
			// 如果启用了 Spread 策略，且该 GPU 上已有同一 PodGroup 的 Pod
			// 在使用资源，则跳过此 GPU，强制分散到不同 GPU
			if currentPodGroupKey != "" && deviceHasPodFromSameGroup(gs.Device[i], currentPodGroupKey) {
				continue
			}

			// ─── 计算实际显存需求 ─────────────────────────────────────
			// vGPU 支持两种显存请求方式：
			//   - 绝对值：如 2048MB
			//   - 百分比：如 50%（表示需要该卡总显存的 50%）
			// MemPercentagereq=101 是哨兵值，表示未设置百分比，使用绝对值
			memreqForCard := uint(0)
			if val.MemPercentagereq != 101 {
				// 百分比模式：将百分比转换为绝对值（基于该卡总显存）
				memreqForCard = uint(float64(gs.Device[i].Memory) * float64(val.MemPercentagereq) / 100.0)
			} else {
				// 绝对值模式：直接使用请求的 MB 数
				memreqForCard = uint(val.Memreq)
			}

			// ─── 约束检查 3：显存容量 ────────────────────────────────
			// 剩余显存 = 总显存 - 已用显存，必须 >= 请求显存
			if int(gs.Device[i].Memory)-int(gs.Device[i].UsedMem) < int(memreqForCard) {
				continue
			}

			// ─── 约束检查 4：算力核心容量 ────────────────────────────
			// 所有共享 Pod 的核心百分比之和不能超过 100%
			if gs.Device[i].UsedCore+uint(val.Coresreq) > 100 {
				continue
			}

			// ─── 约束检查 5：独占整卡约束 ────────────────────────────
			// Coresreq=100 表示该 Pod 需要独占整块 GPU 的全部算力，
			// 此时 GPU 上不能有其他已分配的 Pod（UsedNum 必须为 0）
			if val.Coresreq == 100 && gs.Device[i].UsedNum > 0 {
				continue
			}

			// ─── 约束检查 6：零核心任务约束 ──────────────────────────
			// 如果 GPU 的核心已被 100% 占满（UsedCore=100），
			// 则不能再分配不需要核心（Coresreq=0）的纯显存任务，
			// 因为即使不需要核心，Pod 运行仍需要最低限度的 GPU 访问权
			if gs.Device[i].UsedCore == 100 && val.Coresreq == 0 {
				continue
			}

			// ─── 约束检查 7：GPU 型号匹配 ────────────────────────────
			// 检查 GPU 型号是否满足 Pod 注解中的白名单/黑名单过滤条件
			// 例如 Pod 可指定只要 A100 或排除 T4
			if !checkType(pod.Annotations, *gs.Device[i], val) {
				klog.Errorln("failed checktype", gs.Device[i].Type, val.Type)
				continue
			}

			// ─── 约束检查 8：共享策略最终确认 ────────────────────────
			// SharingFactory.TryAddPod 根据具体的共享模式（hami-core/MIG）
			// 执行最终的可行性判断。不同模式有不同的约束：
			//   - hami-core: 简单检查显存和核心
			//   - MIG: 检查是否有匹配的 MIG 实例模板可用
			// 如果 fit=true，同时会修改设备状态（UsedNum++、UsedMem+=等）
			fit, uuid := gs.Sharing.TryAddPod(gs.Device[i], memreqForCard, uint(val.Coresreq))
			if !fit {
				klog.V(3).Info(gs.Device[i].ID, "not fit")
				continue
			}

			// ─── 记录临时分配（用于可能的回滚） ──────────────────────
			tentativeAllocs = append(tentativeAllocs, tentativeAlloc{
				device: gs.Device[i],
				mem:    memreqForCard,
				core:   uint(val.Coresreq),
			})

			// ─── 分配成功，更新计数和记录结果 ────────────────────────
			if val.Nums > 0 {
				val.Nums-- // 剩余需求减 1
				klog.V(3).Info("fitted uuid: ", uuid)
				// 记录分配的设备信息（UUID、类型、显存、核心）
				devs = append(devs, ContainerDevice{
					UUID:      uuid,
					Type:      val.Type,
					Usedmem:   memreqForCard,
					Usedcores: uint(val.Coresreq),
				})
				// 累加节点分数：
				//   binpack 模式：已用显存比例越高得分越高（鼓励填满）
				//   spread 模式：空闲 GPU 得分高（鼓励分散）
				score += GPUScore(schedulePolicy, gs.Device[i])
			}
			// 当前容器的所有 GPU 需求已满足，跳出设备遍历循环
			if val.Nums == 0 {
				break
			}
		}

		// ═══════════════════════════════════════════════════════════
		// 当前容器的 GPU 需求未完全满足，分配失败
		// ═══════════════════════════════════════════════════════════
		// 遍历完所有 GPU 后仍有未满足的需求（val.Nums > 0），
		// 需要回滚之前所有容器的临时分配，恢复设备状态
		if val.Nums > 0 {
			rollbackTentative(tentativeAllocs)
			return false, []ContainerDevices{}, 0, fmt.Errorf("not enough gpu fitted on this node")
		}
		// 当前容器分配成功，记录结果
		ctrdevs = append(ctrdevs, devs)
	}

	// ═══════════════════════════════════════════════════════════════
	// 阶段 8：所有容器的 GPU 需求均已满足，返回成功
	// ═══════════════════════════════════════════════════════════════
	// 返回值说明：
	//   - true: 节点可行
	//   - ctrdevs: 每个容器分配到的设备详情（UUID、显存、核心）
	//   - score: 节点总得分（用于调度器在多个可行节点间选择最优）
	//   - nil: 无错误
	return true, ctrdevs, score, nil
}

// sortedDeviceIndicesByPolicy 根据调度策略对 GPU 设备索引排序。
//
// 与 gpushare 对比：
//   - gpushare 没有策略概念，始终按 ID 升序遍历，取第一个满足条件的 GPU
//   - vgpu 支持三种排序策略：
//     binpack: 已用显存多的优先（集中任务，留整卡空闲）
//     spread:  已用槽位少的优先（分散任务，减少竞争）
//     default: 按索引逆序遍历
//
// 参数：
//   - gs: GPU 设备集合
//   - schedulePolicy: 调度策略
//
// 返回值：
//   - []int: 排序后的设备索引列表
//
// 支持的策略：
// 1. binpackPolicy（紧凑策略）：
//   - 优先选择已用显存多的 GPU
//   - 目标：让任务集中在少数 GPU 上，留出更多空闲 GPU
//   - 适用场景：提高资源利用率，减少碎片
//
// 2. spreadPolicy（分散策略）：
//   - 优先选择已用槽位少的 GPU
//   - 目标：让任务分散到不同 GPU 上
//   - 适用场景：提高并行性能，减少竞争
//
// 3. 默认策略：
//   - 按设备索引逆序遍历
func sortedDeviceIndicesByPolicy(gs *GPUDevices, schedulePolicy string) []int {
	n := len(gs.Device)
	idx := make([]int, 0, n)
	switch schedulePolicy {
	case binpackPolicy:
		for i := range gs.Device {
			idx = append(idx, i)
		}
		sort.Slice(idx, func(a, b int) bool {
			da, db := gs.Device[idx[a]], gs.Device[idx[b]]
			if da.UsedMem != db.UsedMem {
				return da.UsedMem > db.UsedMem // 已用显存多的排前面
			}
			return idx[a] < idx[b]
		})
	case spreadPolicy:
		for i := range gs.Device {
			idx = append(idx, i)
		}
		sort.Slice(idx, func(a, b int) bool {
			da, db := gs.Device[idx[a]], gs.Device[idx[b]]
			if da.UsedNum != db.UsedNum {
				return da.UsedNum < db.UsedNum // 已用槽位少的排前面
			}
			return idx[a] < idx[b]
		})
	default:
		for i := n - 1; i >= 0; i-- {
			idx = append(idx, i)
		}
	}
	return idx
}

// GPUScore 计算单个 GPU 设备的分数。
//
// 与 gpushare 对比：
//   - gpushare 无打分机制，ScoreNode 固定返回 0
//   - vgpu 根据策略计算每个 GPU 的打分：
//     binpack: 已用显存比例越高，分数越高（鼓励集中）
//     spread:  完全空闲的 GPU 得分最高（鼓励分散）
//
// 参数：
//   - schedulePolicy: 调度策略
//   - device: GPU 设备
//
// 返回值：
//   - float64: 设备分数（越高越优先）
//
// 计分规则：
// 1. binpackPolicy：
//   - 分数 = binpackMultiplier * (已用显存 / 总显存)
//   - 已用比例越高，分数越高
//   - binpackMultiplier 通常为 100，放大差异
//
// 2. spreadPolicy：
//   - 如果 UsedNum == 0（完全空闲），分数 = spreadMultiplier
//   - 否则分数 = 0
//   - spreadMultiplier 通常为 100，鼓励选择空闲 GPU
//
// 3. 默认策略：
//   - 分数 = 0
func GPUScore(schedulePolicy string, device *GPUDevice) float64 {
	var score float64
	switch schedulePolicy {
	case binpackPolicy:
		score = binpackMultiplier * (float64(device.UsedMem) / float64(device.Memory))
	case spreadPolicy:
		if device.UsedNum == 0 {
			score = spreadMultiplier
		}
	default:
		score = float64(0)
	}
	return score
}

// patchPodAnnotations 为 Pod 添加注解（本地版本）。
//
// 这个函数与 devices.PatchPodAnnotations 功能相同，
// 但这里是包内私有函数，可能是历史遗留。
// 建议统一使用 devices 包的公共版本。
func patchPodAnnotations(kubeClient kubernetes.Interface, pod *v1.Pod, annotations map[string]string) error {
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", pod.Name, err)
	}

	return err
}

// getConfig 获取 NVIDIA 配置。
//
// 如果全局配置已初始化，返回全局配置；
// 否则返回默认配置。
//
// 这种设计允许：
// - 动态更新配置（通过 ConfigMap）
// - 在配置未加载时使用安全的默认值
func getConfig() config.NvidiaConfig {
	if config.GetConfig() != nil {
		return config.GetConfig().NvidiaConfig
	}
	return config.GetDefaultDevicesConfig().NvidiaConfig
}
