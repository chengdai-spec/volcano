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

// Package hami 实现了 Volcano 对华为昇腾（Ascend）NPU 的 HAMi 通用 vNPU 调度。
//
// 与 NVIDIA 的 gpushare/vgpu 类似，这里的核心思想也是：
//  1. 通过读取节点注解拿到设备插件上报的真实 NPU 拓扑；
//  2. 根据 Pod 中声明的资源名（huawei.com/AscendXXX、huawei.com/AscendXXX-memory）
//     把请求折算成“需要几个 vNPU、每个 vNPU 多少显存/多少 AI Core”；
//  3. 在 FilterNode/ScoreNode 里判断节点是否满足，并在 Allocate 阶段把分配结果通过
//     JSON Patch 写回 Pod 注解，供设备插件最终挂载对应的虚拟设备。
//
// 实际案例：
//
//	某训练 Pod 声明 limits:
//	  huawei.com/Ascend910A: 2
//	  huawei.com/Ascend910A-memory: 8192
//	节点上存在 8 张 Ascend910A，每张 32GB 显存、30 个 AI Core，并已在注解
//	"hami.io/node-register-Ascend910A" 中上报了设备列表。HAMi 调度器会：
//	  (1) 把每个容器的请求解析为 2 个 Ascend910A，每个显存需求 8192MB；
//	  (2) 使用 trimMemory 把 8192 向上对齐到最近的 vNPU 模板（如 vir08 8738MB）；
//	  (3) 在 fit 中检查每张卡的剩余 Count/Memory/Core；
//	  (4) 如果节点开启了 NetworkID 拓扑（Ascend910 常见），优先把同一 PodGroup
//	      的 Pod 调度到同一个网络域，减少跨交换机通信；
//	  (5) 把选中的设备 UUID、模板名等通过注解写回 Pod，设备插件据此创建 vNPU。
package hami

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock"
	"volcano.sh/volcano/third_party/hami/util"
)

const (
	// DeviceBindAllocating 表示设备正在分配中。
	DeviceBindAllocating = "allocating"
	// DeviceBindFailed 表示设备分配失败。
	DeviceBindFailed = "failed"
	// DeviceBindSuccess 表示设备分配成功。
	DeviceBindSuccess = "success"

	// Ascend910Prefix 是 Ascend910 系列设备 CommonWord 的前缀，用于触发网络拓扑感知。
	Ascend910Prefix = "Ascend910"
	// Ascend910NetworkWeight 是拓扑同域加分权重，占比越大越优先选择同一 NetworkID。
	Ascend910NetworkWeight = 10

	// binpackPolicy 表示“紧凑”策略：设备上已用显存越多，得分越高，目的是把 Pod
	// 尽量堆到少量卡上，留出整块空闲卡给大任务。
	binpackPolicy = "binpack"
	// spreadPolicy 表示“分散”策略：设备上已用数量达到 1 时给分，目的是优先把 Pod
	// 放到空闲卡上，避免共享同一张卡。
	spreadPolicy = "spread"

	// binpackMultiplier / spreadMultiplier 只是计分放大系数，便于 deviceShare
	// 插件在多设备类型间比较得分。
	binpackMultiplier = 100
	spreadMultiplier  = 100
)

// AscendDevice 描述了一张昇腾 NPU 在调度器中的状态。
//
// config:           对应 VNPUConfig，包含资源名、模板、总显存等配置；
// nodeRegisterAnno: 设备插件在节点上注册的注解 key，例如 hami.io/node-register-Ascend910A；
// useUUIDAnno:      强制使用某张卡 UUID 的注解 key；
// noUseUUIDAnno:    强制不使用某张卡 UUID 的注解 key；
// DeviceInfo:       设备插件上报的真实设备信息（ID、显存、核心数、自定义拓扑等）；
// DeviceUsage:      调度器维护的该卡实时使用量；
// Score:            当前打分结果；
// PodMap:           已绑定到该卡上的 Pod 与各自占用情况，用于去重和释放。
type AscendDevice struct {
	config           config.VNPUConfig
	nodeRegisterAnno string
	useUUIDAnno      string
	noUseUUIDAnno    string
	DeviceInfo       *devices.DeviceInfo
	DeviceUsage      *devices.DeviceUsage
	Score            float64
	PodMap           map[string]*devices.DeviceUsage
}

// AscendDevices 描述了一个节点上某一类 Ascend 设备的集合。
//
// NodeName: 节点名；
// Type:     设备 CommonWord，如 Ascend910A、Ascend310P；
// Devices:  以设备 UUID 为 key 的 AscendDevice 集合；
// Policy:   当前使用的调度策略（binpack/spread）。
type AscendDevices struct {
	NodeName string
	Type     string
	Devices  map[string]*AscendDevice
	Policy   string
}

// RuntimeInfo 会写入 Pod 注解 "huawei.com/<CommonWord>"，记录最终挂载给容器的
// 设备 UUID 及其对应的 vNPU 模板名，供设备插件在容器启动时定位具体虚拟设备。
type RuntimeInfo struct {
	UUID string `json:"UUID,omitempty"`
	Temp string `json:"temp,omitempty"`
}

var (
	// AscendHAMiVNPUEnable 是命令行开关，控制是否启用 HAMi Ascend vNPU 调度。
	AscendHAMiVNPUEnable bool
	// NodeLockEnable 控制是否在分配设备时对节点加分布式锁，避免并发调度冲突。
	NodeLockEnable bool
)

// NewAscendDevices 根据节点信息构造该节点上所有已注册的 Ascend 设备集合。
//
// 执行流程：
//  1. 读取全局 VNPU 配置（config.GetConfig 或默认配置）；
//  2. 用 InitDevices 初始化每个 CommonWord 对应的 AscendDevice 模板；
//  3. 遍历这些模板，在 node.Status.Allocatable 中查找对应的资源名；
//  4. 如果资源存在，通过 GetNodeDevices 解析设备插件上报的节点注解，得到真实设备列表；
//  5. 把每张真实设备包装成 AscendDevice 并加入 AscendDevices.Devices。
//
// 实际案例：
//
//	节点 node-ascend-01 的 Allocatable 中有 huawei.com/Ascend310P: 8，
//	注解 hami.io/node-register-Ascend310P 包含 8 张卡的 UUID、显存、AI Core 等信息。
//	该函数会生成 Type=Ascend310P 的 AscendDevices，Devices 里保存 8 个 AscendDevice。
func NewAscendDevices(name string, node *v1.Node) map[string]*AscendDevices {
	ascendDevices := make(map[string]*AscendDevices)
	if node == nil {
		klog.Warningf("Node is nil for node %s, returning empty AscendDevices", name)
		return ascendDevices
	}
	curConfig := config.GetConfig()
	if curConfig == nil {
		klog.V(5).InfoS("cur config is null. call GetDefaultDevicesConfig")
		curConfig = config.GetDefaultDevicesConfig()
	}
	devs := InitDevices(curConfig.VNPUs)
	if node.Status.Allocatable == nil {
		klog.V(3).Infof("Node %s does not have allocatable resources information", node.Name)
		return ascendDevices
	}
	for _, dev := range devs {
		resourceName := dev.config.ResourceName
		num, ok := node.Status.Allocatable[v1.ResourceName(resourceName)]
		if !ok || num.IsZero() {
			klog.V(3).Infof("Node %s does not have allocatable %s resource or value is 0", node.Name, resourceName)
			continue
		}
		// 从节点注解中解析该类型设备的真实拓扑信息。
		nodeDevices, err := dev.GetNodeDevices(*node)
		if err != nil {
			klog.Warningf("Failed to get node devices. nodeName %s, deviceType %s, error %v", node.Name, dev.CommonWord(), err)
			continue
		}
		asDevices := &AscendDevices{
			NodeName: name,
			Type:     dev.CommonWord(),
			Devices:  make(map[string]*AscendDevice),
		}
		for _, nd := range nodeDevices {
			cur_dev := &AscendDevice{
				config:           dev.config,
				nodeRegisterAnno: dev.nodeRegisterAnno,
				useUUIDAnno:      dev.useUUIDAnno,
				noUseUUIDAnno:    dev.noUseUUIDAnno,
				DeviceInfo:       nd,
				DeviceUsage: &devices.DeviceUsage{
					Used:      0,
					Usedmem:   0,
					Usedcores: 0,
				},
				PodMap: make(map[string]*devices.DeviceUsage),
			}
			asDevices.Devices[nd.ID] = cur_dev
			klog.V(5).Infof("add device. ID %s dev_info %+v", cur_dev.DeviceInfo.ID, cur_dev.DeviceInfo)
		}
		ascendDevices[dev.CommonWord()] = asDevices
	}
	return ascendDevices
}

// GetAscendDeviceNames 返回当前已配置的所有 Ascend 设备 CommonWord 列表。
// 常用于 deviceShare 插件判断 Pod 是否请求了支持的设备类型。
func GetAscendDeviceNames() []string {
	curConfig := config.GetConfig()
	if curConfig == nil {
		klog.V(5).InfoS("cur config is null. call GetDefaultDevicesConfig")
		curConfig = config.GetDefaultDevicesConfig()
	}
	deviceNames := make([]string, 0, len(curConfig.VNPUs))
	for _, vnpu := range curConfig.VNPUs {
		deviceNames = append(deviceNames, vnpu.CommonWord)
	}
	return deviceNames
}

// AddResourceUsage 把某个 vNPU 资源占用累加到单张卡上。
//
// Used 加 1 表示又多了一个 Pod/容器占用该卡；Usedcores/Usedmem 累加实际占用的
// AI Core 百分比与显存。注意这里不做上界检查，调用方需先通过 fit 保证可分配。
func (ads *AscendDevices) AddResourceUsage(dev *AscendDevice, cores int32, mem int32) error {
	dev.DeviceUsage.Used++
	dev.DeviceUsage.Usedcores += cores
	dev.DeviceUsage.Usedmem += mem
	return nil
}

// SubResourceUsage 释放单张卡上的 vNPU 资源占用。
//
// 与 AddResourceUsage 对应，Used/Usedcores/Usedmem 分别减去本次释放量。
func (ads *AscendDevices) SubResourceUsage(dev *AscendDevice, cores int32, mem int32) error {
	dev.DeviceUsage.Used--
	dev.DeviceUsage.Usedcores -= cores
	dev.DeviceUsage.Usedmem -= mem
	return nil
}

// AddResource 在 Pod 被调度到本节点后，把其占用的设备资源累加到缓存。
//
// 这里读取 Pod 注解中 devices.SupportDevices[Type] 对应的已分配设备信息，
// 解码后更新 AscendDevice.DeviceUsage 与 PodMap。由于 informer 事件可能重复触发，
// 函数内部会判断 Pod UID 是否已存在，避免重复累加。
func (ads *AscendDevices) AddResource(pod *v1.Pod) {
	if ads == nil {
		return
	}
	ads.addResource(pod.Annotations, pod)
}

// SubResource 在 Pod 被删除或释放时，把其占用的设备资源从缓存中扣减。
//
// 通过 Pod 注解找到已分配设备，校验 Pod UID 存在于 PodMap 后再执行 SubResourceUsage。
func (ads *AscendDevices) SubResource(pod *v1.Pod) {
	if ads == nil {
		return
	}
	ano_key := devices.SupportDevices[ads.Type]
	ano, ok := pod.Annotations[ano_key]
	if !ok {
		return
	}
	con_devs, err := devices.DecodeContainerDevices(ano)
	if err != nil {
		klog.ErrorS(err, "failed to decode container devices", "pod", pod.Name, "annotation", ano)
		return
	}
	for _, cono_dev := range con_devs {
		dev, ok := ads.Devices[cono_dev.UUID]
		if !ok {
			klog.Warningf("ascend device %s not found", cono_dev.UUID)
			continue
		}
		if _, ok := dev.PodMap[string(pod.UID)]; ok {
			delete(dev.PodMap, string(pod.UID))
			ads.SubResourceUsage(dev, cono_dev.Usedcores, cono_dev.Usedmem)
			klog.V(5).Infof("sub resource usage for pod %s. device %s usedmem %d", pod.Name, dev.DeviceInfo.ID, cono_dev.Usedmem)
		}
	}
}

// addResource 是 AddResource 的内部实现，支持传入注解 map 进行复用。
func (ads *AscendDevices) addResource(annotations map[string]string, pod *v1.Pod) {
	ano_key := devices.SupportDevices[ads.Type]
	ano, ok := annotations[ano_key]
	if !ok {
		return
	}
	con_devs, err := devices.DecodeContainerDevices(ano)
	if err != nil {
		klog.ErrorS(err, "failed to decode container devices", "pod", pod.Name, "annotation", ano)
		return
	}
	for _, cono_dev := range con_devs {
		dev, ok := ads.Devices[cono_dev.UUID]
		if !ok {
			klog.Warningf("ascend device %s not found", cono_dev.UUID)
			continue
		}
		if _, ok := dev.PodMap[string(pod.UID)]; !ok {
			dev.PodMap[string(pod.UID)] = &devices.DeviceUsage{
				Used:      1,
				Usedcores: cono_dev.Usedcores,
				Usedmem:   cono_dev.Usedmem,
			}
			ads.AddResourceUsage(dev, cono_dev.Usedcores, cono_dev.Usedmem)
			klog.V(5).Infof("add resource usage for pod %s. device %s usedmem %d", pod.Name, dev.DeviceInfo.ID, cono_dev.Usedmem)
		}
	}
}

// AddQueueResource 返回队列资源增量，当前 Ascend HAMi 未实现队列维度统计。
func (ads *AscendDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	return map[string]float64{}
}

// HasDeviceRequest 判断 Pod 是否请求了当前类型的 Ascend 设备。
//
// 只有 AscendHAMiVNPUEnable 开启时才会真正检查；随后遍历所有容器，看 Limits 中
// 是否包含资源名或显存资源名。
//
// 实际案例：
//
//	若 Pod 中某个容器 limits 有 huawei.com/Ascend310P: 1，则返回 true，
//	deviceShare 插件会继续调用 FilterNode/ScoreNode。
func (ads *AscendDevices) HasDeviceRequest(pod *v1.Pod) bool {
	if !AscendHAMiVNPUEnable {
		return false
	}
	randDev, err := ads.getFirstDevice()
	if randDev == nil || err != nil {
		return false
	}
	var vnpu_config = randDev.config
	for _, container := range pod.Spec.Containers {
		_, ok := container.Resources.Limits[v1.ResourceName(vnpu_config.ResourceName)]
		if ok {
			klog.V(5).Infof("%s check HasDeviceRequest ok. %s", ads.Type, vnpu_config.ResourceName)
			return true
		}
		_, ok = container.Resources.Limits[v1.ResourceName(vnpu_config.ResourceMemoryName)]
		if ok {
			klog.V(5).Infof("%s check HasDeviceRequest ok. %s", ads.Type, vnpu_config.ResourceMemoryName)
			return true
		}
	}
	klog.V(5).Infof("%s check HasDeviceRequest false", ads.Type)
	return false
}

// FilterNode 是 deviceShare 插件的节点过滤入口。
//
// 调用 selectDevices 在设备快照上尝试为 Pod 选卡；只要返回错误即认为节点不可调度。
func (ads *AscendDevices) FilterNode(pod *v1.Pod, policy string) (int, string, error) {
	_, err := ads.selectDevices(pod, policy)
	if err != nil {
		return devices.Error, "no ascend device available", err
	}
	klog.V(4).Infoln("ascend DeviceSharing successfully filters pods. device_type:", ads.Type)
	return devices.Success, "", nil
}

// ScoreNode 为节点打分，供 deviceShare 插件做节点排序。
//
// 打分逻辑：
//  1. 先复用 selectDevices 得到实际要用的设备；
//  2. 对每张被选中的卡按 CalScore 计算 binpack/spread 得分；
//  3. 如果是 Ascend910 系列且所有卡都带 NetworkID，则额外加上拓扑同域分：
//     同一 NetworkID 的设备越多，得分越高。
//
// 实际案例：
//
//	某推理 Pod 请求 2 张 Ascend910B，节点 A 的两张卡都在 NetworkID=1 的域，
//	节点 B 的两张卡分别在不同域，则节点 A 会得到更高 ScoreNode 分数。
func (ads *AscendDevices) ScoreNode(pod *v1.Pod, policy string) float64 {
	ads.Policy = policy
	podDevs, err := ads.selectDevices(pod, policy)
	if err != nil {
		return 0
	}
	score := 0.0
	var usedDevs []*AscendDevice
	for _, dev := range podDevs {
		dev, ok := ads.Devices[dev[0].UUID]
		if !ok {
			return 0
		}
		usedDevs = append(usedDevs, dev)
		score += CalScore(policy, dev.DeviceUsage, dev.DeviceInfo)
	}

	// Ascend910 系列支持 HCCS/RoCE 网络域拓扑感知，优先把 Pod 放到同一 NetworkID。
	if strings.HasPrefix(ads.Type, Ascend910Prefix) && hasNetworkID(usedDevs) {
		klog.V(4).Infof("all devices have NetworkID. device CommonWord %s", ads.Type)
		cntMap := make(map[int]int)
		for _, dev := range usedDevs {
			if dev.DeviceInfo.CustomInfo == nil {
				return 0
			}
			if networkID, ok := dev.DeviceInfo.CustomInfo["NetworkID"]; ok {
				if id, ok := networkID.(float64); ok {
					cntMap[int(id)]++
				}
			} else {
				return 0
			}
		}
		maxCnt, totalCnt := 0, 0
		for _, cnt := range cntMap {
			if cnt > maxCnt {
				maxCnt = cnt
			}
			totalCnt += cnt
		}
		if totalCnt == 0 {
			return 0
		}
		score += (float64(maxCnt) / float64(totalCnt)) * Ascend910NetworkWeight
	}
	return score
}

// Allocate 在 Pod 通过过滤与打分后执行实际分配。
//
// 执行步骤：
//  1. 若开启 NodeLockEnable，先对节点加锁，防止并发分配同类型设备；
//  2. 再次调用 selectDevices 得到最终设备列表；
//  3. 调用 CreateAnnotations 生成需要写入 Pod 的注解；
//  4. 更新本地缓存 addResource；
//  5. 写入 assigned-node、assigned-time、device-bind-phase 等通用 HAMi 注解；
//  6. 通过 devices.PatchPodAnnotations 把注解批量 Patch 到 Pod；
//  7. 释放节点锁。
func (ads *AscendDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	klog.V(4).Infof("Allocate device %s to Pod %s", ads.Type, pod.Name)
	if NodeLockEnable {
		nodelock.UseClient(kubeClient)
		err := nodelock.LockNode(ads.NodeName, ads.Type)
		if err != nil {
			return errors.Errorf("node %s locked for %s hami vnpu. lockname %s", ads.NodeName, pod.Name, err.Error())
		}
	}
	podDevs, err := ads.selectDevices(pod, ads.Policy)
	if err != nil {
		return errors.Errorf("failed to select ascend devices for pod %s: %v", pod.Name, err)
	}
	annotations := ads.CreateAnnotations(pod, podDevs)

	ads.addResource(annotations, pod)
	annotations[util.AssignedNodeAnnotations] = ads.NodeName
	annotations[util.AssignedTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
	annotations[util.DeviceBindPhase] = "allocating"
	annotations[util.BindTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)

	err = devices.PatchPodAnnotations(kubeClient, pod, annotations)
	if err != nil {
		return err
	}
	if NodeLockEnable {
		nodelock.ReleaseNodeLock(ads.NodeName, ads.Type)
	}
	klog.V(4).Infof("Allocate Success. device %s Pod %s", ads.Type, pod.Name)
	return nil
}

// Release 预留接口，当前 Ascend HAMi 未实现显式释放逻辑，资源扣减由 SubResource 处理。
func (ads *AscendDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	return nil
}

// GetIgnoredDevices 返回 deviceShare 插件不需要再次统计的设备资源名。
//
// 例如 Ascend310P 同时定义了 count 资源 huawei.com/Ascend310P 与 memory 资源
// huawei.com/Ascend310P-memory，这里把显存资源标记为“已忽略”，避免重复计算。
func (ads *AscendDevices) GetIgnoredDevices() []string {
	randDev, err := ads.getFirstDevice()
	if randDev == nil || err != nil {
		return []string{""}
	}
	vnpuConfig := randDev.config
	return []string{vnpuConfig.ResourceMemoryName}
}

// GetStatus 返回设备状态字符串，当前未实现。
func (ads *AscendDevices) GetStatus() string {
	return ""
}

// DeepCopy 返回 AscendDevices 的深拷贝，用于 deviceShare 插件的 dry-run 模拟。
//
// 深拷贝会复制 Devices map、每个 AscendDevice 的 DeviceUsage 以及 PodMap，
// 避免在模拟调度过程中污染真实缓存。
func (ads *AscendDevices) DeepCopy() interface{} {
	if ads == nil {
		return nil
	}
	cp := &AscendDevices{
		NodeName: ads.NodeName,
		Type:     ads.Type,
		Policy:   ads.Policy,
		Devices:  make(map[string]*AscendDevice, len(ads.Devices)),
	}
	for id, dev := range ads.Devices {
		newUsage := &devices.DeviceUsage{
			Used:      dev.DeviceUsage.Used,
			Usedmem:   dev.DeviceUsage.Usedmem,
			Usedcores: dev.DeviceUsage.Usedcores,
		}
		newDev := &AscendDevice{
			config:           dev.config,
			nodeRegisterAnno: dev.nodeRegisterAnno,
			useUUIDAnno:      dev.useUUIDAnno,
			noUseUUIDAnno:    dev.noUseUUIDAnno,
			DeviceInfo:       dev.DeviceInfo,
			DeviceUsage:      newUsage,
			Score:            dev.Score,
			PodMap:           make(map[string]*devices.DeviceUsage),
		}
		for k, v := range dev.PodMap {
			newDev.PodMap[k] = &devices.DeviceUsage{
				Used:      v.Used,
				Usedmem:   v.Usedmem,
				Usedcores: v.Usedcores,
			}
		}
		cp.Devices[id] = newDev
	}
	return cp
}

// selectDevices 是 HAMi Ascend 调度的核心：为 Pod 的每个容器挑选满足需求的 vNPU。
//
// 核心流程：
//  1. 对当前节点该类型所有设备做快照（getDeviceSnapshot），避免影响原始缓存；
//  2. 按 CalScore 对快照设备从高到低排序；
//  3. 如果是 Ascend910 且全部设备都有 NetworkID，则启用拓扑感知 needTopology；
//  4. 对每个容器的资源请求：
//     a. 用 verifyReq 校验请求合法性（多设备时不允许部分显存）；
//     b. 依次检查 fit，挑选可用设备，去重已选设备；
//     c. 如果 needTopology 为 true，调用 selectDevicesWithTopology 在已选设备中
//     优先选择同一 NetworkID 的设备；
//     d. 把最终设备组装成 devices.ContainerDevice 列表。
//  5. 返回 devices.PodSingleDevice（每个容器对应一组设备）。
//
// 实际案例：
//
//	Pod 有 2 个容器，每个容器请求 Ascend910A-memory: 8192。
//	节点有 8 张卡，排序后优先选择已用显存最多的卡（binpack）。
//	对第一个容器， fit 检查通过则选择一张卡；第二个容器同理但跳过已选卡。
//	若 8 张卡分属 2 个 NetworkID，selectDevicesWithTopology 会让两张卡尽量来自同一域。
func (ads *AscendDevices) selectDevices(pod *v1.Pod, schedulePolicy string) (devices.PodSingleDevice, error) {
	dupDevs := getDeviceSnapshot(ads)
	if len(dupDevs) == 0 {
		return nil, errors.Errorf("no ascend device available")
	}
	for _, dev := range dupDevs {
		dev.Score = CalScore(schedulePolicy, dev.DeviceUsage, dev.DeviceInfo)
	}
	sort.Slice(dupDevs, func(i, j int) bool {
		return dupDevs[i].Score > dupDevs[j].Score
	})
	needTopology := false
	if strings.HasPrefix(ads.Type, Ascend910Prefix) && hasNetworkID(dupDevs) {
		klog.V(4).Infof("all devices have NetworkID. device CommonWord %s", ads.Type)
		needTopology = true
	}
	reqs := dupDevs[0].ResourceReqs(pod)
	var podDevs devices.PodSingleDevice
	usedDevs := make([]*AscendDevice, 0)
	for _, req := range reqs {
		klog.V(5).Infof("req %+v", req)
		err := verifyReq(req, dupDevs[0])
		if err != nil {
			return nil, err
		}
		// 构造可用设备列表，排除本 Pod 已经选中的设备。
		availableDevs := make([]*AscendDevice, 0)
		for _, dev := range dupDevs {
			selected := false
			for _, usedDev := range usedDevs {
				if usedDev.DeviceInfo.ID == dev.DeviceInfo.ID {
					selected = true
					break
				}
			}
			if !selected {
				availableDevs = append(availableDevs, dev)
			}
		}
		req_nums := req.Nums
		selectedDevs := make([]*AscendDevice, 0)
		for _, dev := range availableDevs {
			klog.V(5).Infof("check fit. req %+v dev_info %+v dev_usage %+v", req, dev.DeviceInfo, dev.DeviceUsage)
			if !fit(&req, dev) {
				klog.V(5).Infof("fit false. dev ID %s", dev.DeviceInfo.ID)
				continue
			}
			selectedDevs = append(selectedDevs, dev)
			req_nums -= 1
			if req_nums <= 0 && !needTopology {
				break
			}
		}
		if req_nums > 0 {
			klog.V(5).Infof("no enough ascend device available! raw req_nums %d cur req_nums %d", req.Nums, req_nums)
			return nil, errors.Errorf("no enough ascend device available")
		}
		// 拓扑感知场景下，从候选设备中按 NetworkID 集中度再精选一次。
		if needTopology {
			selectedDevs = selectDevicesWithTopology(int(req.Nums), selectedDevs)
		}
		usedDevs = append(usedDevs, selectedDevs...)
		var conDevs devices.ContainerDevices
		for _, dev := range selectedDevs {
			conDevs = append(conDevs, devices.ContainerDevice{
				UUID:       dev.DeviceInfo.ID,
				Type:       ads.Type,
				Usedmem:    req.Memreq,
				Usedcores:  req.Coresreq,
				CustomInfo: dev.DeviceInfo.CustomInfo,
			})
		}
		podDevs = append(podDevs, conDevs)
	}
	return podDevs, nil
}

// hasNetworkID 判断设备列表是否全部携带 NetworkID 自定义拓扑信息。
// 只要有一张卡缺失，就不启用拓扑感知，避免错误调度。
func hasNetworkID(devices []*AscendDevice) bool {
	for _, dev := range devices {
		if dev.DeviceInfo.CustomInfo == nil {
			return false
		}
		if _, ok := dev.DeviceInfo.CustomInfo["NetworkID"]; !ok {
			return false
		}
	}
	return true
}

// fit 判断单张卡是否满足一个容器的资源请求。
//
// 校验项：
//  1. 设备类型必须匹配（req.Type == dev.config.CommonWord）；
//  2. 当前卡已用数量必须小于 Count（支持的最大 vNPU 实例数）；
//  3. 剩余显存 >= Memreq；
//  4. 剩余 AI Core >= Coresreq；
//  5. 如果卡是整卡（Devcore==100）且请求也是整卡（Coresreq==100），则不允许与其他 Pod 共享；
//  6. 如果卡已有占用且核心已占满，则不再接受 Coresreq==0 的请求。
func fit(req *devices.ContainerDeviceRequest, dev *AscendDevice) bool {
	if req.Type != dev.config.CommonWord {
		return false
	}
	deviceUsage := dev.DeviceUsage
	deviceInfo := dev.DeviceInfo
	if deviceInfo.Count <= deviceUsage.Used {
		return false
	}
	if deviceInfo.Devmem-deviceUsage.Usedmem < req.Memreq {
		return false
	}
	if deviceInfo.Devcore-deviceUsage.Usedcores < req.Coresreq {
		return false
	}
	if deviceInfo.Devcore == 100 && req.Coresreq == 100 && deviceUsage.Used > 0 {
		return false
	}
	if deviceInfo.Devcore != 0 && deviceUsage.Usedcores == deviceInfo.Devcore && req.Coresreq == 0 {
		return false
	}
	return true
}

// getDeviceSnapshot 复制 AscendDevices.Devices 为切片形式，便于排序和模拟调度。
// 深拷贝 DeviceUsage 与 PodMap，避免污染原始状态。
func getDeviceSnapshot(ads *AscendDevices) []*AscendDevice {
	dupDevs := make([]*AscendDevice, 0, len(ads.Devices))
	for _, dev := range ads.Devices {
		dupDev := &AscendDevice{
			config:           dev.config,
			nodeRegisterAnno: dev.nodeRegisterAnno,
			useUUIDAnno:      dev.useUUIDAnno,
			noUseUUIDAnno:    dev.noUseUUIDAnno,
			DeviceInfo:       dev.DeviceInfo,
			DeviceUsage: &devices.DeviceUsage{
				Used:      dev.DeviceUsage.Used,
				Usedmem:   dev.DeviceUsage.Usedmem,
				Usedcores: dev.DeviceUsage.Usedcores,
			},
			PodMap: make(map[string]*devices.DeviceUsage),
		}
		for k, v := range dev.PodMap {
			dupDev.PodMap[k] = &devices.DeviceUsage{
				Used:      v.Used,
				Usedmem:   v.Usedmem,
				Usedcores: v.Usedcores,
			}
		}
		dupDevs = append(dupDevs, dupDev)
	}
	return dupDevs
}

// selectDevicesWithTopology 在候选设备中按 NetworkID 集中度选择指定数量的设备。
//
// 先按 NetworkID 分组计数，再按数量从大到小排序，优先从设备最多的域取，
// 直到取够 req_nums 张卡。这样可以尽量保证分布式训练/推理的 Pod 内部通信
// 发生在同一网络域内。
func selectDevicesWithTopology(req_nums int, selected_devs []*AscendDevice) []*AscendDevice {
	networkMap := make(map[int][]*AscendDevice)

	for _, dev := range selected_devs {
		if dev.DeviceInfo.CustomInfo != nil {
			if networkID, ok := dev.DeviceInfo.CustomInfo["NetworkID"]; ok {
				if id, ok := networkID.(float64); ok {
					networkMap[int(id)] = append(networkMap[int(id)], dev)
				}
			}
		}
	}
	type NetworkDeviceCount struct {
		NetworkID int
		Count     int
	}
	var sortedNetworks []NetworkDeviceCount
	for networkID, devices := range networkMap {
		sortedNetworks = append(sortedNetworks, NetworkDeviceCount{
			NetworkID: networkID,
			Count:     len(devices),
		})
	}
	sort.Slice(sortedNetworks, func(i, j int) bool {
		return sortedNetworks[i].Count > sortedNetworks[j].Count
	})
	devs := make([]*AscendDevice, 0)
	for _, item := range sortedNetworks {
		for _, dev := range networkMap[item.NetworkID] {
			devs = append(devs, dev)
			if len(devs) == req_nums {
				return devs
			}
		}
	}
	return devs
}

// getFirstDevice 从 Devices map 中任意取一张卡，用于获取该类型设备的配置信息。
func (ads *AscendDevices) getFirstDevice() (*AscendDevice, error) {
	if len(ads.Devices) == 0 {
		return nil, errors.New("no ascend device available")
	}
	for _, dev := range ads.Devices {
		return dev, nil
	}
	return nil, errors.New("no ascend device available")
}

// trimMemory 把用户请求的显存按 vNPU 模板向上取整。
//
// 逻辑：
//  1. 遍历 config.Templates，找到第一个 Memory >= m 的模板，返回该模板内存与模板名；
//  2. 如果没有模板命中，但 m <= MemoryCapacity，则返回 MemoryAllocatable，模板名为空；
//  3. 如果 m > MemoryCapacity，返回 0，表示无法满足。
//
// 实际案例：
//
//	Ascend310P 配置模板 vir01/3072MB、vir02/6144MB、vir04/12288MB，
//	请求 5000MB 会向上对齐到 vir02 的 6144MB；请求 15000MB 则对齐到 memoryAllocatable 21527MB。
func (dev *AscendDevice) trimMemory(m int64) (int64, string) {
	for i := range dev.config.Templates {
		if m <= dev.config.Templates[i].Memory {
			return dev.config.Templates[i].Memory, dev.config.Templates[i].Name
		}
	}
	if m <= dev.config.MemoryCapacity {
		return dev.config.MemoryAllocatable, ""
	}
	return 0, ""
}

// InitDevices 根据全局 VNPUConfig 初始化 AscendDevice 模板列表。
//
// 对每个 VNPUConfig：
//  1. 构造节点注册、使用/不使用 UUID 三类 HAMi 注解 key；
//  2. 按模板内存从小到大排序，便于 trimMemory 向上取整；
//  3. 在全局 devices.InRequestDevices / devices.SupportDevices 中注册该 CommonWord
//     对应的请求注解与已分配注解 key，供后续 AddResource/SubResource 使用。
func InitDevices(config []config.VNPUConfig) []*AscendDevice {
	devs := make([]*AscendDevice, 0)
	for _, vnpu := range config {
		commonWord := vnpu.CommonWord
		dev := &AscendDevice{
			config:           vnpu,
			nodeRegisterAnno: fmt.Sprintf("%s/node-register-%s", util.HAMiAnnotationsPrefix, commonWord),
			useUUIDAnno:      fmt.Sprintf("%s/use-%s-uuid", util.HAMiAnnotationsPrefix, commonWord),
			noUseUUIDAnno:    fmt.Sprintf("%s/no-use-%s-uuid", util.HAMiAnnotationsPrefix, commonWord),
		}
		sort.Slice(dev.config.Templates, func(i, j int) bool {
			return dev.config.Templates[i].Memory < dev.config.Templates[j].Memory
		})
		_, ok := devices.InRequestDevices[commonWord]
		if !ok {
			devices.InRequestDevices[commonWord] = fmt.Sprintf("%s/%s-devices-to-allocate", util.HAMiAnnotationsPrefix, commonWord)
			devices.SupportDevices[commonWord] = fmt.Sprintf("%s/%s-devices-allocated", util.HAMiAnnotationsPrefix, commonWord)
		}
		devs = append(devs, dev)
		klog.Infof("load ascend vnpu config %s: %v", commonWord, dev.config)
	}
	return devs
}

// ParseConfig 注册命令行参数。
func ParseConfig(fs *flag.FlagSet) {
	fs.BoolVar(&AscendHAMiVNPUEnable, "AscendHAMiVNPUEnable", false, "enable ascend device")
}

// CommonWord 返回当前设备类型的 CommonWord。
func (dev *AscendDevice) CommonWord() string {
	return dev.config.CommonWord
}

// GetNodeDevices 从节点注解中解析该类型设备的真实列表。
//
// 读取 key 为 dev.nodeRegisterAnno 的注解，通过 devices.UnMarshalNodeDevices 反序列化，
// 得到每张卡的 ID、显存、核心、自定义拓扑等信息。
func (dev *AscendDevice) GetNodeDevices(n v1.Node) ([]*devices.DeviceInfo, error) {
	anno, ok := n.Annotations[dev.nodeRegisterAnno]
	if !ok {
		return []*devices.DeviceInfo{}, fmt.Errorf("annos not found %s", dev.nodeRegisterAnno)
	}
	nodeDevices, err := devices.UnMarshalNodeDevices(anno)
	if err != nil {
		klog.ErrorS(err, "failed to unmarshal node devices", "node", n.Name, "device annotation", anno)
		return []*devices.DeviceInfo{}, err
	}
	if len(nodeDevices) == 0 {
		klog.InfoS("no ascend device found", "node", n.Name, "device annotation", anno)
		return []*devices.DeviceInfo{}, errors.New("no device found on node")
	}
	return nodeDevices, nil
}

// ResourceReqs 解析 Pod 中每个容器的资源请求，并按模板对齐显存。
//
// 步骤：
//  1. 调用 devices.ExtractResourceRequest 提取请求，得到 Nums、Memreq、MemPercentagereq、Coresreq；
//  2. 如果显存未设置但百分比设置了，按 Devmem * percentage / 100 计算；
//  3. 如果显存大于 0，调用 trimMemory 向上对齐到最近的 vNPU 模板。
//
// 注意：Ascend HAMi 目前不单独处理 cores 请求字段，cores 由后续 fit 判断。
func (dev *AscendDevice) ResourceReqs(pod *v1.Pod) []devices.ContainerDeviceRequest {
	reqs := devices.ExtractResourceRequest(pod, dev.CommonWord(), dev.config.ResourceName, dev.config.ResourceMemoryName, "", "")
	for i := range reqs {
		req := &reqs[i]
		if req.Memreq == 0 && req.MemPercentagereq != 0 {
			req.Memreq = int32(dev.DeviceInfo.Devmem * req.MemPercentagereq / 100)
			klog.V(5).Infof("new memreq %d totalmem %d mempercentage %d", req.Memreq, dev.DeviceInfo.Devmem, req.MemPercentagereq)
		}
		if req.Memreq > 0 {
			m, _ := dev.trimMemory(int64(req.Memreq))
			klog.V(5).Infof("raw mem %d, trimed mem %d", req.Memreq, m)
			req.Memreq = int32(m)
		}
	}
	return reqs
}

// CreateAnnotations 根据选中的设备列表构造需要写入 Pod 的注解。
//
// 生成的注解包括：
//  1. devices.InRequestDevices[CommonWord] / devices.SupportDevices[CommonWord]：
//     编码后的设备分配结果，便于设备插件挂载；
//  2. "predicate-time"：调度时间戳；
//  3. "huawei.com/<CommonWord>"：JSON 编码的 RuntimeInfo 列表，记录 UUID 与模板名。
func (ads *AscendDevices) CreateAnnotations(pod *v1.Pod, devList devices.PodSingleDevice) map[string]string {
	annotations := make(map[string]string)
	dev, err := ads.getFirstDevice()
	if err != nil {
		return annotations
	}
	commonWord := dev.CommonWord()

	annotations[devices.InRequestDevices[commonWord]] = devices.EncodePodSingleDevice(devList)
	annotations[devices.SupportDevices[commonWord]] = devices.EncodePodSingleDevice(devList)
	annotations["predicate-time"] = strconv.FormatInt(time.Now().Unix(), 10)
	allocateStr := fmt.Sprintf("huawei.com/%s", dev.CommonWord())
	var rtInfo []RuntimeInfo
	for _, dp := range devList {
		for _, val := range dp {
			_, temp := dev.trimMemory(int64(val.Usedmem))
			rtInfo = append(rtInfo, RuntimeInfo{
				UUID: val.UUID,
				Temp: temp,
			})
		}
	}
	s, err := json.Marshal(rtInfo)
	if err != nil {
		klog.ErrorS(err, "failed to marshal runtime info", "runtime info", rtInfo)
	}
	annotations[allocateStr] = string(s)

	return annotations
}

// GetResourceNames 返回该设备类型对应的资源名集合。
//
// 这里只返回 count 资源名与 memory 资源名，core 资源名留空，
// 因为 Ascend HAMi 当前通过模板而非独立 core 资源来切分算力。
func (dev *AscendDevice) GetResourceNames() devices.ResourceNames {
	return devices.ResourceNames{
		ResourceCountName:  dev.config.ResourceName,
		ResourceMemoryName: dev.config.ResourceMemoryName,
		ResourceCoreName:   "",
	}
}

// CalScore 根据调度策略计算单张卡的得分。
//
// binpack: 已用显存 / 总显存 越大越好，鼓励堆叠；
// spread:  当该卡已被占用一次（Used==1）时给满分，鼓励把任务放到已占用的卡上，
//
//	留出更多空闲整卡；
//
// 其他策略默认 0 分。
func CalScore(schedulePolicy string, dev_usage *devices.DeviceUsage, dev_info *devices.DeviceInfo) float64 {
	var score float64
	switch schedulePolicy {
	case binpackPolicy:
		score = binpackMultiplier * (float64(dev_usage.Usedmem) / float64(dev_info.Devmem))
	case spreadPolicy:
		if dev_usage.Used == 1 {
			score = spreadMultiplier
		}
	default:
		score = float64(0)
	}
	return score
}

// verifyReq 校验单个容器的资源请求是否合法。
//
// 关键约束：当请求多个设备（req.Nums > 1）时，trimMemory 后的显存必须等于
// config.MemoryAllocatable，即不允许多个设备使用部分显存模板。
// 这是因为多卡训练通常要求每张卡资源一致且为整卡能力。
func verifyReq(req devices.ContainerDeviceRequest, dev *AscendDevice) error {
	trimMem, _ := dev.trimMemory(int64(req.Memreq))
	if req.Nums > 1 {
		if trimMem != dev.config.MemoryAllocatable {
			klog.V(5).Infof("VNPU does not support using partial memory when specifying multiple devices, req_nums %d, req_memreq %d", req.Nums, req.Memreq)
			return errors.Errorf("vNPU does not support partial memory allocation when requesting multiple devices")
		}
	}
	return nil
}
