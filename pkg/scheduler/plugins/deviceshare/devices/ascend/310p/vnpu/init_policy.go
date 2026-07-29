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

package vnpu310p

// ──────────────────────────────────────────────────────────────────────────────
// init_policy.go  —— MindCluster 动态 vNPU 初始化策略
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ 调用链路：                                                                ║
// ║   deviceshare.OnSessionOpen                                              ║
// ║     → initializeDevicesWithSession                                       ║
// ║       → initializeDevice (switch *vnpu.NPUDevices)                       ║
// ║         → vnpu310p.InitVNPUDevice(device, ssn, nodeInfo)                 ║
// ║                                                                          ║
// ║ 三步初始化：                                                              ║
// ║   Step1: initVolcanoFrameFromSsn  注入 Session + 构建 VJobTemplate       ║
// ║   Step2: initCmInformer           初始化 ConfigMap Informer              ║
// ║   Step3: initNodeFromSsn          构建节点 NPU 设备视图                   ║
// ║           ├─ 获取设备信息 (ConfigMap / ClusterD)                          ║
// ║           ├─ 同步注解 (四源合并)                                          ║
// ║           ├─ 更新设备信息 (一致性保护)                                    ║
// ║           ├─ 识别芯片型号 (ChipKind / ServerType / ChipType)              ║
// ║           ├─ 计算资源总量 (TotalRes / TotalChipNum / AiCorePerChip)       ║
// ║           └─ 创建 VChip + 恢复已运行 Pod 资源                             ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
// ──────────────────────────────────────────────────────────────────────────────

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/third_party/mindcluster/common/k8s"
	"volcano.sh/volcano/third_party/mindcluster/common/util"
	"volcano.sh/volcano/third_party/mindcluster/config"
	"volcano.sh/volcano/third_party/mindcluster/plugin"
)

// InitVNPUDevice 是 MindCluster 动态 vNPU 在每个调度 Session 中的初始化入口。
//
// 参数：
//   - device:   当前节点的 NPUDevices 实例（由 deviceshare 插件在 NewNPUDevices 中构造的空壳）
//   - ssn:      Volcano 调度框架的 Session，包含 KubeClient、Informer、配置等
//   - nodeInfo: Volcano 缓存中该节点的资源快照（Capacity/Allocate/Idle/Tasks 等）
//
// 三步初始化流程：
//  1. initVolcanoFrameFromSsn  —— 注入 Session 上下文 + 构建 VJobTemplate 模板表
//  2. initCmInformer           —— 初始化 ConfigMap Informer（监听 device-info/noded/switch）
//  3. initNodeFromSsn          —— 从 K8s 数据构建节点 NPU 设备视图
func InitVNPUDevice(device *vnpu.NPUDevices, ssn *framework.Session, nodeInfo *api.NodeInfo) error {
	if ssn == nil {
		klog.V(util.LogDebugLev).Infof("InitVNPUDevice failed: %s.", util.ArgumentError)
		return errors.New(util.ArgumentError)
	}

	klog.V(util.LogDebugLev).Infof("enter %s InitVNPUDevice.", "DeviceShare")
	defer klog.V(util.LogDebugLev).Infof("leave %s InitNPUSession.", "DeviceShare")

	// ── Step1: 注入 Volcano 框架上下文（KubeClient、Informer、VJobTemplate 模板表等） ──
	// use information in ssn and nodeInfo to initialize device struct, and exclude api package
	initVolcanoFrameFromSsn(device, ssn)

	// ── Step2: 初始化 ConfigMap Informer，监听设备信息/节点状态/交换机状态 ConfigMap ──
	initCmInformer(device)

	// ── Step3: 从节点信息构建设备视图（芯片识别 → 资源计算 → VChip 创建 → Pod 资源恢复） ──
	initNodeFromSsn(device, nodeInfo)

	return nil
}

// initCmInformer 初始化 ConfigMap Informer，用于监听 MindCluster 生态中的各类 ConfigMap：
//   - device-info ConfigMap：上报各节点芯片健康状态、空闲列表
//   - node-info ConfigMap：上报节点健康状态（Healthy/SubHealthy/UnHealthy）
//   - switch-info ConfigMap：上报交换机故障信息
//
// 如果启用了 ClusterD（集群信息管理器），则使用集群级 ConfigMap，否则使用节点级 ConfigMap。
func initCmInformer(device *vnpu.NPUDevices) {
	if device.FrameAttr.KubeClient == nil {
		klog.V(util.LogErrorLev).Info("kube client in session is nil")
		return
	}
	k8s.InitCmInformer(device.FrameAttr.KubeClient, device.FrameAttr.UseClusterD)
}

// initVolcanoFrameFromSsn 是初始化的第一步：将 Volcano Session 中的关键信息注入到 NPUDevices 实例。
//
// 具体完成的工作：
//  1. 从 Session 的 Configurations 中加载配置参数(如 UseClusterD、PresetVirtualDevice 等)
//  2. 注入 KubeClient、InformerFactory、Session UID
//  3. 构建 VJobTemplate —— 芯片型号 → vNPU 模板名 → VResource 的映射表
//  4. 初始化静态参数（UseClusterD、SelfMaintainAvailCard）和动态参数（PresetVirtualDevice）
//
// VJobTemplate 映射表说明（以 Ascend310P 为例）：
// ┌────────────────┬────────┬────────┬────────┬────────────────────────────┐
// │ 模板名          │ Aicore │ Aicpu  │ DVPP   │ 场景说明                     │
// ├────────────────┼────────┼────────┼────────┼────────────────────────────┤
// │ vir01          │   1    │   1    │ null   │ 最小切分，单核推理             │
// │ vir02          │   2    │   2    │ null   │ 两核切分                     │
// │ vir02_1c       │   2    │   1    │ null   │ 两核 + AI CPU 降级到 1       │
// │ vir04          │   4    │   4    │ null   │ 四核切分                     │
// │ vir04_3c       │   4    │   3    │ null   │ 四核 + AI CPU 降级到 3       │
// │ vir04_3c_ndvpp │   4    │   3    │ no     │ 四核 + 3c + 禁用 DVPP       │
// │ vir04_4c_dvpp  │   4    │   4    │ yes    │ 四核 + 强制开启 DVPP         │
// └────────────────┴────────┴────────┴────────┴────────────────────────────┘
//
// DVPP 状态含义：
//   - "null"：无要求,芯片 DVPP 开/关均可
//   - "yes"：必须开启 DVPP(视频预处理加速)
//   - "no"：必须关闭 DVPP
func initVolcanoFrameFromSsn(device *vnpu.NPUDevices, ssn *framework.Session) {
	if ssn == nil {
		klog.V(util.LogErrorLev).Infof("InitVolcanoFrameFromSsn failed: %s.", util.ArgumentError)
		return
	}

	// 从 Session.Configurations 中加载配置，转换为 map[string]string 便于查找
	configs := getConfigurationByKey(initConfsFromSsn(ssn.Configurations))

	// 注入 Session 上下文信息
	device.FrameAttr.UID = ssn.UID
	device.FrameAttr.KubeClient = ssn.KubeClient()
	device.FrameAttr.InformerFactory = ssn.InformerFactory()

	// ── 构建 VJobTemplate：芯片型号 → vNPU 模板名 → VResource(Aicore, Aicpu, DVPP) ──
	// 这张表是硬编码的，覆盖了 7 种芯片型号，供调度时查询模板对应的资源规格。
	device.FrameAttr.VJobTemplate = map[string]map[string]vnpu.VResource{
		// ── Ascend310P：推理芯片，8 个 AI Core，支持 7 种切分模板 ──
		util.Ascend310P: {
			plugin.VNPUTempVir01:        {Aicore: 1, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},                           // vir01: 最小 1 核切分
			plugin.VNPUTempVir02:        {Aicore: util.NPUIndex2, Aicpu: util.NPUIndex2, DVPP: plugin.AscendDVPPEnabledNull}, // vir02: 2 核 2CPU
			plugin.VNPUTempVir02C1:      {Aicore: util.NPUIndex2, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},              // vir02_1c: 2 核 + CPU 降级到 1
			plugin.VNPUTempVir04:        {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex4, DVPP: plugin.AscendDVPPEnabledNull}, // vir04: 4 核 4CPU
			plugin.VNPUTempVir04C3:      {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull}, // vir04_3c: 4 核 + CPU 降级到 3
			plugin.VNPUTempVir04C3NDVPP: {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledOff},  // vir04_3c_ndvpp: 禁用 DVPP
			plugin.VNPUTempVir04C4cDVPP: {Aicore: util.NPUIndex4, Aicpu: util.NPUIndex4, DVPP: plugin.AscendDVPPEnabledOn},   // vir04_4c_dvpp: 强制开启 DVPP
		},
		// ── Ascend910：训练芯片，支持 4 种切分模板（vir02~vir16） ──
		util.Ascend910: {
			plugin.VNPUTempVir02: {Aicore: util.NPUIndex2, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},               // vir02: 2 核
			plugin.VNPUTempVir04: {Aicore: util.NPUIndex4, Aicpu: 1, DVPP: plugin.AscendDVPPEnabledNull},               // vir04: 4 核
			plugin.VNPUTempVir08: {Aicore: util.NPUIndex8, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},  // vir08: 8 核
			plugin.VNPUTempVir16: {Aicore: util.NPUIndex16, Aicpu: util.NPUIndex7, DVPP: plugin.AscendDVPPEnabledNull}, // vir16: 16 核
		},
		// ── 910B1：25 Core 芯片，支持 3 种切分(vir03/vir06/vir12) ──
		plugin.ChipTypeB1: {
			plugin.VNPUTempVir06: {Aicore: util.NPUIndex6, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},  // vir06: 6 核
			plugin.VNPUTempVir03: {Aicore: util.NPUIndex3, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},  // vir03: 3 核
			plugin.VNPUTempVir12: {Aicore: util.NPUIndex12, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull}, // vir12: 12 核
		},
		// ── 910B2C：24 Core 芯片，与 B1 相同的切分方案 ──
		plugin.ChipTypeB2C: {
			plugin.VNPUTempVir06: {Aicore: util.NPUIndex6, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir03: {Aicore: util.NPUIndex3, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir12: {Aicore: util.NPUIndex12, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
		// ── 910B2：24 Core 芯片，与 B1 相同的切分方案 ──
		plugin.ChipTypeB2: {
			plugin.VNPUTempVir06: {Aicore: util.NPUIndex6, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir03: {Aicore: util.NPUIndex3, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},
			plugin.VNPUTempVir12: {Aicore: util.NPUIndex12, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull},
		},
		// ── 910B3：20 Core 芯片，支持 2 种切分（vir05/vir10） ──
		plugin.ChipTypeB3: {
			plugin.VNPUTempVir05: {Aicore: util.NPUIndex5, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},  // vir05: 5 核
			plugin.VNPUTempVir10: {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull}, // vir10: 10 核
		},
		// ── 910B4：20 Core 芯片，支持 4 种切分（含 DVPP 开关变体） ──
		plugin.ChipTypeB4: {
			plugin.VNPUB4TempVir05:     {Aicore: util.NPUIndex5, Aicpu: util.NPUIndex1, DVPP: plugin.AscendDVPPEnabledNull},  // 5 核
			plugin.VNPUB4TempVir10C3NM: {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledOff},  // 10 核 + 禁用 DVPP
			plugin.VNPUB4TempVir10C4M:  {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex4, DVPP: plugin.AscendDVPPEnabledOn},   // 10 核 + 强制 DVPP
			plugin.VNPUB4TempVir10:     {Aicore: util.NPUIndex10, Aicpu: util.NPUIndex3, DVPP: plugin.AscendDVPPEnabledNull}, // 10 核标准
		},
	}
	// 初始化动态参数(PresetVirtualDevice 等，可在运行期通过 ConfigMap 调整)
	initDynamicParameters(device, configs)
	// 初始化静态参数(UseClusterD、SelfMaintainAvailCard，仅首次 Session 生效)
	initStaticParameters(device, configs)
}

// initStaticParameters 初始化静态配置参数。
// 通过 sync.Once 保证全局只初始化一次，避免重复加载。
//   - UseClusterD:           是否使用集群信息管理器（ClusterD），默认 true
//   - SelfMaintainAvailCard: Volcano 是否自行维护可用卡列表，默认 true
func initStaticParameters(device *vnpu.NPUDevices, configs map[string]string) {
	device.FrameAttr.OnceInit.Do(func() {
		device.FrameAttr.UseClusterD = getUseClusterDConfig(configs)
		device.FrameAttr.SelfMaintainAvailCard = getSelfMaintainAvailCard(configs)
		klog.V(util.LogWarningLev).Infof("init static parameters, UseClusterD"+
			" is <%v>", device.FrameAttr.UseClusterD)
	})
}

// getUseClusterDConfig 从 ConfigMap 配置中读取是否启用 ClusterD（集群信息管理器）。
// ClusterD 将设备信息、节点信息汇总到集群级 ConfigMap，而非每个节点单独创建 ConfigMap。
// 默认返回 true（启用）。
func getUseClusterDConfig(conf map[string]string) bool {
	useClusterInfoManager, ok := conf[util.UseClusterInfoManager]
	if !ok {
		klog.V(util.LogDebugLev).Info("CheckUseCIMByConfig doesn't exist useClusterInfoManager.")
		return true
	}
	return useClusterInfoManager == "true"
}

// getSelfMaintainAvailCard 从 ConfigMap 配置中读取是否启用 Volcano 自维护可用卡。
// 启用后调度器会根据设备信息和缓存自行维护可用芯片列表，而非完全依赖设备插件上报。
// 默认返回 true（启用）。
func getSelfMaintainAvailCard(conf map[string]string) bool {
	selfMaintainAvailCard, ok := conf[util.SelfMaintainAvailCard]
	if !ok {
		klog.V(util.LogDebugLev).Info("CheckUseCIMByConfig doesn't exist self-maintain-available-card.")
		return true
	}
	return selfMaintainAvailCard == "true"
}

// initDynamicParameters 初始化动态配置参数，这些参数可以在运行期通过 ConfigMap 调整
//   - PresetVirtualDevice: 是否启用预置虚拟设备(presetVirtualDevice), 默认 false
//     启用后调度器会将已切分的 vNPU 视为'预置'状态，影响过滤和打分逻辑.
func initDynamicParameters(device *vnpu.NPUDevices, configs map[string]string) {
	if device == nil || configs == nil {
		klog.V(util.LogInfoLev).Infof("InitCache failed: %s.", util.ArgumentError)
		return
	}
	device.FrameAttr.PresetVirtualDevice = getPresetVirtualDeviceConfig(configs)
}

// getPresetVirtualDeviceConfig 从 ConfigMap 配置中读取是否启用预置虚拟设备
// 默认返回 false(不启用)
func getPresetVirtualDeviceConfig(conf map[string]string) bool {
	// 从配置 map 中查找 "presetVirtualDevice" 键
	segmentEnable, ok := conf[util.SegmentEnable]
	if !ok {
		klog.V(util.LogDebugLev).Info("checkVNPUSegmentEnable doesn't exist presetVirtualDevice.")
		return false
	}
	return segmentEnable == "true"
}

// initConfsFromSsn 将 Volcano 框架的 Configuration 列表转换为 MindCluster 内部格式。
//
// Volcano 的 conf.Configuration 和 MindCluster 的 config.Configuration 结构相同但包路径不同，
// 这里通过 YAML 序列化/反序列化进行转换。
func initConfsFromSsn(confs []conf.Configuration) []config.Configuration {
	var out []byte
	var err error
	newConfs := make([]config.Configuration, len(confs))
	for idx, cfg := range confs {
		newCfg := &config.Configuration{}
		out, err = yaml.Marshal(cfg)
		if err != nil {
			klog.V(util.LogInfoLev).Infof("Marshal configuration failed: %s.", err)
			continue
		}
		if err = yaml.Unmarshal(out, newCfg); err != nil {
			klog.V(util.LogInfoLev).Infof("Unmarshal configuration failed: %s.", err)
			continue
		}
		newConfs[idx] = *newCfg
	}
	return newConfs
}

// getConfigurationByKey 从配置列表中查找名为 "init-params" 的配置项，
// 并将其 Arguments 字段（map[string]string）返回。
// 该配置项包含了调度器的核心初始化参数。
func getConfigurationByKey(configurations []config.Configuration) map[string]string {
	for _, cf := range configurations {
		if cf.Name == util.CMInitParamKey {
			return cf.Arguments
		}
	}
	return map[string]string{}
}

// initNodeFromSsn 是初始化的第三步：从 Volcano Session 中的节点信息构建 NPU 设备视图。
//
// 该函数从三个数据源获取信息：
//  1. deviceInfos:        设备信息（芯片健康状态、空闲列表等），来自 device-info ConfigMap 或 ClusterD
//  2. nodeInfosOfNodeD:   节点状态信息（Healthy/SubHealthy/UnHealthy），来自 node-info ConfigMap
//  3. nodeInfo:           Volcano 缓存中的节点资源快照（Capacity/Allocate/Idle/Tasks 等）
//
// 最终调用 initNPUNodeByNodeInf 完成所有设备视图的构建。
func initNodeFromSsn(device *vnpu.NPUDevices, nodeInfo *api.NodeInfo) {
	klog.V(util.LogDebugLev).Infof("Entering initNodeFron Ssn function")

	// 1. 获取设备信息（芯片健康状态、空闲卡列表等）
	//    如果节点不在 Session 中，其设备信息不会被缓存
	deviceInfos := k8s.GetDeviceInfoAndSetInformerStart(nodeInfo, device.FrameAttr.UseClusterD,
		device.FrameAttr.SelfMaintainAvailCard)
	// 2. 获取 nodeD（节点守护进程）上报的节点健康状态
	nodeInfosOfNodeD := k8s.GetNodeDInfo(nodeInfo)
	// 3. 获取交换机信息（交换机故障可能影响节点可用性）
	//switchInfos := k8s.GetSwitchInfos(nodeInfo)

	// 将三个数据源的信息合并到 device 中，构建完整的 NPU 设备视图
	// 如果初始化失败且不是“无 NPU 资源”错误，则记录日志
	if err := initNPUNodeByNodeInf(device, nodeInfo, deviceInfos, nodeInfosOfNodeD, device.FrameAttr.VJobTemplate); err != nil &&
		!strings.Contains(err.Error(), vnpu.NoneResourceErr) {
		klog.V(util.LogErrorLev).Infof("InitNodeFromSsn %s %s, not put in nodes.", nodeInfo.Name, err)
	}
}

// initNPUNodeByNodeInf 是节点 NPU 设备视图构建的核心函数。
//
// 它将三个数据源的信息合并到 NPUDevices 实例中，完成以下工作：
//  1. 获取并校验节点 NPU 资源能力（Capacity）
//  2. 填充节点基本信息（Name、Capability、Label、Annotation、Address 等）
//  3. 同步注解（四源合并：节点注解 + 上次 Session 设备信息 + NodeD 状态 + Volcano 缓存）
//  4. 更新设备信息（一致性保护机制，确保只调度健康芯片）
//  5. 调用 setNodeVNPUInfo 完成芯片识别、资源计算、VChip 创建、Pod 资源恢复
func initNPUNodeByNodeInf(
	device *vnpu.NPUDevices,
	npuNode *api.NodeInfo,
	deviceInfo k8s.NodeDeviceInfoWithID,
	nodeInfoOfNodeD k8s.NodeDNodeInfo,
	vJobTemplate map[string]map[string]vnpu.VResource) error {

	klog.V(util.LogDebugLev).Infof("Entering initNPUNodeByNodeInf function")

	if device == nil || npuNode == nil {
		klog.V(util.LogInfoLev).Infof("InitNPUNodeByNodeInf failed: %s.", util.ArgumentError)
		return errors.New(util.ArgumentError)
	}

	// 1. 获取节点 NPU 资源能力(如 huawei.com/Ascend310P: 8000)
	capability := getNPUNodeCapacity(npuNode)
	// 校验节点是否有 NPU 资源（检查 huawei.com/ 前缀的资源）
	if !util.IsMapHasNPUResource(capability, util.HwPreName) {
		return fmt.Errorf("node %s npu resource is not enable", npuNode.Name)
	}
	// 校验设备信息是否可用
	if deviceInfo.DeviceList == nil {
		return fmt.Errorf("node %s device info or clusterd info is not enable", npuNode.Name)
	}

	// 2. 填充节点基本信息
	device.NodeInf.Name = npuNode.Name
	device.Capability = capability                                           // 节点资源能力
	device.BaseDeviceInfo = npuNode.Node.Annotations[util.BaseDeviceInfoKey] // 基础设备信息
	device.NodeInf.Allocate = npuNode.Allocatable.ScalarResources            // 已分配资源
	device.Idle = npuNode.Idle.ScalarResources                               // 空闲资源
	device.Label = npuNode.Node.Labels                                       // 节点标签
	device.Address = getNPUNodeAddress(npuNode)                              // 节点内网 IP

	// 3. 同步注解：四源合并（节点注解 + 上次 Session 设备信息 + NodeD 健康状态）
	//device.Tasks = npuNode.Tasks
	syncAnnotation(device, npuNode, nodeInfoOfNodeD)

	// 4. 更新设备信息（一致性保护：取 ConfigMap 与 Volcano 缓存的交集，确保只调度健康芯片）
	updateNPUNodeDeviceInfos(device, deviceInfo)

	// 5. 构建 VNPU 视图（芯片识别 → 资源计算 → VChip 创建 → Pod 资源恢复）
	if setVNPUErr := setNodeVNPUInfo(device, npuNode, vJobTemplate); setVNPUErr != nil {
		klog.V(util.LogDebugLev).Infof("setNodeVNPUInfo %s %s", npuNode.Name, setVNPUErr)
	}
	klog.V(util.LogDebugLev).Infof("initNPUNodeByNodeInf <%s> success %#v", npuNode.Name, device.NodeInf)
	return nil
}

// setNodeVNPUInfo 构建节点的 vNPU 视图，是设备调度的基础。
//
// 该函数按顺序完成三个步骤：
//  1. setChipPropertiesFromNPUNode —— 识别芯片型号（ChipKind/ServerType/ChipType）
//  2. setTotalResAndChipNumByTemplates —— 计算节点总资源和芯片数量
//  3. initVChips —— 创建 VChip 实例并恢复已运行 Pod 的资源占用
//
// 执行成功后会设置 device.ValidVNode = true，表示该节点可用于 vNPU 调度。
func setNodeVNPUInfo(device *vnpu.NPUDevices, ni *api.NodeInfo, jobTemplate map[string]map[string]vnpu.VResource) error {
	// 前置检查：确认节点 NPU 资源已初始化（Capability 中有 npu-core 资源）
	if !checkDyVNodeResourceInitialized(device) {
		return fmt.Errorf("setNodeVNPUInfo %s: DyVNode resource not initialized", device.NodeInf.Name)
	}

	// 1. 识别芯片属性：ChipKind（如 Ascend910/Ascend310P）、ServerType（如 Ascend310P-10-dual）、
	//    ChipType（如 910B3）、FreeChipNum（从设备信息获取的空闲芯片数）
	if err := setChipPropertiesFromNPUNode(device); err != nil {
		return fmt.Errorf("setNodeVNPUInfo %s: %v", device.NodeInf.Name, err)
	}

	// 2. 计算节点总资源、芯片总数、每芯片核心数
	//    例如：节点 Capability 中 npu-core = 80000 → 80 Core，每芯片 8 Core → 10 张卡
	if err := setTotalResAndChipNumByTemplates(device); err != nil {
		return fmt.Errorf("setNodeVNPUInfo node %s: %v", device.NodeInf.Name, err)
	}

	// 3. 创建 VChip 实例并恢复已运行 Pod 的资源占用
	if err := initVChips(device, ni, jobTemplate); err != nil {
		return fmt.Errorf("setNodeVNPUInfo node %s: %v", device.NodeInf.Name, err)
	}

	// 标记节点为有效的 vNPU 调度节点
	device.ValidVNode = true
	klog.V(util.LogDebugLev).Infof("setNodeVNPUInfo %s initialisation success:<%#v>", device.NodeInf.Name, device.NPUDevice)
	return nil
}

// initVChips 初始化节点上所有 VChip（虚拟芯片实例）。
//
// 流程分为三步：
//  1. createNodeNewVChips —— 根据设备信息中的健康芯片列表，为每张健康芯片创建空 VChip
//  2. setUnhealthyChipIds —— 标记不健康芯片，使其不参与调度
//  3. 遍历节点上所有 Task，对 NPU 任务调用 addNPUResource 恢复资源占用
//     （即已运行 Pod 的 vNPU 资源需要在 VChip 上扣除）
func initVChips(device *vnpu.NPUDevices, ni *api.NodeInfo, taskTemplate map[string]map[string]vnpu.VResource) error {
	// 计算每张芯片的总资源（Aicore = 总核数/芯片数，Aicpu = 总 CPU/芯片数）
	chipTotalRes := getVChipTotalRes(device)

	// 1. 为每张健康芯片创建空 VChip（FreeRes = TotalRes, UsedRes = 0）
	if err := createNodeNewVChips(device, chipTotalRes); err != nil {
		klog.V(util.LogDebugLev).Infof("vNode %s %s.", device.NodeInf.Name, util.SafePrint(err))
	} // 3. create new VChip by freeCardID whole card

	// 2. 标记不健康芯片（从设备信息中读取 -Unhealthy 后缀的注解）
	if err := setUnhealthyChipIds(device); err != nil {
		klog.V(util.LogDebugLev).Infof("vNode %s %s.", device.NodeInf.Name, err)
	}

	// 3. 遍历节点上所有 Task，恢复已运行 Pod 的 vNPU 资源占用
	for _, ti := range ni.Tasks {
		if !isNPUTask(ti) {
			continue // 跳过非 NPU 任务
		}
		// 根据 Pod 注解恢复芯片资源占用
		addNPUResource(device, ti.Pod, chipTotalRes, taskTemplate)
	} // 4. update VChips and create VChips for chips being occupied

	return nil
}

// addNPUResource 恢复单个 Pod 的 vNPU 资源占用到节点的 VChip 上。
//
// 根据 Pod 注解 "huawei.com/npu-core" 的值判断是整卡占用还是切分占用：
//   - 整卡格式："0,1" 或 "0"       → 调用 addNPUResourceWholeCard
//   - 切分格式："0-vir04"           → 调用 addNPUResourceVNPUCard
//
// 这是调度器重启后恢复现场的关键逻辑：
// 调度器重启后缓存丢失，但已运行的 Pod 仍在占用芯片资源，
// 必须通过 Pod 注解将资源占用重新恢复到 VChip 上，否则会导致资源超分。
func addNPUResource(device *vnpu.NPUDevices, pod *v1.Pod, chipTotalRes vnpu.VResource,
	taskTemplate map[string]map[string]vnpu.VResource) {
	// 读取 Pod 注解中的 NPU 核心分配信息
	coreNameStr, ok := pod.Annotations[util.AscendNPUCore]
	if !ok {
		klog.V(util.LogDebugLev).Infof("addNPUResource pod %s %s no value", pod.Name, util.AscendNPUCore)
		return
	}

	// 判断是整卡还是切分：整卡格式 "0,1"，切分格式 "0-vir04"
	if isPodWholeCardFromAscendCore(coreNameStr) {
		addNPUResourceWholeCard(device, pod) // 整卡占用：扣除整张芯片的全部资源
		return
	}
	addNPUResourceVNPUCard(device, pod, chipTotalRes, taskTemplate) // 切分占用：按模板扣除部分资源
}

// addNPUResourceVNPUCard 恢复切分占用场景下 Pod 的资源。
//
// Pod 注解格式示例："Ascend310P-4c.3cpu.ndvpp-100(物理ID)-1(vgroupID)-0-vir04"
// 实际 AscendNPUCore 注解格式："chipID-templateName"，如 "0-vir04"
//
// 流程：
//  1. 解析物理芯片 ID（从 AscendNPUCore 注解中取 "-" 前的部分）
//  2. 查找或创建对应的 VChip
//  3. 从 VJobTemplate 中查找模板对应的资源规格（Aicore/Aicpu/DVPP）
//  4. 更新 VChip 的 UsedRes 和 FreeRes
func addNPUResourceVNPUCard(device *vnpu.NPUDevices, pod *v1.Pod, chipTotalRes vnpu.VResource,
	taskTemplate map[string]map[string]vnpu.VResource) {
	// 1. 解析物理芯片 ID
	physicsID, err := getCardPhysicsIDFromAscendCore(pod, false)
	if err != nil || len(physicsID) != util.NPUIndex1 {
		klog.V(util.LogErrorLev).Infof("addNPUResourceVNPUCard get pod<%s> card physics id failed", pod.Name)
		return
	}
	// 检查芯片是否不健康，不健康则跳过
	_, isCardunhealthy := device.UnhealthyChipIds[physicsID[0]]
	if isCardunhealthy {
		klog.V(util.LogErrorLev).Infof("addNPUResourceVNPUCard get pod<%s> card is unhealthy", pod.Name)
		return
	}

	// 2. 查找或创建对应的 VChip
	curVChip, ok := device.Chips[physicsID[0]]
	if !ok {
		// 如果 VChip 不存在（可能是被健康列表遗漏的芯片），临时创建
		curVChip = NewVChip(device, physicsID[0], chipTotalRes)
		device.Chips[physicsID[0]] = curVChip
	}
	// 更新芯片状态：标记不稳定、添加真实卡 ID、记录 Pod、设置切分标志
	curVChip.Unstable = curVChip.IsPodResUnstable(pod) || curVChip.Unstable
	curVChip.AddRealCardID(pod.Annotations[util.AscendNPUPodRealUse])
	curVChip.AddPodToPodMap(pod)
	curVChip.SetSegmentFlag(true) // 标记为已切分

	// 3. 从 VJobTemplate 中查找模板对应的资源规格
	podVResource := getPodUsedRes(device, pod, taskTemplate)
	if podVResource == nil {
		klog.V(util.LogErrorLev).Infof("addNPUResource resolving pod<%s> resource failed", pod.Name)
		return
	}

	// 4. 更新 VChip 的 UsedRes 和 FreeRes
	curVChip.UsedRes.Add(*podVResource)    // 累加已用资源
	curVChip.FreeRes.Sub(*podVResource)    // 扣减空闲资源
	curVChip.UpdateDVPP(podVResource.DVPP) // 更新 DVPP 状态
}

// getPodUsedRes 从 Pod 注解中解析其占用的 VResource。
//
// Pod 的 "huawei.com/npu-core" 注解格式为 "chipID-templateName"，
// 例如 "0-vir04"，其中 "vir04" 是模板名。
// 函数会用该模板名在 VJobTemplate 中查找对应的 Aicore/Aicpu/DVPP 资源规格。
func getPodUsedRes(device *vnpu.NPUDevices, pod *v1.Pod, taskTemplate map[string]map[string]vnpu.VResource) *vnpu.VResource {
	// 读取 Pod 注解中的芯片分配信息
	realStr, ok := pod.Annotations[util.AscendNPUCore]
	if !ok {
		klog.V(util.LogErrorLev).Infof("getPodUsedRes get pod<%s> %s value failed", pod.Name,
			util.AscendNPUCore)
		return nil
	}
	// 拆分 "chipID-templateName"，例如 "0-vir04" → ["0", "vir04"]
	ascendRealSplit := strings.Split(realStr, "-")
	if len(ascendRealSplit) != util.NPUIndex2 {
		klog.V(util.LogErrorLev).Infof("getPodUsedRes get pod<%s> %s format error", pod.Name, realStr)
		return nil
	}
	// Ascend310P 用 ChipKind 查找模板，其他芯片用 ChipType（如 910B3）查找
	if device.ChipKind == util.Ascend310P {
		return getResourceFromTemplate(device.ChipKind, ascendRealSplit[1], taskTemplate)
	}
	return getResourceFromTemplate(device.ChipType, ascendRealSplit[1], taskTemplate)
}

// getResourceFromTemplate 从 VJobTemplate 模板表中查找指定芯片型号和模板名对应的资源规格。
//
// 参数：
//   - nodeType:       芯片型号，如 "Ascend310P"、"910B3"
//   - templateString: 模板名，如 "vir04"、"vir04_3c_ndvpp"
//   - taskTemplate:   VJobTemplate 映射表
func getResourceFromTemplate(nodeType string, templateString string,
	taskTemplate map[string]map[string]vnpu.VResource) *vnpu.VResource {
	taskNodeTemplate, ok := taskTemplate[nodeType]
	if !ok {
		return nil
	}
	taskResource, ok := taskNodeTemplate[templateString]
	if !ok {
		return nil
	}
	return &taskResource
}

// NewVChip 创建一个新的 VChip（虚拟芯片实例）。
//
// VChip 代表一张物理 NPU 芯片的虚拟化视图，包含：
//   - PodMap:   已绑定到该芯片的 Pod 映射
//   - Name:     芯片显示名称，如 "Ascend310P-0"
//   - Kind:     芯片大类，如 Ascend910/Ascend310P
//   - CoreNum:  该芯片的 AI Core 总数
//   - TotalRes: 芯片总资源（FreeRes 初始等于 TotalRes）
//   - DVPP:     初始状态为 Off，后续 Pod 分配时会动态更新
//
// 如果芯片是双槽卡（ServerType 包含 "dual"），会设置 IsDual 标志。
func NewVChip(device *vnpu.NPUDevices, id int, totalRes vnpu.VResource) *vnpu.VChip {
	if device == nil {
		klog.V(util.LogDebugLev).Infof("NewVChip failed: %s", util.ArgumentError)
		return nil
	}
	chipName := device.ChipKind + "-" + strconv.Itoa(id) // 如 "Ascend310P-0"
	vChip := vnpu.VChip{
		PodMap:   make(map[string]*v1.Pod, util.MapInitNum),
		Name:     chipName,
		Kind:     device.ChipKind,
		CoreNum:  device.AiCorePerChip,
		TotalRes: totalRes,
		FreeRes:  totalRes, // 初始空闲 = 总资源
	}
	// DVPP 初始状态：Total 和 Used 为 Off，Free 为 On（表示可分配 DVPP）
	vChip.TotalRes.DVPP = plugin.AscendDVPPEnabledOff
	vChip.UsedRes.DVPP = plugin.AscendDVPPEnabledOff
	vChip.FreeRes.DVPP = plugin.AscendDVPPEnabledOn

	// 如果服务器类型为双槽卡（如 "Ascend310P-10-dual"），设置 IsDual 标志
	if strings.HasPrefix(device.ServerType, util.ServerTypeDual) {
		vChip.SetIsDual(true)
	}

	return &vChip
}

// getCardPhysicsIDFromAscendCore 从 Pod 的 "huawei.com/npu-core" 注解中解析物理芯片 ID。
//
// 注解格式：
//   - 整卡："0,1,2"   （逗号分隔的芯片 ID 列表）
//   - 切分："0-vir04"  （芯片 ID + "-" + 模板名）
//
// 参数 isWholeCard=true 时，解析逗号分隔的多个芯片 ID；
// 参数 isWholeCard=false 时，解析 "chipID-templateName" 中的单个芯片 ID。
func getCardPhysicsIDFromAscendCore(pod *v1.Pod, isWholeCard bool) ([]int, error) {
	physicsIDs := make([]int, 0)
	if pod == nil {
		return physicsIDs, fmt.Errorf("pod is nil")
	}
	coreNameStr, ok := pod.Annotations[util.AscendNPUCore]
	if !ok {
		return physicsIDs, fmt.Errorf("getCardPhysicsIDFromAscendCore vnpu device <%s> get %s value failed",
			pod.Name, util.AscendNPUCore)
	}

	// 切分场景：解析 "chipID-templateName" 中的芯片 ID
	if !isWholeCard {
		phyCardID, err := getVNPUCardIDFromAscendCore(coreNameStr)
		if err != nil {
			return physicsIDs, fmt.Errorf("getCardPhysicsIDFromAscendCore vnpu device <%s> get id failed",
				coreNameStr)
		}
		physicsIDs = append(physicsIDs, phyCardID)
		return physicsIDs, nil
	}
	// 整卡场景：解析逗号分隔的芯片 ID 列表
	coreNameSplit := strings.Split(coreNameStr, ",")
	for _, id := range coreNameSplit {
		phyCardID, err := strconv.Atoi(id)
		if err != nil {
			return physicsIDs, fmt.Errorf("getCardPhysicsIDFromAscendCore device <%s> get physics id failed",
				coreNameStr)
		}
		physicsIDs = append(physicsIDs, phyCardID)
	}
	return physicsIDs, nil
}

// getVNPUCardIDFromAscendCore 从 "chipID-templateName" 格式中解析物理芯片 ID。
// 例如 "0-vir04" 返回 0。
func getVNPUCardIDFromAscendCore(coreNameStr string) (int, error) {
	coreNameSplit := strings.Split(coreNameStr, "-")
	if len(coreNameSplit) != util.NPUIndex2 {
		return 0, fmt.Errorf("getVNPUCardIDFromAscendCore vnpu real device <%s> format error", coreNameStr)
	}
	phyCardID, err := strconv.Atoi(coreNameSplit[0])
	if err != nil {
		return 0, fmt.Errorf("getVNPUCardIDFromAscendCore vnpu device <%s> get physics id failed", coreNameStr)
	}
	return phyCardID, nil
}

// addNPUResourceWholeCard 恢复整卡占用场景下 Pod 的资源。
//
// 整卡场景下，Pod 的 AscendNPUCore 注解格式为 "0,1"（逗号分隔的芯片 ID 列表），
// 每张芯片的全部资源（TotalRes）被扣除，FreeRes 变为 0。
func addNPUResourceWholeCard(device *vnpu.NPUDevices, pod *v1.Pod) {
	// 解析整卡场景下的所有物理芯片 ID
	physicsID, err := getCardPhysicsIDFromAscendCore(pod, true)
	if err != nil || len(physicsID) == 0 {
		return
	}
	for _, id := range physicsID {
		// 跳过不健康芯片
		_, isCardunhealthy := device.UnhealthyChipIds[id]
		if isCardunhealthy {
			continue
		}
		// 1. 整卡占用时，Pod 的资源等于芯片总资源
		podVResource := getVChipTotalRes(device)

		// 2. 查找或创建对应的 VChip
		curVChip, ok := device.Chips[id]
		if !ok {
			curVChip = NewVChip(device, id, podVResource)
			device.Chips[id] = curVChip
		}

		// 3. 更新 VChip：标记不稳定、添加卡 ID、记录 Pod、扣除全部资源
		curVChip.Unstable = curVChip.IsPodResUnstable(pod) || curVChip.Unstable
		curVChip.AddRealCardID(strconv.Itoa(id))
		curVChip.AddPodToPodMap(pod)
		curVChip.UsedRes.Add(podVResource) // UsedRes = TotalRes
		curVChip.FreeRes.Sub(podVResource) // FreeRes = 0
	}
}

// isPodWholeCardFromAscendCore 判断 Pod 的 NPU 核心注解是否为整卡格式。
//
// 整卡格式："0" 或 "0,1,2"（无 "-" 分隔符）
// 切分格式："0-vir04"（有 "-" 分隔符）
// 判断依据：逗号分隔后的每个部分，如果不包含 "-" 则为整卡。
func isPodWholeCardFromAscendCore(coreCardName string) bool {
	temp := strings.Split(coreCardName, ",")
	for _, cardName := range temp {
		singleCardTemp := strings.Split(cardName, "-")
		if len(singleCardTemp) == util.NPUIndex1 {
			return true
		}
	}
	return false
}

// isNPUTask 判断一个 Task 是否为 NPU 任务。
// 判断依据：其资源请求中是否包含 "huawei.com/" 前缀的扩展资源。
func isNPUTask(nT *api.TaskInfo) bool {
	for k := range nT.Resreq.ScalarResources {
		// 检查资源名是否包含 "huawei.com/" 前缀
		if strings.Contains(string(k), util.HwPreName) {
			return true
		}
	}
	return false
}

// setUnhealthyChipIds 从设备信息中读取不健康芯片列表，并记录到 UnhealthyChipIds 中。
// 不健康芯片通过节点注解 "huawei.com/{ChipKind}-Unhealthy" 获取，
// 例如 "huawei.com/Ascend310P-Unhealthy: Ascend310P-3,Ascend310P-7"。
func setUnhealthyChipIds(device *vnpu.NPUDevices) error {
	unhealthyCardIDs, getErr := getCardIDsFromNodeAndDeviceInfo(device, vnpu.UnhealthyCardSuffix)
	if getErr != nil {
		return fmt.Errorf("getFreeCardIDsFromDeviceInfo %s", getErr)
	}
	for _, unhealthyCardID := range unhealthyCardIDs {
		device.NPUDevice.UnhealthyChipIds[unhealthyCardID] = struct{}{}
	}
	return nil
}

// getCardIDsFromNodeAndDeviceInfo 从节点注解中解析芯片 ID 列表。
//
// 通过拼接前缀 + ChipKind + 后缀构造注解键名：
//   - 健康芯片键："huawei.com/Ascend310P"（后缀为空字符串）
//   - 不健康芯片键："huawei.com/Ascend310P-Unhealthy"
//
// 注解值格式为逗号分隔的芯片名列表，如 "Ascend310P-0,Ascend310P-3"。
func getCardIDsFromNodeAndDeviceInfo(device *vnpu.NPUDevices, cardHealthTypeSuffix string) ([]int, error) {
	// 1. 从节点注解中获取芯片列表字符串
	ChipsStr, ok := device.Annotation[util.HwPreName+device.NPUDevice.ChipKind+cardHealthTypeSuffix]
	if !ok {
		klog.V(util.LogDebugLev).Infof("%s get healthy card failed", device.NodeInf.Name)
		return nil, fmt.Errorf("no key: %s", util.HwPreName+device.NPUDevice.ChipKind)
	}

	CardIDs := make([]int, 0)
	Chips := strings.Split(ChipsStr, ",") // 拆分逗号分隔列表
	for _, chip := range Chips {
		if chip == "" {
			continue
		}
		strID := strings.TrimPrefix(chip, device.NPUDevice.ChipKind+"-") // 去掉前缀如 "Ascend310P-"
		chipID, aErr := strconv.Atoi(strID)
		if aErr != nil {
			klog.V(util.LogDebugLev).Infof("%s %s covert to int %s", chip, strID, util.SafePrint(aErr))
			continue
		}
		CardIDs = append(CardIDs, chipID)
	}

	if len(CardIDs) == 0 {
		return nil, fmt.Errorf("nil cards in %s", device.NodeInf.Name)
	}
	return CardIDs, nil
}

// createNodeNewVChips 为节点上的每张健康芯片创建空 VChip。
//
// 从设备信息中读取健康芯片列表（注解键 "huawei.com/{ChipKind}"，后缀为空），
// 为每张芯片创建一个新的 VChip，FreeRes = TotalRes，UsedRes = 0。
func createNodeNewVChips(device *vnpu.NPUDevices, chipTotalRes vnpu.VResource) error {
	// 获取健康芯片 ID 列表
	healthyCardIDs, getErr := getCardIDsFromNodeAndDeviceInfo(device, vnpu.CardHealthySuffix)
	if getErr != nil {
		return fmt.Errorf("getFreeCardIDsFromDeviceInfo %s", util.SafePrint(getErr))
	}
	klog.V(util.LogDebugLev).Infof("createNodeNewVChips healthy chips: %#v", healthyCardIDs)
	// 为每张健康芯片创建空 VChip
	for _, freeCardID := range healthyCardIDs {
		device.NPUDevice.Chips[freeCardID] = NewVChip(device, freeCardID, chipTotalRes)
	}
	return nil
}

// setTotalResAndChipNumByTemplates 计算节点的总资源、芯片总数和每芯片 AI CPU 数。
//
// 计算过程：
//  1. 从节点 Capability 中读取总 AI Core 数（如 huawei.com/npu-core: 80000 → 80 Core）
//  2. 从 ServerType 标签中提取每芯片核心数（如 "Ascend310P-8-dual" → 8 Core/Chip）
//  3. 计算芯片总数 = 总核心数 / 每芯片核心数（如 80 / 8 = 10 张卡）
//  4. 通过 VTemplate 查找每芯片 AI CPU 数，再乘以芯片总数得到总 AI CPU
func setTotalResAndChipNumByTemplates(device *vnpu.NPUDevices) error {
	// 1. 从节点 Capability 中获取总 AI Core 数
	totalCore, ok := device.Capability[util.AscendNPUCore]
	if !ok {
		return fmt.Errorf("getTotalResFromNpuNode no resource <%s>", util.AscendNPUCore)
	}

	// 将毫核转换为核数（Volcano 框架使用毫核单位，除以 1000）
	// 将毫核转换为核数（Volcano 框架使用毫核单位，除以 1000）
	device.NPUDevice.TotalRes.Aicore = int(totalCore / util.NPUHexKilo)
	klog.V(util.LogDebugLev).Infof("DEBUG: node %s, totalCore from Capability: %f", device.NodeInf.Name, totalCore)
	klog.V(util.LogDebugLev).Infof("DEBUG: node %s, after division: %d", device.NodeInf.Name, int(totalCore/util.NPUHexKilo))

	// 从 ServerType 标签中提取每芯片 AI Core 数（如 "Ascend310P-8-dual" → 8）
	// 从 ServerType 标签中提取每芯片 AI Core 数（如 "Ascend310P-8-dual" → 8）
	numCorePerChip, err := getVChipCoreNum(device)
	if err != nil || numCorePerChip == 0 {
		return fmt.Errorf("getTotalChipNum error: %v or numCorePerChip zero number: %d",
			util.SafePrint(err), numCorePerChip)
	}
	device.AiCorePerChip = numCorePerChip

	// 2. 计算芯片总数 = 总 AI Core / 每芯片 Core 数
	totalChipNum, err := getTotalChipNum(device)
	if err != nil {
		return fmt.Errorf("getTotalResFromNpuNode failed: %v", err)
	}
	device.NPUDevice.TotalChipNum = totalChipNum

	// 3. 通过 VTemplate 查找每芯片 AI CPU 数，再乘以芯片总数得到总 AI CPU
	templates := initTemplate()
	cpuPerChip := getCpuNumPerChip(device, templates)
	if cpuPerChip == util.ErrorInt {
		return errors.New("getTotalResFromNpuNode get aicpu from template failed")
	}
	device.NPUDevice.TotalRes.Aicpu = cpuPerChip * totalChipNum  // 总 AI CPU = 每芯片 CPU * 芯片数
	device.NPUDevice.TotalRes.DVPP = plugin.AscendDVPPEnabledOff // DVPP 初始状态为 Off

	return nil
}

// getCpuNumPerChip 通过 VTemplate 查找指定芯片型号对应的每芯片 AI CPU 数。
//
// 匹配条件：ChipKind 或 ChipType 匹配，且 AICore * TotalChipNum == TotalRes.Aicore。
// 如果找不到匹配项，返回 ErrorInt (-1)。
func getCpuNumPerChip(device *vnpu.NPUDevices, templates []VTemplate) int {
	cpuPerChip := util.ErrorInt
	for _, temp := range templates {
		if (temp.ChipKind != device.NPUDevice.ChipKind && temp.ChipKind != device.NPUDevice.ChipType) ||
			temp.AICore*device.TotalChipNum != device.NPUDevice.TotalRes.Aicore {
			continue
		}
		cpuPerChip = temp.AICPU
	}
	return cpuPerChip
}

// VTemplate 描述一种芯片的硬件规格（内部用于 getCpuNumPerChip 查找）。
type VTemplate struct {
	// ChipKind 芯片大类，如 Ascend910、Ascend310P
	ChipKind   string
	AICore     int    // 每芯片 AI Core 数
	AICPU      int    // 每芯片 AI CPU 数
	DVPPEnable string // DVPP 能力标志
}

// initTemplate 初始化芯片规格表，包含所有支持的芯片型号及其硬件参数。
//
// 规格表内容：
// ┌─────────────┬────────┬────────┐
// │ 芯片型号    │ AI Core │ AI CPU │
// ├─────────────┼────────┼────────┤
// │ Ascend310P  │   8    │   7    │
// │ 910B1       │  25    │   6    │
// │ 910B2C      │  24    │   6    │
// │ 910B3       │  20    │   6    │
// │ 910B4       │  20    │   6    │
// │ 910B2       │  24    │   6    │
// │ Ascend310P  │  10    │   7    │ （另一种 310P 规格）
// └─────────────┴────────┴────────┘
func initTemplate() []VTemplate {
	nodeTemplate := make([]VTemplate, util.NPUIndex7)
	if len(nodeTemplate) < util.NPUIndex7 {
		return nodeTemplate
	}
	nodeTemplate[0] = VTemplate{
		ChipKind: util.Ascend310P,
		AICore:   util.NPUIndex8,
		AICPU:    util.NPUIndex7,
	}
	nodeTemplate[util.NPUIndex1] = VTemplate{
		ChipKind: plugin.ChipTypeB1,
		AICore:   util.CoreNum25,
		AICPU:    util.CpuNum6,
	}
	nodeTemplate[util.NPUIndex2] = VTemplate{
		ChipKind: plugin.ChipTypeB2C,
		AICore:   util.CoreNum24,
		AICPU:    util.CpuNum6,
	}
	nodeTemplate[util.NPUIndex3] = VTemplate{
		ChipKind: plugin.ChipTypeB3,
		AICore:   util.CoreNum20,
		AICPU:    util.CpuNum6,
	}
	nodeTemplate[util.NPUIndex4] = VTemplate{
		ChipKind: plugin.ChipTypeB4,
		AICore:   util.CoreNum20,
		AICPU:    util.CpuNum6,
	}
	nodeTemplate[util.NPUIndex5] = VTemplate{
		ChipKind: plugin.ChipTypeB2,
		AICore:   util.CoreNum24,
		AICPU:    util.CpuNum6,
	}
	nodeTemplate[util.NPUIndex6] = VTemplate{
		ChipKind: util.Ascend310P,
		AICore:   util.CoreNum10,
		AICPU:    util.NPUIndex7,
	}
	return nodeTemplate
}

// getTotalChipNum 计算节点芯片总数 = 总 AI Core 数 / 每芯片 Core 数。
// 如果无法整除，则返回错误（说明配置不一致）。
func getTotalChipNum(device *vnpu.NPUDevices) (int, error) {
	totalChipNum := device.TotalRes.Aicore / device.AiCorePerChip
	if device.TotalRes.Aicore%device.AiCorePerChip != 0 {
		return 0, errors.New("getTotalChipNum error: total resource cannot be divided by coreNumPerChip")
	}
	if totalChipNum == 0 {
		return 0, errors.New("getTotalChipNum error: total chip number zero")
	}
	return totalChipNum, nil
}

// getVChipCoreNum 从 ServerType 标签中提取每芯片的 AI Core 数。
//
// ServerType 格式为 "{ChipKind}-{CoreNum}[-{DualFlag}]"，
// 例如 "Ascend310P-8-dual" 中的 8 即为每芯片 Core 数。
func getVChipCoreNum(device *vnpu.NPUDevices) (int, error) {
	serverTypeSplit := strings.Split(device.ServerType, "-")
	if len(serverTypeSplit) < util.NPUIndex2 {
		return 0, fmt.Errorf("getVChipCoreNum serverType %s format error", device.ServerType)
	}
	coreNum, err := strconv.Atoi(serverTypeSplit[1])
	if err != nil {
		return 0, fmt.Errorf("getVChipCoreNum serverType %s split error", device.ServerType)
	}
	return coreNum, nil
}

// getVChipTotalRes 计算单张芯片的总资源。
// 即节点总资源除以芯片总数，得到每张芯片的 Aicore 和 Aicpu。
func getVChipTotalRes(device *vnpu.NPUDevices) vnpu.VResource {
	AiCore := device.TotalRes.Aicore / device.TotalChipNum
	AiCpu := device.TotalRes.Aicpu / device.TotalChipNum
	return vnpu.VResource{
		Aicore: AiCore,
		Aicpu:  AiCpu,
		DVPP:   plugin.AscendDVPPEnabledOff,
	}
}

// checkDyVNodeResourceInitialized 检查节点 NPU 资源是否已初始化。
// 通过检查 Capability 中是否存在 npu-core 资源来判断。
func checkDyVNodeResourceInitialized(device *vnpu.NPUDevices) bool {
	return device.Capability[util.AscendNPUCore] > 0
}

// setChipPropertiesFromNPUNode 从节点标签和注解中识别芯片属性。
//
// 具体获取三个关键属性：
//  1. ChipKind:   芯片大类，从 "accelerator" 标签中提取（如 "huawei-Ascend910" → "Ascend910"）
//  2. ServerType: 服务器类型，从 "servertype" 标签获取（如 "Ascend310P-10-dual"）
//  3. ChipType:   具体芯片型号，从 "node.kubernetes.io/npu.chip.name" 标签获取（如 "910B3"）
//  4. FreeChipNum: 空闲芯片数，从设备信息注解中获取
func setChipPropertiesFromNPUNode(device *vnpu.NPUDevices) error {
	chipKind, err := GetChipKindFromNpuNode(device) // 1. 从 "accelerator" 标签提取芯片大类
	if err != nil {
		return fmt.Errorf("setNodeVNPUInfo node %s: %v", device.NodeInf.Name, err)
	}
	device.NPUDevice.ChipKind = chipKind

	chipLabel, ok := device.Label[util.ServerType] // 2. 获取服务器类型标签
	if !ok {
		return fmt.Errorf("setNodeVNPUInfo node %s no node label <%s>", device.NodeInf.Name, util.ServerType)
	}
	device.NPUDevice.ServerType = chipLabel

	chipType, ok := device.Label[vnpu.ChipTypeKey] // 3. 获取具体芯片型号
	if !ok {
		return fmt.Errorf("setNodeVNPUInfo node %s no node label <%s>", device.NodeInf.Name, vnpu.ChipTypeKey)
	}
	device.NPUDevice.ChipType = chipType

	nodeFreeChips, ok := device.Annotation[util.HwPreName+device.NPUDevice.ChipKind] // 4. 获取空闲芯片列表
	if !ok {
		return errors.New("getFreeChipNum failed")
	}

	nodeFreeChipsSplit := strings.Split(nodeFreeChips, ",")
	device.NPUDevice.FreeChipNum = len(nodeFreeChipsSplit) // 空闲芯片数 = 逗号分隔列表的长度

	return nil
}

// GetChipKindFromNpuNode 从节点 "accelerator" 标签中提取芯片大类。
//
// 标签格式为 "huawei-{ChipKind}"，例如：
//   - "huawei-Ascend910" → "Ascend910"
//   - "huawei-Ascend310P" → "Ascend310P"
func GetChipKindFromNpuNode(device *vnpu.NPUDevices) (string, error) {
	tempVal, ok := device.Label[util.Accelerator]
	if !ok {
		return "", fmt.Errorf("getChipKindFromNpuNode label %s absent", util.Accelerator)
	}
	chipKind := strings.Split(tempVal, "-")
	if len(chipKind) < util.NPUIndex2 {
		return "", fmt.Errorf("getChipKindFromNpuNode label %s value %s %s", util.Accelerator,
			chipKind, plugin.FormatIncorrectError)
	}
	return chipKind[1], nil
}

// updateNPUNodeDeviceInfos 更新设备信息，包含一致性保护机制。
//
// 核心原则：只允许更新比当前缓存更新的设备信息，防止旧数据覆盖新数据。
// 通过 DevInfoUpdateTime 时间戳控制更新顺序。
func updateNPUNodeDeviceInfos(device *vnpu.NPUDevices, data k8s.NodeDeviceInfoWithID) {
	// 如果当前缓存已是最新，跳过更新
	if device.DevInfoUpdateTime >= data.UpdateTime {
		klog.V(util.LogDebugLev).Infof("device info is not update, skip refresh cache")
		return
	}
	device.SuperPodID = data.SuperPodID

	// 将新设备信息与 Volcano 缓存合并（一致性保护）
	updateNPUNodeDeviceInfosWithVolcanoCache(device, data, data.UpdateTime)

	device.DevInfoUpdateTime = data.UpdateTime
	klog.V(util.LogDebugLev).Infof("update device info for node<%s> annotations: %v", device.NodeInf.Name, device.Annotation)
}

// updateNPUNodeDeviceInfosWithVolcanoCache 将新设备信息与 Volcano 缓存合并，实现一致性保护。
//
// 核心逻辑：
//   - 对于非资源键（如 "huawei.com/Ascend310P-Unhealthy"），直接用新值覆盖
//   - 对于资源键（如 "huawei.com/Ascend310P"），根据时间间隔和 Volcano 缓存状态决定更新策略：
//   - 时间间隔超过 10s：强制更新（信任设备信息）
//   - 时间间隔未超过 10s：取 ConfigMap 与 Volcano 缓存的交集（只保留两者都健康的芯片）
//
// 这样做的目的是避免在设备信息延迟上报时，误认为某些芯片已恢复健康。
func updateNPUNodeDeviceInfosWithVolcanoCache(device *vnpu.NPUDevices, data k8s.NodeDeviceInfoWithID, updateTime int64) {
	for k, v := range data.DeviceList {
		// 非资源键（如 "-Unhealthy" 后缀）直接覆盖
		if len(strings.Split(k, "-")) > 1 {
			device.Annotation[k] = v
			continue
		}
		// 时间间隔超过 10s，强制更新（信任设备信息）
		if updateTime-device.DevInfoUpdateTime > vnpu.DeviceInfoForceUpdateInterval {
			device.Annotation[k] = v
			continue
		}
		// 时间间隔未超过 10s，取交集（一致性保护）
		device.Annotation[k] = getRealHealthyDeviceList(device, k, device.Annotation[k], v)
	}
}

// getRealHealthyDeviceList 计算真实的健康芯片列表（取 ConfigMap 与 Volcano 缓存的交集）。
//
// 这是设备信息一致性保护的核心算法：
//   - 如果缓存或设备信息为空，直接使用新值
//   - 如果 Volcano 缓存的空闲芯片数与旧列表长度不一致（说明有新 Pod 被调度），
//     或与新列表长度一致（说明设备信息未变化），直接使用新值
//   - 否则取旧列表和新列表的交集，只保留两者都认为健康的芯片
//
// 实际场景：
//
//	调度器在 Session N 调度 Pod 到芯片 0，Session N+1 时设备信息可能还未更新，
//	此时通过取交集避免将已被 Pod 占用的芯片误认为空闲。
func getRealHealthyDeviceList(device *vnpu.NPUDevices, deviceKey, oldList, newList string) string {
	// 如果缓存或设备信息为空，直接使用新值
	if len(oldList) == 0 || len(newList) == 0 {
		return newList
	}
	newDeviceList := strings.Split(newList, ",")
	oldDeviceList := strings.Split(oldList, ",")

	// 如果 Volcano 缓存与旧列表不一致，或缓存与新列表一致，使用新值
	if int(device.Idle[v1.ResourceName(deviceKey)]/util.NPUHexKilo) != len(oldDeviceList) ||
		int(device.Idle[v1.ResourceName(deviceKey)]/util.NPUHexKilo) == len(newDeviceList) {
		return newList
	}

	klog.V(util.LogDebugLev).Infof("DEBUG: node %s, totalIdle from Capability: %d", device.NodeInf.Name, int(device.Idle[v1.ResourceName(deviceKey)]/util.NPUHexKilo))

	// 取旧列表和新列表的交集
	oldDevices := make(map[string]struct{})
	for _, device := range oldDeviceList {
		oldDevices[device] = struct{}{}
	}
	var deviceListCache []string
	for _, newDevice := range newDeviceList {
		if _, ok := oldDevices[newDevice]; !ok {
			continue // 新列表中的芯片不在旧列表中，跳过
		}
		deviceListCache = append(deviceListCache, newDevice)
	}
	klog.V(util.LogWarningLev).Infof("update device info for node<%s> annotations: %#v", device.NodeInf.Name, deviceListCache)
	return strings.Join(deviceListCache, ",")
}

// syncAnnotation 同步节点注解，合并四个数据源的注解信息。
//
// 四个数据源：
//  1. v1.Node 节点的原始注解（最基础的信息）
//  2. 上次 Session 的设备信息（带 "huawei.com/" 前缀的注解，用于跨 Session 状态保持）
//  3. NodeD 节点守护进程上报的健康状态（Healthy/SubHealthy/UnHealthy）
//  4. （预留）交换机信息
//
// 注意：带 "huawei.com/" 前缀的注解会从上次 Session 继承，保证设备信息在调度器重启后不丢失。
func syncAnnotation(device *vnpu.NPUDevices, npuNode *api.NodeInfo, nodeInfoOfNodeD k8s.NodeDNodeInfo) {
	existAnno := make(map[string]string)
	// 1. 复制节点原始注解
	for k, v := range npuNode.Node.Annotations {
		existAnno[k] = v
	}
	// 2. 继承上次 Session 的设备信息注解（带 "huawei.com/" 前缀）
	for annoKey, annoValue := range device.Annotation {
		if strings.Contains(annoKey, util.HwPreName) {
			existAnno[annoKey] = annoValue
			continue
		}
	}
	// 3. 同步 NodeD 健康状态：如果 NodeD 上报了状态则使用，否则默认为 "Healthy"
	if nodeInfoOfNodeD.NodeStatus != "" {
		existAnno[vnpu.NodedNodeHealtyStatuskey] = nodeInfoOfNodeD.NodeStatus
	} else {
		existAnno[vnpu.NodedNodeHealtyStatuskey] = util.NodeHealthyByNodeD
	}
	device.Annotation = existAnno
}

// getNPUNodeCapacity 获取节点的 NPU 资源能力（Capacity）。
//
// 使用反射来兼容不同版本的 Volcano NodeInfo 结构体（字段名可能是 "Capability"）。
// 返回标量资源 map，如 {"huawei.com/Ascend310P": 8000, "huawei.com/npu-core": 80000}。
func getNPUNodeCapacity(npuNode *api.NodeInfo) map[v1.ResourceName]float64 {
	klog.V(util.LogDebugLev).Infof("Enter getNPUNodeCapacity function")

	valueOfP := reflect.ValueOf(*npuNode)
	if valueOfP.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < valueOfP.NumField(); i++ {
		if valueOfP.Type().Field(i).Name != vnpu.OldCapacity && valueOfP.Type().Field(i).Name != vnpu.NewCapacity {
			continue
		}
		if capacity, ok := valueOfP.Field(i).Interface().(*api.Resource); ok {
			return capacity.ScalarResources
		}
		klog.V(util.LogErrorLev).Info("get capacity failed by not meet the resource type")
		return nil
	}
	return nil
}

// getNPUNodeAddress 获取节点的内网 IP 地址。
func getNPUNodeAddress(npuNode *api.NodeInfo) string {
	for _, addr := range npuNode.Node.Status.Addresses {
		if addr.Type == v1.NodeInternalIP {
			return addr.Address
		}
	}
	return ""
}
