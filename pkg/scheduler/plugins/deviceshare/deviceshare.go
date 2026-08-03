/*
Copyright 2024 The Volcano Authors.

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

package deviceshare

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/hami"
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/mindcluster/ascend310p/vnpu"
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/gpushare"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
	"volcano.sh/volcano/pkg/scheduler/framework"
	vnpu310p "volcano.sh/volcano/pkg/scheduler/plugins/deviceshare/devices/ascend/310p/vnpu"
)

// PluginName 表示 deviceshare 插件在调度器中的名字
const (
	PluginName = "deviceshare"

	// 以下这些 key 用于从调度器配置参数中读取开关/配置项

	// GPUSharingPredicate 控制是否启用 GPU 共享模式(多个 Pod 共享同一块物理 GPU)
	GPUSharingPredicate = "deviceshare.GPUSharingEnable"

	// NodeLockEnable 控制是否启用节点锁机制，防止并发修改设备状态
	NodeLockEnable = "deviceshare.NodeLockEnable"

	// GPUNumberPredicate 控制是否启用 GPU 编号模式(指定具体 GPU 卡号)
	GPUNumberPredicate = "deviceshare.GPUNumberEnable"

	// VGPUEnable 控制是否启用 vGPU 虚拟化模式(将物理 GPU 切分为多个虚拟 GPU)
	VGPUEnable = "deviceshare.VGPUEnable"

	// AscendMindClusterVNPU 控制是否启用昇腾 MindCluster VNPU 模式
	AscendMindClusterVNPU = "deviceshare.AscendMindClusterVNPUEnable"

	// AscendHAMiVNPUEnable 控制是否启用昇腾 HAMi VNPU 模式
	AscendHAMiVNPUEnable = "deviceshare.AscendHAMiVNPUEnable"

	// SchedulePolicyArgument 指定调度策略，例如 binpack(紧凑)或 spread(分散)
	SchedulePolicyArgument = "deviceshare.SchedulePolicy"

	// ScheduleWeight 指定设备打分的权重系数
	ScheduleWeight = "deviceshare.ScheduleWeight"

	// KnownGeometriesCMName vGPU 几何配置 ConfigMap 的名称
	KnownGeometriesCMName = "deviceshare.KnownGeometriesCMName"

	// KnownGeometriesCMNamespace vGPU 几何配置 ConfigMap 的命名空间
	KnownGeometriesCMNamespace = "deviceshare.KnownGeometriesCMNamespace"
)

// once 用于保证某些注册逻辑只执行一次（线程安全）
var (
	once sync.Once
)

// deviceSharePlugin 是 deviceshare 插件的核心结构体
type deviceSharePlugin struct {
	// 插件配置参数，从 Volcano 调度器配置文件读取
	pluginArguments framework.Arguments

	// 调度策略，例如 binpack(优先填满节点)/ spread(优先分散到不同节点)等
	schedulePolicy string

	// 设备调度权重，影响最终节点打分
	scheduleWeight int

	// lock 用于保护跨调度周期持久化的数据，避免并发读写冲突
	lock sync.RWMutex

	// persistedGPUs 用于保存跨 session 的 GPU 占用信息
	// 结构：nodeName -> namespace/name -> GPU index 集合
	// 作用：在多次调度周期之间保留 GPU 独占关系，防止重启后丢失状态
	persistedGPUs map[string]map[string]map[int]struct{}

	// persistedPodRules 保存跨 session 的 pod 命中规则信息
	// 结构：nodeName -> namespace/name -> rule index 集合
	// 作用：配合 persistedGPUs 恢复 GPU 独占规则状态
	persistedPodRules map[string]map[string]map[int]struct{}
}

// New 创建 deviceshare 插件实例
//
// 这是插件的构造函数，由 Volcano 调度器框架调用。
// 初始化后会立即调用 enablePredicate 来读取配置并注册设备。
func New(arguments framework.Arguments) framework.Plugin {
	dsp := &deviceSharePlugin{
		pluginArguments:   arguments,
		schedulePolicy:    "",
		scheduleWeight:    0,
		persistedGPUs:     make(map[string]map[string]map[int]struct{}),
		persistedPodRules: make(map[string]map[string]map[int]struct{}),
	}
	// 初始化并启用 predicate / 配置项 / 设备注册
	enablePredicate(dsp)
	return dsp
}

// Name 返回插件名称
//
// 这个名称用于在 Volcano 框架中标识此插件，
// 必须与配置文件中指定的插件名一致。
func (dp *deviceSharePlugin) Name() string {
	return PluginName
}

// enablePredicate 负责读取配置并初始化全局设备开关
//
// 这个函数在插件创建时调用，主要完成：
// 1. 从配置参数中读取各类设备开关
// 2. 进行冲突检查（例如 GPU Sharing 和 VGPU 不能同时开启）
// 3. 初始化设备配置（如 vGPU 几何形状 ConfigMap）
// 4. 注册启用的设备类型到全局列表
func enablePredicate(dsp *deviceSharePlugin) {
	// nodeLockEnable 控制是否启用节点锁机制
	// 节点锁用于防止多个调度器实例同时修改同一节点的设备状态
	nodeLockEnable := false
	args := dsp.pluginArguments

	// 从配置中读取各类开关
	// args.GetBool 会从配置文件中查找对应的 key，并将值写入第一个参数指向的变量
	args.GetBool(&gpushare.GpuSharingEnable, GPUSharingPredicate)
	args.GetBool(&gpushare.GpuNumberEnable, GPUNumberPredicate)
	args.GetBool(&nodeLockEnable, NodeLockEnable)
	args.GetBool(&vgpu.VGPUEnable, VGPUEnable)
	args.GetBool(&vnpu.AscendMindClusterVNPUEnable, AscendMindClusterVNPU)
	args.GetBool(&hami.AscendHAMiVNPUEnable, AscendHAMiVNPUEnable)

	// 将 nodeLockEnable 写入不同设备实现中
	// 注意：这里是直接修改全局变量，所以需要用 once 保证线程安全
	gpushare.NodeLockEnable = nodeLockEnable
	vgpu.NodeLockEnable = nodeLockEnable
	hami.NodeLockEnable = nodeLockEnable

	// 读取调度策略与权重
	// schedulePolicy 决定打分策略，例如 binpack 倾向于让任务集中在少数节点
	args.GetString(&dsp.schedulePolicy, SchedulePolicyArgument)
	args.GetInt(&dsp.scheduleWeight, ScheduleWeight)
	vgpu.SchedulePolicy = dsp.schedulePolicy

	// 配置冲突检查：GPUSharing 和 GPUNumber 不能同时开启
	// 因为这两种模式互斥：共享模式允许多个 Pod 共用 GPU，编号模式要求独占整卡
	if gpushare.GpuSharingEnable && gpushare.GpuNumberEnable {
		klog.Fatal("can not define true in both gpu sharing and gpu number")
	}

	// GPUShare/GPUNumber 与 VGPU 不能同时开启
	// vGPU 是硬件级虚拟化，与软件层的共享/编号机制不兼容
	if (gpushare.GpuSharingEnable || gpushare.GpuNumberEnable) && vgpu.VGPUEnable {
		klog.Fatal("gpu-share and vgpu can't be used together")
	}

	// 读取 vGPU 几何配置 ConfigMap 的名称与命名空间
	// vGPU 几何配置定义了不同 GPU 型号的内存切分规则
	knownGeometriesCMName := "volcano-vgpu-device-config"
	args.GetString(&knownGeometriesCMName, KnownGeometriesCMName)

	knownGeometriesCMNamespace := "kube-system"
	args.GetString(&knownGeometriesCMNamespace, KnownGeometriesCMNamespace)

	// 初始化设备配置
	// 这里会监听 ConfigMap 变化，动态更新 vGPU 几何配置
	config.InitDevicesConfig(knownGeometriesCMName, knownGeometriesCMNamespace)

	// 注册设备类型
	// 只有启用的设备才会被注册到全局设备列表中
	registerDevices()
}

// registerDevices 将启用的设备注册到 Volcano 全局设备列表中
//
// 使用 once.Do 保证注册逻辑只执行一次，避免重复注册。
// 注册后的设备类型会被 Volcano 框架识别和管理。
func registerDevices() {
	once.Do(func() {
		// 如果启用了 GPU 共享或编号模式，注册 gpushare 设备
		if gpushare.GpuSharingEnable || gpushare.GpuNumberEnable {
			api.RegisterDevice(gpushare.DeviceName)
		}

		// 如果启用了 vGPU 模式，注册 vgpu 设备
		if vgpu.VGPUEnable {
			api.RegisterDevice(vgpu.DeviceName)
		}

		// 如果启用了昇腾 MindCluster VNPU，注册 vnpu 设备
		if vnpu.AscendMindClusterVNPUEnable {
			api.RegisterDevice(vnpu.DeviceName)
		}

		// 如果启用了昇腾 HAMi VNPU(异构算力融合方案)，动态注册所有配置的 VNPU 设备类型
		//
		// 与前面几种设备(gpushare/vgpu/MindCluster VNPU)不同，
		// HAMi VNPU 不是注册一个固定的设备名，而是从 ConfigMap 加载的配置列表中
		// 读取所有芯片型号定义(如 Ascend910B3、Ascend310P 等)，逐一注册
		//
		// 数据来源链路：
		//   ConfigMap(device-config.yaml) → InitDevicesConfig() 解析 → config.GetConfig().VNPUs
		//
		// 注册后，每种芯片型号的 CommonWord(如 "Ascend910B3")会作为独立的设备类型
		// 参与后续的 Predicate 过滤和 Score 打分流程
		if hami.AscendHAMiVNPUEnable {
			// 遍历配置中定义的所有 VNPU 芯片型号
			for _, vnpu := range config.GetConfig().VNPUs {
				// 打印注册日志，方便排查哪些设备类型被成功注册
				klog.V(3).Infof("register device %s", vnpu.CommonWord)
				// 将该芯片型号注册到 Volcano 全局设备列表（api.RegisteredDevices）中
				// RegisterDevice 内部已做去重，重复注册不会造成问题
				api.RegisterDevice(vnpu.CommonWord)
			}
		}
	})
}

// createStatus 创建一个 api.Status，方便返回错误状态
//
// 这是一个辅助函数，用于统一创建状态对象。
// code 表示状态码(如 Success、Error 等)，reason 是错误描述。
func createStatus(code int, reason string) *api.Status {
	status := api.Status{
		Code:   code,
		Reason: reason,
	}
	return &status
}

// getDeviceScore 计算单节点的设备评分
//
// 逻辑：
// 1. 遍历节点上的所有设备类型
// 2. 如果当前 Pod 请求了该设备，则调用设备自己的 ScoreNode
// 3. 累加所有设备评分，得到该节点的最终设备分数
//
// 注意：
// - vnpu 设备使用 BatchNodeOrderFn，这里跳过
// - 这里只处理 NodeOrderFn 相关的设备（如 vgpu、gpushare）
func getDeviceScore(ctx context.Context, pod *v1.Pod, node *api.NodeInfo, schedulePolicy string) (int64, *fwk.Status) {
	s := float64(0)
	for deviceType, device := range node.Others {
		if device.(api.Devices).HasDeviceRequest(pod) {
			// Only process device types that use NodeOrderFn (vgpu and gpushare)
			// vnpu devices use BatchNodeOrderFn, skip them here
			if deviceType != vnpu.DeviceName {
				ns := device.(api.Devices).ScoreNode(pod, schedulePolicy)
				s += ns
			}
		}
	}
	klog.V(4).Infof("deviceScore for task %s/%s is: %v", pod.Namespace, pod.Name, s)
	// math.Floor(s + 0.5) 实现四舍五入
	return int64(math.Floor(s + 0.5)), nil
}

// getDeviceScoresInBatch 批量计算多个节点的设备评分
//
// 某些设备（如 VNPU）需要同时比较多个节点的资源分布情况，
// 才能做出最优分配决策。这种场景下使用批量打分。
func getDeviceScoresInBatch(pod *v1.Pod, schedulePolicy string, allDevices []api.Devices) []float64 {
	switch d := allDevices[0].(type) {
	case *vnpu.NPUDevices:
		// 如果需要扩展其他设备的批量打分策略，可以在这里增加分支
		return vnpu310p.ScoreBatchNodes(pod, schedulePolicy, d, allDevices)
	default:
		score := make([]float64, 0)
		return score
	}
}

// initScoreMap 初始化节点分数字典
//
// 为每个节点创建一个初始分数为 0.0 的条目，
// 后续会根据设备打分结果累加分数。
func initScoreMap(nodes []*api.NodeInfo) map[string]float64 {
	scoreMap := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		if reflect.ValueOf(node).IsNil() {
			continue
		}
		scoreMap[node.Name] = 0.0
	}
	return scoreMap
}

// initializeDevicesWithSession 对所有节点上的设备进行 session 级初始化
//
// 某些设备需要依赖当前调度会话里的节点/任务信息进行初始化，
// 例如 VNPU 设备需要根据当前 session 中的资源状态建立内部索引。
//
// 这个函数在每个调度周期开始时调用，确保设备对象拥有最新的集群状态。
func initializeDevicesWithSession(ssn *framework.Session) {
	for _, nodeInfo := range ssn.Nodes { // initialize every device in every node with global ssn
		for _, val := range api.RegisteredDevices {
			if dev, ok := nodeInfo.Others[val].(api.Devices); ok {
				if err := initializeDevice(dev, ssn, nodeInfo); err != nil {
					klog.Warningf("Failed to initialize devices with session for node %s: %v", nodeInfo.Name, err)
				}
			}
		}
	}
}

// initializeDevice 初始化单个设备对象
//
// 根据不同的设备类型，调用相应的初始化函数。
// 目前只有 VNPU 设备需要 session 级初始化。
func initializeDevice(device api.Devices, ssn *framework.Session, nodeInfo *api.NodeInfo) error {
	switch d := device.(type) {
	case *vnpu.NPUDevices:
		if vnpu.AscendMindClusterVNPUEnable {
			klog.V(3).Infof("initialize ascend310p device.")
			return vnpu310p.InitVNPUDevice(d, ssn, nodeInfo)
		}
	}
	return nil
}

// OnSessionOpen 是插件在每个调度周期开始时的入口
//
// 这个函数做了几件重要的事情：
// 1. 为 GPU 设备包装 exclusivity（如果配置了 GPUExclusiveRules）
// 2. 初始化依赖 session 的设备对象
// 3. 注册 PredicateFn：用于判断一个 pod 是否能放到某节点
// 4. 注册 NodeOrderFn：用于对单节点打分
// 5. 注册 BatchNodeOrderFn：用于批量节点打分
//
// 调度周期（Session）是 Volcano 的核心概念，每次调度一批任务时会创建一个新 session。
func (dp *deviceSharePlugin) OnSessionOpen(ssn *framework.Session) {
	// 在初始化和注册之前，先把 GPU 设备包装成支持"独占规则"的版本
	// 这样整个调度周期内使用的都是包装后的设备对象
	// 如果配置中没有 GPUExclusiveRules，这个函数会直接返回，不做任何操作
	dp.wrapGPUDevicesForExclusivity(ssn)

	// 初始化设备(某些设备需要 session 作为输入)
	// 例如 VNPU 设备需要根据当前 session 中的任务分布建立索引
	initializeDevicesWithSession(ssn)

	// =========================================================
	// 注册 PredicateFn
	// =========================================================
	// Predicate 的作用：判断某个 task/pod 能不能放到某个节点上
	// 这里逐个检查节点上的每一种设备，如果该 pod 请求了该设备，
	// 就调用设备本身的 FilterNode 来做可行性检查。
	ssn.AddPredicateFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {

		predicateStatus := make([]*api.Status, 0)

		// 遍历当前节点上的所有已注册设备
		for _, val := range api.RegisteredDevices {
			if dev, ok := node.Others[val].(api.Devices); ok {
				// 如果设备对象为空，跳过
				if reflect.ValueOf(dev).IsNil() {
					klog.V(4).Infof("device %s is null, skipping it", val)
					continue
				}

				// 如果 pod 没请求这个设备，也没必要做检查
				if !dev.HasDeviceRequest(task.Pod) {
					klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it", task.Pod.Namespace, task.Pod.Name, val, node.Name)
					continue
				}

				// 调用设备插件自己的 FilterNode 进行过滤
				// 对于包装后的 exclusiveGPUDevices，这里会先屏蔽已占用的 GPU 再过滤
				code, msg, err := dev.FilterNode(task.Pod, dp.schedulePolicy)
				if err != nil {
					klog.V(4).Infof("pod %s/%s fit failed. device %s node %s err %v", task.Pod.Namespace, task.Pod.Name, val, node.Name, err)
					predicateStatus = append(predicateStatus, createStatus(code, msg))
					return api.NewFitErrWithStatus(task, node, predicateStatus...)
				}

				// 如果 FilterNode 返回的状态码不是 Success，也视为失败
				filterNodeStatus := createStatus(code, msg)
				if filterNodeStatus.Code != api.Success {
					predicateStatus = append(predicateStatus, filterNodeStatus)
					return api.NewFitErrWithStatus(task, node, predicateStatus...)
				}
			} else {
				klog.Warningf("Devices %s assertion conversion failed, skip", val)
			}
		}

		klog.V(4).Infof("checkDevices predicates Task <%s/%s> on Node <%s>: fit ",
			task.Namespace, task.Name, node.Name)

		return nil
	})

	// =========================================================
	// 注册 NodeOrderFn
	// =========================================================
	// NodeOrderFn 用于给单个节点打分，分数越高/越低取决于调度器策略
	// 这里会根据设备得分和 scheduleWeight 计算最终节点分数。
	//
	// 打分阶段在所有通过 Predicate 的节点上进行，
	// 调度器会选择分数最优的节点来放置 Pod。
	ssn.AddNodeOrderFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		nodeScore := float64(0)
		if dp.scheduleWeight > 0 {
			score, status := getDeviceScore(context.TODO(), task.Pod, node, dp.schedulePolicy)
			if !status.IsSuccess() {
				klog.Warningf("Node: %s, Calculate Device Score Failed because of Error: %v", node.Name, status.AsError())
				return 0, status.AsError()
			}

			// 最终设备分数 = 设备原始分数 * 插件配置权重
			// 权重为 0 时，设备打分不影响节点选择
			nodeScore = float64(score) * float64(dp.scheduleWeight)
			klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f", node.Name, task.Namespace, task.Name, dp.scheduleWeight, nodeScore)
		}
		return nodeScore, nil
	})

	// =========================================================
	// 注册 BatchNodeOrderFn
	// =========================================================
	// 批量打分用于某些设备（例如 vnpu）需要同时比较多个节点的场景
	// 这与 NodeOrderFn 的区别是：
	// - NodeOrderFn：独立计算每个节点的分数
	// - BatchNodeOrderFn：可以同时看到所有候选节点，做全局优化
	ssn.AddBatchNodeOrderFn(dp.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		scoreMap := initScoreMap(nodes)

		if dp.scheduleWeight > 0 {
			for _, deviceType := range api.RegisteredDevices {
				// 这里只处理需要 batch scoring 的设备类型
				// 目前只有 vnpu 设备使用批量打分
				if deviceType != vnpu.DeviceName {
					continue
				}

				// 收集所有节点上该类设备实例
				allDevices := make([]api.Devices, 0)
				for _, node := range nodes {
					device, ok := node.Others[deviceType]
					if ok {
						if deviceInterface, isDeviceInterface := device.(api.Devices); isDeviceInterface {
							if reflect.ValueOf(deviceInterface).IsNil() {
								// 如果节点没初始化该设备，但 pod 又请求了它，则直接返回错误
								if deviceInterface == nil || deviceInterface.HasDeviceRequest(task.Pod) {
									return nil, fmt.Errorf("node not initialized with device %s", deviceType)
								}
								klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it", task.Pod.Namespace, task.Pod.Name, deviceType, nodes[0].Name)
								continue
							}
							allDevices = append(allDevices, deviceInterface)
						}
					} else {
						klog.Warningf("Devices %s assertion conversion failed, skip", deviceType)
					}
				}

				// 如果没有可用于评分的设备，跳过
				if len(allDevices) == 0 {
					klog.V(4).Infof("No devices of type %s found for scoring", deviceType)
					continue
				}

				// 调用 batch scoring 逻辑得到每个节点的分数
				scores := getDeviceScoresInBatch(task.Pod, dp.schedulePolicy, allDevices)

				// 返回结果长度必须和 nodes 长度一致
				if len(scores) != len(nodes) {
					klog.Warningf("Score array length (%d) doesn't match nodes length (%d) for device type %s", len(scores), len(nodes), deviceType)
					continue
				}

				// 将 batch score 写回到 scoreMap
				for i := range nodes {
					finalScore := scores[i] * float64(dp.scheduleWeight)
					scoreMap[nodes[i].Node.Name] += finalScore
					klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f", nodes[i].Name, task.Namespace, task.Name, dp.scheduleWeight, finalScore)
				}
			}
		}
		return scoreMap, nil
	})
}

// OnSessionClose 在调度会话结束时调用
//
// 目前这个函数没有额外清理逻辑，因为：
// - persistedGPUs 和 persistedPodRules 需要跨 session 保留
// - 设备对象的清理由各自的生命周期管理
//
// 如果未来需要在 session 结束时执行某些操作（如统计、日志），可以在这里添加。
func (dp *deviceSharePlugin) OnSessionClose(ssn *framework.Session) {}
