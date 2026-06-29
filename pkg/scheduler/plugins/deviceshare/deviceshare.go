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
	GPUSharingPredicate = "deviceshare.GPUSharingEnable"
	NodeLockEnable      = "deviceshare.NodeLockEnable"
	GPUNumberPredicate  = "deviceshare.GPUNumberEnable"
	VGPUEnable          = "deviceshare.VGPUEnable"

	AscendMindClusterVNPU = "deviceshare.AscendMindClusterVNPUEnable"
	AscendHAMiVNPUEnable  = "deviceshare.AscendHAMiVNPUEnable"

	SchedulePolicyArgument = "deviceshare.SchedulePolicy"
	ScheduleWeight         = "deviceshare.ScheduleWeight"

	KnownGeometriesCMName      = "deviceshare.KnownGeometriesCMName"
	KnownGeometriesCMNamespace = "deviceshare.KnownGeometriesCMNamespace"
)

// once 用于保证某些注册逻辑只执行一次
var (
	once sync.Once
)

// deviceSharePlugin 是 deviceshare 插件的核心结构体
type deviceSharePlugin struct {
	// 插件配置参数
	pluginArguments framework.Arguments

	// 调度策略，例如 binpack / spread 等
	schedulePolicy string

	// 设备调度权重
	scheduleWeight int

	// lock 用于保护跨调度周期持久化的数据
	lock sync.RWMutex

	// persistedGPUs 用于保存跨 session 的 GPU 占用信息
	// 结构：nodeName -> namespace/name -> GPU index 集合
	persistedGPUs map[string]map[string]map[int]struct{}

	// persistedPodRules 保存跨 session 的 pod 命中规则信息
	// 结构：nodeName -> namespace/name -> rule index 集合
	persistedPodRules map[string]map[string]map[int]struct{}
}

// New 创建 deviceshare 插件实例
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
func (dp *deviceSharePlugin) Name() string {
	return PluginName
}

// enablePredicate 负责读取配置并初始化全局设备开关
func enablePredicate(dsp *deviceSharePlugin) {
	// nodeLockEnable 控制是否启用节点锁机制
	nodeLockEnable := false
	args := dsp.pluginArguments

	// 从配置中读取各类开关
	args.GetBool(&gpushare.GpuSharingEnable, GPUSharingPredicate)
	args.GetBool(&gpushare.GpuNumberEnable, GPUNumberPredicate)
	args.GetBool(&nodeLockEnable, NodeLockEnable)
	args.GetBool(&vgpu.VGPUEnable, VGPUEnable)
	args.GetBool(&vnpu.AscendMindClusterVNPUEnable, AscendMindClusterVNPU)
	args.GetBool(&hami.AscendHAMiVNPUEnable, AscendHAMiVNPUEnable)

	// 将 nodeLockEnable 写入不同设备实现中
	gpushare.NodeLockEnable = nodeLockEnable
	vgpu.NodeLockEnable = nodeLockEnable
	hami.NodeLockEnable = nodeLockEnable

	// 读取调度策略与权重
	args.GetString(&dsp.schedulePolicy, SchedulePolicyArgument)
	args.GetInt(&dsp.scheduleWeight, ScheduleWeight)
	vgpu.SchedulePolicy = dsp.schedulePolicy

	// 配置冲突检查：GPUSharing 和 GPUNumber 不能同时开启
	if gpushare.GpuSharingEnable && gpushare.GpuNumberEnable {
		klog.Fatal("can not define true in both gpu sharing and gpu number")
	}

	// GPUShare/GPUNumber 与 VGPU 不能同时开启
	if (gpushare.GpuSharingEnable || gpushare.GpuNumberEnable) && vgpu.VGPUEnable {
		klog.Fatal("gpu-share and vgpu can't be used together")
	}

	// 读取 vGPU 几何配置 ConfigMap 的名称与命名空间
	knownGeometriesCMName := "volcano-vgpu-device-config"
	args.GetString(&knownGeometriesCMName, KnownGeometriesCMName)

	knownGeometriesCMNamespace := "kube-system"
	args.GetString(&knownGeometriesCMNamespace, KnownGeometriesCMNamespace)

	// 初始化设备配置
	config.InitDevicesConfig(knownGeometriesCMName, knownGeometriesCMNamespace)

	// 注册设备类型
	registerDevices()
}

// registerDevices 将启用的设备注册到 Volcano 全局设备列表中
func registerDevices() {
	once.Do(func() {
		if gpushare.GpuSharingEnable || gpushare.GpuNumberEnable {
			api.RegisterDevice(gpushare.DeviceName)
		}
		if vgpu.VGPUEnable {
			api.RegisterDevice(vgpu.DeviceName)
		}
		if vnpu.AscendMindClusterVNPUEnable {
			api.RegisterDevice(vnpu.DeviceName)
		}
		if hami.AscendHAMiVNPUEnable {
			for _, vnpu := range config.GetConfig().VNPUs {
				klog.V(3).Infof("register device %s", vnpu.CommonWord)
				api.RegisterDevice(vnpu.CommonWord)
			}
		}
	})
}

// createStatus 创建一个 api.Status，方便返回错误状态
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
// - 这里只处理 NodeOrderFn 相关的设备
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
	return int64(math.Floor(s + 0.5)), nil
}

// getDeviceScoresInBatch 批量计算多个节点的设备评分
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
func (dp *deviceSharePlugin) OnSessionOpen(ssn *framework.Session) {
	// 在初始化和注册之前，先把 GPU 设备包装成支持“独占规则”的版本
	// 这样整个调度周期内使用的都是包装后的设备对象
	dp.wrapGPUDevicesForExclusivity(ssn)

	// 初始化设备（某些设备需要 session 作为输入）
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
	ssn.AddNodeOrderFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		nodeScore := float64(0)
		if dp.scheduleWeight > 0 {
			score, status := getDeviceScore(context.TODO(), task.Pod, node, dp.schedulePolicy)
			if !status.IsSuccess() {
				klog.Warningf("Node: %s, Calculate Device Score Failed because of Error: %v", node.Name, status.AsError())
				return 0, status.AsError()
			}

			// 最终设备分数 = 设备原始分数 * 插件配置权重
			nodeScore = float64(score) * float64(dp.scheduleWeight)
			klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f", node.Name, task.Namespace, task.Name, dp.scheduleWeight, nodeScore)
		}
		return nodeScore, nil
	})

	// =========================================================
	// 注册 BatchNodeOrderFn
	// =========================================================
	// 批量打分用于某些设备（例如 vnpu）需要同时比较多个节点的场景
	ssn.AddBatchNodeOrderFn(dp.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		scoreMap := initScoreMap(nodes)

		if dp.scheduleWeight > 0 {
			for _, deviceType := range api.RegisteredDevices {
				// 这里只处理需要 batch scoring 的设备类型
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
// 这里目前没有额外清理逻辑
func (dp *deviceSharePlugin) OnSessionClose(ssn *framework.Session) {}
