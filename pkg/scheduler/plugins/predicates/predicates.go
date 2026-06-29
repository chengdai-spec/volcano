/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Migrated from custom predicate logic to Kubernetes native scheduler plugins (NodeAffinity, NodePorts, InterPodAffinity, VolumeBinding, DRA, etc.) for better compatibility
- Added multiple extension points: PrePredicate, BatchNodeOrder, SimulateAddTask, SimulateRemoveTask, etc.

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

// Package predicates 实现了 Volcano 调度器中的 predicates 插件。
// 该插件将 Kubernetes 原生调度框架中的各种 Filter/PreFilter/Reserve/PreBind/Score 插件
// 封装到 Volcano 的插件体系中，统一处理节点亲和性、端口、污点容忍、Pod 亲和性、
// 卷限制、拓扑分布、卷绑定、DRA 动态资源分配等调度约束。
package predicates

import (
	"context"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilFeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodevolumelimits"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/podtopologyspread"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumezone"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	vbcap "volcano.sh/volcano/pkg/scheduler/capabilities/volumebinding"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/k8s"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodescore"
)

const (
	// PluginName 表示 Volcano 调度器插件的名称。
	PluginName = "predicates"

	// NodeAffinityEnable 是 scheduler configmap 中用于启用 Node Affinity 谓词的键。
	NodeAffinityEnable = "predicate.NodeAffinityEnable"

	// NodePortsEnable 是 scheduler configmap 中用于启用 Node Ports 谓词的键。
	NodePortsEnable = "predicate.NodePortsEnable"

	// TaintTolerationEnable 是 scheduler configmap 中用于启用 Taint Toleration 谓词的键。
	TaintTolerationEnable = "predicate.TaintTolerationEnable"

	// PodAffinityEnable 是 scheduler configmap 中用于启用 Pod Affinity 谓词的键。
	PodAffinityEnable = "predicate.PodAffinityEnable"

	// NodeVolumeLimitsEnable 是 scheduler configmap 中用于启用 Node Volume Limits 谓词的键。
	NodeVolumeLimitsEnable = "predicate.NodeVolumeLimitsEnable"

	// VolumeZoneEnable 是 scheduler configmap 中用于启用 Volume Zone 谓词的键。
	VolumeZoneEnable = "predicate.VolumeZoneEnable"

	// PodTopologySpreadEnable 是 scheduler configmap 中用于启用 Pod Topology Spread 谓词的键。
	PodTopologySpreadEnable = "predicate.PodTopologySpreadEnable"

	// VolumeBindingEnable 是 scheduler configmap 中用于启用 Volume Binding 谓词的键。
	VolumeBindingEnable = "predicate.VolumeBindingEnable"

	// DynamicResourceAllocationEnable 是 scheduler configmap 中用于启用 DRA 谓词的键。
	DynamicResourceAllocationEnable = "predicate.DynamicResourceAllocationEnable"

	// CachePredicate 控制 predicate cache 特性的开关。
	CachePredicate = "predicate.CacheEnable"
)

var (
	// volumeBindingPluginInstance 是 VolumeBinding 插件的全局单例。
	// 由于 VolumeBinding 插件包含 AssumeCache 和 eventHandler，重复初始化会导致内存泄漏，
	// 因此使用 sync.Once 保证只初始化一次。
	volumeBindingPluginInstance *vbcap.VolumeBinding
	volumeBindingPluginOnce     sync.Once
)

// PredicatesPlugin 是 predicates 插件的主体结构体。
// 它聚合了多个 Kubernetes 原生调度框架插件，并按扩展点分类管理。

/*
PredicatesPlugin

	├── New()              → 创建插件实例，读取配置覆盖默认值
	├── OnSessionOpen()    → 会话开启：初始化 Handle + 注册所有回调
	├── InitPlugin()       → 初始化 10 个原生插件，按扩展点分类
	├── PrePredicate()     → PreFilter 阶段：为所有 pending pod 预处理
	├── Predicate()         → Filter 阶段：判断节点是否适合运行任务
	├── BatchNodeOrder()    → Score 阶段：批量计算节点得分
	├── PreBind()          → PreBind 阶段：完成 PV/PVC 真实绑定等
	├── PreBindRollBack()  → PreBind 失败回滚
	└── OnSessionClose()   → 会话关闭（当前为空）
*/
type PredicatesPlugin struct {
	// pluginArguments 是插件初始化时传入的参数。
	pluginArguments framework.Arguments

	// enabledPredicates 记录各谓词插件是否启用。
	enabledPredicates predicateEnable

	// features 保存 Kubernetes 特性门控状态。
	features feature.Features

	// FilterPlugins 是所有 Filter 扩展点插件的集合。
	FilterPlugins map[string]fwk.FilterPlugin
	// StableFilterPlugins 是 FilterPlugins 的子集，包含那些结果稳定的、可用于缓存的过滤器。
	StableFilterPlugins map[string]fwk.FilterPlugin
	// PreFilterPlugins 是所有 PreFilter 扩展点插件的集合。
	PreFilterPlugins map[string]fwk.PreFilterPlugin
	// ReservePlugins 是所有 Reserve 扩展点插件的集合。
	ReservePlugins map[string]fwk.ReservePlugin
	// PreBindPlugins 是所有 PreBind 扩展点插件的集合。
	PreBindPlugins map[string]fwk.PreBindPlugin
	// ScorePlugins 是所有 Score 扩展点插件的集合。
	ScorePlugins map[string]nodescore.BaseScorePlugin
	// ScoreWeights 记录每个 Score 插件的权重。
	ScoreWeights map[string]int
	// 以下 Order 切片记录各扩展点插件的执行顺序。
	FilterOrder       []string
	StableFilterOrder []string
	PreFilterOrder    []string
	ReserveOrder      []string
	PreBindOrder      []string
	ScoreOrder        []string
	// PredicateCache 是 predicate 等价缓存，用于加速谓词判断。
	PredicateCache *predicateCache
	// Handle 是调度框架句柄，提供 SnapshotSharedLister 等能力。
	Handle fwk.Handle
}

// New 创建并返回 predicates 插件实例。
// 该函数初始化各谓词插件的默认启用状态，并从插件参数中读取用户配置覆盖默认值。
func New(arguments framework.Arguments) framework.Plugin {
	// 默认启用大部分谓词，仅 cache 默认关闭，DRA 跟随特性门控。
	predicate := predicateEnable{
		nodeAffinityEnable:              true,
		nodePortEnable:                  true,
		taintTolerationEnable:           true,
		podAffinityEnable:               true,
		nodeVolumeLimitsEnable:          true,
		volumeZoneEnable:                true,
		podTopologySpreadEnable:         true,
		cacheEnable:                     false,
		volumeBindingEnable:             true,
		dynamicResourceAllocationEnable: utilFeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation),
	}

	// 如果 scheduler configmap 中提供了对应参数，则覆盖默认值。
	arguments.GetBool(&predicate.nodeAffinityEnable, NodeAffinityEnable)
	arguments.GetBool(&predicate.nodePortEnable, NodePortsEnable)
	arguments.GetBool(&predicate.taintTolerationEnable, TaintTolerationEnable)
	arguments.GetBool(&predicate.podAffinityEnable, PodAffinityEnable)
	arguments.GetBool(&predicate.nodeVolumeLimitsEnable, NodeVolumeLimitsEnable)
	arguments.GetBool(&predicate.volumeZoneEnable, VolumeZoneEnable)
	arguments.GetBool(&predicate.podTopologySpreadEnable, PodTopologySpreadEnable)
	arguments.GetBool(&predicate.volumeBindingEnable, VolumeBindingEnable)
	arguments.GetBool(&predicate.cacheEnable, CachePredicate)

	// 从 Kubernetes 默认特性门控读取各特性开关，传递给原生调度插件。
	features := feature.Features{
		EnableStorageCapacityScoring:                 utilFeature.DefaultFeatureGate.Enabled(features.StorageCapacityScoring),
		EnableNodeInclusionPolicyInPodTopologySpread: utilFeature.DefaultFeatureGate.Enabled(features.NodeInclusionPolicyInPodTopologySpread),
		EnableMatchLabelKeysInPodTopologySpread:      utilFeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpread),
		EnableSidecarContainers:                      utilFeature.DefaultFeatureGate.Enabled(features.SidecarContainers),
		EnableDRAAdminAccess:                         utilFeature.DefaultFeatureGate.Enabled(features.DRAAdminAccess),
		EnableDynamicResourceAllocation:              utilFeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation),
		EnableVolumeAttributesClass:                  utilFeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass),
		EnableCSIMigrationPortworx:                   utilFeature.DefaultFeatureGate.Enabled(features.CSIMigrationPortworx),
		EnableDRAExtendedResource:                    utilFeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource),
		EnableDRAPrioritizedList:                     utilFeature.DefaultFeatureGate.Enabled(features.DRAPrioritizedList),
		EnableDRAConsumableCapacity:                  utilFeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity),
		EnableDRADeviceTaints:                        utilFeature.DefaultFeatureGate.Enabled(features.DRADeviceTaints),
		EnableDRASchedulerFilterTimeout:              utilFeature.DefaultFeatureGate.Enabled(features.DRASchedulerFilterTimeout),
		EnableDRAResourceClaimDeviceStatus:           utilFeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus),
		EnableDRADeviceBindingConditions:             utilFeature.DefaultFeatureGate.Enabled(features.DRADeviceBindingConditions),
		EnableDRAPartitionableDevices:                utilFeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevices),
	}
	return &PredicatesPlugin{pluginArguments: arguments, enabledPredicates: predicate, features: features}
}

func (pp *PredicatesPlugin) Name() string {
	return PluginName
}

// predicateEnable 记录各谓词插件的启用状态。
type predicateEnable struct {
	nodeAffinityEnable              bool
	nodePortEnable                  bool
	taintTolerationEnable           bool
	podAffinityEnable               bool
	nodeVolumeLimitsEnable          bool
	volumeZoneEnable                bool
	podTopologySpreadEnable         bool
	cacheEnable                     bool
	volumeBindingEnable             bool
	dynamicResourceAllocationEnable bool
}

// BindContextExtension 保存 predicates 插件在绑定上下文中的扩展信息。
// 主要用来在 PreBind 阶段传递 CycleState。
type BindContextExtension struct {
	State *k8sframework.CycleState
}

// OnSessionOpen 在 Volcano 调度会话开启时被调用，负责初始化 predicates 插件并注册各类回调。
//
// 主要工作：
//  1. 构建 Kubernetes 调度框架句柄（Handle），使原生插件能访问节点快照、客户端、informer 等；
//  2. 调用 InitPlugin 初始化所有原生 Filter/PreFilter/Reserve/PreBind/Score 插件；
//  3. 如启用 cache，创建 predicate 等价缓存；
//  4. 注册事件处理器，在 Pod 被分配/释放时执行 Reserve/Unreserve 以及设备分配/释放；
//  5. 注册 PrePredicate、Predicate、BatchNodeOrder、SimulateAdd/Remove/Predicate 等扩展函数。
func (pp *PredicatesPlugin) OnSessionOpen(ssn *framework.Session) {
	pl := ssn.PodLister

	nodeMap := ssn.NodeMap
	// 构造原生调度框架句柄，注入 DRA 管理器、CSI 管理器、客户端和 informer 工厂。
	handle := k8s.NewFramework(nodeMap,
		k8s.WithSharedDRAManager(ssn.SharedDRAManager()),
		k8s.WithSharedCSIManager(nodevolumelimits.NewCSIManager(ssn.InformerFactory().Storage().V1().CSINodes().Lister())),
		k8s.WithClientSet(ssn.KubeClient()),
		k8s.WithInformerFactory(ssn.InformerFactory()),
	)
	pp.Handle = handle

	pp.InitPlugin()

	// 如果启用了 predicate cache，则初始化等价缓存以加速稳定谓词判断。
	if pp.enabledPredicates.cacheEnable {
		pp.PredicateCache = predicateCacheNew()
	}

	// 注册分配/释放事件处理器，用于更新 PodLister、nodeMap 以及执行设备相关操作。
	ssn.AddEventHandler(&framework.EventHandler{
		// AllocateFunc 在任务被成功分配到节点时调用。
		AllocateFunc: func(event *framework.Event) {
			klog.V(4).Infoln("predicates, allocate", event.Task.NodeName)
			pod := pl.UpdateTask(event.Task, event.Task.NodeName)
			nodeName := event.Task.NodeName
			node, err := pp.Handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
			if err != nil {
				klog.Errorf("predicates, get node %s info failed: %v", nodeName, err)
				return
			}
			nodeInfo, ok := ssn.Nodes[nodeName]
			if !ok {
				klog.Errorf("Failed to get node %s info from cache", nodeName)
				return
			}
			// 执行所有 Reserve 插件，在内存中假设占用资源（如 VolumeBinding 假设 PV/PVC 绑定）。
			pp.runReservePlugins(ssn, event)
			if event.Err != nil {
				return
			}
			// 处理 GPU 等自定义设备的分配逻辑。
			for _, val := range api.RegisteredDevices {
				if devices, ok := nodeInfo.Others[val].(api.Devices); ok {
					if api.IsNilDevice(devices) {
						continue
					}
					if !devices.HasDeviceRequest(pod) {
						continue
					}

					err := devices.Allocate(ssn.KubeClient(), pod)
					if err != nil {
						klog.Errorf("AllocateToPod failed %s", err.Error())
						event.Err = err
						return
					}
				} else {
					klog.Warningf("Devices %s assertion conversion failed, skip", val)
				}
			}
			// apiserver 对 affinity terms 的校验存在历史兼容性问题，因此忽略 NewPodInfo 错误。
			podInfo, _ := k8sframework.NewPodInfo(pod)
			// 将 Pod 加入节点快照，供后续 Pod 亲和性/拓扑分布计算使用。
			node.AddPodInfo(podInfo)
			klog.V(4).Infof("predicates, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, nodeName)
		},
		// DeallocateFunc 在任务从节点上被释放时调用。
		DeallocateFunc: func(event *framework.Event) {
			klog.V(4).Infoln("predicates, deallocate", event.Task.NodeName)
			pod := pl.UpdateTask(event.Task, "")
			nodeName := event.Task.NodeName
			node, err := pp.Handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
			if err != nil {
				klog.Errorf("predicates, get node %s info failed: %v", nodeName, err)
				return
			}

			// 从 Volcano session 中获取节点信息，用于设备释放。
			nodeInfo, ok := ssn.Nodes[nodeName]
			if !ok {
				klog.Errorf("Failed to get node %s info from cache", nodeName)
				return
			}

			// 执行所有 Unreserve 插件，回滚 Reserve 阶段的假设占用。
			pp.runUnReservePlugins(ssn, event)

			// 释放 GPU 等自定义设备资源。
			for _, val := range api.RegisteredDevices {
				if devices, ok := nodeInfo.Others[val].(api.Devices); ok {
					if api.IsNilDevice(devices) {
						continue
					}
					if !devices.HasDeviceRequest(pod) {
						continue
					}

					// 释放该 Pod 占用的设备资源。
					err := devices.Release(ssn.KubeClient(), pod)
					if err != nil {
						klog.Errorf("Device %s release failed for pod %s/%s, err:%s", val, pod.Namespace, pod.Name, err.Error())
						return
					}
				} else {
					klog.Warningf("Devices %s assertion conversion failed, skip", val)
				}
			}

			if err = node.RemovePod(klog.FromContext(context.TODO()), pod); err != nil {
				klog.Errorf("predicates, remove pod %s/%s from node [%s] error: %v", pod.Namespace, pod.Name, nodeName, err)
				return
			}
			klog.V(4).Infof("predicates, update pod %s/%s deallocate from node [%s]", pod.Namespace, pod.Name, nodeName)
		},
	})

	// 注册 PrePredicate 扩展函数：在谓词过滤前执行所有 PreFilter 插件，生成 CycleState。
	ssn.AddPrePredicateFn(pp.Name(), func(task *api.TaskInfo) error {
		state := ssn.GetCycleState(task.UID)
		nodeInfoList, err := pp.Handle.SnapshotSharedLister().NodeInfos().List()
		if err != nil {
			klog.Errorf("Failed to list nodes from snapshot: %v", err)
			return err
		}
		return pp.PrePredicate(task, state, nodeInfoList)
	})

	// 注册 Predicate 扩展函数：对指定任务和节点执行所有 Filter 插件。
	ssn.AddPredicateFn(pp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		state := ssn.GetCycleState(task.UID)
		return pp.Predicate(task, node, state)
	})

	// TODO: 需要将 nodeorder 插件中的相关逻辑统一合并到 predicates 中。
	// 当前 volumebinding 插件同时跨越 predicates 和 nodeorder 两个插件时会触发两次初始化，增加内存开销。
	// 因此在这里增加 BatchNodeOrder 扩展点，因为 volumebinding 涉及 PreScore 和 Score 扩展点。
	ssn.AddBatchNodeOrderFn(pp.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		state := ssn.GetCycleState(task.UID)
		nodeInfoList, err := pp.Handle.SnapshotSharedLister().NodeInfos().List()
		if err != nil {
			klog.Errorf("Failed to list nodes from snapshot: %v", err)
			return nil, err
		}
		return pp.BatchNodeOrder(task, nodeInfoList, state)
	})

	// 注册 binder，使 predicates 插件能参与 PreBind/PreBindRollBack 阶段。
	ssn.RegisterBinder(pp.Name(), pp)

	// 注册 SimulateAddTask 函数：用于抢占等场景模拟将某个任务加入节点后的影响。
	ssn.AddSimulateAddTaskFn(pp.Name(), func(ctx context.Context, cycleState fwk.CycleState, taskToSchedule *api.TaskInfo, taskToAdd *api.TaskInfo, nodeInfo *api.NodeInfo) error {
		podInfoToAdd, err := k8sframework.NewPodInfo(taskToAdd.Pod)
		if err != nil {
			return fmt.Errorf("failed to create pod info: %w", err)
		}

		k8sNodeInfo := k8sframework.NewNodeInfo(nodeInfo.Pods()...)
		k8sNodeInfo.SetNode(nodeInfo.Node)

		// 如果启用了 Pod 亲和性，模拟将待添加 Pod 加入节点，检查亲和性状态变化。
		if pp.enabledPredicates.podAffinityEnable {
			if podAffinityFilter, exist := pp.FilterPlugins[interpodaffinity.Name].(*interpodaffinity.InterPodAffinity); exist {
				isSkipInterPodAffinity := handleSkipPredicatePlugin(cycleState, podAffinityFilter.Name())
				if !isSkipInterPodAffinity {
					status := podAffinityFilter.AddPod(ctx, cycleState, taskToSchedule.Pod, podInfoToAdd, k8sNodeInfo)
					if !status.IsSuccess() {
						return fmt.Errorf("failed to add pod to node %s: %w", nodeInfo.Name, status.AsError())
					}
				}
			} else {
				return fmt.Errorf("failed to call %s plugin for task %s/%s on node %s, plugin does not exist", interpodaffinity.Name, taskToAdd.Namespace, taskToAdd.Name, nodeInfo.Name)
			}
		}

		return nil
	})

	// 注册 SimulateRemoveTask 函数：用于抢占等场景模拟将某个任务从节点移除后的影响。
	ssn.AddSimulateRemoveTaskFn(pp.Name(), func(ctx context.Context, cycleState fwk.CycleState, taskToSchedule *api.TaskInfo, taskToRemove *api.TaskInfo, nodeInfo *api.NodeInfo) error {
		podInfoToRemove, err := k8sframework.NewPodInfo(taskToRemove.Pod)
		if err != nil {
			return fmt.Errorf("failed to create pod info: %w", err)
		}

		k8sNodeInfo := k8sframework.NewNodeInfo(nodeInfo.Pods()...)
		k8sNodeInfo.SetNode(nodeInfo.Node)

		// 如果启用了 Pod 亲和性，模拟将待移除 Pod 从节点移除，检查亲和性状态变化。
		if pp.enabledPredicates.podAffinityEnable {
			if podAffinityFilter, exist := pp.FilterPlugins[interpodaffinity.Name].(*interpodaffinity.InterPodAffinity); exist {
				isSkipInterPodAffinity := handleSkipPredicatePlugin(cycleState, podAffinityFilter.Name())
				if !isSkipInterPodAffinity {
					status := podAffinityFilter.RemovePod(ctx, cycleState, taskToSchedule.Pod, podInfoToRemove, k8sNodeInfo)
					if !status.IsSuccess() {
						return fmt.Errorf("failed to remove pod from node %s: %w", nodeInfo.Name, status.AsError())
					}
				}
			} else {
				return fmt.Errorf("failed to call %s plugin for task %s/%s on node %s, plugin does not exist", interpodaffinity.Name, taskToRemove.Namespace, taskToRemove.Name, nodeInfo.Name)
			}
		}
		return nil
	})

	// 注册 SimulatePredicate 函数：用于抢占等场景对节点进行模拟谓词过滤。
	ssn.AddSimulatePredicateFn(pp.Name(), func(ctx context.Context, cycleState fwk.CycleState, task *api.TaskInfo, node *api.NodeInfo) error {
		k8sNodeInfo := k8sframework.NewNodeInfo(node.Pods()...)
		k8sNodeInfo.SetNode(node.Node)

		if pp.enabledPredicates.podAffinityEnable {
			isSkipInterPodAffinity := handleSkipPredicatePlugin(cycleState, interpodaffinity.Name)
			if !isSkipInterPodAffinity {
				if podAffinityFilter, exist := pp.FilterPlugins[interpodaffinity.Name]; exist {
					status := podAffinityFilter.Filter(ctx, cycleState, task.Pod, k8sNodeInfo)
					if !status.IsSuccess() {
						return fmt.Errorf("failed to filter pod on node %s: %w", node.Name, status.AsError())
					} else {
						klog.Infof("pod affinity for task %s/%s filter success on node %s", task.Namespace, task.Name, node.Name)
					}
				} else {
					return fmt.Errorf("failed to call %s plugin for task %s/%s on node %s, plugin does not exist", interpodaffinity.Name, task.Namespace, task.Name, node.Name)
				}
			}
		}

		// 设备感知检查：使用 FilterNode（纯读取，无副作用），
		// 以便在拓扑感知抢占的 dry-run 中正确计算模拟释放 victim 后的设备可用性。
		for _, val := range api.RegisteredDevices {
			devObj, ok := node.Others[val]
			if !ok {
				continue
			}
			devs, ok := devObj.(api.Devices)
			if !ok {
				continue
			}
			if api.IsNilDevice(devs) {
				continue
			}
			if !devs.HasDeviceRequest(task.Pod) {
				continue
			}
			if code, msg, err := devs.FilterNode(task.Pod, ""); code != 0 || err != nil {
				klog.Errorf("SimulatePredicate device %s FilterNode failed for task %s/%s on node %s: code=%d msg=%s err=%v",
					val, task.Namespace, task.Name, node.Name, code, msg, err)
				return fmt.Errorf("device %s cannot fit task %s/%s on node %s: %s", val, task.Namespace, task.Name, node.Name, msg)
			}
		}
		return nil
	})
}

// PrePredicate 为给定任务运行所有 PreFilter 插件。
// 这里可以直接使用 state，因为在 session 打开时已经为每个 pending pod 初始化了 cycle state，不会出现 nil state。
func (pp *PredicatesPlugin) PrePredicate(task *api.TaskInfo, state *k8sframework.CycleState, nodeInfoList []fwk.NodeInfo) error {
	// 检查可重启初始化容器特性。
	// 如果 Pod 包含可重启 init 容器但特性未启用，则直接返回不可调度，
	// 避免调度器与旧版本 kubelet（v1.28 之前）在资源计算上出现不一致。
	if !pp.features.EnableSidecarContainers && task.HasRestartableInitContainer {
		return fmt.Errorf("pod has a restartable init container and the SidecarContainers feature is disabled")
	}

	// 按顺序执行所有 PreFilter 插件。
	for _, name := range pp.PreFilterOrder {
		plugin, exists := pp.PreFilterPlugins[name]
		if !exists {
			continue
		}
		_, status := plugin.PreFilter(context.TODO(), state, task.Pod, nodeInfoList)
		if err := handleSkipPrePredicatePlugin(status, state, task, name); err != nil {
			return err
		}
	}

	return nil
}

// InitPlugin 初始化所有 Kubernetes 原生调度插件，并按扩展点分类注册到 PredicatesPlugin 中。
func (pp *PredicatesPlugin) InitPlugin() {
	filterPlugins := map[string]fwk.FilterPlugin{}
	stableFilterPlugins := map[string]fwk.FilterPlugin{} // 可用于缓存的稳定过滤器子集
	prefilterPlugins := map[string]fwk.PreFilterPlugin{}
	reservePlugins := map[string]fwk.ReservePlugin{}
	scorePlugins := map[string]nodescore.BaseScorePlugin{}
	preBindPlugins := map[string]fwk.PreBindPlugin{}
	scoreWeights := map[string]int{} // 每个 score 插件的权重
	var filterOrder []string
	var stableFilterOrder []string
	var preFilterOrder []string
	var reserveOrder []string
	var preBindOrder []string
	var scoreOrder []string

	addFilterPlugin := func(name string, plugin fwk.FilterPlugin) {
		filterPlugins[name] = plugin
		filterOrder = append(filterOrder, name)
	}
	addStableFilterPlugin := func(name string, plugin fwk.FilterPlugin) {
		stableFilterPlugins[name] = plugin
		stableFilterOrder = append(stableFilterOrder, name)
	}
	addPreFilterPlugin := func(name string, plugin fwk.PreFilterPlugin) {
		prefilterPlugins[name] = plugin
		preFilterOrder = append(preFilterOrder, name)
	}
	addReservePlugin := func(name string, plugin fwk.ReservePlugin) {
		reservePlugins[name] = plugin
		reserveOrder = append(reserveOrder, name)
	}
	addPreBindPlugin := func(name string, plugin fwk.PreBindPlugin) {
		preBindPlugins[name] = plugin
		preBindOrder = append(preBindOrder, name)
	}
	addScorePlugin := func(name string, plugin nodescore.BaseScorePlugin, weight int) {
		scorePlugins[name] = plugin
		scoreOrder = append(scoreOrder, name)
		scoreWeights[name] = weight
	}

	// 初始化 Kubernetes 原生插件。
	// TODO: 可以按需添加更多谓词插件，参考 k8s.io/kubernetes/pkg/scheduler/framework/plugins/legacy_registry.go

	// 1. NodeUnschedulable：过滤掉不可调度节点（如节点被标记为 unschedulable）。
	// 属于稳定过滤器，可用于 cache。
	if plugin, err := nodeunschedulable.New(context.TODO(), nil, pp.Handle, pp.features); err == nil {
		nodeUnscheduleFilter := plugin.(*nodeunschedulable.NodeUnschedulable)
		addFilterPlugin(nodeunschedulable.Name, nodeUnscheduleFilter)
		addStableFilterPlugin(nodeunschedulable.Name, nodeUnscheduleFilter)
	} else {
		klog.Errorf("Failed to init %s plugin %v", nodeunschedulable.Name, err)
	}

	// 2. NodeAffinity：根据 Pod 的 nodeAffinity/nodeSelector 过滤节点。
	// 属于稳定过滤器，可用于 cache。
	if pp.enabledPredicates.nodeAffinityEnable {
		nodeAffinityArgs := config.NodeAffinityArgs{
			AddedAffinity: &v1.NodeAffinity{},
		}
		if plugin, err := nodeaffinity.New(context.TODO(), &nodeAffinityArgs, pp.Handle, pp.features); err == nil {
			nodeAffinityFilter := plugin.(*nodeaffinity.NodeAffinity)
			addFilterPlugin(nodeaffinity.Name, nodeAffinityFilter)
			addStableFilterPlugin(nodeaffinity.Name, nodeAffinityFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", nodeaffinity.Name, err)
		}
	}
	// 3. NodePorts：检查节点上请求的端口是否冲突。
	if pp.enabledPredicates.nodePortEnable {
		if plugin, err := nodeports.New(context.TODO(), nil, pp.Handle, pp.features); err == nil {
			nodePortFilter := plugin.(*nodeports.NodePorts)
			addFilterPlugin(nodeports.Name, nodePortFilter)
			addPreFilterPlugin(nodeports.Name, nodePortFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", nodeports.Name, err)
		}
	}
	// 4. TaintToleration：检查 Pod 是否能容忍节点的 taint。
	// 属于稳定过滤器，可用于 cache。
	if pp.enabledPredicates.taintTolerationEnable {
		if plugin, err := tainttoleration.New(context.TODO(), nil, pp.Handle, pp.features); err == nil {
			tolerationFilter := plugin.(*tainttoleration.TaintToleration)
			addFilterPlugin(tainttoleration.Name, tolerationFilter)
			addStableFilterPlugin(tainttoleration.Name, tolerationFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", tainttoleration.Name, err)
		}
	}
	// 5. InterPodAffinity：处理 Pod 间亲和性与反亲和性约束。
	if pp.enabledPredicates.podAffinityEnable {
		plArgs := &config.InterPodAffinityArgs{}
		if plugin, err := interpodaffinity.New(context.TODO(), plArgs, pp.Handle, pp.features); err == nil {
			podAffinityFilter := plugin.(*interpodaffinity.InterPodAffinity)
			addFilterPlugin(interpodaffinity.Name, podAffinityFilter)
			addPreFilterPlugin(interpodaffinity.Name, podAffinityFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", interpodaffinity.Name, err)
		}
	}
	// 6. NodeVolumeLimits：检查节点可挂载的 CSI/本地卷数量是否达到上限。
	if pp.enabledPredicates.nodeVolumeLimitsEnable {
		if plugin, err := nodevolumelimits.NewCSI(context.TODO(), nil, pp.Handle, pp.features); err == nil {
			nodeVolumeLimitsCSIFilter := plugin.(*nodevolumelimits.CSILimits)
			addFilterPlugin(nodevolumelimits.CSIName, nodeVolumeLimitsCSIFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", nodevolumelimits.CSIName, err)
		}
	}
	// 7. VolumeZone：检查 PV 的区域/可用区拓扑是否与节点匹配。
	if pp.enabledPredicates.volumeZoneEnable {
		if plugin, err := volumezone.New(context.TODO(), nil, pp.Handle, pp.features); err == nil {
			volumeZoneFilter := plugin.(*volumezone.VolumeZone)
			addFilterPlugin(volumezone.Name, volumeZoneFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", volumezone.Name, err)
		}
	}
	// 8. PodTopologySpread：根据拓扑约束分散 Pod。
	if pp.enabledPredicates.podTopologySpreadEnable {
		// 暂不支持设置集群级默认拓扑约束。
		ptsArgs := &config.PodTopologySpreadArgs{DefaultingType: config.SystemDefaulting}
		if plugin, err := podtopologyspread.New(context.TODO(), ptsArgs, pp.Handle, pp.features); err == nil {
			podTopologySpreadFilter := plugin.(*podtopologyspread.PodTopologySpread)
			addFilterPlugin(podtopologyspread.Name, podTopologySpreadFilter)
			addPreFilterPlugin(podtopologyspread.Name, podTopologySpreadFilter)
		} else {
			klog.Errorf("Failed to init %s plugin %v", podtopologyspread.Name, err)
		}
	}
	// 9. VolumeBinding：处理 PVC/PV 绑定、动态供给以及存储容量评分。
	if pp.enabledPredicates.volumeBindingEnable {
		vbArgs := defaultVolumeBindingArgs()
		// 当前，我们支持对 VolumeBinding 插件进行一次初始化,
		// 但不支持在修改 VolumeBinding 参数后热加载该插件.
		// 这是因为 VolumeBinding 涉及 AssumeCache，而 AssumeCache 中包含 eventHandler.
		// 如果多次初始化 VolumeBinding，eventHandler 会被不断重复添加，导致内存泄漏.
		// 详情参见：https://github.com/volcano-sh/volcano/issues/2554.
		// 因此，如果用户需要修改 VolumeBinding 参数，需要重启 scheduler.
		volumeBindingPluginOnce.Do(func() {
			setUpVolumeBindingArgs(vbArgs, pp.pluginArguments)

			plugin, err := vbcap.New(context.TODO(), vbArgs.VolumeBindingArgs, pp.Handle, pp.features)
			if err != nil {
				klog.Fatalf("failed to create volume binding plugin with args %+v: %v", vbArgs, err)
			}
			volumeBindingPluginInstance = plugin.(*vbcap.VolumeBinding)
		})

		addFilterPlugin(vbcap.Name, volumeBindingPluginInstance)
		addPreFilterPlugin(vbcap.Name, volumeBindingPluginInstance)
		addReservePlugin(vbcap.Name, volumeBindingPluginInstance)
		addPreBindPlugin(vbcap.Name, volumeBindingPluginInstance)
		addScorePlugin(vbcap.Name, volumeBindingPluginInstance, vbArgs.Weight)
	}
	// 10. DRA（Dynamic Resource Allocation）：处理动态资源分配（ResourceClaim）。
	if pp.enabledPredicates.dynamicResourceAllocationEnable {
		draArgs := defaultDynamicResourcesArgs()
		setUpDynamicResourcesArgs(draArgs, pp.pluginArguments)
		plugin, err := dynamicresources.New(context.TODO(), draArgs.DynamicResourcesArgs, pp.Handle, pp.features)
		if err != nil {
			klog.Fatalf("failed to create dra plugin with err: %v", err)
		}
		dynamicResourceAllocationPlugin := plugin.(*dynamicresources.DynamicResources)
		addFilterPlugin(dynamicresources.Name, dynamicResourceAllocationPlugin)
		addPreFilterPlugin(dynamicresources.Name, dynamicResourceAllocationPlugin)
		addReservePlugin(dynamicresources.Name, dynamicResourceAllocationPlugin)
		addPreBindPlugin(dynamicresources.Name, dynamicResourceAllocationPlugin)
	}

	pp.FilterPlugins = filterPlugins
	pp.StableFilterPlugins = stableFilterPlugins
	pp.PreFilterPlugins = prefilterPlugins
	pp.ReservePlugins = reservePlugins
	pp.PreBindPlugins = preBindPlugins
	pp.ScorePlugins = scorePlugins
	pp.ScoreWeights = scoreWeights
	pp.FilterOrder = filterOrder
	pp.StableFilterOrder = stableFilterOrder
	pp.PreFilterOrder = preFilterOrder
	pp.ReserveOrder = reserveOrder
	pp.PreBindOrder = preBindOrder
	pp.ScoreOrder = scoreOrder
}

// Predicate 为给定任务和节点运行所有 Filter 插件，判断该节点是否适合运行该任务。
func (pp *PredicatesPlugin) Predicate(task *api.TaskInfo, node *api.NodeInfo, state *k8sframework.CycleState) error {
	predicateStatus := make([]*api.Status, 0)
	// 从节点快照中获取对应节点的 NodeInfo。
	nodeInfo, err := pp.Handle.SnapshotSharedLister().NodeInfos().Get(node.Name)
	if err != nil {
		klog.V(4).Infof("NodeInfo predicates Task <%s/%s> on Node <%s> failed, node info not found",
			task.Namespace, task.Name, node.Name)
		nodeInfoStatus := &api.Status{
			Code:   api.Error,
			Reason: "node info not found",
			Plugin: pp.Name(),
		}
		predicateStatus = append(predicateStatus, nodeInfoStatus)
		return api.NewFitErrWithStatus(task, node, predicateStatus...)
	}

	// Volcano 自定义节点最大任务数检查：若节点上 Pod 数已达上限，直接标记不可调度。
	if node.Allocatable.MaxTaskNum <= len(nodeInfo.GetPods()) {
		klog.V(4).Infof("NodePodNumber predicates Task <%s/%s> on Node <%s> failed, allocatable <%d>, existed <%d>",
			task.Namespace, task.Name, node.Name, node.Allocatable.MaxTaskNum, len(nodeInfo.GetPods()))
		podsNumStatus := &api.Status{
			Code:   api.Unschedulable,
			Reason: api.NodePodNumberExceeded,
			Plugin: pp.Name(),
		}
		predicateStatus = append(predicateStatus, podsNumStatus)
	}

	// predicateByStablefilter 执行所有稳定的 Filter 插件，结果可用于缓存。
	predicateByStablefilter := func(nodeInfo fwk.NodeInfo) ([]*api.Status, bool, error) {
		predicateStatus := make([]*api.Status, 0)

		for _, name := range pp.StableFilterOrder {
			plugin, exists := pp.StableFilterPlugins[name]
			if !exists {
				continue
			}
			status := plugin.Filter(context.TODO(), state, task.Pod, nodeInfo)
			filterStatus := api.ConvertPredicateStatus(status)
			if filterStatus.Code != api.Success {
				predicateStatus = append(predicateStatus, filterStatus)
				if util.ShouldAbort(filterStatus) {
					return predicateStatus, false, fmt.Errorf("plugin %s predicates failed %s", name, status.Message())
				}
			}
		}

		return predicateStatus, true, nil
	}

	// 检查 predicate cache。如果启用，优先从缓存查询；缓存未命中时执行稳定过滤器并更新缓存。
	var fit bool
	predicateCacheStatus := make([]*api.Status, 0)
	if pp.enabledPredicates.cacheEnable {
		fit, err = pp.PredicateCache.PredicateWithCache(node.Name, task.Pod)
		if err != nil {
			predicateCacheStatus, fit, _ = predicateByStablefilter(nodeInfo)
			pp.PredicateCache.UpdateCache(node.Name, task.Pod, fit)
		} else {
			if !fit {
				err = fmt.Errorf("plugin equivalence cache predicates failed")
				predicateCacheStatus = append(predicateCacheStatus, &api.Status{
					Code: api.Error, Reason: err.Error(), Plugin: CachePredicate,
				})
			}
		}
	} else {
		predicateCacheStatus, fit, _ = predicateByStablefilter(nodeInfo)
	}

	predicateStatus = append(predicateStatus, predicateCacheStatus...)
	if !fit {
		return api.NewFitErrWithStatus(task, node, predicateStatus...)
	}

	// 执行剩余的 Filter 插件（排除已在 predicateByStablefilter 中处理过的稳定过滤器）。
	for _, name := range pp.FilterOrder {
		plugin, exists := pp.FilterPlugins[name]
		if !exists {
			continue
		}
		// 跳过已在稳定过滤器中执行过的插件。
		if _, isStable := pp.StableFilterPlugins[name]; isStable {
			continue
		}

		// 如果 PreFilter 阶段指示跳过该插件，则跳过。
		if handleSkipPredicatePlugin(state, name) {
			continue
		}

		status := plugin.Filter(context.TODO(), state, task.Pod, nodeInfo)
		filterStatus := api.ConvertPredicateStatus(status)
		if filterStatus.Code != api.Success {
			predicateStatus = append(predicateStatus, filterStatus)
			if util.ShouldAbort(filterStatus) {
				return api.NewFitErrWithStatus(task, node, predicateStatus...)
			}
		}
	}

	if len(predicateStatus) > 0 {
		return api.NewFitErrWithStatus(task, node, predicateStatus...)
	}

	return nil
}

// BatchNodeOrder 为给定任务和一批节点运行所有 Score 插件，返回各节点得分。
func (pp *PredicatesPlugin) BatchNodeOrder(task *api.TaskInfo, nodes []fwk.NodeInfo, state *k8sframework.CycleState) (map[string]float64, error) {
	nodeScores := make(map[string]float64, len(nodes))

	// 依次执行所有 Score 插件并累加分数。
	for _, name := range pp.ScoreOrder {
		plugin, exists := pp.ScorePlugins[name]
		if !exists {
			continue
		}
		// 获取归一化器。大部分插件不需要归一化，默认使用 EmptyNormalizer。
		normalizer := &nodescore.EmptyNormalizer{}

		// 从 ScoreWeights 中获取权重，未设置则默认为 1。
		weight := 1
		if w, exists := pp.ScoreWeights[name]; exists {
			weight = w
		}

		// 调用辅助函数计算该插件对各节点的加权得分。
		pluginScores, err := nodescore.CalculatePluginScore(name, plugin, normalizer, state, task.Pod, nodes, weight)
		if err != nil {
			return nil, err
		}

		// 将该插件的得分累加到各节点总分中。
		for _, node := range nodes {
			nodeName := node.Node().Name
			nodeScores[nodeName] += pluginScores[nodeName]
		}
	}

	klog.V(4).Infof("Batch Total Score for task %s/%s is: %v", task.Namespace, task.Name, nodeScores)
	return nodeScores, nil
}

// runReservePlugins 按顺序执行所有 Reserve 插件。
// 主要用于在 Pod 被真实绑定前，在内存中假设占用相关资源（如 VolumeBinding、DRA）。
func (pp *PredicatesPlugin) runReservePlugins(ssn *framework.Session, event *framework.Event) {
	state := ssn.GetCycleState(event.Task.UID)

	for _, name := range pp.ReserveOrder {
		plugin, exists := pp.ReservePlugins[name]
		if !exists {
			continue
		}
		status := plugin.Reserve(context.TODO(), state, event.Task.Pod, event.Task.Pod.Spec.NodeName)
		if !status.IsSuccess() {
			klog.Errorf("Reserve plugin %s failed for pod %s/%s: %v", name, event.Task.Namespace, event.Task.Name, status.AsError())
			// 将错误记录到 event.Err，由上层决定是否回滚。
			event.Err = status.AsError()
			return
		}
	}
}

// runUnReservePlugins 在任务释放时执行所有 Unreserve 插件，回滚 Reserve 阶段的假设占用。
func (pp *PredicatesPlugin) runUnReservePlugins(ssn *framework.Session, event *framework.Event) {
	state := ssn.GetCycleState(event.Task.UID)
	pp.runUnreservePluginsWithState(context.TODO(), state, event.Task.Pod, event.Task.Pod.Spec.NodeName)
}

// runUnreservePluginsWithState 使用给定的 CycleState 按 Reserve 的逆序执行 Unreserve。
// 逆序执行是为了保证资源回滚与资源占用的顺序对称。
func (pp *PredicatesPlugin) runUnreservePluginsWithState(ctx context.Context, state *k8sframework.CycleState, pod *v1.Pod, nodeName string) {
	for i := len(pp.ReserveOrder) - 1; i >= 0; i-- {
		plugin, exists := pp.ReservePlugins[pp.ReserveOrder[i]]
		if !exists {
			continue
		}
		plugin.Unreserve(ctx, state, pod, nodeName)
	}
}

// needsPreBind 判断 Pod 是否需要在 bind context 中设置扩展信息以执行 PreBind 等额外扩展点。
// 目前，如果 Pod 具有以下任一资源，就认为需要传递扩展信息：
//  1. 包含 PVC 或 Ephemeral 卷（用于 VolumeBinding 插件）
//  2. 包含 ResourceClaim（用于 DRA 插件）
//
// 否则不需要在 bind context 中设置扩展信息。
func (pp *PredicatesPlugin) needsPreBind(task *api.TaskInfo) bool {
	// 检查 Pod 是否包含需要绑定的卷（仅在 VolumeBinding 启用时）。
	if pp.enabledPredicates.volumeBindingEnable {
		for _, vol := range task.Pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil || vol.Ephemeral != nil {
				return true
			}
		}
	}

	// 检查 Pod 是否包含 resource claim（仅在 DRA 启用时）。
	if pp.enabledPredicates.dynamicResourceAllocationEnable {
		if len(task.Pod.Spec.ResourceClaims) > 0 {
			return true
		}
	}

	return false
}

// PreBind 在绑定前执行所有 PreBind 插件，完成诸如 PV/PVC 真实绑定、DRA 资源预留等操作。
func (pp *PredicatesPlugin) PreBind(ctx context.Context, bindCtx *cache.BindContext) error {
	if !pp.needsPreBind(bindCtx.TaskInfo) {
		return nil
	}

	// 从 bind context 扩展信息中获取 CycleState。
	state := bindCtx.Extensions[pp.Name()].(*BindContextExtension).State

	// 按顺序执行所有 PreBind 插件。
	for _, name := range pp.PreBindOrder {
		plugin, exists := pp.PreBindPlugins[name]
		if !exists {
			continue
		}
		status := plugin.PreBind(ctx, state, bindCtx.TaskInfo.Pod, bindCtx.TaskInfo.Pod.Spec.NodeName)
		if !status.IsSuccess() {
			klog.Errorf("PreBind plugin %s failed for pod %s/%s: %v", name, bindCtx.TaskInfo.Namespace, bindCtx.TaskInfo.Name, status.AsError())
			return status.AsError()
		}
	}

	return nil
}

// PreBindRollBack 在 PreBind 失败时回滚 Reserve 阶段的假设占用。
func (pp *PredicatesPlugin) PreBindRollBack(ctx context.Context, bindCtx *cache.BindContext) {
	if !pp.needsPreBind(bindCtx.TaskInfo) {
		return
	}

	state := bindCtx.Extensions[pp.Name()].(*BindContextExtension).State
	pp.runUnreservePluginsWithState(ctx, state, bindCtx.TaskInfo.Pod, bindCtx.TaskInfo.Pod.Spec.NodeName)
}

// SetupBindContextExtension 在 bind context 中设置 predicates 插件的扩展信息，
// 以便 PreBind/PreBindRollBack 阶段能获取到 CycleState。
func (pp *PredicatesPlugin) SetupBindContextExtension(state *k8sframework.CycleState, bindCtx *cache.BindContext) {
	if !pp.needsPreBind(bindCtx.TaskInfo) {
		return
	}

	bindCtx.Extensions[pp.Name()] = &BindContextExtension{State: state}
}

// handleSkipPredicatePlugin 判断指定 Filter 插件是否应被跳过。
func handleSkipPredicatePlugin(state fwk.CycleState, pluginName string) bool {
	return state.GetSkipFilterPlugins().Has(pluginName)
}

// handleSkipPrePredicatePlugin 处理 PreFilter 插件返回的状态：
//   - Skip：将该插件加入跳过集合，后续 Filter 阶段不再执行；
//   - 失败：返回错误。
func handleSkipPrePredicatePlugin(status *fwk.Status, state *k8sframework.CycleState, task *api.TaskInfo, pluginName string) error {
	if state.GetSkipFilterPlugins() == nil {
		state.SetSkipFilterPlugins(sets.New[string]())
	}

	if status.IsSkip() {
		state.GetSkipFilterPlugins().Insert(pluginName)
		klog.V(5).Infof("The predicate of plugin %s will skip execution for pod <%s/%s>, because the status returned by pre-predicate is skip",
			pluginName, task.Namespace, task.Name)
	} else if !status.IsSuccess() {
		return fmt.Errorf("plugin %s pre-predicates failed %s", pluginName, status.Message())
	}

	return nil
}

// OnSessionClose 在调度会话关闭时调用。当前 predicates 插件无需额外清理。
func (pp *PredicatesPlugin) OnSessionClose(ssn *framework.Session) {}

// ResetVolumeBindingPluginForTest 仅在测试中使用，用于重置 VolumeBinding 插件的全局单例。
//
// 由于 volumeBindingPluginInstance 是使用 sync.Once 初始化的全局变量，正常流程只会初始化一次。
// 但在测试环境中，多个测试用例可能使用不同的 PVC/PV 配置，需要重置实例以保证每个测试都有干净状态。
// 如果不重置，后续测试会使用前面测试初始化的实例，可能导致 PVC/PV 信息不匹配而失败。
//
// 警告：此函数严禁在生产代码中使用。
func ResetVolumeBindingPluginForTest() {
	volumeBindingPluginInstance = nil
	volumeBindingPluginOnce = sync.Once{}
}
