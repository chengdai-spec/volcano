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

package networktopologyaware

import (
	"fmt"
	"math"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/utils/set"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	// PluginName 为插件名称，用于在调度器配置中引用。
	PluginName            = "network-topology-aware"
	FullScore             = 1.0
	ZeroScore             = 0.0
	NetworkTopologyWeight = "weight"
	// HyperNodeBinPackCPU 是 CPU 资源 binpacking 权重的配置键。
	HyperNodeBinPackCPU = "hypernode.binpack.cpu"
	// HyperNodeBinPackMemory 是内存资源 binpacking 权重的配置键。
	HyperNodeBinPackMemory = "hypernode.binpack.memory"
	// HyperNodeBinPackResources 是额外资源（如 GPU）列表的配置键。
	HyperNodeBinPackResources = "hypernode.binpack.resources"
	// HyperNodeBinPackResourcesPrefix 是额外资源权重配置键的前缀。
	HyperNodeBinPackResourcesPrefix = HyperNodeBinPackResources + "."
	// HyperNodeBinPackNormalPodEnable 控制是否对没有网络拓扑需求的普通 Pod 启用 HyperNode 级 binpacking。
	HyperNodeBinPackNormalPodEnable = "hypernode.binpack.normal-pod.enable"
	// HyperNodeBinPackNormalPodFading 是普通 Pod binpacking 中各 tier 权重的衰减系数配置键。
	HyperNodeBinPackNormalPodFading = "hypernode.binpack.normal-pod.fading"
	// HyperNodeGradientEvictMaxHyperNodes 是驱逐场景下梯度搜索返回 HyperNode 数量上限的配置键。
	HyperNodeGradientEvictMaxHyperNodes = "hypernode.gradient.evict.max-hypernodes"
)

const (
	// DefaultWeight 是插件默认权重及各资源默认权重。
	DefaultWeight = 1
	// DefaultNormalPodEnable 默认开启普通 Pod 的 HyperNode 级 binpacking。
	DefaultNormalPodEnable = true
	// DefaultNormalPodFading 是普通 Pod tier 权重衰减系数的默认值。
	DefaultNormalPodFading = 0.8
	// DefaultEvictMaxHyperNodes 是驱逐场景默认返回的最大 HyperNode 数量。
	DefaultEvictMaxHyperNodes = 8
)

// networkTopologyAwarePlugin 是网络拓扑感知调度插件的核心结构。
//
// 该插件基于 HyperNode 层次结构做两件事：
//  1. 为网络拓扑感知的作业/子作业选择拓扑近邻的 HyperNode/节点；
//  2. 为普通 Pod 做 HyperNode 级别的 binpacking，提升资源利用率。
type networkTopologyAwarePlugin struct {
	// pluginArguments 是调度器配置中传给该插件的参数。
	pluginArguments framework.Arguments
	// weight 是各资源 binpacking 的权重配置。
	weight *priorityWeight
	// normalPodConfig 是普通 Pod（无网络拓扑需求）的 binpacking 配置。
	*normalPodConfig
	// hyperNodesTier 记录当前集群 HyperNode 的最大/最小 tier。
	*hyperNodesTier
	// maxHyperNodesForEviction 是驱逐场景下返回 HyperNode 的最大数量。
	maxHyperNodesForEviction int
	// hyperNodeResourceCache 缓存每个 HyperNode 的资源状态，避免重复计算。
	// key 为 HyperNode 名称，value 为该 HyperNode 的资源汇总。
	hyperNodeResourceCache map[string]*resourceStatus
}

// priorityWeight 存储 binpacking 各维度的权重。
type priorityWeight struct {
	GlobalWeight                 int                         // 插件全局权重
	HyperNodeBinPackingCPU       int                         // CPU binpacking 权重
	HyperNodeBinPackingMemory    int                         // 内存 binpacking 权重
	HyperNodeBinPackingResources map[corev1.ResourceName]int // 其他扩展资源的 binpacking 权重
}

// normalPodConfig 存储普通 Pod（无网络拓扑需求）的 binpacking 配置。
type normalPodConfig struct {
	hyperNodeBinPackingEnable bool    // 是否启用
	hyperNodeBinPackingFading float64 // tier 权重衰减系数
}

// hyperNodesTier 记录集群中 HyperNode 的 tier 范围。
type hyperNodesTier struct {
	maxTier int // 最大 tier（数值最大，通常是最底层如 node）
	minTier int // 最小 tier（数值最小，通常是最顶层如 root）
}

// resourceStatus 汇总一个 HyperNode 的资源状态。
type resourceStatus struct {
	allocatable *api.Resource // 可分配资源总量
	used        *api.Resource // 已使用资源量
	idle        *api.Resource // 当前空闲资源量
	futureIdle  *api.Resource // 考虑待调度任务后的未来空闲资源量
}

// init 初始化 HyperNode 的 tier 范围。
//
// 参数 hyperNodesSetByTier 是已排序的 tier 列表（升序）。
func (h *hyperNodesTier) init(hyperNodesSetByTier []int) {
	if len(hyperNodesSetByTier) == 0 {
		return
	}
	h.minTier = hyperNodesSetByTier[0]
	h.maxTier = hyperNodesSetByTier[len(hyperNodesSetByTier)-1]
}

// initHyperNodeResourceCache 初始化每个 HyperNode 的资源状态缓存。
//
// 对 session 中的每个 HyperNode，汇总其包含的所有真实节点的：
// 可分配资源、已使用资源、空闲资源、未来空闲资源。
func (nta *networkTopologyAwarePlugin) initHyperNodeResourceCache(ssn *framework.Session) {
	if nta.hyperNodeResourceCache == nil {
		nta.hyperNodeResourceCache = make(map[string]*resourceStatus)
	}

	for hyperNode := range ssn.HyperNodes {
		nta.hyperNodeResourceCache[hyperNode] = &resourceStatus{
			allocatable: api.EmptyResource(),
			used:        api.EmptyResource(),
			idle:        api.EmptyResource(),
			futureIdle:  api.EmptyResource(),
		}
		for node := range ssn.RealNodesSet[hyperNode] {
			nta.hyperNodeResourceCache[hyperNode].allocatable.Add(ssn.Nodes[node].Allocatable)
			nta.hyperNodeResourceCache[hyperNode].used.Add(ssn.Nodes[node].Used)
			nta.hyperNodeResourceCache[hyperNode].idle.Add(ssn.Nodes[node].Idle)
			nta.hyperNodeResourceCache[hyperNode].futureIdle.Add(ssn.Nodes[node].FutureIdle())
		}
	}
}

/*
   network-topology-aware 插件的参数配置示例：

   tiers:
   - plugins:
     - name: network-topology-aware
       arguments:
         weight: 10                          # 插件全局权重
         hypernode.binpack.cpu: 5            # CPU binpacking 权重
         hypernode.binpack.memory: 1         # 内存 binpacking 权重
         hypernode.binpack.resources: nvidia.com/gpu, example.com/foo  # 扩展资源列表
         hypernode.binpack.resources.nvidia.com/gpu: 2   # GPU 权重
         hypernode.binpack.resources.example.com/foo: 3   # 自定义资源权重
         hypernode.binpack.normal-pod.enable: true       # 是否对普通 Pod 启用 binpacking
         hypernode.binpack.normal-pod.fading: 0.8        # 普通 Pod tier 权重衰减系数
         hypernode.gradient.evict.max-hypernodes: 8      # 驱逐场景返回 HyperNode 数量上限
*/

// New 创建并返回网络拓扑感知插件实例。
//
// 解析调度器配置中的参数，初始化权重、普通 Pod 配置、tier 范围、
// 驱逐 HyperNode 数量上限以及资源缓存。
func New(arguments framework.Arguments) framework.Plugin {
	plugin := networkTopologyAwarePlugin{
		pluginArguments:          arguments,
		weight:                   getPriorityWeight(arguments),
		normalPodConfig:          getNormalPodConfig(arguments),
		hyperNodesTier:           &hyperNodesTier{},
		maxHyperNodesForEviction: getMaxHyperNodesForEviction(arguments),
		hyperNodeResourceCache:   make(map[string]*resourceStatus),
	}
	klog.V(5).InfoS("successfully built plugin", "name", PluginName, "arguments", plugin.String())
	return &plugin
}

// getMaxHyperNodesForEviction 从配置中解析驱逐场景下返回的最大 HyperNode 数量。
//
// 若配置值小于等于 0，则使用默认值。
func getMaxHyperNodesForEviction(args framework.Arguments) int {
	maxHyperNodes := DefaultEvictMaxHyperNodes
	args.GetInt(&maxHyperNodes, HyperNodeGradientEvictMaxHyperNodes)
	if maxHyperNodes <= 0 {
		maxHyperNodes = DefaultEvictMaxHyperNodes
	}
	return maxHyperNodes
}

// Name 返回插件名称，实现 framework.Plugin 接口。
func (nta *networkTopologyAwarePlugin) Name() string {
	return PluginName
}

// getPriorityWeight 从插件参数中解析各资源的 binpacking 权重。
//
// 解析项包括：全局权重、CPU 权重、内存权重、以及用户自定义扩展资源权重。
// 任何小于 0 的权重都会被重置为默认值。
func getPriorityWeight(args framework.Arguments) *priorityWeight {
	weight := priorityWeight{
		GlobalWeight:                 DefaultWeight,
		HyperNodeBinPackingCPU:       DefaultWeight,
		HyperNodeBinPackingMemory:    DefaultWeight,
		HyperNodeBinPackingResources: make(map[corev1.ResourceName]int),
	}

	// 解析全局权重。
	args.GetInt(&weight.GlobalWeight, NetworkTopologyWeight)
	if weight.GlobalWeight < 0 {
		weight.GlobalWeight = DefaultWeight
	}
	// 解析 CPU 权重。
	args.GetInt(&weight.HyperNodeBinPackingCPU, HyperNodeBinPackCPU)
	if weight.HyperNodeBinPackingCPU < 0 {
		weight.HyperNodeBinPackingCPU = DefaultWeight
	}
	// 解析内存权重。
	args.GetInt(&weight.HyperNodeBinPackingMemory, HyperNodeBinPackMemory)
	if weight.HyperNodeBinPackingMemory < 0 {
		weight.HyperNodeBinPackingMemory = DefaultWeight
	}

	// 解析扩展资源列表及对应权重。
	resourcesStr, ok := args[HyperNodeBinPackResources].(string)
	if !ok {
		resourcesStr = ""
	}

	resources := strings.Split(resourcesStr, ",")
	for _, resource := range resources {
		resource = strings.TrimSpace(resource)
		if resource == "" {
			continue
		}

		// 扩展资源权重键格式：hypernode.binpack.resources.[ResourceName]
		resourceKey := HyperNodeBinPackResourcesPrefix + resource
		resourceWeight := DefaultWeight
		args.GetInt(&resourceWeight, resourceKey)
		if resourceWeight < 0 {
			resourceWeight = DefaultWeight
		}
		weight.HyperNodeBinPackingResources[corev1.ResourceName(resource)] = resourceWeight
	}

	return &weight
}

// getNormalPodConfig 从插件参数中解析普通 Pod 的 binpacking 配置。
//
// fading 为 0 时，表示只有 tier 1 的 HyperNode 影响 Pod 的 binpacking 分数。
func getNormalPodConfig(args framework.Arguments) *normalPodConfig {
	config := normalPodConfig{
		hyperNodeBinPackingEnable: DefaultNormalPodEnable,
		hyperNodeBinPackingFading: DefaultNormalPodFading,
	}
	args.GetBool(&config.hyperNodeBinPackingEnable, HyperNodeBinPackNormalPodEnable)
	args.GetFloat64(&config.hyperNodeBinPackingFading, HyperNodeBinPackNormalPodFading)
	// fading 不能为负数；为 0 是合法值，表示只考虑 tier 1。
	if config.hyperNodeBinPackingFading < 0 {
		config.hyperNodeBinPackingFading = DefaultNormalPodFading
	}
	return &config
}

// getBinPackWeight 返回指定资源名称对应的 binpacking 权重及是否存在。
func (w *priorityWeight) getBinPackWeight(name corev1.ResourceName) (int, bool) {
	switch name {
	case corev1.ResourceCPU:
		return w.HyperNodeBinPackingCPU, true
	case corev1.ResourceMemory:
		return w.HyperNodeBinPackingMemory, true
	default:
		weight, ok := w.HyperNodeBinPackingResources[name]
		return weight, ok
	}
}

// String 返回插件当前配置的可读字符串，用于日志输出。
func (nta *networkTopologyAwarePlugin) String() string {
	length := 5
	if extendLength := len(nta.weight.HyperNodeBinPackingResources); extendLength == 0 {
		length++
	} else {
		length += extendLength
	}
	msg := make([]string, 0, length)
	msg = append(msg,
		fmt.Sprintf("%s[%d]", NetworkTopologyWeight, nta.weight.GlobalWeight),
		fmt.Sprintf("%s[%d]", corev1.ResourceCPU, nta.weight.HyperNodeBinPackingCPU),
		fmt.Sprintf("%s[%d]", corev1.ResourceMemory, nta.weight.HyperNodeBinPackingMemory),
	)

	if len(nta.weight.HyperNodeBinPackingResources) == 0 {
		msg = append(msg, "no extend resources")
	} else {
		for name, weight := range nta.weight.HyperNodeBinPackingResources {
			msg = append(msg, fmt.Sprintf("%s[%d]", name, weight))
		}
	}
	msg = append(msg, fmt.Sprintf("%s[%t]", HyperNodeBinPackNormalPodEnable, nta.normalPodConfig.hyperNodeBinPackingEnable),
		fmt.Sprintf("%s[%g]", HyperNodeBinPackNormalPodFading, nta.normalPodConfig.hyperNodeBinPackingFading))

	return strings.Join(msg, ", ")
}

// OnSessionOpen 在每个调度会话开始时调用，是插件的初始化入口。
//
// 主要工作：
//  1. 初始化 HyperNode 的 tier 范围；
//  2. 初始化 HyperNode 资源缓存；
//  3. 注册各类调度回调函数：
//     - HyperNodeOrderFn：子作业的 HyperNode 排序（binpacking）；
//     - BatchNodeOrderFn：任务的节点排序（拓扑感知 / 普通 Pod）；
//     - HyperNodeGradientForJobFn：作业的 HyperNode 梯度搜索；
//     - HyperNodeGradientForSubJobFn：子作业的 HyperNode 梯度搜索；
//  4. 注册事件处理器，在任务分配/释放时更新 HyperNode 资源缓存。
func (nta *networkTopologyAwarePlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter networkTopologyAwarePlugin plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving networkTopologyAware plugin ...")
	}()
	nta.hyperNodesTier.init(ssn.HyperNodesTiers)
	nta.initHyperNodeResourceCache(ssn)

	// 注册子作业的 HyperNode 排序函数：选择 binpacking 得分最高的 HyperNode。
	ssn.AddHyperNodeOrderFn(nta.Name(), func(subJob *api.SubJobInfo, hyperNodes map[string][]*api.NodeInfo) (map[string]float64, error) {
		return nta.HyperNodeOrderFn(ssn, subJob, hyperNodes)
	})

	// 注册任务的批量节点排序函数：根据任务是否有网络拓扑需求走不同逻辑。
	ssn.AddBatchNodeOrderFn(nta.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		return nta.batchNodeOrderFn(ssn, task, nodes)
	})

	// 注册作业的 HyperNode 梯度搜索函数：硬拓扑模式下按层级返回候选 HyperNode。
	ssn.AddHyperNodeGradientForJobFn(nta.Name(), func(job *api.JobInfo, hyperNode *api.HyperNodeInfo, purpose api.SearchPurpose) [][]*api.HyperNodeInfo {
		if hardMode, highestAllowedTier := job.IsHardTopologyMode(); hardMode {
			jobMinResource := job.GetMinResources()
			result, err := nta.hyperNodeGradientFn(ssn, hyperNode, highestAllowedTier, job.AllocatedHyperNode, jobMinResource, purpose)
			if err != nil {
				klog.ErrorS(err, "build hyperNode gradient fail", "job", job.UID, "hyperNode", hyperNode.Name,
					"highestAllowedTier", highestAllowedTier, "allocatedHyperNode", job.AllocatedHyperNode)
				return nil
			}
			if purpose != api.PurposeEvict {
				return result
			}
			return nta.reverseAndCapEvictionGradients(result)
		}
		// 非硬拓扑模式下，只返回当前可用的 HyperNode 本身。
		return [][]*api.HyperNodeInfo{{hyperNode}}
	})

	// 注册子作业的 HyperNode 梯度搜索函数：逻辑与作业版本相同。
	ssn.AddHyperNodeGradientForSubJobFn(nta.Name(), func(subJob *api.SubJobInfo, hyperNode *api.HyperNodeInfo, purpose api.SearchPurpose) [][]*api.HyperNodeInfo {
		if hardMode, highestAllowedTier := subJob.IsHardTopologyMode(); hardMode {
			subJobMinResource := subJob.GetMinResources()
			result, err := nta.hyperNodeGradientFn(ssn, hyperNode, highestAllowedTier, subJob.AllocatedHyperNode, subJobMinResource, purpose)
			if err != nil {
				klog.ErrorS(err, "build hyperNode gradient fail", "subJob", subJob.UID, "hyperNode", hyperNode.Name,
					"highestAllowedTier", highestAllowedTier, "allocatedHyperNode", subJob.AllocatedHyperNode)
				return nil
			}
			if purpose != api.PurposeEvict {
				return result
			}
			return nta.reverseAndCapEvictionGradients(result)
		}
		return [][]*api.HyperNodeInfo{{hyperNode}}
	})

	// 注册资源分配/释放事件处理器，维护 HyperNode 资源缓存的 used 字段。
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			task := event.Task
			node := task.NodeName
			for hyperNode := range ssn.HyperNodes {
				if ssn.RealNodesSet[hyperNode].Has(node) {
					status, ok := nta.hyperNodeResourceCache[hyperNode]
					if !ok {
						klog.Warningf("plugin %s failed to find the resource status cache of hyperNode %s, which should not happen", PluginName, hyperNode)
						continue
					}
					status.used.Add(task.Resreq)
				}
			}
		},
		DeallocateFunc: func(event *framework.Event) {
			task := event.Task
			node := task.NodeName
			for hyperNode := range ssn.HyperNodes {
				if ssn.RealNodesSet[hyperNode].Has(node) {
					status, ok := nta.hyperNodeResourceCache[hyperNode]
					if !ok {
						klog.Warningf("plugin %s failed to find the resource status cache of hyperNode %s, which should not happen", PluginName, hyperNode)
						continue
					}
					status.used.Sub(task.Resreq)
				}
			}
		},
	})
}

// HyperNodeOrderFn 为子作业计算每个候选 HyperNode 的 binpacking 得分并排序。
//
// 算法流程：
//  1. 汇总子作业所有任务对各资源的总需求；
//  2. 对每个候选 HyperNode 计算加权 binpacking 分数；
//  3. 若最高分并列，则加上"已调度任务数"这一辅助分，优先选择任务更集中的 HyperNode；
//  4. 最终按插件全局权重缩放到 framework 分数范围。
func (nta *networkTopologyAwarePlugin) HyperNodeOrderFn(ssn *framework.Session, subJob *api.SubJobInfo, hyperNodes map[string][]*api.NodeInfo) (map[string]float64, error) {
	hyperNodeScores := nta.getSubJobHyperNodeBinPackingScore(subJob, hyperNodes)

	// 按分数分组，找到最高分。
	scoreToHyperNodes := map[float64][]string{}
	var maxScore float64 = -1
	for hyperNode, score := range hyperNodeScores {
		if score >= maxScore {
			maxScore = score
			scoreToHyperNodes[maxScore] = append(scoreToHyperNodes[maxScore], hyperNode)
		}
	}

	// 若最高分的 HyperNode 有多个，用任务集中度进一步区分。
	if len(scoreToHyperNodes[maxScore]) > 1 {
		candidateHyperNodes := scoreToHyperNodes[maxScore]
		for _, hyperNode := range candidateHyperNodes {
			taskNumScore := nta.scoreWithTaskNum(hyperNode, subJob.Tasks, ssn.RealNodesList)
			hyperNodeScores[hyperNode] += taskNumScore
		}
	}

	hyperNodeScores = nta.scaleFinalScore(hyperNodeScores)
	klog.V(4).Infof("networkTopologyAware hyperNode score is: %v", hyperNodeScores)
	return hyperNodeScores, nil
}

// getSubJobHyperNodeBinPackingScore 计算子作业在每个候选 HyperNode 上的 binpacking 得分。
//
// binpacking 核心思想：尽量把任务填到同一个 HyperNode，使 (已用+本次需求)/可分配 接近 1，
// 从而提高资源利用率，同时避免跨 HyperNode 的网络通信。
//
// 步骤：
//  1. 汇总子作业对所有配置了权重的资源的总需求；
//  2. 对每个 HyperNode，检查资源是否超售（used+request > allocatable），超售则得 0 分；
//  3. 否则计算加权平均的 binpacking 分数：sum(weight * (used+request)/allocatable) / sum(weight)。
func (nta *networkTopologyAwarePlugin) getSubJobHyperNodeBinPackingScore(subJob *api.SubJobInfo, hyperNodes map[string][]*api.NodeInfo) map[string]float64 {
	tasksRequest := make(map[corev1.ResourceName]float64)
	// 当前子作业只能整体调度（minAvailable == taskNum），因此汇总全部任务需求。
	for _, task := range subJob.Tasks {
		for _, resourceName := range task.Resreq.ResourceNames() {
			if _, ok := nta.weight.getBinPackWeight(resourceName); !ok {
				continue
			}
			tasksRequest[resourceName] += task.Resreq.Get(resourceName)
		}
	}

	hyperNodeBinPackingScores := make(map[string]float64)
	for hyperNode := range hyperNodes {
		totalScore := 0.0
		totalWeight := 0
		overused := false

		for resourceName, request := range tasksRequest {
			weight, ok := nta.weight.getBinPackWeight(resourceName)
			if !ok {
				continue
			}

			status, ok := nta.hyperNodeResourceCache[hyperNode]
			if !ok {
				klog.Warningf("plugin %s failed to find the resource status cache of hyperNode %s, which should not happen", PluginName, hyperNode)
				continue
			}
			allocatable := status.allocatable.Get(resourceName)
			used := status.used.Get(resourceName)

			// 资源超售，该 HyperNode 不可用。
			if used+request > allocatable {
				klog.V(4).InfoS("cannot binpack the hyperNode", "subJob", subJob.UID, "hyperNode", hyperNode,
					"resource", resourceName, "allocatable", allocatable, "used", used, "request", request)
				overused = true
				break
			}
			// binpacking 分数：填得越满越高。
			score := (used + request) / allocatable
			klog.V(5).InfoS("hyperNode binpacking score calculation", "subJob", subJob.UID, "hyperNode", hyperNode,
				"resource", resourceName, "allocatable", allocatable, "used", used, "request", request)

			totalScore += float64(weight) * score
			totalWeight += weight
		}

		if overused || totalWeight <= 0 {
			hyperNodeBinPackingScores[hyperNode] = ZeroScore
		} else {
			hyperNodeBinPackingScores[hyperNode] = totalScore / float64(totalWeight)
		}
	}
	return hyperNodeBinPackingScores
}

// batchNodeOrderFn 是批量节点排序入口。
//
// 根据任务所属子作业是否有网络拓扑需求，分别走：
//   - 网络感知 Pod：基于 LCA（最近公共祖先）tier 评分；
//   - 普通 Pod：基于 HyperNode 级 binpacking 评分。
//
// 如果任务找不到对应作业或子作业，则降级为普通 Pod 逻辑。
func (nta *networkTopologyAwarePlugin) batchNodeOrderFn(ssn *framework.Session, task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
	var nodeScores map[string]float64
	var err error

	job := ssn.Jobs[task.Job]
	if job == nil {
		klog.Warningf("[network-topology-aware] Skip batch node ordering for task <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
			task.Namespace, task.Name, task.Job)
		return make(map[string]float64), nil
	}

	subJobID, found := job.TaskToSubJob[task.UID]
	if !found {
		klog.V(4).Infof("[network-topology-aware] Skip batch node ordering for task <%s/%s>: task not mapped to any subJob",
			task.Namespace, task.Name)
		return nta.batchNodeOrderFnForNormalPods(ssn, task, nodes)
	}

	subJob, found := job.SubJobs[subJobID]
	if !found || subJob == nil {
		klog.V(4).Infof("[network-topology-aware] Skip batch node ordering for task <%s/%s>: subJob <%s> not found in job",
			task.Namespace, task.Name, subJobID)
		return nta.batchNodeOrderFnForNormalPods(ssn, task, nodes)
	}

	if subJob.WithNetworkTopology() {
		nodeScores, err = nta.batchNodeOrderFnForNetworkAwarePods(ssn, task, subJob, nodes)
	} else {
		nodeScores, err = nta.batchNodeOrderFnForNormalPods(ssn, task, nodes)
	}

	if err != nil {
		return nil, err
	}
	nodeScores = nta.scaleFinalScore(nodeScores)
	klog.V(4).Infof("networkTopologyAware node score is: %v", nodeScores)
	return nodeScores, nil
}

// batchNodeOrderFnForNormalPods 为没有网络拓扑需求的普通 Pod 计算节点分数。
//
// 思路：
//  1. 对每一层 tier 计算权重，tier 越小（越靠近顶层）权重越高，按 fading 系数递减；
//  2. 对每个节点，找到其所属的最高 tier 的 HyperNode，计算该 HyperNode 的 binpacking 分数；
//  3. 如果节点不属于任何 HyperNode，则该 tier 得分为满分（FullScore），表示鼓励不绑定到 HyperNode；
//  4. 最终得分为各 tier 加权平均。
func (nta *networkTopologyAwarePlugin) batchNodeOrderFnForNormalPods(ssn *framework.Session, task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
	nodeScores := make(map[string]float64)

	if !nta.normalPodConfig.hyperNodeBinPackingEnable {
		return nodeScores, nil
	}

	// 计算每个 tier 的权重：tier 1 权重为 fading^0 = 1，tier 2 为 fading^1，依此类推。
	totalTierWeight := 0.0
	tierWeights := make(map[int]float64)
	for tier := nta.hyperNodesTier.minTier; tier <= nta.hyperNodesTier.maxTier; tier++ {
		// 注意：math.Pow(0, 0) = 1
		tierWeight := math.Pow(nta.hyperNodeBinPackingFading, float64(tier-1))
		totalTierWeight += tierWeight
		tierWeights[tier] = tierWeight
	}
	if totalTierWeight <= 0 {
		// 正常情况不会发生，因为至少有一个 tier 且权重为 1。
		klog.Warningf("the total tier weight of plugin %s should be greater than zero, but got %g", PluginName, totalTierWeight)
		return nodeScores, nil
	}

	for _, node := range nodes {
		totalScore := 0.0
		for tier := nta.hyperNodesTier.minTier; tier <= nta.hyperNodesTier.maxTier; tier++ {
			// 若该 tier 没有包含该节点的 HyperNode，则该 tier 得满分，
			// 因为我们倾向于把未归属任何 HyperNode 的节点也纳入调度考虑。
			tierScore := FullScore
			for hyperNodeName := range ssn.HyperNodesSetByTier[tier] {
				if ssn.RealNodesSet[hyperNodeName].Has(node.Name) {
					tierScore = nta.getPodHyperNodeBinPackingScore(task, hyperNodeName)
					break
				}
			}
			totalScore += tierWeights[tier] * tierScore
		}
		nodeScores[node.Name] = totalScore / totalTierWeight
	}
	return nodeScores, nil
}

// getPodHyperNodeBinPackingScore 计算单个任务在指定 HyperNode 上的 binpacking 分数。
//
// 与子作业版本类似，但只考虑单个任务的资源需求。
func (nta *networkTopologyAwarePlugin) getPodHyperNodeBinPackingScore(task *api.TaskInfo, hyperNode string) float64 {
	totalScore := 0.0
	totalWeight := 0

	for _, resource := range task.Resreq.ResourceNames() {
		weight, ok := nta.weight.getBinPackWeight(resource)
		if !ok {
			continue
		}

		status, ok := nta.hyperNodeResourceCache[hyperNode]
		if !ok {
			klog.Warningf("plugin %s failed to find the resource status cache of hyperNode %s, which should not happen", PluginName, hyperNode)
			continue
		}
		allocatable := status.allocatable.Get(resource)
		used := status.used.Get(resource)

		request := task.Resreq.Get(resource)
		// 单任务资源超售，该 HyperNode 得 0 分。
		if used+request > allocatable {
			klog.V(4).InfoS("cannot binpack the hyperNode", "task", task.UID, "hyperNode", hyperNode,
				"resource", resource, "allocatable", allocatable, "used", used, "request", request)
			return ZeroScore
		}

		score := (used + request) / allocatable
		klog.V(5).InfoS("hyperNode binpacking score calculation", "task", task.UID, "hyperNode", hyperNode,
			"resource", resource, "allocatable", allocatable, "used", used, "request", request)

		totalScore += float64(weight) * score
		totalWeight += weight
	}

	if totalWeight > 0 {
		totalScore /= float64(totalWeight)
		klog.V(5).Infof("the hyperNode-level binpacking score of task %s on hyperNode %s is: %g", task.UID, hyperNode, totalScore)
		return totalScore
	}
	return ZeroScore
}

// batchNodeOrderFnForNetworkAwarePods 为具有网络拓扑需求的任务计算节点分数。
//
// 核心逻辑：
//  1. 获取任务所属子作业已分配的 HyperNode（jobAllocatedHyperNode）；
//  2. 对每个候选节点，找到其所属的 HyperNode，计算该 HyperNode 与 jobAllocatedHyperNode 的 LCA；
//  3. LCA 的 tier 越低（越靠近顶层），表示网络距离越远，得分越低；tier 越高（越靠近叶子），得分越高；
//  4. 若最高分并列，则加上任务集中度辅助分。
func (nta *networkTopologyAwarePlugin) batchNodeOrderFnForNetworkAwarePods(ssn *framework.Session, task *api.TaskInfo, subJob *api.SubJobInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
	nodeScores := make(map[string]float64)

	allocatedHyperNode := task.JobAllocatedHyperNode
	if allocatedHyperNode == "" {
		return nodeScores, nil
	}
	// 基于 LCAHyperNode 的 tier 计算拓扑分数。
	var maxScore float64 = -1
	scoreToNodes := map[float64][]string{}
	for _, node := range nodes {
		hyperNode := util.FindHyperNodeForNode(node.Name, ssn.RealNodesList, ssn.HyperNodesTiers, ssn.HyperNodesSetByTier)
		score := nta.networkTopologyAwareScore(hyperNode, allocatedHyperNode, ssn.HyperNodes)
		nodeScores[node.Name] = score
		if score >= maxScore {
			maxScore = score
			scoreToNodes[maxScore] = append(scoreToNodes[maxScore], node.Name)
		}
	}
	// 若最高分的节点有多个，用任务集中度进一步区分。
	if len(scoreToNodes[maxScore]) > 1 {
		candidateNodes := scoreToNodes[maxScore]
		for _, node := range candidateNodes {
			hyperNode := util.FindHyperNodeForNode(node, ssn.RealNodesList, ssn.HyperNodesTiers, ssn.HyperNodesSetByTier)
			taskNumScore := nta.scoreWithTaskNum(hyperNode, subJob.Tasks, ssn.RealNodesList)
			nodeScores[node] += taskNumScore
		}
	}

	return nodeScores, nil
}

// hyperNodeGradientFn 执行 HyperNode 梯度搜索。
//
// 所谓"梯度"是指：从给定的 HyperNode 出发，按 BFS 遍历其子孙 HyperNode，
// 并将满足条件的 HyperNode 按 tier 升序分组返回。
// tier 越小（越靠近顶层），表示网络范围越大；tier 越大（越靠近叶子），表示网络距离越近。
// 返回结果按 tier 升序排列，调用方会优先尝试 tier 更小（范围更大）的 HyperNode。
//
// 参数说明：
//   - ssn: 当前调度会话，包含所有 HyperNode 与节点状态
//   - hyperNode: 搜索起点，通常是外部调用者可用的 HyperNode 子树根节点
//   - highestAllowedTier: 允许搜索的最大 tier，用于限制搜索范围
//   - allocatedHyperNode: 作业/子作业已分配的 HyperNode(部分运行场景非空，首次调度为空)
//   - minResource: 最小资源需求，用于资源预过滤(nil 表示跳过资源检查)
//   - purpose: 搜索目的(调度分配或驱逐)
func (nta *networkTopologyAwarePlugin) hyperNodeGradientFn(ssn *framework.Session, hyperNode *api.HyperNodeInfo, highestAllowedTier int, allocatedHyperNode string, minResource *api.Resource, purpose api.SearchPurpose) ([][]*api.HyperNodeInfo, error) {
	enqueued := set.New[string]()
	var processQueue []*api.HyperNodeInfo

	// 先确定实际搜索根节点：需要同时满足外部调用者约束和作业已分配 HyperNode 约束。
	searchRoot, err := getSearchRoot(ssn.HyperNodes, hyperNode, highestAllowedTier, allocatedHyperNode)
	if err != nil {
		return nil, fmt.Errorf("getSearchRoot failed: %w", err)
	}

	processQueue = append(processQueue, searchRoot)
	enqueued.Insert(searchRoot.Name)

	eligibleHyperNodes := make(map[int][]*api.HyperNodeInfo)
	for len(processQueue) > 0 {
		// 从队列头部弹出一个 HyperNode。
		current := processQueue[0]
		processQueue = processQueue[1:]

		if nta.isEligibleHyperNode(current, highestAllowedTier, allocatedHyperNode, minResource, purpose) {
			eligibleHyperNodes[current.Tier()] = append(eligibleHyperNodes[current.Tier()], current)
		}

		// 将未访问的子 HyperNode 入队。
		for child := range current.Children {
			if enqueued.Has(child) {
				continue
			}
			processQueue = append(processQueue, ssn.HyperNodes[child])
			enqueued.Insert(child)
		}
	}

	// 按 tier 升序组织结果。
	var tiers []int
	for tier := range eligibleHyperNodes {
		tiers = append(tiers, tier)
	}
	sort.Ints(tiers)

	var result [][]*api.HyperNodeInfo
	for _, tier := range tiers {
		result = append(result, eligibleHyperNodes[tier])
	}

	return result, nil
}

// isEligibleHyperNode 判断某个 HyperNode 是否满足梯度搜索条件。
//
// 判断逻辑：
//  1. tier 不能超过 highestAllowedTier；
//  2. 若是部分运行场景(allocatedHyperNode 非空)，跳过资源预过滤；
//  3. 若资源缓存不存在，跳过资源预过滤；
//  4. 驱逐场景：要求 HyperNode 的可分配资源能容纳 minResource；
//  5. 分配场景：要求 HyperNode 的 idle 或 futureIdle 能容纳 minResource。
func (nta *networkTopologyAwarePlugin) isEligibleHyperNode(hn *api.HyperNodeInfo, highestAllowedTier int, allocatedHyperNode string, minResource *api.Resource, purpose api.SearchPurpose) bool {
	if hn.Tier() > highestAllowedTier {
		return false // tier 不能超过允许的最大 tier
	}

	if allocatedHyperNode != "" {
		return true // 部分运行场景跳过资源预过滤
	}

	hnResourceStatus, found := nta.hyperNodeResourceCache[hn.Name]
	if !found {
		return true // 缓存中找不到该 HyperNode 资源状态，跳过预过滤
	}

	if purpose == api.PurposeEvict {
		return minResource.LessEqual(hnResourceStatus.allocatable, api.Zero)
	}

	if minResource.LessEqual(hnResourceStatus.idle, api.Zero) || minResource.LessEqual(hnResourceStatus.futureIdle, api.Zero) {
		return true
	}
	return false
}

// getSearchRoot 计算作业/子作业的 HyperNode 搜索根节点。
//
// 需要同时满足两个约束：
//  1. 外部调用者传入的可用 HyperNode 子树(hyperNodeAvailable)；
//  2. 作业/子作业基于已分配 HyperNode 推导出的最大允许 HyperNode 子树。
//
// 通过求两棵子树的"交集"（即 LCA），确保返回的根节点同时满足双方约束。
func getSearchRoot(hyperNodes api.HyperNodeInfoMap, hyperNodeAvailable *api.HyperNodeInfo, highestAllowedTier int, allocatedHyperNode string) (*api.HyperNodeInfo, error) {
	if allocatedHyperNode == "" {
		return hyperNodeAvailable, nil
	}

	hyperNodeHighestAllowed, err := getHighestAllowedHyperNode(hyperNodes, highestAllowedTier, allocatedHyperNode)
	if err != nil {
		return nil, fmt.Errorf("get highest allowed hyperNode failed: %w", err)
	}

	// 取 hyperNodeAvailable 与 hyperNodeHighestAllowed 的最近公共祖先。
	lca := hyperNodes.GetLCAHyperNode(hyperNodeAvailable.Name, hyperNodeHighestAllowed)
	if lca == hyperNodeHighestAllowed {
		return hyperNodeAvailable, nil
	}
	if lca == hyperNodeAvailable.Name {
		hni, ok := hyperNodes[hyperNodeHighestAllowed]
		if !ok {
			return nil, fmt.Errorf("failed to get highest allowed HyperNode info for %s", hyperNodeHighestAllowed)
		}
		return hni, nil
	}

	return nil, fmt.Errorf("there is no intersection between hyperNodeAvailable %s and hyperNodeHighestAllowed %s",
		hyperNodeAvailable.Name, hyperNodeHighestAllowed)
}

// getHighestAllowedHyperNode 根据已分配 HyperNode 和允许的最高 tier，
// 返回向上回溯时 tier 不超过 highestAllowedTier 的最高祖先 HyperNode。
//
// 例如：已分配 HyperNode 在 tier 4，highestAllowedTier 为 2，
// 则向上找到 tier 2 的祖先作为最大允许搜索范围。
func getHighestAllowedHyperNode(hyperNodes api.HyperNodeInfoMap, highestAllowedTier int, allocatedHyperNode string) (string, error) {
	var highestAllowedHyperNode string

	ancestors := hyperNodes.GetAncestors(allocatedHyperNode)
	for _, ancestor := range ancestors {
		hni, ok := hyperNodes[ancestor]
		if !ok {
			return "", fmt.Errorf("allocated hyperNode %s ancestor %s not found", allocatedHyperNode, ancestor)
		}
		if hni.Tier() > highestAllowedTier {
			break
		}
		highestAllowedHyperNode = ancestor
	}

	if highestAllowedHyperNode == "" {
		return "", fmt.Errorf("allocated hyperNode %s tier is greater than highest allowed tier %d", allocatedHyperNode, highestAllowedTier)
	}

	return highestAllowedHyperNode, nil
}

// OnSessionClose 在调度会话关闭时调用，当前插件无需清理资源。
func (nta *networkTopologyAwarePlugin) OnSessionClose(ssn *framework.Session) {
}

// reverseAndCapEvictionGradients 反转梯度层级顺序，并限制返回的 HyperNode 总数。
//
// 普通梯度按 tier 升序排列（tier 小在前）。驱逐场景下，我们希望优先尝试范围更大的
// 高层级 HyperNode（tier 更小），因此从后向前遍历并截断到 maxHyperNodesForEviction。
func (nta *networkTopologyAwarePlugin) reverseAndCapEvictionGradients(gradients [][]*api.HyperNodeInfo) [][]*api.HyperNodeInfo {
	if nta.maxHyperNodesForEviction <= 0 {
		return gradients
	}

	remaining := nta.maxHyperNodesForEviction
	result := make([][]*api.HyperNodeInfo, 0, len(gradients))
	// 梯度原本从低 tier 到高 tier，从尾部开始遍历以优先选择范围更大的高层级 HyperNode。
	for i := len(gradients) - 1; i >= 0; i-- {
		gradient := gradients[i]
		if remaining == 0 {
			break
		}
		if len(gradient) <= remaining {
			result = append(result, gradient)
			remaining -= len(gradient)
			continue
		}
		result = append(result, gradient[len(gradient)-remaining:])
		remaining = 0
	}
	return result
}

// networkTopologyAwareScore 计算候选 HyperNode 相对于作业已分配 HyperNode 的拓扑分数。
//
// 目标：
//   - 候选 HyperNode 与作业已分配 HyperNode 的 LCA（最近公共祖先）tier 越低，得分越高。
//
// 也就是说，两个 HyperNode 在拓扑树上越"亲近"，得分越高；若完全重合则满分。
func (nta *networkTopologyAwarePlugin) networkTopologyAwareScore(hyperNodeName, jobAllocatedHyperNode string, hyperNodeMap api.HyperNodeInfoMap) float64 {
	if hyperNodeName == "" || jobAllocatedHyperNode == "" {
		return ZeroScore
	}
	if hyperNodeName == jobAllocatedHyperNode {
		return FullScore
	}
	LCAHyperNode := hyperNodeMap.GetLCAHyperNode(hyperNodeName, jobAllocatedHyperNode)
	hyperNodeInfo, ok := hyperNodeMap[LCAHyperNode]
	if !ok {
		return ZeroScore
	}
	// 分数公式：(maxTier - LCA.tier) / (maxTier - minTier)
	hyperNodeTierScore := nta.scoreHyperNodeWithTier(hyperNodeInfo.Tier())
	return hyperNodeTierScore
}

// scoreWithTaskNum 根据某 HyperNode 上已调度的任务数计算辅助分数。
//
// 目标：
//   - 同一作业/子作业的任务尽量集中在同一个 HyperNode 上。
func (nta *networkTopologyAwarePlugin) scoreWithTaskNum(hyperNodeName string, tasks api.TasksMap, realNodesList map[string][]*api.NodeInfo) float64 {
	taskNum := util.FindJobTaskNumOfHyperNode(hyperNodeName, tasks, realNodesList)
	taskNumScore := ZeroScore
	if len(tasks) > 0 {
		// 分数公式：当前 HyperNode 上的任务数 / 总任务数
		taskNumScore = scoreHyperNodeWithTaskNum(taskNum, len(tasks))
	}
	return taskNumScore
}

// scoreHyperNodeWithTier 根据 tier 计算分数，并将结果映射到 [0, 1] 区间。
//
// 公式：当 minTier <= tier <= maxTier 时，返回 (maxTier - tier) / (maxTier - minTier)。
// tier 越小（越顶层），分数越高；tier 越大（越底层），分数越低。
func (nta *networkTopologyAwarePlugin) scoreHyperNodeWithTier(tier int) float64 {
	if nta.minTier == nta.maxTier {
		return FullScore
	}
	if nta.minTier <= tier && tier <= nta.maxTier {
		return float64(nta.maxTier-tier) / float64(nta.maxTier-nta.minTier)
	}
	return ZeroScore
}

// scoreHyperNodeWithTaskNum 计算任务分布率作为分数，并将结果映射到 [0, 1] 区间。
func scoreHyperNodeWithTaskNum(taskNum int, allTaskNum int) float64 {
	if allTaskNum == 0 {
		return FullScore
	}
	return float64(taskNum) / float64(allTaskNum)
}

// scaleFinalScore 将原始分数缩放到 kube-scheduler framework 的分数范围。
//
// 公式：fwk.MaxNodeScore * 插件全局权重 * 原始分数。
// 这样不同权重插件的分数可以在 framework 中正确累加。
func (nta *networkTopologyAwarePlugin) scaleFinalScore(scores map[string]float64) map[string]float64 {
	scaledScores := make(map[string]float64)
	for name, score := range scores {
		scaledScores[name] = float64(fwk.MaxNodeScore) * float64(nta.weight.GlobalWeight) * score
	}
	return scaledScores
}
