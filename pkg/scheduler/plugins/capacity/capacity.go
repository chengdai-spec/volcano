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

// ============================================================================
// Capacity 调度插件 — 中文注释深度解析
// ============================================================================
//
// 【插件概述】
//
//	capacity 是 Volcano 调度器中的核心队列管理插件，负责基于队列维度的资源容量管控。
//	它实现了类 Hadoop Capacity Scheduler 的多租户资源分配机制，主要功能包括：
//
//	1. 队列容量管理：每个队列可配置 deserved（应得资源）、capability（容量上限）、
//	   guarantee（保障资源），确保各租户按比例公平分配集群资源。
//
//	2. 层次化队列支持：支持父-子队列树形结构，资源配额从根队列向下逐级分配，
//	   子队列的 realCapability 受父队列约束。
//
//	3. 资源回收（Reclaim）：当某队列实际使用超过其应得资源时，其他资源不足的队列
//	   可以回收其超额部分的资源（通过驱逐任务实现）。
//
//	4. 抢占（Preempt）：当队列的 allocated 未达到 deserved 时，允许该队列通过
//	   抢占其他队列的任务来获取应得资源。
//
//	5. 公平排序（QueueOrder）：基于 DRF（Dominant Resource Fairness）份额值排序，
//	   份额值越小的队列优先获得调度机会。
//
//	6. DRA（Dynamic Resource Allocation）支持：对 Kubernetes DRA 资源（如 GPU 等
//	   特殊设备）进行配额管控，支持按 DeviceClass 维度跟踪和限制。
//
//	7. SchedulingGates 队列准入：配合 Kubernetes SchedulingGates 特性，对通过容量
//	   检查但尚未调度的任务进行资源预留，防止超额分配。
//
// 【核心数据流】
//
//	OnSessionOpen → buildQueueAttrs / buildHierarchicalQueueAttrs
//	               → 注册各回调函数 (Reclaimable/Preemptive/Allocatable/JobEnqueueable 等)
//	               → 注册事件处理器 (AllocateFunc/DeallocateFunc)
//	OnSessionClose → 更新指标、清理状态
//
// 【关键概念】
//   - share：队列的 DRF 份额值 = max(allocated_i / deserved_i)，值越小越优先调度
//   - realCapability：队列实际可用的资源上限 = min(capability, 集群剩余资源 + guarantee)
//   - elastic：弹性资源 = job.allocated - job.minAvailable，表示已分配但非最低保障的部分
//   - inqueue：已在队列中等待的资源量，用于防止入队时超额
//
// ============================================================================
package capacity

import (
	"context"
	"fmt"
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	kubefeatures "k8s.io/kubernetes/pkg/features"

	"volcano.sh/apis/pkg/apis/scheduling"

	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	PluginName              = "capacity"             // 插件名称，在调度框架中注册的唯一标识
	ancestorReclaimLevelKey = "ancestorReclaimLevel" // 插件参数键：祖先回收层级，控制回收时检查多少层祖先队列

	// capacityStateKey 是调度周期状态中的键，用于存储 capacity 插件的预计算数据
	// 使用插件名作为键以避免与其他插件冲突
	capacityStateKey = PluginName
	rootQueueID      = "root" // 层次化队列的根队列ID

	// DynamicResourceAllocationEnable 是启用 DRA 配额强制执行的参数键
	DynamicResourceAllocationEnable = "capacity.DynamicResourceAllocationEnable"

	// DRAConsumableCapacityEnable 是启用 DRA 可消费容量配额的参数键
	DRAConsumableCapacityEnable = "capacity.DRAConsumableCapacityEnable"

	DeviceClassCountPrefix = "deviceclass/"  // DRA 资源名中设备类计数的前缀，如 deviceclass/gpu
	DeviceClassCapacitySep = ".deviceclass/" // DRA 资源名中容量维度的分隔符，如 memory.deviceclass/gpu
)

// capacityPlugin 是 capacity 调度插件的主结构体
// 负责管理所有队列的资源属性、容量检查逻辑和调度回调注册
type capacityPlugin struct {
	rootQueue            string        // 层次化队列的根队列名称，默认为 "root"
	totalResource        *api.Resource // 集群总资源量
	totalGuarantee       *api.Resource // 所有队列保障资源之和（用于计算 realCapability）
	ancestorReclaimLevel int           // 祖先回收层级：回收时检查多少层祖先队列的 deserved 超额情况

	queueOpts map[api.QueueID]*queueAttr // 队列ID → 队列属性映射，存储每个队列的资源跟踪状态
	// pluginArguments 保存插件的配置参数
	pluginArguments framework.Arguments
	// queueGateReservedTasks 跟踪已通过容量检查但尚未被调度的任务
	// 这些任务会预留队列容量，防止其他任务消耗该容量
	// 在每个调度周期的 OnSessionOpen 开始时重新构建
	queueGateReservedTasks map[api.QueueID]map[api.TaskID]*api.TaskInfo

	// dynamicResourceAllocationEnable 控制是否启用 DRA 配额强制执行
	dynamicResourceAllocationEnable bool
	// draConsumableCapacityEnable 控制是否启用 DRA 内部容量维度（Capacity dimensions）的强制执行
	draConsumableCapacityEnable bool
}

// queueAttr 是队列的资源属性跟踪结构体
// 记录了队列在各维度的资源配置和使用情况，是容量调度的核心数据结构
type queueAttr struct {
	queueID   api.QueueID                // 队列唯一标识
	name      string                     // 队列名称
	share     float64                    // DRF 份额值 = max(allocated_i / deserved_i)，值越小优先级越高
	ancestors []api.QueueID              // 祖先队列ID列表（从近到远），层次化队列使用
	children  map[api.QueueID]*queueAttr // 子队列映射，层次化队列使用

	deserved  *api.Resource // 应得资源：队列应该获得的资源量，用于 DRF 份额计算和回收判定
	allocated *api.Resource // 已分配资源：队列中所有已调度任务占用的资源总量
	request   *api.Resource // 请求资源：队列中所有任务（含 Pending）的资源请求总量
	// elastic 表示队列中所有 Job 的弹性资源之和
	// 弹性资源 = job.allocated - job.minAvailable，即超出最小保障的部分
	// 在入队检查时，弹性资源可被扣除，因为它不是硬性保障
	elastic *api.Resource
	// inqueue 表示已在队列中等待（Inqueue 阶段）的 Job 的资源请求量
	// 用于入队检查时防止超额：入队时需确保 allocated + inqueue - elastic <= realCapability
	inqueue    *api.Resource
	capability *api.Resource // 容量上限：用户配置的队列资源硬限制
	// realCapability 表示队列实际可用的资源上限，取 capability 与 (集群剩余资源 + guarantee) 的较小值
	// 计算公式：realCapability = min(capability, totalResource - totalGuarantee + guarantee)
	realCapability    *api.Resource
	guarantee         *api.Resource  // 保障资源：队列的硬性资源保障，不会被其他队列回收
	dra               *draQuotaAttr  // DRA 配额属性，跟踪按 DeviceClass 维度的设备资源
	resourceClaimRefs map[string]int // ResourceClaim 引用计数，用于 DRA 去重：同一 claim 被多个 task 引用时只计一次
}

// draQuotaAttr 记录队列在 DRA（Dynamic Resource Allocation）资源上的配额跟踪状态
// 按 DeviceClass（设备类，如 gpu、fpga 等）维度分别记录 capability/deserved/guarantee/allocated/inqueue
type draQuotaAttr struct {
	// capability：每个 DeviceClass 的硬上限（来自 queue.spec.capability 中 deviceclass/* 相关配置）
	capability map[string]*api.DRAResource
	// deserved：每个 DeviceClass 的软限制（来自 queue.spec.deserved 中 deviceclass/* 相关配置）
	deserved map[string]*api.DRAResource
	// guarantee：每个 DeviceClass 的保障资源下限（来自 queue.spec.guarantee 中 deviceclass/* 相关配置）
	guarantee map[string]*api.DRAResource
	// allocated：每个 DeviceClass 的当前已分配资源量
	allocated map[string]*api.DRAResource
	// inqueue：当前调度会话中已准入队列（Inqueue 阶段）但尚未分配的 DRA 资源量
	inqueue map[string]*api.DRAResource
}

func (da *draQuotaAttr) Clone() *draQuotaAttr {
	if da == nil {
		return nil
	}
	out := &draQuotaAttr{
		capability: cloneDRAResourceMap(da.capability),
		deserved:   cloneDRAResourceMap(da.deserved),
		guarantee:  cloneDRAResourceMap(da.guarantee),
		allocated:  cloneDRAResourceMap(da.allocated),
		inqueue:    cloneDRAResourceMap(da.inqueue),
	}
	return out
}

// cloneDRAResourceMap 深拷贝 DRA 资源映射
// 用于 draQuotaAttr.Clone() 中创建独立的资源副本
func cloneDRAResourceMap(m map[string]*api.DRAResource) map[string]*api.DRAResource {
	if m == nil {
		return nil
	}
	out := make(map[string]*api.DRAResource, len(m))
	for k, v := range m {
		out[k] = v.Clone()
	}
	return out
}

// parseDRAResourceList 解析 Kubernetes ResourceList 中的 DRA 资源
//
// 支持的资源名格式：
//   - deviceclass/<class>：设备类计数，如 deviceclass/gpu
//   - <dim>.deviceclass/<class>：设备类容量维度，如 memory.deviceclass/gpu
//
// 返回按 DeviceClass 组织的 DRA 资源映射
func parseDRAResourceList(resources v1.ResourceList) map[string]*api.DRAResource {
	if len(resources) == 0 {
		return nil
	}
	out := make(map[string]*api.DRAResource)
	for name, quantity := range resources {
		resourceName := string(name)
		if idx := strings.Index(resourceName, DeviceClassCapacitySep); idx > 0 {
			dim := resourceName[:idx]
			deviceClass := resourceName[idx+len(DeviceClassCapacitySep):]
			if dim == "" || deviceClass == "" {
				continue
			}
			if out[deviceClass] == nil {
				out[deviceClass] = &api.DRAResource{Capacity: make(map[string]resource.Quantity)}
			}
			if out[deviceClass].Capacity == nil {
				out[deviceClass].Capacity = make(map[string]resource.Quantity)
			}
			out[deviceClass].Capacity[dim] = quantity.DeepCopy()
			continue
		}
		if strings.HasPrefix(resourceName, DeviceClassCountPrefix) {
			deviceClass := strings.TrimPrefix(resourceName, DeviceClassCountPrefix)
			if deviceClass == "" {
				continue
			}
			if out[deviceClass] == nil {
				out[deviceClass] = &api.DRAResource{}
			}
			out[deviceClass].Count = quantity.Value()
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// newDRAQuotaAttr 创建队列的 DRA 配额跟踪对象
// 如果 capability/deserved/guarantee 都未配置 DRA 资源，则返回 nil
func newDRAQuotaAttr(capability, deserved, guarantee v1.ResourceList) *draQuotaAttr {
	da := &draQuotaAttr{
		capability: parseDRAResourceList(capability),
		deserved:   parseDRAResourceList(deserved),
		guarantee:  parseDRAResourceList(guarantee),
		allocated:  make(map[string]*api.DRAResource),
		inqueue:    make(map[string]*api.DRAResource),
	}
	if da.capability == nil && da.deserved == nil && da.guarantee == nil {
		return nil
	}
	return da
}

// getDRADelta 计算两次 DRA 配额状态之间的增量
// 用于层次化队列中把叶子队列的资源变化向上传播到祖先队列
func getDRADelta(newAttr, oldAttr *draQuotaAttr) map[string]*api.DRAResource {
	if newAttr == nil {
		return nil
	}
	delta := make(map[string]*api.DRAResource)

	// 收集所有出现过的 DeviceClass 键，取并集
	keys := make(map[string]struct{})
	if newAttr.allocated != nil {
		for k := range newAttr.allocated {
			keys[k] = struct{}{}
		}
	}
	if oldAttr != nil && oldAttr.allocated != nil {
		for k := range oldAttr.allocated {
			keys[k] = struct{}{}
		}
	}

	for k := range keys {
		var newRes, oldRes *api.DRAResource
		if newAttr.allocated != nil {
			newRes = newAttr.allocated[k]
		}
		if oldAttr != nil && oldAttr.allocated != nil {
			oldRes = oldAttr.allocated[k]
		}

		res := &api.DRAResource{Capacity: make(map[string]resource.Quantity)}
		if newRes != nil {
			res.Count = newRes.Count
			for dim, val := range newRes.Capacity {
				res.Capacity[dim] = val.DeepCopy()
			}
		}
		if oldRes != nil {
			res.Count -= oldRes.Count
			for dim, val := range oldRes.Capacity {
				q := res.Capacity[dim]
				q.Sub(val)
				res.Capacity[dim] = q
			}
		}
		delta[k] = res
	}
	return delta
}

// checkDRAAllocatable 检查任务请求的 DRA 资源是否超出队列的 DRA capability 限制
//
// 参数：
//   - dra：队列的 DRA 配额跟踪状态
//   - taskDRA：任务请求的 DRA 资源（按 DeviceClass 组织）
//   - consumableCapacityEnabled：是否同时检查 Capacity 维度（如显存、带宽等）
//   - includeInqueue：是否为入队检查；入队时需把已准入但未调度的 inqueue 资源也计入已占用
//
// 返回值：true 表示 DRA 资源充足，允许分配/入队
func checkDRAAllocatable(dra *draQuotaAttr, taskDRA map[string]*api.DRAResource, consumableCapacityEnabled bool, includeInqueue bool) bool {
	if dra == nil || taskDRA == nil {
		// 队列未配置 DRA 配额，或任务没有 DRA 请求，直接放行
		return true
	}

	for deviceClass, request := range taskDRA {
		capability, exists := dra.capability[deviceClass]
		if !exists {
			// 该 DeviceClass 未配置 capability，表示不做配额限制，允许通过
			klog.V(5).Infof("checkDRAAllocatable: No capability for %s, passing through", deviceClass)
			continue
		}

		allocated := dra.allocated[deviceClass]
		allocatedCount := int64(0)
		if allocated != nil {
			allocatedCount = allocated.Count
		}
		var inqueue *api.DRAResource
		if includeInqueue {
			inqueue = dra.inqueue[deviceClass]
		}
		inqueueCount := int64(0)
		if inqueue != nil {
			inqueueCount = inqueue.Count
		}

		klog.V(5).Infof("checkDRAAllocatable: deviceClass=%s, allocated=%d, inqueue=%d, request=%d, capability=%d",
			deviceClass, allocatedCount, inqueueCount, request.Count, capability.Count)

		if capability.Count > 0 && allocatedCount+inqueueCount+request.Count > capability.Count {
			klog.V(3).Infof("checkDRAAllocatable: count exceeded for %s: allocated=%d, inqueue=%d, requested=%d, capability=%d",
				deviceClass, allocatedCount, inqueueCount, request.Count, capability.Count)
			return false
		}

		// 检查容量维度（仅当 DRAConsumableCapacity 特性启用时才检查）
		// 例如：显存、带宽等设备可消费容量维度
		if consumableCapacityEnabled {
			for dim, reqQty := range request.Capacity {
				capQty, ok := capability.Capacity[dim]
				if !ok {
					// 请求的容量维度在 capability 中未定义，视为超配
					return false
				}
				var allocQty resource.Quantity
				if allocated != nil {
					allocQty = allocated.Capacity[dim]
				}
				futureUsed := allocQty.DeepCopy()
				if inqueue != nil {
					futureUsed.Add(inqueue.Capacity[dim])
				}
				futureUsed.Add(reqQty)
				if futureUsed.Cmp(capQty) > 0 {
					return false
				}
			}
		}
	}
	return true
}

// mergeTaskDRA 将 src 中的 DRA 资源累加到 dst 中
// 用于合并多个任务或 Job 的 DRA 请求
func mergeTaskDRA(dst map[string]*api.DRAResource, src map[string]*api.DRAResource) {
	for deviceClass, request := range src {
		if request == nil {
			continue
		}
		if dst[deviceClass] == nil {
			dst[deviceClass] = &api.DRAResource{
				Capacity: make(map[string]resource.Quantity),
			}
		}
		dst[deviceClass].Add(request)
	}
}

// incrementalTaskDRA 计算任务相对于队列当前状态的 DRA 增量请求
//
// 如果任务使用 ResourceClaim 且该 claim 已被队列中其他任务引用，
// 则不再重复计算该 claim 的 DRA 资源，避免同一 claim 被多个任务共享时重复扣配额。
func incrementalTaskDRA(attr *queueAttr, task *api.TaskInfo) map[string]*api.DRAResource {
	if task == nil {
		return nil
	}
	if len(task.ResourceClaimDRAResreq) == 0 {
		return task.DRAResreq
	}

	incremental := make(map[string]*api.DRAResource)
	for _, claimKey := range task.ResourceClaimKeys {
		if attr != nil && attr.resourceClaimRefs[claimKey] > 0 {
			continue
		}
		mergeTaskDRA(incremental, task.ResourceClaimDRAResreq[claimKey])
	}
	if len(incremental) == 0 {
		return nil
	}
	return incremental
}

// updateDRAAllocated 将任务的 DRA 请求累加到队列的 allocated 跟踪中
func updateDRAAllocated(dra *draQuotaAttr, taskDRA map[string]*api.DRAResource) {
	mergeTaskDRA(dra.allocated, taskDRA)
}

// updateDRAInqueue 将 Job 的 DRA 请求累加到队列的 inqueue 跟踪中
// 在 Job 通过入队检查但尚未调度时使用
func updateDRAInqueue(dra *draQuotaAttr, jobDRA map[string]*api.DRAResource) {
	mergeTaskDRA(dra.inqueue, jobDRA)
}

// addTaskDRAAllocated 将任务的 DRA 资源计入队列的 allocated
// 对 ResourceClaim 进行引用计数，同一 claim 被多个任务引用时只计一次
func addTaskDRAAllocated(attr *queueAttr, task *api.TaskInfo) {
	if attr == nil || attr.dra == nil || task == nil {
		return
	}
	if len(task.ResourceClaimDRAResreq) == 0 {
		if task.DRAResreq != nil {
			updateDRAAllocated(attr.dra, task.DRAResreq)
		}
		return
	}
	if attr.resourceClaimRefs == nil {
		attr.resourceClaimRefs = make(map[string]int)
	}
	for _, claimKey := range task.ResourceClaimKeys {
		claimReq := task.ResourceClaimDRAResreq[claimKey]
		if len(claimReq) == 0 {
			continue
		}
		if attr.resourceClaimRefs[claimKey] == 0 {
			updateDRAAllocated(attr.dra, claimReq)
		}
		attr.resourceClaimRefs[claimKey]++
	}
}

// subtractDRAAllocated 将任务的 DRA 请求从队列的 allocated 跟踪中扣除
func subtractDRAAllocated(dra *draQuotaAttr, taskDRA map[string]*api.DRAResource) {
	for deviceClass, request := range taskDRA {
		allocated := dra.allocated[deviceClass]
		if allocated == nil {
			continue
		}
		allocated.Sub(request)
	}
}

// removeTaskDRAAllocated 将任务的 DRA 资源从队列的 allocated 中扣除
// 对 ResourceClaim 进行引用计数递减，引用归零时才真正扣除配额
func removeTaskDRAAllocated(attr *queueAttr, task *api.TaskInfo) {
	if attr == nil || attr.dra == nil || task == nil {
		return
	}
	if len(task.ResourceClaimDRAResreq) == 0 {
		if task.DRAResreq != nil {
			subtractDRAAllocated(attr.dra, task.DRAResreq)
		}
		return
	}
	for _, claimKey := range task.ResourceClaimKeys {
		claimReq := task.ResourceClaimDRAResreq[claimKey]
		if len(claimReq) == 0 {
			continue
		}
		refCount := attr.resourceClaimRefs[claimKey]
		if refCount <= 1 {
			subtractDRAAllocated(attr.dra, claimReq)
			delete(attr.resourceClaimRefs, claimKey)
			continue
		}
		attr.resourceClaimRefs[claimKey] = refCount - 1
	}
}

// New 创建 capacity 插件实例
// 读取 Kubernetes Feature Gate 默认值，并允许通过插件参数覆盖 DRA 相关开关
func New(arguments framework.Arguments) framework.Plugin {
	// 默认使用 Kubernetes 的 feature gate 值，插件参数可覆盖
	dynamicResourceAllocationEnable := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation)
	draConsumableCapacityEnable := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRAConsumableCapacity)

	arguments.GetBool(&dynamicResourceAllocationEnable, DynamicResourceAllocationEnable)
	arguments.GetBool(&draConsumableCapacityEnable, DRAConsumableCapacityEnable)

	return &capacityPlugin{
		totalResource:                   api.EmptyResource(),
		totalGuarantee:                  api.EmptyResource(),
		queueOpts:                       map[api.QueueID]*queueAttr{},
		ancestorReclaimLevel:            0,
		pluginArguments:                 arguments,
		queueGateReservedTasks:          make(map[api.QueueID]map[api.TaskID]*api.TaskInfo),
		dynamicResourceAllocationEnable: dynamicResourceAllocationEnable,
		draConsumableCapacityEnable:     draConsumableCapacityEnable,
	}
}

// Name 返回插件名称，用于在调度框架中注册和识别
func (cp *capacityPlugin) Name() string {
	return PluginName
}

// OnSessionOpen 在调度会话开始时调用
// 是 capacity 插件的核心入口，负责：
//  1. 解析插件参数
//  2. 汇总集群总资源
//  3. 重建 SchedulingGates 保留缓存
//  4. 构建队列属性（扁平或层次化）
//  5. 注册各类调度回调函数
func (cp *capacityPlugin) OnSessionOpen(ssn *framework.Session) {
	cp.parseArguments()

	// 准备本次调度会话的集群资源数据
	cp.totalResource.Add(ssn.TotalResource)

	klog.V(4).Infof("The total resource is <%v>", cp.totalResource)

	// 每个调度周期开始时重建保留缓存
	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) {
		cp.buildQueueReservedTasksCache(ssn)
	}

	hierarchyEnabled := ssn.HierarchyEnabled(cp.Name())
	readyToSchedule := true
	if hierarchyEnabled {
		readyToSchedule = cp.buildHierarchicalQueueAttrs(ssn)
		klog.V(4).Infof("Hierarchy is enabled in capacity plugin")
	} else {
		cp.buildQueueAttrs(ssn)
	}

	ssn.AddReclaimableFn(cp.Name(), func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		allocations := map[api.QueueID]*api.Resource{}
		if !readyToSchedule {
			klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
			return victims, util.Reject
		}

		reclaimerJob := ssn.Jobs[reclaimer.Job]
		if reclaimerJob == nil {
			klog.Warningf("[capacity] Skip reclaim for reclaimer <%s/%s>: job <%s> not found in session",
				reclaimer.Namespace, reclaimer.Name, reclaimer.Job)
			return victims, util.Reject
		}
		reclaimerAttr := cp.queueOpts[reclaimerJob.Queue]
		if reclaimerAttr == nil {
			klog.Warningf("[capacity] Skip reclaim for reclaimer <%s/%s>: queue <%s> not found in queueOpts",
				reclaimer.Namespace, reclaimer.Name, reclaimerJob.Queue)
			return victims, util.Reject
		}

		reclaimeesQueue := ssn.BuildVictimsPriorityQueue(reclaimees, reclaimer)
		for !reclaimeesQueue.Empty() {
			reclaimee := reclaimeesQueue.Pop().(*api.TaskInfo)
			job := ssn.Jobs[reclaimee.Job]
			if job == nil {
				klog.Warningf("[capacity] Skip reclaimee <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					reclaimee.Namespace, reclaimee.Name, reclaimee.Job)
				continue
			}

			attr := cp.queueOpts[job.Queue]
			if attr == nil {
				klog.Warningf("[capacity] Skip reclaimee <%s/%s>: queue <%s> not found in queueOpts",
					reclaimee.Namespace, reclaimee.Name, job.Queue)
				continue
			}

			klog.V(5).Infof("[capacity] Considering reclaimee <%s/%s> from queue <%s> for reclaimer <%s/%s>.",
				reclaimee.Namespace, reclaimee.Name, attr.queueID, reclaimer.Namespace, reclaimer.Name)

			// 如果被回收任务与回收方没有任何资源维度交集，则跳过（例如 CPU 任务无法回收 GPU 任务）
			if skip, reason := cp.shouldSkipReclaimee(reclaimee, reclaimer); skip {
				klog.V(5).Infof("%s, skip it.", reason)
				continue
			}

			// allocations 记录每个队列的当前 allocated 资源克隆。
			// 当选择受害者时，通过该指针从对应队列的 allocated 中扣除被回收任务的资源，
			// 确保同一队列的后续受害者选择基于更新后的分配状态。
			if _, found := allocations[job.Queue]; !found {
				allocations[job.Queue] = attr.allocated.Clone()
			}
			allocated := allocations[job.Queue]
			ancestorAllocations := make(map[api.QueueID]*api.Resource)

			// 检查回收后剩余资源是否仍满足队列的 guarantee 下限
			if satisfies, _ := cp.checkGuaranteeConstraint(allocated, reclaimee, attr.guarantee); !satisfies {
				continue
			}

			// 判定被回收任务是否可作为受害者：
			// 1) 是即时受害者（任务资源与队列 deserved 无交集）；或
			// 2) 队列在任务相关维度上的 allocated 已超过 deserved
			childEligible := false
			if isVictim, reason := cp.isImmediateVictim(reclaimee, attr.deserved); isVictim {
				// 如果启用层次化队列且配置了祖先回收层级，即使该任务是即时受害者，
				// 仍需检查 reclaimer 与 reclaimee 在配置层级内是否共享非根祖先，
				// 且在共享祖先范围内双方的叶子 deserved 对请求资源均为空。
				// 若是，则避免无实际竞争时的不必要抢占。
				if hierarchyEnabled && cp.ancestorReclaimLevel > 0 && cp.sharesAnyNonRootAncestorWithinLevel(reclaimerAttr, attr) &&
					!hasRelevantDeserved(reclaimer, reclaimerAttr.deserved) {
					klog.V(5).Infof("[capacity] Skip reclaim for reclaimee <%s/%s> from queue <%s>: both leaf deserved signals are empty for requested resources under shared ancestor scope ancestorReclaimLevel=%d",
						reclaimee.Namespace, reclaimee.Name, attr.queueID, cp.ancestorReclaimLevel)
					continue
				}
				childEligible = true
				klog.V(5).Infof("%s. It's a victim for queue <%s>.", reason, attr.name)
			} else if exceeds, dims, reason := cp.checkDeservedExceedance(
				allocated, attr.deserved, reclaimee, reclaimer, attr.name); exceeds {
				childEligible = true
				klog.V(5).Infof("[capacity] Reclaimee <%s/%s> is a victim from queue <%s> for reclaimer <%s/%s>. "+
					"Allocated: <%v>, Deserved: <%v>, Reclaimee Resreq: <%v>, Reclaimable on dimensions: %v.",
					reclaimee.Namespace, reclaimee.Name, attr.queueID, reclaimer.Namespace, reclaimer.Name,
					allocated, attr.deserved, reclaimee.Resreq, dims)
			} else {
				klog.V(5).Infof("%s.", reason)
			}

			if !childEligible {
				continue
			}

			// 若启用层次化队列且配置了祖先回收层级，
			// 向上检查配置层数的祖先队列，避免无实际竞争时误回收。
			ancestorEligible := true
			if hierarchyEnabled && cp.ancestorReclaimLevel > 0 {
				for level := 1; level <= cp.ancestorReclaimLevel; level++ {
					ancestorAttr, needCheck := cp.getReclaimeeAncestorToCheck(reclaimerAttr, attr, level)
					if !needCheck {
						continue
					}
					if ancestorAttr == nil {
						ancestorEligible = false
						klog.Warningf("[capacity] Skip reclaimee <%s/%s>: ancestor check target at level %d is nil", reclaimee.Namespace, reclaimee.Name, level)
						break
					}
					if _, found := allocations[ancestorAttr.queueID]; !found {
						allocations[ancestorAttr.queueID] = ancestorAttr.allocated.Clone()
					}
					ancestorAllocated := allocations[ancestorAttr.queueID]
					ancestorAllocations[ancestorAttr.queueID] = ancestorAllocated

					if isVictim, reason := cp.isImmediateVictim(reclaimee, ancestorAttr.deserved); isVictim {
						klog.V(5).Infof("%s. It's a victim for ancestor queue <%s>.", reason, ancestorAttr.name)
						continue
					}

					exceeds, dims, reason := cp.checkDeservedExceedance(
						ancestorAllocated, ancestorAttr.deserved, reclaimee, reclaimer, ancestorAttr.name)
					if !exceeds {
						ancestorEligible = false
						klog.V(5).Infof("%s.", reason)
						break
					}
					klog.V(5).Infof("[capacity] Reclaimee <%s/%s> is a victim from ancestor queue <%s> for reclaimer <%s/%s>. "+
						"Allocated: <%v>, Deserved: <%v>, Reclaimee Resreq: <%v>, Reclaimable on dimensions: %v.",
						reclaimee.Namespace, reclaimee.Name, ancestorAttr.name, reclaimer.Namespace, reclaimer.Name,
						ancestorAllocated, ancestorAttr.deserved, reclaimee.Resreq, dims)
				}
			}

			if !ancestorEligible {
				continue
			}

			allocated.Sub(reclaimee.Resreq)
			for _, ancestorAllocated := range ancestorAllocations {
				ancestorAllocated.Sub(reclaimee.Resreq)
			}
			victims = append(victims, reclaimee)
			klog.V(5).Infof("[capacity] Current victims: %+v.", victims)
		}
		klog.V(4).Infof("[capacity] Victims from capacity plugin: victims=%+v reclaimer=%s.", victims, reclaimer)
		return victims, util.Permit
	})

	// UnifiedEvictableFn：当前仅处理 GangReclaim 和 GangPreempt 两种驱逐场景
	// 未来会扩展支持传统的任务级抢占/回收
	ssn.AddUnifiedEvictableFn(cp.Name(), func(evictCtx *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		if evictCtx.Kind == api.EvictionKindGangPreempt {
			// GangPreempt 路径：capacity 不筛选受害者（与传统抢占一致，未注册 PreemptableFn），
			// 直接放行所有候选者
			return candidates, util.Permit
		}
		// GangReclaim 路径：对每个受害者队列应用 guarantee 和 deserved 检查。
		// 此处未调用 shouldSkipReclaimee，因为 Gang 与受害者资源维度完全无交集的概率很低，
		// 后续如有需要可补充。
		if !readyToSchedule {
			klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
			return nil, util.Reject
		}
		var victims []*api.TaskInfo
		allocations := map[api.QueueID]*api.Resource{}
		for _, reclaimee := range candidates {
			job := ssn.Jobs[reclaimee.Job]
			attr := cp.queueOpts[job.Queue]
			if _, found := allocations[job.Queue]; !found {
				allocations[job.Queue] = attr.allocated.Clone()
			}
			allocated := allocations[job.Queue]
			if satisfies, _ := cp.checkGuaranteeConstraint(allocated, reclaimee, attr.guarantee); !satisfies {
				continue
			}
			if isVictim, _ := cp.isImmediateVictim(reclaimee, attr.deserved); isVictim {
				allocated.Sub(reclaimee.Resreq)
				victims = append(victims, reclaimee)
				continue
			}
			reclaimable, _ := allocated.GreaterPartlyWithRelevantDimensions(attr.deserved, reclaimee.Resreq)
			if reclaimable {
				allocated.Sub(reclaimee.Resreq)
				victims = append(victims, reclaimee)
			}
		}
		klog.V(4).Infof("[capacity] Victims from capacity UnifiedEvictableFn: victims=%+v", victims)
		return victims, util.Permit
	})

	ssn.AddPreemptiveFn(cp.Name(), func(obj interface{}, candidates []*api.TaskInfo) bool {
		if !readyToSchedule {
			klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
			return false
		}

		queue := obj.(*api.QueueInfo)
		if queue.Queue.Status.State != scheduling.QueueStateOpen {
			klog.V(3).Infof("Queue <%s> current state: %s, is not open state, can not reclaim for tasks.",
				queue.Name, queue.Queue.Status.State)
			return false
		}

		attr := cp.queueOpts[queue.UID]
		totalReq := api.EmptyResource()
		for _, task := range candidates {
			if task != nil {
				totalReq.Add(task.Resreq)
			}
		}
		futureUsed := attr.allocated.Clone().Add(totalReq)

		if allocatable, _ := futureUsed.LessEqualWithDimensionAndResourcesName(attr.realCapability, totalReq); !allocatable {
			klog.V(3).Infof("Queue <%v> cannot reclaim because futureUsed <%v> exceeds realCapability <%v>.",
				queue.Name, futureUsed, attr.realCapability)
			return false
		}

		// 只要存在任意一个资源维度的 deserved 大于 allocated，当前任务就可通过抢占他人来回收资源
		isPreemptive, resourceNames := futureUsed.LessEqualPartlyWithDimensionZeroFiltered(attr.deserved, totalReq)
		if isPreemptive {
			klog.V(3).Infof("Queue <%v> can reclaim on resource dimensions: %v. "+
				"The futureUsed: %v, deserved: %v, allocated: %v, tasks requested: %v",
				queue.Name, resourceNames, futureUsed, attr.deserved, attr.allocated, totalReq)
		} else {
			klog.V(4).Infof("Queue <%v> itself can not reclaim, futureUsed: %v, deserved: %v, requested: %v",
				queue.Name, futureUsed, attr.deserved, totalReq)
			if hierarchyEnabled && cp.ancestorReclaimLevel > 0 {
				for level := 1; level <= cp.ancestorReclaimLevel; level++ {
					ancestorID, found := queueAncestorAtDepth(attr, level)
					if !found || ancestorID == rootQueueID {
						continue
					}
					ancestorAttr := cp.queueOpts[ancestorID]
					if ancestorAttr == nil {
						continue
					}

					futureUsedAncestor := ancestorAttr.allocated.Clone().Add(totalReq)
					isPreemptive, resourceNames = futureUsedAncestor.LessEqualPartlyWithDimensionZeroFiltered(ancestorAttr.deserved, totalReq)
					if isPreemptive {
						klog.V(3).Infof("Queue's ancestor <%v> can reclaim on resource dimensions: %v. "+
							"The futureUsedAncestor: %v, deserved: %v, allocated: %v, task requested: %v",
							ancestorAttr.name, resourceNames, futureUsedAncestor, ancestorAttr.deserved, ancestorAttr.allocated, totalReq)
						break
					}
				}
			}
			if !isPreemptive {
				klog.V(4).Infof("Queue <%v> and its ancestors can not reclaim. futureUsed: %v, deserved: %v, requested: %v",
					queue.Name, futureUsed, attr.deserved, totalReq)
			}
		}

		// PreemptiveFn 的语义与 proportion 插件的 OverusedFn 相反：
		// 只要任一资源维度的 deserved 大于 allocated，就允许当前任务抢占资源
		return isPreemptive
	})

	ssn.AddAllocatableFn(cp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		if queue.Queue.Status.State != scheduling.QueueStateOpen {
			klog.V(3).Infof("Queue <%s> current state: %s, cannot allocate task <%s>.", queue.Name, queue.Queue.Status.State, candidate.Name)
			return false
		}
		if !readyToSchedule {
			klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
			return false
		}
		if hierarchyEnabled && !cp.isLeafQueue(queue.UID) {
			klog.V(3).Infof("Queue <%s> is not a leaf queue, can not allocate task <%s>.", queue.Name, candidate.Name)
			return false
		}

		allocatable := cp.checkQueueAllocatableHierarchically(ssn, queue, candidate)

		// 队列有容量且任务带有 QueueAllocationGate 注解时，将其加入保留缓存以预留容量
		if allocatable && utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
			api.HasQueueAllocationGateAnnotation(candidate.Pod) {
			cp.addTaskToReservedCache(queue.UID, candidate)
		}

		return allocatable
	})

	ssn.AddJobEnqueueableFn(cp.Name(), func(obj interface{}) int {
		if !readyToSchedule {
			klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
			return util.Reject
		}

		job := obj.(*api.JobInfo)
		queueID := job.Queue
		if hierarchyEnabled && !cp.isLeafQueue(queueID) {
			return util.Reject
		}

		attr := cp.queueOpts[queueID]
		queue := ssn.Queues[queueID]
		// 队列未处于 Open 状态时拒绝入队
		if queue.Queue.Status.State != scheduling.QueueStateOpen {
			klog.V(3).Infof("Queue <%s> current state: %s, is not open state, reject job <%s/%s>.",
				queue.Name, queue.Queue.Status.State, job.Namespace, job.Name)
			return util.Reject
		}
		// 未设置 capability 时不对资源做上限检查，允许入队
		if attr.realCapability == nil {
			klog.V(4).Infof("Capability of queue <%s> was not set, allow job <%s/%s> to Inqueue.",
				queue.Name, job.Namespace, job.Name)
			return util.Permit
		}

		if job.PodGroup.Spec.MinResources == nil && !(cp.dynamicResourceAllocationEnable && attr.dra != nil && job.GetMinDRAResources() != nil) {
			klog.V(4).Infof("job %s MinResources is null.", job.Name)
			return util.Permit
		}

		if !cp.checkJobEnqueueableHierarchically(ssn, queue, job) {
			return util.Reject
		}

		// Job 通过入队检查，将其最小资源请求计入 inqueue
		deductedResources := job.DeductSchGatedResources(job.GetMinResources())
		attr.inqueue.Add(deductedResources)
		var minDRAReq map[string]*api.DRAResource
		if cp.dynamicResourceAllocationEnable && attr.dra != nil {
			minDRAReq = job.GetMinDRAResources()
			if minDRAReq != nil {
				updateDRAInqueue(attr.dra, minDRAReq)
			}
		}
		// 若启用层次化队列，将 inqueue 资源同步更新到所有祖先队列
		if hierarchyEnabled {
			for _, ancestorID := range attr.ancestors {
				ancestorAttr := cp.queueOpts[ancestorID]
				ancestorAttr.inqueue.Add(deductedResources)
				if cp.dynamicResourceAllocationEnable && ancestorAttr.dra != nil && minDRAReq != nil {
					updateDRAInqueue(ancestorAttr.dra, minDRAReq)
				}
			}
		}
		klog.V(5).Infof("job <%s/%s> enqueued", job.Namespace, job.Name)
		return util.Permit
	})

	ssn.AddPrePredicateFn(cp.Name(), func(task *api.TaskInfo) error {
		state := &capacityState{
			queueAttrs: make(map[api.QueueID]*queueAttr),
		}

		for _, queue := range cp.queueOpts {
			state.queueAttrs[queue.queueID] = queue.Clone()
		}

		ssn.GetCycleState(task.UID).Write(capacityStateKey, state)
		return nil
	})

	ssn.AddSimulateAddTaskFn(cp.Name(), func(ctx context.Context, cycleState fwk.CycleState, taskToSchedule *api.TaskInfo, taskToAdd *api.TaskInfo, nodeInfo *api.NodeInfo) error {
		state, err := getCapacityState(cycleState)
		if err != nil {
			return fmt.Errorf("failed to get capacity state: %w", err)
		}

		job := ssn.Jobs[taskToAdd.Job]
		if job == nil {
			return fmt.Errorf("[capacity] job %s not found in session (orphaned task from deleted PodGroup)", taskToAdd.Job)
		}
		attr := state.queueAttrs[job.Queue]
		if attr == nil {
			return fmt.Errorf("[capacity] queue %s not found", job.Queue)
		}
		attr.allocated.Add(taskToAdd.Resreq)
		if cp.dynamicResourceAllocationEnable && attr.dra != nil && taskToAdd.DRAResreq != nil {
			addTaskDRAAllocated(attr, taskToAdd)
		}
		updateQueueAttrShare(attr)
		if hierarchyEnabled {
			for _, ancestorID := range attr.ancestors {
				ancestorAttr := state.queueAttrs[ancestorID]
				ancestorAttr.allocated.Add(taskToAdd.Resreq)
				if cp.dynamicResourceAllocationEnable && ancestorAttr.dra != nil && taskToAdd.DRAResreq != nil {
					addTaskDRAAllocated(ancestorAttr, taskToAdd)
				}
			}
		}
		return nil
	})

	ssn.AddSimulateRemoveTaskFn(cp.Name(), func(ctx context.Context, cycleState fwk.CycleState, taskToSchedule *api.TaskInfo, taskToRemove *api.TaskInfo, nodeInfo *api.NodeInfo) error {
		state, err := getCapacityState(cycleState)
		if err != nil {
			return fmt.Errorf("failed to get capacity state: %w", err)
		}
		job := ssn.Jobs[taskToRemove.Job]
		if job == nil {
			return fmt.Errorf("[capacity] job %s not found in session (orphaned task from deleted PodGroup)", taskToRemove.Job)
		}
		attr := state.queueAttrs[job.Queue]
		if attr == nil {
			return fmt.Errorf("[capacity] queue %s not found", job.Queue)
		}
		attr.allocated.Sub(taskToRemove.Resreq)
		if cp.dynamicResourceAllocationEnable && attr.dra != nil && taskToRemove.DRAResreq != nil {
			removeTaskDRAAllocated(attr, taskToRemove)
		}
		updateQueueAttrShare(attr)
		if hierarchyEnabled {
			for _, ancestorID := range attr.ancestors {
				ancestorAttr := state.queueAttrs[ancestorID]
				ancestorAttr.allocated.Sub(taskToRemove.Resreq)
				if cp.dynamicResourceAllocationEnable && ancestorAttr.dra != nil && taskToRemove.DRAResreq != nil {
					removeTaskDRAAllocated(ancestorAttr, taskToRemove)
				}
			}
		}
		return nil
	})

	ssn.AddSimulateAllocatableFn(cp.Name(), func(ctx context.Context, cycleState fwk.CycleState, queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		state, err := getCapacityState(cycleState)
		if err != nil {
			return false
		}

		if !readyToSchedule {
			klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
			return false
		}
		if hierarchyEnabled && !cp.isLeafQueue(queue.UID) {
			klog.V(3).Infof("Queue <%s> is not a leaf queue, can not allocate task <%s>.", queue.Name, candidate.Name)
			return false
		}

		simulateQueueAllocatable := func(state *capacityState, queue *api.QueueInfo, candidate *api.TaskInfo) bool {
			attr := state.queueAttrs[queue.UID]
			return cp.queueAllocatableWithReserved(attr, candidate, queue, cp.dynamicResourceAllocationEnable, cp.draConsumableCapacityEnable)
		}

		list := append(state.queueAttrs[queue.UID].ancestors, queue.UID)
		for i := len(list) - 1; i >= 0; i-- {
			if !simulateQueueAllocatable(state, ssn.Queues[list[i]], candidate) {
				if klog.V(5).Enabled() {
					for i--; i >= 0; i-- {
						simulateQueueAllocatable(state, ssn.Queues[list[i]], candidate)
					}
				}
				return false
			}
		}
		return true
	})

	// --------------------------------------------------------------------------
	// 注册事件处理器：AllocateFunc / DeallocateFunc
	// --------------------------------------------------------------------------
	// 这些回调在任务实际被分配或释放时触发，更新真实的队列资源跟踪状态
	// 与 Simulate 回调的区别：Simulate 修改 CycleState 快照，EventHandler 修改真实状态
	// --------------------------------------------------------------------------
	ssn.AddEventHandler(&framework.EventHandler{
		// AllocateFunc：任务被分配时触发
		// 1. 将任务资源添加到队列的 allocated
		// 2. 更新 DRA allocated（如果启用）
		// 3. 更新队列 share 和指标
		// 4. 如果启用层次化队列，同步更新所有祖先队列
		// 5. 从保留缓存中移除该任务
		AllocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				klog.Warningf("[capacity] Skip allocate event for task <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					event.Task.Namespace, event.Task.Name, event.Task.Job)
				return
			}
			attr := cp.queueOpts[job.Queue]
			if attr == nil {
				klog.Warningf("[capacity] Skip allocate event for task <%s/%s>: queue <%s> not found in queueOpts",
					event.Task.Namespace, event.Task.Name, job.Queue)
				return
			}
			attr.allocated.Add(event.Task.Resreq)
			if cp.dynamicResourceAllocationEnable && attr.dra != nil && event.Task.DRAResreq != nil {
				addTaskDRAAllocated(attr, event.Task)
			}
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory, attr.allocated.ScalarResources)

			cp.updateShare(attr)
			if hierarchyEnabled {
				for _, ancestorID := range attr.ancestors {
					ancestorAttr := cp.queueOpts[ancestorID]
					ancestorAttr.allocated.Add(event.Task.Resreq)
					if cp.dynamicResourceAllocationEnable && ancestorAttr.dra != nil && event.Task.DRAResreq != nil {
						addTaskDRAAllocated(ancestorAttr, event.Task)
					}
				}
			}

			klog.V(4).Infof("[capacity] AllocateFunc: task <%v/%v>, resreq <%v>, share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)

			// 从保留缓存中移除已分配的任务
			if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) {
				cp.removeTaskFromReservedCache(event.Task.UID)
			}
		},
		// DeallocateFunc：任务被释放时触发（调度回滚）
		// 与 AllocateFunc 相反，从队列的 allocated 中扣除任务资源
		// 如果任务有 QueueAllocationGate 注解，将其重新加入保留缓存
		DeallocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				klog.Warningf("[capacity] Skip deallocate event for task <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					event.Task.Namespace, event.Task.Name, event.Task.Job)
				return
			}
			attr := cp.queueOpts[job.Queue]
			if attr == nil {
				klog.Warningf("[capacity] Skip deallocate event for task <%s/%s>: queue <%s> not found in queueOpts",
					event.Task.Namespace, event.Task.Name, job.Queue)
				return
			}
			attr.allocated.Sub(event.Task.Resreq)
			if cp.dynamicResourceAllocationEnable && attr.dra != nil && event.Task.DRAResreq != nil {
				removeTaskDRAAllocated(attr, event.Task)
			}
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory, attr.allocated.ScalarResources)

			cp.updateShare(attr)
			if hierarchyEnabled {
				for _, ancestorID := range attr.ancestors {
					ancestorAttr := cp.queueOpts[ancestorID]
					ancestorAttr.allocated.Sub(event.Task.Resreq)
					if cp.dynamicResourceAllocationEnable && ancestorAttr.dra != nil && event.Task.DRAResreq != nil {
						removeTaskDRAAllocated(ancestorAttr, event.Task)
					}
				}
			}

			klog.V(4).Infof("[capacity] DeallocateFunc: task <%v/%v>, resreq <%v>, share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)

			// 调度回滚时，将任务重新加入保留缓存以保留容量计数
			if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
				api.HasQueueAllocationGateAnnotation(event.Task.Pod) {
				cp.addTaskToReservedCache(job.Queue, event.Task)
			}
		},
	})
}

// parseArguments 解析插件配置参数
// 目前支持参数：
//   - ancestorReclaimLevel: 祖先回收层级，控制回收时检查多少层祖先队列（默认 0）
func (cp *capacityPlugin) parseArguments() {
	ancestorReclaimLevel := 0
	cp.pluginArguments.GetInt(&ancestorReclaimLevel, ancestorReclaimLevelKey)

	if ancestorReclaimLevel < 0 {
		klog.Warningf("%s should be non-negative, got %d. Falling back to 0.", ancestorReclaimLevelKey, ancestorReclaimLevel)
		ancestorReclaimLevel = 0
	}

	cp.ancestorReclaimLevel = ancestorReclaimLevel
	klog.V(4).Infof("[capacity] reclaim ancestor level configured as %d", cp.ancestorReclaimLevel)
}

// getReclaimeeAncestorToCheck 获取回收检查中需要检查的 reclaimee 祖先队列
//
// 参数：
//   - reclaimerAttr: 回收方（reclaimer）的队列属性
//   - reclaimeeAttr: 被回收方（reclaimee）的队列属性
//   - level: 要检查的祖先层级
//
// 返回值：
//   - *queueAttr: 需要检查的祖先队列属性（nil 表示无需检查或未找到）
//   - bool: 是否需要检查（false 表示跳过此层级）
//
// 跳过检查的情况：
//  1. reclaimee 的祖先为根队列（根队列不需要检查）
//  2. reclaimer 和 reclaimee 在该层级共享同一祖先（属于同一家族，不回收）
func (cp *capacityPlugin) getReclaimeeAncestorToCheck(reclaimerAttr, reclaimeeAttr *queueAttr, level int) (*queueAttr, bool) {
	if reclaimerAttr == nil || reclaimeeAttr == nil || level <= 0 {
		return nil, false
	}

	reclaimerAncestors := ancestorsByLevel(reclaimerAttr, level)
	reclaimeeAncestors := ancestorsByLevel(reclaimeeAttr, level)

	reclaimeeAncestorID, reclaimeeFound := reclaimeeAncestors[level]
	if !reclaimeeFound || reclaimeeAncestorID == rootQueueID {
		return nil, false
	}

	reclaimerAncestorID, reclaimerFound := reclaimerAncestors[level]
	if reclaimerFound && reclaimerAncestorID == reclaimeeAncestorID {
		return nil, false
	}

	ancestorAttr := cp.queueOpts[reclaimeeAncestorID]
	if ancestorAttr == nil {
		return nil, true
	}

	return ancestorAttr, true
}

// OnSessionClose 在调度会话结束时调用
// 更新所有队列的 overused 指标，清理插件状态
func (cp *capacityPlugin) OnSessionClose(ssn *framework.Session) {
	for _, attr := range cp.queueOpts {
		overused := attr.share > 1 // share > 1 表示队列已超额使用
		metrics.UpdateQueueOverused(attr.name, overused)
	}
	cp.totalResource = nil
	cp.totalGuarantee = nil
	cp.queueOpts = nil
	cp.queueGateReservedTasks = nil
}

// buildQueueAttrs 构建扁平队列属性（非层次化模式）
//
// 工作流程：
//  1. 计算集群总保障资源（totalGuarantee）
//  2. 遍历所有 Job，为每个 Job 所在队列初始化 queueAttr
//  3. 统计各队列的 allocated/request/inqueue/elastic 资源
//  4. 计算 realCapability = min(capability, totalResource - totalGuarantee + guarantee)
//  5. 确保 deserved >= guarantee（保障资源不能低于已承诺的保障）
//  6. 更新 share 并记录指标
//  7. 注册 QueueOrderFn：按优先级 + share 排序
func (cp *capacityPlugin) buildQueueAttrs(ssn *framework.Session) {
	// 计算集群总保障资源：所有队列 guarantee 之和
	for _, queue := range ssn.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
			continue
		}
		guarantee := api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		cp.totalGuarantee.Add(guarantee)
	}
	klog.V(4).Infof("The total guarantee resource is <%v>", cp.totalGuarantee)
	// 遍历所有 Job，构建队列属性
	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		// 首次遇到某队列时，初始化其属性
		if _, found := cp.queueOpts[job.Queue]; !found {
			queue := ssn.Queues[job.Queue]
			attr := &queueAttr{
				queueID: queue.UID,
				name:    queue.Name,

				deserved:          api.NewResource(queue.Queue.Spec.Deserved),
				allocated:         api.EmptyResource(),
				request:           api.EmptyResource(),
				elastic:           api.EmptyResource(),
				inqueue:           api.EmptyResource(),
				guarantee:         api.EmptyResource(),
				resourceClaimRefs: make(map[string]int),
			}
			if len(queue.Queue.Spec.Capability) != 0 {
				attr.capability = api.NewResource(queue.Queue.Spec.Capability)
				// 未设置 CPU/Memory 上限时，设为 MaxFloat64（即不限制该维度）
				if attr.capability.MilliCPU <= 0 {
					attr.capability.MilliCPU = math.MaxFloat64
				}
				if attr.capability.Memory <= 0 {
					attr.capability.Memory = math.MaxFloat64
				}
			}
			// 初始化 DRA 配额属性
			attr.dra = newDRAQuotaAttr(queue.Queue.Spec.Capability, queue.Queue.Spec.Deserved, queue.Queue.Spec.Guarantee.Resource)
			if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
				attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
			}
			// 计算 realCapability = min(capability, totalResource - totalGuarantee + guarantee)
			// 逻辑：集群剩余可分配资源 = 总资源 - 所有队列保障之和 + 当前队列保障
			// 即：当前队列可用的非保障资源 + 自身保障
			realCapability := api.ExceededPart(cp.totalResource, cp.totalGuarantee).Add(attr.guarantee)
			if attr.capability == nil {
				attr.capability = api.EmptyResource()
				attr.realCapability = realCapability // 无 capability 限制时，realCapability = 集群可用资源
			} else {
				realCapability.MinDimensionResource(attr.capability, api.Infinity) // 取 capability 和可用资源的较小值
				attr.realCapability = realCapability
			}
			cp.queueOpts[job.Queue] = attr
			klog.V(4).Infof("Added Queue <%s> attributes.", job.Queue)
		}

		// 统计已分配和请求中的任务资源
		attr := cp.queueOpts[job.Queue]
		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				// 已分配状态的任务计入 allocated 和 request
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
					attr.request.Add(t.Resreq)
					if cp.dynamicResourceAllocationEnable && attr.dra != nil && t.DRAResreq != nil {
						addTaskDRAAllocated(attr, t)
					}
				}
			} else if status == api.Pending {
				for _, t := range tasks {
					attr.request.Add(t.Resreq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			// 计算 Inqueue 阶段 Job 的 inqueue 资源
			// 扣除已分配任务资源以避免重复计数：
			// Allocated/Binding 状态的任务已通过 AllocatedStatus 计入 attr.allocated，
			// 但 PodGroup 在所有任务达到 Running/Bound 之前仍保持 Inqueue 状态
			// 不扣除会导致同一资源同时出现在 attr.allocated 和 attr.inqueue 中
			if job.PodGroup.Spec.MinResources != nil {
				inqueued := util.GetInqueueResource(job, job.Allocated)
				attr.inqueue.Add(job.DeductSchGatedResources(inqueued))
			}
		}

		// 计算 Running 阶段 Job 的 inqueue 资源（扁平模式）
		// 判断条件 'job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember' 适用于以下场景：
		// 当 Spark Job 完成（driver pod 已完成）但 PodGroup 仍保持 Running 状态时，
		// 如果不加此判断，已分配资源会被再次预留
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			inqueued := util.GetInqueueResource(job, job.Allocated)
			attr.inqueue.Add(job.DeductSchGatedResources(inqueued))
		}
		attr.elastic.Add(job.GetElasticResources())
		klog.V(5).Infof("Queue %s allocated <%s> request <%s> inqueue <%s> elastic <%s>",
			attr.name, attr.allocated.String(), attr.request.String(), attr.inqueue.String(), attr.elastic.String())
	}

	// 最终调整：确保 deserved 不超过 realCapability，且 deserved >= guarantee
	for _, attr := range cp.queueOpts {
		if attr.realCapability != nil {
			attr.deserved.MinDimensionResource(attr.realCapability, api.Infinity) // deserved 不能超过 realCapability
		}

		attr.deserved = helpers.Max(attr.deserved, attr.guarantee) // deserved 至少等于 guarantee
		cp.updateShare(attr)
		klog.V(4).Infof("The attributes of queue <%s> in capacity: deserved <%v>, realCapability <%v>, allocate <%v>, request <%v>, elastic <%v>, share <%0.2f>",
			attr.name, attr.deserved, attr.realCapability, attr.allocated, attr.request, attr.elastic, attr.share)
	}

	// 记录指标（包括尚未分配任何 Job 的队列）
	for queueID, queueInfo := range ssn.Queues {
		queue := ssn.Queues[queueID]
		if attr, ok := cp.queueOpts[queueID]; ok {
			metrics.UpdateQueueDeserved(attr.name, attr.deserved.MilliCPU, attr.deserved.Memory, attr.deserved.ScalarResources)
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory, attr.allocated.ScalarResources)
			metrics.UpdateQueueRequest(attr.name, attr.request.MilliCPU, attr.request.Memory, attr.request.ScalarResources)
			if attr.capability != nil {
				metrics.UpdateQueueCapacity(attr.name, attr.capability.MilliCPU, attr.capability.Memory, attr.capability.ScalarResources)
			}
			metrics.UpdateQueueRealCapacity(attr.name, attr.realCapability.MilliCPU, attr.realCapability.Memory, attr.realCapability.ScalarResources)
			continue
		}
		deservedCPU, deservedMem, scalarResources := 0.0, 0.0, map[v1.ResourceName]float64{}
		if queue.Queue.Spec.Deserved != nil {
			attr := api.NewResource(queue.Queue.Spec.Deserved)
			deservedCPU = attr.MilliCPU
			deservedMem = attr.Memory
			scalarResources = attr.ScalarResources
		}
		metrics.UpdateQueueDeserved(queueInfo.Name, deservedCPU, deservedMem, scalarResources)
		metrics.UpdateQueueAllocated(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		metrics.UpdateQueueRequest(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		guarantee := api.EmptyResource()
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		}
		realCapacity := api.ExceededPart(cp.totalResource, cp.totalGuarantee).Add(guarantee)
		if len(queue.Queue.Spec.Capability) > 0 {
			capacity := api.NewResource(queue.Queue.Spec.Capability)
			realCapacity.MinDimensionResource(capacity, api.Infinity)
			metrics.UpdateQueueCapacity(queueInfo.Name, capacity.MilliCPU, capacity.Memory, capacity.ScalarResources)
		}
		metrics.UpdateQueueRealCapacity(queueInfo.Name, realCapacity.MilliCPU, realCapacity.Memory, realCapacity.ScalarResources)
	}

	// 注册 QueueOrderFn：队列排序回调（扁平模式）
	// 排序规则：
	//   1. 优先级高的队列优先（Priority 降序）
	//   2. 优先级相同时，share 值小的队列优先（DRF 公平性）
	//   3. share 相同时，有 deserved 的队列优先于 best-effort 队列
	ssn.AddQueueOrderFn(cp.Name(), func(l, r interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		if lv.Queue.Spec.Priority != rv.Queue.Spec.Priority {
			// 返回负数表示 lv 优先级更高
			return int(rv.Queue.Spec.Priority) - int(lv.Queue.Spec.Priority)
		}

		return cp.compareShareWithDeserved(cp.queueOpts[lv.UID], cp.queueOpts[rv.UID])
	})
}

// buildHierarchicalQueueAttrs 构建层次化队列属性（树形队列模式）
//
// 与 buildQueueAttrs 的区别：
//  1. 队列具有父子关系，形成树形结构
//  2. 父队列的 allocated/inqueue 等于所有子队列对应值之和
//  3. 子队列的 realCapability 受父队列约束
//  4. 额外注册 VictimQueueOrderFn（受害者队列排序）
//
// 返回值：false 表示层次结构检查失败，调度应中止
func (cp *capacityPlugin) buildHierarchicalQueueAttrs(ssn *framework.Session) bool {
	// 设置根队列
	cp.rootQueue = rootQueueID

	// 初始化所有队列属性并建立层次关系
	for _, queue := range ssn.Queues {
		_, found := cp.queueOpts[queue.UID]
		if found {
			continue
		}

		attr := cp.newQueueAttr(queue)
		cp.queueOpts[queue.UID] = attr
		visited := make(map[api.QueueID]struct{})
		err := cp.updateAncestors(queue, ssn, visited)
		if err != nil {
			klog.Errorf("Failed to update Queue <%s> attributes, error: %v", queue.Name, err)
			return false
		}
	}

	// 遍历所有 Job，统计叶子队列的资源并传播到祖先队列
	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		attr := cp.queueOpts[job.Queue]
		if attr == nil {
			klog.Warningf("Job <%s/%s> references queue %s which is not found in session queues, skipping",
				job.Namespace, job.Name, job.Queue)
			continue
		}
		// 层次化队列约束：Job 只能属于叶子队列
		if len(attr.children) > 0 {
			klog.Errorf("The Queue <%s> of Job <%s/%s> is not leaf queue", attr.name, job.Namespace, job.Name)
			return false
		}

		// 保存旧值用于计算增量，稍后传播到祖先队列
		oldAllocated := attr.allocated.Clone()
		oldRequest := attr.request.Clone()
		oldInqueue := attr.inqueue.Clone()
		oldElastic := attr.elastic.Clone()
		var oldDRA *draQuotaAttr
		if attr.dra != nil {
			oldDRA = attr.dra.Clone()
		}

		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
					attr.request.Add(t.Resreq)
					if cp.dynamicResourceAllocationEnable && attr.dra != nil && t.DRAResreq != nil {
						addTaskDRAAllocated(attr, t)
					}
				}
			} else if status == api.Pending {
				for _, t := range tasks {
					attr.request.Add(t.Resreq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			// 同扁平模式的重复计数修复：扣除已分配资源，避免 allocated 和 inqueue 双重计算
			if job.PodGroup.Spec.MinResources != nil {
				inqueued := util.GetInqueueResource(job, job.Allocated)
				attr.inqueue.Add(job.DeductSchGatedResources(inqueued))
			}
		}

		// 计算 Running 阶段 Job 的 inqueue 资源（层次化模式）
		// 逻辑同扁平模式
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			inqueued := util.GetInqueueResource(job, job.Allocated)
			attr.inqueue.Add(job.DeductSchGatedResources(inqueued))
		}
		attr.elastic.Add(job.GetElasticResources())

		// 计算增量并传播到所有祖先队列
		allocatedDelta := attr.allocated.Clone().Sub(oldAllocated)
		requestDelta := attr.request.Clone().Sub(oldRequest)
		inqueueDelta := attr.inqueue.Clone().Sub(oldInqueue)
		elasticDelta := attr.elastic.Clone().Sub(oldElastic)

		var draDelta map[string]*api.DRAResource
		if attr.dra != nil {
			draDelta = getDRADelta(attr.dra, oldDRA)
		}

		for _, ancestor := range attr.ancestors {
			ancestorAttr := cp.queueOpts[ancestor]
			ancestorAttr.allocated.Add(allocatedDelta)
			ancestorAttr.request.Add(requestDelta)
			ancestorAttr.inqueue.Add(inqueueDelta)
			ancestorAttr.elastic.Add(elasticDelta)
			if cp.dynamicResourceAllocationEnable && ancestorAttr.dra != nil && draDelta != nil {
				// 只传播祖先队列 capability 中配置了的 DeviceClass 的增量
				// 未在祖先中配置的 DeviceClass 不向上传播
				filteredDelta := make(map[string]*api.DRAResource)
				for dc, res := range draDelta {
					if _, ok := ancestorAttr.dra.capability[dc]; ok {
						filteredDelta[dc] = res
					}
				}
				if len(filteredDelta) > 0 {
					updateDRAAllocated(ancestorAttr.dra, filteredDelta)
				}
			}
		}

		klog.V(5).Infof("Queue %s allocated <%s> request <%s> inqueue <%s> elastic <%s>",
			attr.name, attr.allocated.String(), attr.request.String(), attr.inqueue.String(), attr.elastic.String())
	}

	// 初始化根队列：
	//   - realCapability = capability（根队列无上限约束）
	//   - 如果未设置 capability，设为无穷大（用户可以提交超过集群总资源的 Job，
	//     根队列不应限制，用户应通过手动创建子队列来限制资源）
	//   - 根队列的 guarantee 和 deserved 由所有子队列之和决定
	rootQueueAttr := cp.queueOpts[api.QueueID(cp.rootQueue)]
	if rootQueueAttr == nil {
		klog.Warningf("Root queue %q not found in session queues, skipping hierarchy initialization", cp.rootQueue)
		return false
	}
	infinityResource := api.InfiniteResource()
	if cp.totalResource.ScalarResources != nil {
		for k := range cp.totalResource.ScalarResources {
			infinityResource.SetScalar(k, math.MaxInt64)
		}
	}
	if rootQueueAttr.capability.IsEmpty() {
		rootQueueAttr.capability = infinityResource
	}
	rootQueueAttr.realCapability = rootQueueAttr.capability
	// checkHierarchicalQueue 仅记录警告，永不返回错误
	// 避免因配置问题中止整个调度周期
	cp.checkHierarchicalQueue(rootQueueAttr)

	// 更新所有队列的 share 值
	for _, attr := range cp.queueOpts {
		cp.updateShare(attr)
		klog.V(4).Infof("The attributes of queue <%s> in capacity: deserved <%v>, realCapability <%v>, allocate <%v>, request <%v>, elastic <%v>, share <%0.2f>",
			attr.name, attr.deserved, attr.realCapability, attr.allocated, attr.request, attr.elastic, attr.share)
	}

	// 记录指标
	for queueID := range ssn.Queues {
		attr := cp.queueOpts[queueID]
		metrics.UpdateQueueDeserved(attr.name, attr.deserved.MilliCPU, attr.deserved.Memory, attr.deserved.ScalarResources)
		metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory, attr.allocated.ScalarResources)
		metrics.UpdateQueueRequest(attr.name, attr.request.MilliCPU, attr.request.Memory, attr.request.ScalarResources)
		metrics.UpdateQueueCapacity(attr.name, attr.capability.MilliCPU, attr.capability.Memory, attr.capability.ScalarResources)
		metrics.UpdateQueueRealCapacity(attr.name, attr.realCapability.MilliCPU, attr.realCapability.Memory, attr.realCapability.ScalarResources)
	}

	// 注册 QueueOrderFn：队列排序回调（层次化模式）
	// 排序规则：
	//   1. 优先级高的队列优先
	//   2. 叶子队列优先于非叶子队列
	//   3. 两个叶子队列比较时，使用最近共同祖先的下一层父队列的 share 排序
	//   4. 两个非叶子队列比较时，使用自身 share 排序
	ssn.AddQueueOrderFn(cp.Name(), func(l, r interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		if lv.Queue.Spec.Priority != rv.Queue.Spec.Priority {
			// 返回负数表示 lv 优先级更高
			return int(rv.Queue.Spec.Priority) - int(lv.Queue.Spec.Priority)
		}

		lvLeaf := cp.isLeafQueue(lv.UID)
		rvLeaf := cp.isLeafQueue(rv.UID)

		// 叶子队列优先于非叶子队列（任务只能分配到叶子队列）
		if lvLeaf && !rvLeaf {
			return -1
		} else if !lvLeaf && rvLeaf {
			return 1
		} else if !lvLeaf && !rvLeaf {
			// 两个非叶子队列按 share 排序
			return cp.compareShareWithDeserved(cp.queueOpts[lv.UID], cp.queueOpts[rv.UID])
		}

		// 两个叶子队列：找到最近共同祖先，比较其下一层父队列的 share
		// 这确保同一父队列下的子队列按公平性排序
		lvAttr := cp.queueOpts[lv.UID]
		rvAttr := cp.queueOpts[rv.UID]
		level := getQueueLevel(lvAttr, rvAttr) // 最近共同祖先层级
		lvParentID := lvAttr.queueID
		rvParentID := rvAttr.queueID
		if level+1 < len(lvAttr.ancestors) {
			lvParentID = lvAttr.ancestors[level+1]
		}
		if level+1 < len(rvAttr.ancestors) {
			rvParentID = rvAttr.ancestors[level+1]
		}

		return cp.compareShareWithDeserved(cp.queueOpts[lvParentID], cp.queueOpts[rvParentID])
	})

	// 注册 VictimQueueOrderFn：受害者队列排序回调
	// 在抢占/回收时决定从哪个队列选择受害者
	// 排序规则：与抢占者层次距离越远的队列优先被选中（更远 = 更不相关 = 优先剥夺）
	ssn.AddVictimQueueOrderFn(cp.Name(), func(l, r, preemptor interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)
		pv := preemptor.(*api.QueueInfo)

		// 计算两个候选受害者队列与抢占者的最近共同祖先层级
		lLevel := getQueueLevel(cp.queueOpts[lv.UID], cp.queueOpts[pv.UID])
		rLevel := getQueueLevel(cp.queueOpts[rv.UID], cp.queueOpts[pv.UID])

		// 层级越大（距离越远）越优先被选为受害者
		if lLevel == rLevel {
			return 0 // 同层级，无偏好
		}

		if lLevel > rLevel {
			return -1 // l 距离更远，优先选 l
		}

		return 1 // r 距离更远，优先选 r
	})

	return true
}

// newQueueAttr 创建新的队列属性（层次化模式）
// 与扁平模式的区别：初始化 ancestors/children 结构，capability/realCapability 默认为空
// 稍后由 checkHierarchicalQueue 计算和填充
func (cp *capacityPlugin) newQueueAttr(queue *api.QueueInfo) *queueAttr {
	attr := &queueAttr{
		queueID:   queue.UID,
		name:      queue.Name,
		ancestors: make([]api.QueueID, 0),
		children:  make(map[api.QueueID]*queueAttr),

		deserved:          api.NewResource(queue.Queue.Spec.Deserved),
		allocated:         api.EmptyResource(),
		request:           api.EmptyResource(),
		elastic:           api.EmptyResource(),
		inqueue:           api.EmptyResource(),
		guarantee:         api.EmptyResource(),
		capability:        api.EmptyResource(),
		realCapability:    api.EmptyResource(),
		resourceClaimRefs: make(map[string]int),
	}
	if len(queue.Queue.Spec.Capability) != 0 {
		attr.capability = api.NewResource(queue.Queue.Spec.Capability)
	}

	if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
		attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
	}

	attr.dra = newDRAQuotaAttr(queue.Queue.Spec.Capability, queue.Queue.Spec.Deserved, queue.Queue.Spec.Guarantee.Resource)

	return attr
}

// updateAncestors 递归构建队列的层次关系
//
// 工作流程：
//  1. 检测循环依赖（防止 A→B→A 等环状结构）
//  2. 确定父队列（未指定 Parent 时默认为 root）
//  3. 验证父队列存在
//  4. 递归初始化父队列属性
//  5. 建立父-子关系：children 映射和 ancestors 列表
func (cp *capacityPlugin) updateAncestors(queue *api.QueueInfo, ssn *framework.Session, visited map[api.QueueID]struct{}) error {
	if queue.Name == cp.rootQueue {
		return nil
	}

	// 检测队列层次结构中的循环依赖
	if _, exist := visited[queue.UID]; exist {
		return fmt.Errorf("cycle detected in queue hierarchy for queue %s", queue.Name)
	}
	visited[queue.UID] = struct{}{}
	defer delete(visited, queue.UID)

	// 确定父队列：未指定 Parent 时默认为 root
	parent := cp.rootQueue
	if queue.Queue.Spec.Parent != "" {
		parent = queue.Queue.Spec.Parent
	}
	if _, exist := ssn.Queues[api.QueueID(parent)]; !exist {
		return fmt.Errorf("the queue %s has invalid parent queue %s", queue.Name, parent)
	}

	parentInfo := ssn.Queues[api.QueueID(parent)]
	if _, found := cp.queueOpts[parentInfo.UID]; !found {
		parentAttr := cp.newQueueAttr(parentInfo)
		cp.queueOpts[parentAttr.queueID] = parentAttr
		err := cp.updateAncestors(parentInfo, ssn, visited)
		if err != nil {
			return err
		}
	}

	// 将当前队列加入父队列的 children 映射
	cp.queueOpts[parentInfo.UID].children[queue.UID] = cp.queueOpts[queue.UID]
	// 将父队列加入当前队列的 ancestors 列表
	cp.queueOpts[queue.UID].ancestors = append(cp.queueOpts[parentInfo.UID].ancestors, parentInfo.UID)
	return nil
}

// checkHierarchicalQueue 递归检查和修正层次化队列的配额关系
//
// 主要工作：
//  1. 确保 deserved >= guarantee（每个子队列）
//  2. 继承父队列的 capability：子队列未设置的维度继承父队列值
//  3. 检查子队列 capability 是否超过父队列
//  4. 设置根队列的 guarantee/deserved 为所有子队列之和
//  5. 计算子队列的 realCapability = min(capability, parentRealCapability - siblingGuaranteeSum + guarantee)
//  6. 检查父队列 deserved/guarantee 是否小于子队列之和
//  7. 递归检查所有子队列
//
// 注意：此函数只记录警告，不返回错误，避免因配置问题中止调度
func (cp *capacityPlugin) checkHierarchicalQueue(attr *queueAttr) {
	// 汇总所有子队列的 guarantee 和 deserved
	totalGuarantee := api.EmptyResource()
	totalDeserved := api.EmptyResource()
	for _, childAttr := range attr.children {
		childAttr.deserved = helpers.Max(childAttr.deserved, childAttr.guarantee) // deserved 不能低于 guarantee
		totalDeserved.Add(childAttr.deserved)
		totalGuarantee.Add(childAttr.guarantee)
		// 如果用户未设置 CPU 或 Memory 的 capability，继承父队列的值
		// （不考虑用户设置 <=0 的情况）
		if childAttr.capability.MilliCPU <= 0 {
			childAttr.capability.MilliCPU = attr.capability.MilliCPU
		}
		if childAttr.capability.Memory <= 0 {
			childAttr.capability.Memory = attr.capability.Memory
		}

		// 从父队列继承标量资源：子队列未设置的维度继承父队列值
		if attr.capability.ScalarResources != nil {
			if childAttr.capability.ScalarResources == nil {
				childAttr.capability.ScalarResources = make(map[v1.ResourceName]float64)
			}
			for k, v := range attr.capability.ScalarResources {
				if _, exists := childAttr.capability.ScalarResources[k]; !exists {
					childAttr.capability.ScalarResources[k] = v
				}
			}
		}

		// 检查子队列 capability 是否超过父队列
		if exceeds, resources := childAttr.capability.GreaterPartly(attr.capability, api.Infinity); exceeds {
			klog.Errorf("Child queue %s capability (%s) exceeds parent queue %s capability (%s) on resources %v. "+
				"Child's effective capability will be limited by parent.",
				childAttr.name, childAttr.capability, attr.name, attr.capability, resources)
		}
	}

	// 根队列的 guarantee 和 deserved 由所有子队列之和决定
	if attr.name == cp.rootQueue {
		attr.guarantee = totalGuarantee
		attr.deserved = totalDeserved
	}

	for _, childAttr := range attr.children {
		realCapability := api.ExceededPart(attr.realCapability, totalGuarantee).Add(childAttr.guarantee)
		if childAttr.capability == nil {
			childAttr.capability = api.EmptyResource()
			childAttr.realCapability = realCapability
		} else {
			realCapability.MinDimensionResource(childAttr.capability, api.Infinity)
			childAttr.realCapability = realCapability
		}
	}

	// 检查父队列 deserved 是否小于子队列 deserved 之和
	if attr.deserved.LessPartly(totalDeserved, api.Zero) {
		klog.Errorf("Sum of child queue deserved (%s) exceeds parent queue %s deserved (%s). "+
			"This may affect resource distribution during scheduling.",
			totalDeserved, attr.name, attr.deserved)
	}

	// 检查父队列 guarantee 是否小于子队列 guarantee 之和
	if attr.guarantee.LessPartly(totalGuarantee, api.Zero) {
		klog.Errorf("Sum of child queue guarantees (%s) exceeds parent queue %s guarantee (%s). "+
			"Not all child guarantees can be satisfied simultaneously.",
			totalGuarantee, attr.name, attr.guarantee)
	}

	// 递归检查所有子队列
	for _, childAttr := range attr.children {
		cp.checkHierarchicalQueue(childAttr)
	}
}

// compareShareWithDeserved 比较两个队列的调度优先级
//
// 排序规则：
//  1. share 值小的队列优先（DRF 公平性）
//  2. share 相同时，有 deserved 的队列优先于 best-effort（无 deserved）队列
//
// 返回值：负数表示 lattr 优先级更高
func (cp *capacityPlugin) compareShareWithDeserved(lattr, rattr *queueAttr) int {
	if lattr.share == rattr.share {
		lHasDeserved := !lattr.deserved.IsEmpty()
		rHasDeserved := !rattr.deserved.IsEmpty()
		if lHasDeserved == rHasDeserved {
			return 0
		}
		if lHasDeserved {
			return -1
		}
		return 1
	}

	if lattr.share < rattr.share {
		return -1
	}
	return 1
}

// updateShare 更新队列的 share 值并记录指标
func (cp *capacityPlugin) updateShare(attr *queueAttr) {
	updateQueueAttrShare(attr)
	metrics.UpdateQueueShare(attr.name, attr.share)
}

// isLeafQueue 判断队列是否为叶子队列（无子队列）
// 层次化队列中，只有叶子队列可以分配任务
func (cp *capacityPlugin) isLeafQueue(queueID api.QueueID) bool {
	return len(cp.queueOpts[queueID].children) == 0
}

// queueAllocatable 检查队列是否有容量分配任务（单队列级别）
func (cp *capacityPlugin) queueAllocatable(queue *api.QueueInfo, candidate *api.TaskInfo, draEnabled bool, consumableCapacityEnabled bool) bool {
	attr := cp.queueOpts[queue.UID]
	return cp.queueAllocatableWithReserved(attr, candidate, queue, draEnabled, consumableCapacityEnabled)
}

// addTaskToReservedCache 将任务添加到保留缓存
// 在任务通过容量检查时调用，预留队列容量
func (cp *capacityPlugin) addTaskToReservedCache(queueID api.QueueID, task *api.TaskInfo) {
	if cp.queueGateReservedTasks[queueID] == nil {
		cp.queueGateReservedTasks[queueID] = make(map[api.TaskID]*api.TaskInfo)
	}
	cp.queueGateReservedTasks[queueID][task.UID] = task
	klog.V(4).Infof("Added task <%s/%s> to reserved cache for queue <%s>", task.Namespace, task.Name, queueID)
}

// removeTaskFromReservedCache 从保留缓存中移除指定任务
// 在任务被分配(不再需要预留)时调用
// 遍历所有队列查找并移除任务
func (cp *capacityPlugin) removeTaskFromReservedCache(taskID api.TaskID) {
	for queueID, tasks := range cp.queueGateReservedTasks {
		if _, exists := tasks[taskID]; exists {
			delete(tasks, taskID)
			// 清理空的队列条目
			if len(tasks) == 0 {
				delete(cp.queueGateReservedTasks, queueID)
			}
			klog.V(4).Infof("Removed task <%s> from reserved cache for queue <%s>", taskID, queueID)
			return
		}
	}
}

// buildQueueReservedTasksCache 重建调度门保留缓存
//
// 在每个调度周期开始时调用，扫描所有 Pending 任务并重建缓存
// 被加入缓存的任务满足以下条件：
//   - 无 QueueAllocation 调度门（门已移除，表示已通过容量检查）
//   - 有 QueueAllocationGate 注解（证明其选择加入并通过了容量检查）
//   - 处于 Pending 状态
func (cp *capacityPlugin) buildQueueReservedTasksCache(ssn *framework.Session) {
	// 初始化当前会话的缓存
	cp.queueGateReservedTasks = make(map[api.QueueID]map[api.TaskID]*api.TaskInfo)

	// 扫描所有 Pending 任务并重建缓存
	for _, job := range ssn.Jobs {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			// 已通过容量检查的任务特征：无调度门 + 有注解 + Pending 状态
			if !task.SchGated && api.HasQueueAllocationGateAnnotation(task.Pod) {
				if cp.queueGateReservedTasks[job.Queue] == nil {
					cp.queueGateReservedTasks[job.Queue] = make(map[api.TaskID]*api.TaskInfo)
				}
				cp.queueGateReservedTasks[job.Queue][task.UID] = task
				klog.V(4).Infof("Added task <%s/%s> to reserved cache for queue <%s>",
					task.Namespace, task.Name, job.Queue)
			}
		}
	}
}

// queueAllocatableWithReserved 检查队列是否有容量分配任务（含保留缓存）
//
// 检查逻辑：
//  1. 如果启用 DRA，先检查 DRA 资源是否充足
//  2. 计算保留缓存中的资源总量（跳过当前候选任务避免重复计数）
//  3. futureUsed = allocated + reserved + candidate
//  4. 检查 futureUsed <= realCapability
func (cp *capacityPlugin) queueAllocatableWithReserved(attr *queueAttr, candidate *api.TaskInfo, queue *api.QueueInfo, draEnabled bool, consumableCapacityEnabled bool) bool {
	if draEnabled && attr.dra != nil {
		candidateDRA := incrementalTaskDRA(attr, candidate)
		if candidateDRA != nil {
			if !checkDRAAllocatable(attr.dra, candidateDRA, consumableCapacityEnabled, false) {
				klog.V(3).Infof("Queue <%v> DRA resource insufficient for candidate <%v>", queue.Name, candidate.Name)
				return false
			}
		}
	}
	// 计算保留缓存中的资源总量
	reserved := api.EmptyResource()
	if queueGateReserved := cp.queueGateReservedTasks[queue.UID]; queueGateReserved != nil {
		for _, task := range queueGateReserved {
			if task.UID != candidate.UID {
				// 跳过候选任务以避免重复计数（它将在 futureUsed 中加入）
				reserved.Add(task.Resreq)
			}
		}
	}

	// 检查加入候选任务后是否超过 realCapability
	futureUsed := attr.allocated.Clone().Add(reserved).Add(candidate.Resreq)
	allocatable, _ := futureUsed.LessEqualWithDimensionAndResourcesName(attr.realCapability, candidate.Resreq)

	if !allocatable {
		klog.V(3).Infof("Queue <%v>: realCapability <%v>, allocated <%v>, reserved <%v>; Candidate <%v>: resource request <%v>",
			queue.Name, attr.realCapability, attr.allocated, reserved, candidate.Name, candidate.Resreq)
	}

	return allocatable
}

// checkQueueAllocatableHierarchically 层次化检查队列可分配性
//
// 从祖先到叶子逐级检查，确保每一级队列都有足够的容量
// 检查顺序：最远祖先 → ... → 直接父队列 → 当前队列
// 任一级别容量不足则拒绝
func (cp *capacityPlugin) checkQueueAllocatableHierarchically(ssn *framework.Session, queue *api.QueueInfo, candidate *api.TaskInfo) bool {
	// 如果未启用层次化队列，list 仅包含队列自身
	list := append(cp.queueOpts[queue.UID].ancestors, queue.UID)
	// 从祖先到叶子逐级检查
	for i := len(list) - 1; i >= 0; i-- {
		if !cp.queueAllocatable(ssn.Queues[list[i]], candidate, cp.dynamicResourceAllocationEnable, cp.draConsumableCapacityEnable) {
			// 当日志级别为 5 时，打印从叶子到祖先的所有队列信息（用于调试）
			if klog.V(5).Enabled() {
				for j := i - 1; j >= 0; j-- {
					cp.queueAllocatable(ssn.Queues[list[j]], candidate, cp.dynamicResourceAllocationEnable, cp.draConsumableCapacityEnable)
				}
			}
			return false
		}
	}
	return true
}

// jobEnqueueable 检查 Job 是否可以入队（单队列级别）
//
// 检查逻辑：
//  1. 计算 r = minReq + allocated + inqueue - elastic
//     （弹性资源可被扣除，因为不是硬性保障）
//  2. 检查 r <= realCapacity（不超过队列容量上限）
//  3. 如果启用 DRA，检查 DRA 配额是否充足
func (cp *capacityPlugin) jobEnqueueable(queue *api.QueueInfo, job *api.JobInfo) (bool, []string) {
	attr := cp.queueOpts[queue.UID]
	minReq := job.GetMinResources()

	klog.V(5).Infof("job %s min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s>",
		job.Name, minReq.String(), queue.Name, attr.realCapability.String(), attr.allocated.String(), attr.inqueue.String(), attr.elastic.String())
	// 检查队列资源配额是否已达到上限
	// r = minReq + allocated + inqueue - elastic
	r := minReq.Clone().Add(attr.allocated).Add(attr.inqueue).Sub(attr.elastic)

	valid, reasons := r.LessEqualWithDimensionAndResourcesName(attr.realCapability, minReq)
	if !valid {
		return valid, reasons
	}

	// 如果启用 DRA，检查 DRA 配额是否充足
	if cp.dynamicResourceAllocationEnable && attr.dra != nil {
		minDRAReq := job.GetMinDRAResources()
		if minDRAReq != nil {
			if !checkDRAAllocatable(attr.dra, minDRAReq, cp.draConsumableCapacityEnable, true) {
				klog.V(3).Infof("job %s exceeds queue %s DRA capability", job.Name, queue.Name)
				return false, append(reasons, "dra-resource-exceeded")
			}
		}
	}

	return true, reasons
}

// checkJobEnqueueableHierarchically 层次化检查 Job 入队资格
//
// 从祖先到叶子逐级检查，确保每一级队列都有足够的入队容量
// 任一级别容量不足则拒绝并记录 PodGroup 事件
func (cp *capacityPlugin) checkJobEnqueueableHierarchically(ssn *framework.Session, queue *api.QueueInfo, job *api.JobInfo) bool {
	// 如果未启用层次化队列，list 仅包含队列自身
	list := append(cp.queueOpts[queue.UID].ancestors, queue.UID)
	// 从祖先到叶子逐级检查
	for i := len(list) - 1; i >= 0; i-- {
		if inqueue, resourceNames := cp.jobEnqueueable(ssn.Queues[list[i]], job); !inqueue {
			// 当日志级别为 5 时，打印从叶子到祖先的所有队列信息（用于调试）
			if klog.V(5).Enabled() {
				for j := i - 1; j >= 0; j-- {
					cp.jobEnqueueable(ssn.Queues[list[j]], job)
				}
			}

			ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), util.FormatResourceNames("queue resource quota insufficient", "insufficient", resourceNames))
			return false
		}
	}

	return true
}

// getCapacityState 从调度周期状态中获取 capacity 插件的快照数据
func getCapacityState(cycleState fwk.CycleState) (*capacityState, error) {
	c, err := cycleState.Read(capacityStateKey)
	if err != nil {
		// capacityState 不存在，可能是 PreFilter 未被调用
		return nil, fmt.Errorf("error reading %q from cycleState: %w", capacityStateKey, err)
	}

	s, ok := c.(*capacityState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to capacity.state error", c)
	}
	return s, nil
}

// capacityState 是调度周期状态数据，存储所有队列属性的快照
// 用于 Simulate 回调中，与真实队列状态隔离
type capacityState struct {
	queueAttrs map[api.QueueID]*queueAttr
}

// Clone 深拷贝 queueAttr，创建完全独立的新实例
// 包括所有资源字段、ancestors 切片、children 映射和 resourceClaimRefs 计数
func (qa *queueAttr) Clone() *queueAttr {
	if qa == nil {
		return nil
	}

	cloned := &queueAttr{
		queueID:           qa.queueID,
		name:              qa.name,
		share:             qa.share,
		deserved:          qa.deserved.Clone(),
		allocated:         qa.allocated.Clone(),
		request:           qa.request.Clone(),
		elastic:           qa.elastic.Clone(),
		inqueue:           qa.inqueue.Clone(),
		dra:               qa.dra.Clone(),
		capability:        qa.capability.Clone(),
		realCapability:    qa.realCapability.Clone(),
		guarantee:         qa.guarantee.Clone(),
		resourceClaimRefs: make(map[string]int, len(qa.resourceClaimRefs)),
		children:          make(map[api.QueueID]*queueAttr),
	}

	if len(qa.ancestors) > 0 {
		cloned.ancestors = make([]api.QueueID, len(qa.ancestors))
		copy(cloned.ancestors, qa.ancestors)
	}

	for childID, childNode := range qa.children {
		cloned.children[childID] = childNode.Clone()
	}
	for claimKey, refCount := range qa.resourceClaimRefs {
		cloned.resourceClaimRefs[claimKey] = refCount
	}

	return cloned
}

// Clone 实现 fwk.StateData 接口，深拷贝整个 capacityState
// 用于调度框架的 CycleState 快照机制
func (s *capacityState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}

	newState := &capacityState{
		queueAttrs: make(map[api.QueueID]*queueAttr, len(s.queueAttrs)),
	}

	for qID, qa := range s.queueAttrs {
		newState.queueAttrs[qID] = qa.Clone()
	}

	return newState
}

// updateQueueAttrShare 计算并更新队列的 DRF 份额值
//
// share 的计算逻辑：
//   - 无 deserved 配置的队列（best-effort）：share = 1
//     这确保 best-effort 队列总是在有 deserved 且 share < 1 的队列之后被调度
//   - 有 deserved 配置的队列：share = max(allocated_i / deserved_i)
//     取所有资源维度中的最大比值作为份额值
//
// 份额值语义：
//   - best-effort (无 deserved) → share = 1（总是在 share<1 的队列之后）
//   - 有 deserved, share < 1 → 优先调度
//   - 有 deserved, share >= 1 → 与 best-effort 同等优先级
//
// 当 share 值相等时，QueueOrderFn 使用决胜逻辑：有 deserved 的队列优先于 best-effort
// 这进一步防止了回收振荡（reclaim thrashing）
func updateQueueAttrShare(attr *queueAttr) {
	res := float64(0)

	// 无 deserved 配置的队列视为 best-effort 队列
	// best-effort 队列应具有最低调度优先级：只有当所有有 deserved 的队列
	// 的 share >= 1 时，best-effort 队列才会被调度
	// 为确保这一点，将所有 best-effort 队列的 share 设为 1，
	// 这样它们总是在 share < 1 的队列之后被调度
	if attr.deserved.IsEmpty() {
		attr.share = 1
		return
	}

	// DRF 份额计算：取所有资源维度中的最大比值
	for _, rn := range attr.deserved.ResourceNames() {
		res = max(res, helpers.Share(attr.allocated.Get(rn), attr.deserved.Get(rn)))
	}

	attr.share = res
}

// shouldSkipReclaimee 检查是否应跳过某个被回收候选任务
// 判断依据：被回收任务与回收方（reclaimer）是否有资源维度交集
// 如果完全没有交集（如一个只需 CPU 的任务尝试回收一个只需 GPU 的任务），则跳过
// 返回值：true 表示应跳过，附带原因消息
func (cp *capacityPlugin) shouldSkipReclaimee(reclaimee, reclaimer *api.TaskInfo) (bool, string) {
	reclaimerIntersecting := len(api.IntersectionWithIgnoredScalarResources(reclaimee.Resreq, reclaimer.InitResreq)) > 0
	if !reclaimerIntersecting {
		return true, fmt.Sprintf("[capacity] Reclaimee <%s/%s>: <%v> does not have intersecting resource dimensions with reclaimer <%s/%s>: <%v>",
			reclaimee.Namespace, reclaimee.Name, reclaimee.Resreq, reclaimer.Namespace, reclaimer.Name, reclaimer.InitResreq)
	}
	return false, ""
}

// checkGuaranteeConstraint 检查移除被回收任务后是否会违反队列的 guarantee 约束
// 如果 allocated - reclaimee < guarantee，说明回收后剩余资源低于保障下限，不允许回收
// 返回值：true 表示 guarantee 约束满足（允许回收）
func (cp *capacityPlugin) checkGuaranteeConstraint(
	allocated *api.Resource,
	reclaimee *api.TaskInfo,
	guarantee *api.Resource,
) (bool, *api.Resource) {
	exceptReclaimee := allocated.Clone().Sub(reclaimee.Resreq)
	reclaimable := guarantee.LessEqual(exceptReclaimee, api.Zero)
	return reclaimable, exceptReclaimee
}

// isImmediateVictim 检查被回收任务是否为“即时受害者”
// 即：任务的资源维度与队列的 deserved 无任何交集
// 这意味着该任务占用的资源不属于队列应得资源的范畴，可以立即回收
// 返回值：true 表示是即时受害者，附带原因消息
func (cp *capacityPlugin) isImmediateVictim(
	reclaimee *api.TaskInfo,
	deserved *api.Resource,
) (bool, string) {
	if !hasRelevantDeserved(reclaimee, deserved) {
		return true, fmt.Sprintf("[capacity] No intersection between deserved: <%v> and reclaimee <%s/%s>: <%v>",
			deserved, reclaimee.Namespace, reclaimee.Name, reclaimee.Resreq)
	}
	return false, ""
}

// hasRelevantDeserved 检查任务是否有与 deserved 相关的资源维度
// 即任务的资源请求与 deserved 是否有交集
func hasRelevantDeserved(task *api.TaskInfo, deserved *api.Resource) bool {
	if task == nil || deserved == nil {
		return false
	}
	return len(api.Intersection(task.Resreq, deserved)) > 0
}

// checkDeservedExceedance 检查队列的 allocated 是否在相关维度上超过 deserved
// 如果超过，说明该队列超额使用了资源，其中的任务可以被回收
//
// 返回值：
//   - bool: 是否超过 deserved
//   - []string: 超过的资源维度列表
//   - string: 原因消息
func (cp *capacityPlugin) checkDeservedExceedance(
	allocated *api.Resource,
	deserved *api.Resource,
	reclaimee *api.TaskInfo,
	reclaimer *api.TaskInfo,
	queueName string,
) (bool, []string, string) {
	reclaimable, dims := allocated.GreaterPartlyWithRelevantDimensions(deserved, reclaimee.Resreq)
	if !reclaimable {
		reason := fmt.Sprintf(
			"[capacity] Queue <%v> allocated resources are not greater than deserved on any relevant dimension of reclaimee. "+
				"Hence reclaimee <%s/%s> cannot be reclaimed for reclaimer <%s/%s>. "+
				"Deserved: <%v>, Allocated: <%v>, Reclaimee Resreq: <%v>",
			queueName, reclaimee.Namespace, reclaimee.Name, reclaimer.Namespace, reclaimer.Name, deserved, allocated, reclaimee.Resreq,
		)
		return false, nil, reason
	}
	return true, dims, ""
}

// sharesAnyNonRootAncestorWithinLevel 检查两个队列在配置的层级内是否共享非根祖先
// 用于回收检查中避免在同族队列之间进行不必要的回收
func (cp *capacityPlugin) sharesAnyNonRootAncestorWithinLevel(a, b *queueAttr) bool {
	if cp == nil || a == nil || b == nil || cp.ancestorReclaimLevel <= 0 {
		return false
	}

	aAncestors := ancestorIDSet(ancestorsByLevel(a, cp.ancestorReclaimLevel))
	bAncestors := ancestorIDSet(ancestorsByLevel(b, cp.ancestorReclaimLevel))

	for ancestor := range aAncestors {
		if ancestor == rootQueueID {
			continue
		}
		if _, found := bAncestors[ancestor]; found {
			return true
		}
	}

	return false
}

// getQueueLevel 计算两个队列的最近共同祖先层级
// 返回值：0 表示根队列是最近共同祖先
func getQueueLevel(l *queueAttr, r *queueAttr) int {
	level := 0

	for i := range min(len(l.ancestors), len(r.ancestors)) {
		if l.ancestors[i] == r.ancestors[i] {
			level = i
		} else {
			return level
		}
	}

	return level
}

// queueAncestorAtDepth 获取队列在指定深度的祖先
// depth=1 表示直接父队列，depth=2 表示祖父队列
// ancestors 列表按从近到远排序，最远祖先在末尾
func queueAncestorAtDepth(attr *queueAttr, depth int) (api.QueueID, bool) {
	if attr == nil || depth <= 0 || len(attr.ancestors) < depth {
		return "", false
	}

	return attr.ancestors[len(attr.ancestors)-depth], true
}

// ancestorsByLevel 构建按层级组织的祖先映射
// 返回值：map[level]ancestorID，层级从 1 开始
func ancestorsByLevel(attr *queueAttr, maxLevel int) map[int]api.QueueID {
	ancestors := make(map[int]api.QueueID)
	if attr == nil || maxLevel <= 0 {
		return ancestors
	}

	for level := 1; level <= maxLevel; level++ {
		ancestorID, found := queueAncestorAtDepth(attr, level)
		if !found {
			continue
		}
		ancestors[level] = ancestorID
	}

	return ancestors
}

// ancestorIDSet 将祖先映射转换为 ID 集合，用于快速查找
func ancestorIDSet(ancestorsByDepth map[int]api.QueueID) map[api.QueueID]struct{} {
	set := make(map[api.QueueID]struct{}, len(ancestorsByDepth))
	for _, ancestorID := range ancestorsByDepth {
		set[ancestorID] = struct{}{}
	}
	return set
}
