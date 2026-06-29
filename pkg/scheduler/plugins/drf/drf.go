/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added support for hierarchical DRF (HDRF) scheduling with tree-based resource allocation
- Enhanced DRF plugin with namespace-aware resource sharing and metrics integration

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

// Package drf 实现了 Dominant Resource Fairness（主导资源公平）调度插件。
//
// DRF 核心思想：
//
//	在多种资源（CPU、内存等）的集群中，每个作业的"主导份额"（dominant share）
//	是它在所有资源类型中占比最大的那个。DRF 优先调度主导份额最小的作业，
//	从而让所有作业公平地分享集群资源。
//
// 举例：集群有 100 CPU + 200G 内存
//   - 作业 A 已用 30 CPU + 10G 内存 → CPU 占 30%，内存占 5% → 主导份额 = 30%（CPU）
//   - 作业 B 已用 10 CPU + 80G 内存 → CPU 占 10%，内存占 40% → 主导份额 = 40%（内存）
//     → A 的主导份额(30%) < B 的(40%)，所以 A 优先被调度
//
// HDRF 扩展：
//
//	当开启 enableHierarchy 时，DRF 升级为分层 DRF（Hierarchical DRF），
//	支持按队列的树形层次结构进行加权公平调度。每个队列节点可以设置权重，
//	调度器在树的每一层都按照 DRF 原则比较加权利益份额，决定队列优先级。
//
// 插件注册的能力：
//   - JobOrderFn：按作业主导份额排序，份额小的优先
//   - QueueOrderFn：按队列层级加权利益排序（仅 HDRF 模式）
//   - PreemptableFn：抢占时选择被抢占者，抢占后主导份额更小的保留
//   - ReclaimableFn：回收时选择被回收者，基于 HDRF 树模拟（仅 HDRF 模式）
//   - EventHandler：分配/释放资源后实时更新主导份额
package drf

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName 插件名称，在调度器配置中引用时使用
const PluginName = "drf"

// shareDelta 份额比较的浮点容差
// 当两个份额差值小于此值时认为相等，避免浮点精度问题导致判断错误
var shareDelta = 0.000001

// hierarchicalNode 表示 HDRF 层次树中的一个节点
//
// 树的结构示例（对应队列 Hierarchy="root/eng/dev"，Weights="100/50/50"）：
//
//	root (weight=100)
//	 ├── eng (weight=50)
//	 │    ├── dev (weight=50)  ← 内部节点
//	 │    │    ├── job-A       ← 叶子节点（代表一个作业）
//	 │    │    └── job-B
//	 │    └── prod (weight=50)
//	 │         └── job-C
//	 └── sci (weight=50)
//	      └── job-D
//
// 调度比较时，从 root 开始逐层向下比较 share/weight，
// 份额更小的队列优先得到资源。
type hierarchicalNode struct {
	parent *hierarchicalNode // 指向父节点的指针，用于向上遍历
	attr   *drfAttr          // 该节点的 DRF 属性（份额、主导资源、已分配资源）
	// request 仅对叶子节点有意义，存储该作业的总资源需求量
	// 用于判断该作业是否已"吃饱"（saturated）
	request   *api.Resource
	weight    float64                      // 该节点的权重，用于加权 DRF 比较
	saturated bool                         // 该节点是否已"吃饱"——资源需求已完全满足或无可争抢资源
	hierarchy string                       // 该节点的层级名称（如 "eng"、"dev"）
	children  map[string]*hierarchicalNode // 子节点映射，key 为层级名称
}

// Clone 深拷贝整个层次树
// 参数 parent 指定克隆后新节点的父节点（用于递归时建立正确的 parent 指针）
// 用途：在 reclaim（回收）场景中，需要模拟抢占效果，
// 所以先克隆整棵树，在克隆树上做试算，不影响原始数据
func (node *hierarchicalNode) Clone(parent *hierarchicalNode) *hierarchicalNode {
	newNode := &hierarchicalNode{
		parent: parent,
		attr: &drfAttr{
			share:            node.attr.share,
			dominantResource: node.attr.dominantResource,
			allocated:        node.attr.allocated.Clone(),
		},
		request:   node.request.Clone(),
		weight:    node.weight,
		saturated: node.saturated,
		hierarchy: node.hierarchy,
		children:  nil,
	}
	if node.children != nil {
		newNode.children = map[string]*hierarchicalNode{}
		for _, child := range node.children {
			newNode.children[child.hierarchy] = child.Clone(newNode)
		}
	}
	return newNode
}

// resourceSaturated 判断一个作业是否已"吃饱"(资源饱和)
//
// 饱和的定义(满足任一即视为饱和)：
//  1. 某种资源的已分配量 >= 作业需求量(作业对该资源已完全满足)
//  2. 某种资源不是集群中的"紧缺资源"，但作业仍然需要它
//     (即集群中该资源总量已满，没有争抢空间，作业无法获得更多)
//
// 参数：
//   - allocated: 作业当前已分配的资源
//   - jobRequest: 作业的总资源需求量
//   - demandingResources: 集群中当前仍有剩余的(紧缺的)资源类型集合
//
// 为什么要判断饱和? 因为已饱和的作业/队列不应该再参与 DRF 竞争，
// 应该让资源优先给那些还没吃饱的作业，实现真正的公平
/*
	1.已饱和的作业不参与 mdr（最小主导份额）的计算
	2.已饱和的子节点在向上汇总时不做缩放，直接累加其实际分配量
	这样设计的目的：避免"吃饱了还占着名额"导致 DRF 公平性失真。
	如果不管饱不饱都按份额排序，那些需求很小的作业（很快吃饱）会一直占着高优先级，而真正需要大量资源的作业永远排不上。
*/
func resourceSaturated(allocated *api.Resource, jobRequest *api.Resource, demandingResources map[v1.ResourceName]bool) bool {

	for _, rn := range allocated.ResourceNames() {
		// 条件1：已分配量 >= 需求量，说明该资源类型已完全满足
		if allocated.Get(rn) != 0 && jobRequest.Get(rn) != 0 &&
			allocated.Get(rn) >= jobRequest.Get(rn) {
			return true
		}
		// 条件2：该资源不是紧缺资源（集群总量已满），但作业还需要它
		// 意味着作业在该资源上无法获得更多，视为饱和
		// 为什么这也算饱和？ 因为集群已经没有这种资源的余量了，作业再排队也拿不到。
		// 既然永远拿不到，不如把它从竞争中移除，让其他作业去抢那些还有剩余的资源
		if !demandingResources[rn] && jobRequest.Get(rn) != 0 {
			return true
		}
	}
	return false
}

// drfAttr 存储一个作业或层次节点的 DRF 调度属性
type drfAttr struct {
	share float64 // 主导份额（dominant share）：所有资源占比中的最大值
	// 例如 CPU 占 30%、内存占 5%，则 share=0.3, dominantResource=cpu
	dominantResource string        // 主导资源名称（"cpu" 或 "memory" 等），即占比最大的那种资源
	allocated        *api.Resource // 该作业/节点已分配到的资源总量
}

// String 返回 drfAttr 的可读字符串表示，用于日志输出
func (attr *drfAttr) String() string {
	return fmt.Sprintf("dominant resource <%s>, dominant share %f, allocated %s",
		attr.dominantResource, attr.share, attr.allocated)
}

// drfPlugin DRF 调度插件的核心数据结构
type drfPlugin struct {
	totalResource  *api.Resource // 集群总可分配资源量（所有节点资源之和）
	totalAllocated *api.Resource // 集群已分配资源总量（仅 HDRF 模式使用）

	// jobAttrs 存储每个作业的 DRF 属性，key 为 Job ID
	// 这是基础 DRF 的核心数据，用于 JobOrderFn 排序和 PreemptableFn 抢占判断
	jobAttrs map[api.JobID]*drfAttr

	// namespaceOpts 存储每个命名空间的 DRF 属性（预留字段，当前未使用）
	namespaceOpts map[string]*drfAttr

	// hierarchicalRoot HDRF 层次树的根节点（仅 HDRF 模式使用）
	// 树的结构由队列的 Hierarchy 和 Weights 注解决定
	hierarchicalRoot *hierarchicalNode

	// pluginArguments 传给插件的配置参数
	pluginArguments framework.Arguments
}

// New 创建并返回 DRF 插件实例
// 这是调度器框架要求的工厂函数，在调度器启动时被调用
func New(arguments framework.Arguments) framework.Plugin {
	return &drfPlugin{
		totalResource:  api.EmptyResource(),
		totalAllocated: api.EmptyResource(),
		jobAttrs:       map[api.JobID]*drfAttr{},
		namespaceOpts:  map[string]*drfAttr{},
		hierarchicalRoot: &hierarchicalNode{
			attr:      &drfAttr{allocated: api.EmptyResource()},
			request:   api.EmptyResource(),
			hierarchy: "root",
			weight:    1,
			children:  map[string]*hierarchicalNode{},
		},
		pluginArguments: arguments,
	}
}

// Name 返回插件名称，实现 framework.Plugin 接口
func (drf *drfPlugin) Name() string {
	return PluginName
}

// HierarchyEnabled 检查调度器配置中是否启用了层次 DRF（HDRF）
// 通过遍历调度器配置的 Tier 列表，找到 drf 插件的 EnabledHierarchy 配置项
// 只有当配置中明确设置了 enableHierarchy: true 时才启用
func (drf *drfPlugin) HierarchyEnabled(ssn *framework.Session) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if plugin.Name != PluginName {
				continue
			}
			return plugin.EnabledHierarchy != nil && *plugin.EnabledHierarchy
		}
	}
	return false
}

// compareQueues 在 HDRF 模式下比较两个队列的优先级
//
// 核心思路：从层次树的根节点开始，逐层向下比较两个队列路径上的节点
// 每一层的比较规则：
//  1. 未饱和的队列优先级高于已饱和的队列（饱和 = 资源已满足，不需要更多）
//  2. 如果都没饱和（或都饱和），比较 加权份额 = share/weight，份额小的优先
//  3. 如果该层相等，继续比较下一层
//
// 返回值：
//
//	< 0 表示 lqueue 优先级更高（应先调度）
//	> 0 表示 rqueue 优先级更高
//	= 0 表示两者优先级相同
//
// 举例：
//
//	root
//	├── eng (weight=50, share=0.4) → 加权份额 = 0.4/50 = 0.008
//	└── sci (weight=50, share=0.2) → 加权份额 = 0.2/50 = 0.004
//	→ sci 的加权份额更小，sci 优先
func (drf *drfPlugin) compareQueues(root *hierarchicalNode, lqueue *api.QueueInfo, rqueue *api.QueueInfo) float64 {
	lnode := root
	lpaths := strings.Split(lqueue.Hierarchy, "/") // 如 "root/eng/dev" → ["root", "eng", "dev"]
	rnode := root
	rpaths := strings.Split(rqueue.Hierarchy, "/")
	for i, depth := 0, min(len(lpaths), len(rpaths)); i < depth; i++ {
		// 已饱和的节点优先级最低，让未饱和的节点先获得资源
		if !lnode.saturated && rnode.saturated {
			return -1 // lqueue 未饱和，优先级更高
		}
		if lnode.saturated && !rnode.saturated {
			return 1 // rqueue 未饱和，优先级更高
		}
		// 两个都未饱和或都已饱和，比较加权份额
		if lnode.attr.share/lnode.weight == rnode.attr.share/rnode.weight {
			if i < depth-1 {
				// 该层相等，进入下一层子节点继续比较
				lnode = lnode.children[lpaths[i+1]]
				rnode = rnode.children[rpaths[i+1]]
			}
		} else {
			// 该层不等，加权份额小的优先级高
			return lnode.attr.share/lnode.weight - rnode.attr.share/rnode.weight
		}
	}
	return 0 // 所有层都相等
}

// OnSessionOpen 在每个调度周期开始时被调用，是 DRF 插件的核心初始化入口
//
// 主要做 4 件事：
//  1. 累加集群总资源
//  2. 遍历所有作业，计算每个作业的初始主导份额
//  3. 如果开启了 HDRF，构建层次树并计算层次份额
//  4. 注册各种调度回调函数（排序、抢占、回收、事件处理）
func (drf *drfPlugin) OnSessionOpen(ssn *framework.Session) {
	// 第1步：累加集群总资源(totalResource 是跨 session 累积的)
	drf.totalResource.Add(ssn.TotalResource)

	klog.V(4).Infof("Total Allocatable %s", drf.totalResource)

	// 检查是否启用了层次 DRF
	hierarchyEnabled := drf.HierarchyEnabled(ssn)

	// 第2步：遍历所有作业，计算初始主导份额
	for _, job := range ssn.Jobs {
		attr := &drfAttr{
			allocated: api.EmptyResource(),
		}

		// 统计该作业所有已分配状态任务的资源总量
		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
				}
			}
		}

		// 计算作业的初始主导份额
		drf.updateShare(attr)
		if !ssn.IsJobTerminated(job.UID) {
			metrics.UpdateJobShare(job.Namespace, job.Name, attr.share)
		}

		drf.jobAttrs[job.UID] = attr

		// 第3步：如果 HDRF 模式，构建/更新层次树
		if hierarchyEnabled {
			queue := ssn.Queues[job.Queue]
			drf.totalAllocated.Add(attr.allocated)
			drf.UpdateHierarchicalShare(drf.hierarchicalRoot, drf.totalAllocated, job, attr, queue.Hierarchy, queue.Weights)
		}
	}

	// ========== 注册抢占回调（基础 DRF + HDRF 通用）==========
	// 抢占(Preemptable)场景：高优先级任务要抢低优先级任务的资源时，
	// 用这个函数决定哪些被抢者可以成为受害者
	//
	// 判断规则：如果抢走被抢者后，抢者的份额仍然 <= 被抢者的份额，
	// 说明被抢者的份额更多，被抢走一些还是比抢者多，所以允许抢占
	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo

		addVictim := func(candidate *api.TaskInfo) {
			victims = append(victims, candidate)
		}

		// 获取抢者的作业属性，计算抢者假设抢到资源后的份额
		latt := drf.jobAttrs[preemptor.Job]
		if latt == nil {
			klog.Warningf("[drf] Skip preemption: preemptor job <%s> not found in jobAttrs (orphaned task from deleted PodGroup)",
				preemptor.Job)
			return nil, util.Permit
		}

		// 模拟抢者获得资源后的已分配量
		lalloc := latt.allocated.Clone().Add(preemptor.Resreq)
		_, ls := drf.calculateShare(lalloc, drf.totalResource)

		// allocations 缓存每个被抢作业的已分配资源，避免重复查询
		allocations := map[api.JobID]*api.Resource{}

		for _, preemptee := range preemptees {
			if _, found := allocations[preemptee.Job]; !found {
				ratt := drf.jobAttrs[preemptee.Job]
				if ratt == nil {
					klog.Warningf("[drf] Skip preemptee <%s/%s>: job <%s> not found in jobAttrs (orphaned task from deleted PodGroup)",
						preemptee.Namespace, preemptee.Name, preemptee.Job)
					continue
				}
				allocations[preemptee.Job] = ratt.allocated.Clone()
			}
			// 模拟被抢者失去资源后的已分配量
			ralloc := allocations[preemptee.Job].Sub(preemptee.Resreq)
			_, rs := drf.calculateShare(ralloc, drf.totalResource)

			// 判断是否允许抢占：
			// 如果抢者份额(ls) < 被抢者份额(rs) → 允许(抢者更"穷"，应该先拿资源)
			// 如果两者份额几乎相等(差值 < shareDelta)→ 也允许(浮点容差)
			if ls < rs || math.Abs(ls-rs) <= shareDelta {
				addVictim(preemptee)
			}
		}

		klog.V(4).Infof("[drf] Victims from DRF preemptable plugin are %+v", victims)

		return victims, util.Permit
	}

	// 注册抢占回调到调度会话
	ssn.AddPreemptableFn(drf.Name(), preemptableFn)

	// ========== HDRF 模式专属逻辑 ==========
	if hierarchyEnabled {
		// QueueOrderFn：按 HDRF 加权份额排序队列
		// 份额更小的队列优先调度，让资源更公平地分配
		queueOrderFn := func(l interface{}, r interface{}) int {
			lv := l.(*api.QueueInfo)
			rv := r.(*api.QueueInfo)
			ret := drf.compareQueues(drf.hierarchicalRoot, lv, rv)
			if ret < 0 {
				return -1
			}
			if ret > 0 {
				return 1
			}
			return 0
		}
		ssn.AddQueueOrderFn(drf.Name(), queueOrderFn)

		// ReclaimFn：HDRF 模式下的回收（Reclaim）回调
		//
		// 回收场景：一个队列中的任务需要资源，但集群资源不足，
		// 需要从其他队列"回收"（回收 = 温和的抢占，跨队列的重新分配）
		//
		// 核心思路：
		//   1. 克隆 HDRF 树（避免影响原始数据）
		//   2. 模拟回收者获得资源后的 HDRF 状态
		//   3. 逐个遍历被回收者，模拟被回收者失去资源后的 HDRF 状态
		//   4. 比较两个队列的 HDRF 优先级，如果回收者队列优先级更高，则允许回收
		//   5. 每次模拟后恢复状态，继续下一个被回收者
		/*
			当某个任务 reclaimer 需要资源时，调度器会尝试从一批候选任务 reclaimees 中“回收”资源；
			但是不是随便回收，而是要先模拟回收前后的层次 DRF 状态，判断回收后是否仍然公平
		*/
		reclaimFn := func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
			// victims 保存最终允许被回收资源的任务列表
			var victims []*api.TaskInfo

			// =========================================================
			// 第1步：克隆 HDRF 树和总分配资源
			// =========================================================
			// 这里不能直接修改真实的 drf.totalAllocated 和 hierarchicalRoot，
			// 因为 reclaim 只是“试算”过程，需要在副本上模拟回收效果，
			// 避免影响调度器当前的真实状态。
			totalAllocated := drf.totalAllocated.Clone()
			root := drf.hierarchicalRoot.Clone(nil)

			// =========================================================
			// 第2步：获取回收者所在的 job 和 queue
			// =========================================================
			// reclaimer 是“想要资源”的任务。
			// 先找到它所属的 Job，再找到它所在的 Queue，
			// 这样才能在 HDRF 树中模拟它获取资源后的变化。
			ljob := ssn.Jobs[reclaimer.Job]
			if ljob == nil {
				klog.Warningf("[drf] Skip reclaim: reclaimer job <%s> not found in session (orphaned task from deleted PodGroup)",
					reclaimer.Job)
				// 找不到 job，说明任务可能是孤儿任务，直接放行
				return nil, util.Permit
			}

			lqueue := ssn.Queues[ljob.Queue]
			if lqueue == nil {
				klog.V(4).Infof("[drf] Skip reclaim: reclaimer queue <%s> not found in session",
					ljob.Queue)
				return nil, util.Permit
			}

			// 克隆一份 job，避免后续修改影响原始对象
			ljob = ljob.Clone()

			// 从 drf.jobAttrs 中取出该 job 当前的 DRF 属性
			attr := drf.jobAttrs[ljob.UID]
			if attr == nil {
				klog.V(4).Infof("[drf] Skip reclaim: reclaimer job <%s> not found in jobAttrs",
					ljob.UID)
				return nil, util.Permit
			}

			// 构造回收者的试算属性：
			// 先复制它当前已分配资源，再假设它拿到了这次请求的资源
			lattr := &drfAttr{
				allocated: attr.allocated.Clone(),
			}
			lattr.allocated.Add(reclaimer.Resreq)

			// 由于回收者假设拿到了资源，所以集群总已分配量也要相应增加
			totalAllocated.Add(reclaimer.Resreq)

			// 更新回收者的 share，并把这个变化同步到 HDRF 树中
			drf.updateShare(lattr)
			drf.UpdateHierarchicalShare(root, totalAllocated, ljob, lattr, lqueue.Hierarchy, lqueue.Weights)

			// =========================================================
			// 第3步：遍历所有候选被回收者，逐个试算
			// =========================================================
			for _, preemptee := range reclaimees {
				// 找到被回收者所属的 job
				rjob := ssn.Jobs[preemptee.Job]
				if rjob == nil {
					klog.Warningf("[drf] Skip reclaimee <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
						preemptee.Namespace, preemptee.Name, preemptee.Job)
					continue
				}

				// 找到被回收者所在的 queue
				rqueue := ssn.Queues[rjob.Queue]
				if rqueue == nil {
					klog.V(4).Infof("[drf] Skip reclaimee <%s/%s>: queue <%s> not found in session",
						preemptee.Namespace, preemptee.Name, rjob.Queue)
					continue
				}

				// -----------------------------------------------------
				// 第3.1步：模拟被回收者失去资源
				// -----------------------------------------------------
				// 试算时，把总已分配量减掉被回收任务的资源
				totalAllocated.Sub(preemptee.Resreq)

				// 同样克隆被回收者所在的 job，避免影响原对象
				rjob = rjob.Clone()

				// 获取被回收者当前属性
				attr := drf.jobAttrs[rjob.UID]
				if attr == nil {
					klog.V(4).Infof("[drf] Skip reclaimee <%s/%s>: job <%s> not found in jobAttrs",
						preemptee.Namespace, preemptee.Name, rjob.UID)
					// 如果找不到属性，要把前面减掉的资源恢复回来
					totalAllocated.Add(preemptee.Resreq)
					continue
				}

				// 构造被回收者的试算属性：先复制当前 allocated，再减去被回收的资源
				rattr := &drfAttr{
					allocated: attr.allocated.Clone(),
				}
				rattr.allocated.Sub(preemptee.Resreq)

				// 更新被回收者的 share，并刷新 HDRF 树
				drf.updateShare(rattr)
				drf.UpdateHierarchicalShare(root, totalAllocated, rjob, rattr, rqueue.Hierarchy, rqueue.Weights)

				// -----------------------------------------------------
				// 第3.2步：比较回收者队列和被回收者队列的 HDRF 优先级
				// -----------------------------------------------------
				// ret < 0：说明回收者队列更“穷”，优先级更高，可以回收
				// ret > 0：说明被回收者队列更“穷”，不应该回收
				ret := drf.compareQueues(root, lqueue, rqueue)

				// -----------------------------------------------------
				// 第3.3步：恢复被回收者状态
				// -----------------------------------------------------
				// 因为这里只是试算，所以需要把刚才对被回收者造成的修改回滚
				totalAllocated.Add(preemptee.Resreq)
				rattr.allocated.Add(preemptee.Resreq)
				drf.updateShare(rattr)
				drf.UpdateHierarchicalShare(root, totalAllocated, rjob, rattr, rqueue.Hierarchy, rqueue.Weights)

				// -----------------------------------------------------
				// 第3.4步：根据比较结果决定是否加入 victims
				// -----------------------------------------------------
				// 如果回收者队列优先级更高，则允许回收该任务
				if ret < 0 {
					victims = append(victims, preemptee)
				}

				// 如果回收者队列明显更差，则继续看下一个候选任务
				if ret > shareDelta {
					continue
				}
			}

			klog.V(4).Infof("[drf] Victims from HDRF plugin are %+v", victims)

			// 返回被允许回收的 victims，以及 Permit 表示该插件不阻止后续流程
			return victims, util.Permit
		}
		ssn.AddReclaimableFn(drf.Name(), reclaimFn)
	}

	// ========== 注册作业排序回调（基础 DRF + HDRF 通用）==========
	// JobOrderFn：按作业的主导份额排序，份额小的优先调度
	// 这是 DRF 最核心的排序逻辑——总是优先调度"拥有资源最少"的作业
	jobOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		klog.V(4).Infof("DRF JobOrderFn: <%v/%v> share state: %v, <%v/%v> share state: %v",
			lv.Namespace, lv.Name, drf.jobAttrs[lv.UID].share, rv.Namespace, rv.Name, drf.jobAttrs[rv.UID].share)

		if drf.jobAttrs[lv.UID].share == drf.jobAttrs[rv.UID].share {
			return 0 // 份额相等，不分先后
		}

		if drf.jobAttrs[lv.UID].share < drf.jobAttrs[rv.UID].share {
			return -1 // lv 份额更小，优先级更高
		}

		return 1 // rv 份额更小，优先级更高
	}

	ssn.AddJobOrderFn(drf.Name(), jobOrderFn)

	// ========== 注册事件处理回调（分配/释放资源后更新份额）==========
	// 当调度器为任务分配或释放资源时，需要实时更新该作业的 DRF 份额
	ssn.AddEventHandler(&framework.EventHandler{
		// AllocateFunc：任务被分配资源后的回调
		// 将任务的资源需求加到作业的已分配量中，重新计算主导份额
		AllocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				klog.Warningf("[drf] Skip allocate event for task <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					event.Task.Namespace, event.Task.Name, event.Task.Job)
				return
			}
			attr := drf.jobAttrs[event.Task.Job]
			if attr == nil {
				klog.Warningf("[drf] Skip allocate event for task <%s/%s>: job <%s> not found in jobAttrs",
					event.Task.Namespace, event.Task.Name, event.Task.Job)
				return
			}
			attr.allocated.Add(event.Task.Resreq)
			drf.updateShare(attr)
			if !ssn.IsJobTerminated(job.UID) {
				metrics.UpdateJobShare(job.Namespace, job.Name, attr.share)
			}

			nsShare := -1.0
			if hierarchyEnabled {
				queue := ssn.Queues[job.Queue]
				if queue != nil {
					drf.totalAllocated.Add(event.Task.Resreq)
					drf.UpdateHierarchicalShare(drf.hierarchicalRoot, drf.totalAllocated, job, attr, queue.Hierarchy, queue.Weights)
				}
			}

			klog.V(4).Infof("[drf] AllocateFunc: task <%v/%v>, resreq <%v>, share <%v>, namespace share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share, nsShare)
		},
		// DeallocateFunc：任务被释放资源后的回调
		// 将任务的资源从作业的已分配量中减去，重新计算主导份额
		DeallocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			if job == nil {
				klog.Warningf("[drf] Skip deallocate event for task <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					event.Task.Namespace, event.Task.Name, event.Task.Job)
				return
			}
			attr := drf.jobAttrs[event.Task.Job]
			if attr == nil {
				klog.Warningf("[drf] Skip deallocate event for task <%s/%s>: job <%s> not found in jobAttrs",
					event.Task.Namespace, event.Task.Name, event.Task.Job)
				return
			}
			attr.allocated.Sub(event.Task.Resreq)
			drf.updateShare(attr)
			if !ssn.IsJobTerminated(job.UID) {
				metrics.UpdateJobShare(job.Namespace, job.Name, attr.share)
			}

			nsShare := -1.0
			if hierarchyEnabled {
				queue := ssn.Queues[job.Queue]
				if queue != nil {
					drf.totalAllocated.Sub(event.Task.Resreq)
					drf.UpdateHierarchicalShare(drf.hierarchicalRoot, drf.totalAllocated, job, attr, queue.Hierarchy, queue.Weights)
				}
			}

			klog.V(4).Infof("[drf] DeallocateFunc: task <%v/%v>, resreq <%v>, share <%v>, namespace share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share, nsShare)
		},
	})
}

// buildHierarchy 根据 Queues 的 Hierarchy 和 Weights 注解构建层次树
//
// 参数：
//   - root: 层次树的根节点
//   - job: 当前作业信息
//   - attr: 当前作业的 DRF 属性
//   - hierarchy: 队列的层级路径，如 "root/eng/dev"
//   - hierarchicalWeights: 对应的权重路径，如 "100/50/50"
//
// 构建过程：
//  1. 从 root 开始，按路径逐层查找或创建内部节点（如 eng、dev）
//  2. 在最底层（叶子节点的父节点）挂载作业节点（key 为 job.UID）
//  3. 作业节点是叶子节点，没有子节点，attr 指向作业的 DRF 属性
//
// 为什么 i 从 1 开始？因为 paths[0] 是 "root"，根节点已存在
func (drf *drfPlugin) buildHierarchy(root *hierarchicalNode, job *api.JobInfo, attr *drfAttr,
	hierarchy, hierarchicalWeights string) {
	inode := root
	paths := strings.Split(hierarchy, "/")             // 如 "root/eng/dev" → ["root", "eng", "dev"]
	weights := strings.Split(hierarchicalWeights, "/") // 如 "100/50/50" → ["100", "50", "50"]

	for i := 1; i < len(paths); i++ {
		if child, ok := inode.children[paths[i]]; ok {
			inode = child // 路径已存在，直接下移
		} else {
			// 路径不存在，创建新节点
			fweight, _ := strconv.ParseFloat(weights[i], 64)
			if fweight < 1 {
				fweight = 1 // 权重最小为 1，避免除零
			}
			child = &hierarchicalNode{
				weight:    fweight,
				hierarchy: paths[i],
				request:   api.EmptyResource(),
				attr: &drfAttr{
					allocated: api.EmptyResource(),
				},
				children: make(map[string]*hierarchicalNode),
			}
			klog.V(4).Infof("Node %s added to %s, weight %f",
				child.hierarchy, inode.hierarchy, fweight)
			inode.children[paths[i]] = child
			child.parent = inode
			inode = child
		}
	}

	// 在最底层挂载作业叶子节点
	// 作业节点的 weight=1，因为作业内部不再有子分组
	// request 存储作业的总资源需求，用于判断是否饱和
	child := &hierarchicalNode{
		weight:    1,
		attr:      attr,
		hierarchy: string(job.UID),
		request:   job.TotalRequest.Clone(),
		children:  nil, // 叶子节点没有子节点
	}
	inode.children[string(job.UID)] = child
	// update drf attribute bottom up
	klog.V(4).Infof("Job <%s/%s> added to %s, weights %s, attr %v, total request: %s",
		job.Namespace, job.Name, inode.hierarchy, hierarchicalWeights, child.attr, job.TotalRequest)
}

// updateHierarchicalShare 自底向上递归更新层次树中每个节点的 DRF 属性
//
// 核心算法（两趟遍历）：
//
// 第1趟——找最小主导资源份额（mdr = minimum dominant resource share）：
//
//	遍历所有子节点，找到所有未饱和、非空子节点中，
//	已分配资源相对于集群总资源的最小占比。这个值代表了"最穷"的子节点有多穷。
//
// 第2趟——计算父节点的等效已分配量：
//
//	对于未饱和的子节点：将其已分配量按 mdr/share 的比例缩放后累加
//	  （因为 DRF 追求份额公平，缩放后所有子节点在公平线上的分配量一致）
//	对于已饱和的子节点：直接累加其已分配量（已经吃饱了，不需要缩放）
//
// 最后根据等效已分配量计算父节点的主导份额和主导资源。
//
// 为什么要缩放？
//
//	假设子节点 A 的 share=0.3，子节点 B 的 share=0.6，mdr=0.3
//	→ A 的缩放系数 = 0.3/0.3 = 1（已经是最穷，不用缩放）
//	→ B 的缩放系数 = 0.3/0.6 = 0.5（B 比较富裕，缩小到公平线的份额）
//	→ 这样父节点的等效分配量反映了"如果每个子节点都只占 mdr 这么多，总共需要多少"
/*
root
 └── team (weight=1)
      ├── job-A (已分配: 30 CPU + 10G 内存)
      └── job-B (已分配: 10 CPU + 80G 内存)

# 先算两个作业的 DRF 份额
| 作业  | CPU 占比      | 内存占比       | 主导份额 | 主导资源    |
|------|--------------|---------------|---------|-----------|
| A    | 30/100 = 0.3 | 10/200 = 0.05 | 0.3     | CPU 		|
| B    | 10/100 = 0.1 | 80/200 = 0.4  | 0.4 	| 内存 		|

现在问题来了：team 队列的份额应该算多少？
如果直接把 A 和 B 的已分配量相加，得到 40 CPU + 90G 内存：
CPU 占比 = 40/100 = 0.4
内存占比 = 90/200 = 0.45
主导份额 = 0.45
但这样算有问题！A 的主导份额只有 0.3，B 是 0.4。A 更"穷"，按照 DRF 公平原则，A 应该优先获得更多资源。
如果父队列直接简单相加，A 的"穷"就被 B 的"富"给拉平了，父队列看起来比实际情况更富裕，导致 A 在跨队列竞争时吃亏。
--------------------------------------------------------------------------------------------
算法的核心思想：把所有人拉到同一条"公平线"
假设 A 和 B 都是 team 队列的孩子，DRF 说它们应该被同等对待。既然 A 的最小份额是 0.3，那理想情况下 B 也应该只算 0.3，而不是 0.4。
怎么做？缩放——把 B 的已分配量按 0.3/0.4 = 0.75 缩小，这样 B 的"等效分配量"就和 A 站在了同一条公平线上。
这就是 updateHierarchicalShare 两趟遍历的本质：
---------------------------------------------------------------------------------------------
第一趟：找最穷的孩子（mdr）
var mdr float64 = 1
for _, child := range node.children {
    drf.updateHierarchicalShare(child, demandingResources)  // 先递归处理子节点
    if child.attr.share != 0 && !child.saturated {
        _, resShare := drf.calculateShare(child.attr.allocated, drf.totalResource)
        if resShare < mdr {
            mdr = resShare  // 找到最穷的
        }
    }
}

mdr = minimum dominant resource share（最小主导份额）
在本例中，A 的 share=0.3，B 的 share=0.4 → mdr = 0.3
--------------------------------------------------------------------------------------------
第二趟：缩放后汇总
node.attr.allocated = api.EmptyResource()
for _, child := range node.children {
    if child.attr.share != 0 {
        if child.saturated {
            // 已饱和的孩子：实际分配量直接累加（已经吃饱了，不用缩放）
            node.attr.allocated.Add(child.attr.allocated)
        } else {
            // 未饱和的孩子：按 mdr/share 缩放后累加
            t := child.attr.allocated.Clone().Multi(mdr / child.attr.share)
            node.attr.allocated.Add(t)
        }
    }
}
对本例：

| 作业  | 实际分配     | share | 缩放系数         | 等效分配      |
|------|-------------|-------|----------------|--------------|
| A    | 30 CPU, 10G | 0.3   | 0.3/0.3 = 1.0  | 30 CPU, 10G  |
| B    | 10 CPU, 80G | 0.4   | 0.3/0.4 = 0.75 | 7.5 CPU, 60G |
父队列 team 的等效已分配量 = 37.5 CPU + 70G 内存
然后重新计算 team 的主导份额：
CPU: 37.5/100 = 0.375
内存: 70/200 = 0.35
主导份额 = 0.375
对比不加缩放的情况（0.45），加了缩放后父队列的份额更真实地反映了内部最穷作业的状况，
使得 team 在与其他队列竞争时不会因为 B 很"富"而被误判为高份额。
*/
func (drf *drfPlugin) updateHierarchicalShare(node *hierarchicalNode,
	demandingResources map[v1.ResourceName]bool) {
	if node.children == nil {
		// 叶子节点：直接判断是否饱和
		node.saturated = resourceSaturated(node.attr.allocated,
			node.request, demandingResources)
		klog.V(4).Infof("Update hierarchical node %s, share %f, dominant %s, resource %v, saturated: %t",
			node.hierarchy, node.attr.share, node.attr.dominantResource, node.attr.allocated, node.saturated)
	} else {
		// 内部节点：需要递归处理子节点
		var mdr float64 = 1
		// 第1趟：递归更新子节点 + 找最小主导资源份额
		for _, child := range node.children {
			drf.updateHierarchicalShare(child, demandingResources)
			// 跳过空子节点和已饱和子节点
			if child.attr.share != 0 && !child.saturated {
				_, resShare := drf.calculateShare(child.attr.allocated, drf.totalResource)
				if resShare < mdr {
					mdr = resShare // 找到最穷子节点的份额
				}
			}
		}

		// 第2趟：根据 mdr 计算父节点的等效已分配量
		node.attr.allocated = api.EmptyResource()
		saturated := true
		for _, child := range node.children {
			if !child.saturated {
				saturated = false // 只要有一个子节点未饱和，父节点就未饱和
			}
			// 只处理非空子节点
			if child.attr.share != 0 {
				if child.saturated {
					// 已饱和的子节点直接累加（已经吃饱，不参与缩放）
					t := child.attr.allocated
					node.attr.allocated.Add(t)
				} else {
					// 未饱和的子节点按 mdr/share 缩放后累加
					t := child.attr.allocated.Clone().Multi(mdr / child.attr.share)
					node.attr.allocated.Add(t)
				}
			}
		}
		// 根据等效已分配量计算父节点的主导份额
		node.attr.dominantResource, node.attr.share = drf.calculateShare(
			node.attr.allocated, drf.totalResource)
		node.saturated = saturated
		klog.V(4).Infof("Update hierarchical node %s, share %f, dominant resource %s, resource %v, saturated: %t",
			node.hierarchy, node.attr.share, node.attr.dominantResource, node.attr.allocated, node.saturated)
	}
}

// UpdateHierarchicalShare 更新 HDRF 层次树的入口方法
//
// 执行流程：
//  1. 计算哪些资源是"紧缺的"(已分配量 < 总量，还有争抢空间)
//  2. 调用 buildHierarchy 将作业挂载到层次树上
//  3. 调用 updateHierarchicalShare 自底向上更新所有节点的 DRF 属性
func (drf *drfPlugin) UpdateHierarchicalShare(root *hierarchicalNode, totalAllocated *api.Resource, job *api.JobInfo, attr *drfAttr, hierarchy, hierarchicalWeights string) {
	// 计算紧缺资源：已分配量 < 集群总量的资源类型
	// 只有紧缺资源才值得争抢，不紧缺的资源无法获得更多
	demandingResources := map[v1.ResourceName]bool{}
	for _, rn := range drf.totalResource.ResourceNames() {
		if totalAllocated.Get(rn) < drf.totalResource.Get(rn) {
			demandingResources[rn] = true
		}
	}
	drf.buildHierarchy(root, job, attr, hierarchy, hierarchicalWeights)
	drf.updateHierarchicalShare(root, demandingResources)
}

// updateShare 更新单个作业的 DRF 属性
// 调用 calculateShare 计算主导资源和主导份额
func (drf *drfPlugin) updateShare(attr *drfAttr) {
	attr.dominantResource, attr.share = drf.calculateShare(attr.allocated, drf.totalResource)
}

// calculateShare 计算 DRF 的核心函数——主导资源和主导份额
//
// 算法：遍历所有资源类型，计算每种资源的占比（已分配量 / 总量），
// 取最大的占比作为主导份额（dominant share），对应的资源类型作为主导资源（dominant resource）
//
// 举例：
//
//	集群有 100 CPU + 200G 内存
//	作业已用 30 CPU + 10G 内存
//	→ CPU 占比 = 30/100 = 0.3
//	→ 内存占比 = 10/200 = 0.05
//	→ 主导份额 = 0.3, 主导资源 = "cpu"
//
// 返回值：(主导资源名称, 主导份额)
func (drf *drfPlugin) calculateShare(allocated, totalResource *api.Resource) (string, float64) {
	res := float64(0)
	dominantResource := ""
	for _, rn := range totalResource.ResourceNames() {
		share := helpers.Share(allocated.Get(rn), totalResource.Get(rn))
		if share > res {
			res = share
			dominantResource = string(rn)
		}
	}

	return dominantResource, res
}

// OnSessionClose 在调度会话结束时被调用
// 清理所有调度数据，为下一个调度周期做准备
// 注意：不清理 namespaceOpts（预留字段），也不清理 hierarchicalRoot
func (drf *drfPlugin) OnSessionClose(session *framework.Session) {
	// 清理调度数据
	drf.totalResource = api.EmptyResource()
	drf.totalAllocated = api.EmptyResource()
	drf.jobAttrs = map[api.JobID]*drfAttr{}
}
