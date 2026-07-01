/*
Copyright 2021 The Volcano Authors.

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

// Package overcommit 实现了 Volcano 调度器中的超分（overcommit）插件。
//
// 【插件核心职责】
// 超分插件用于控制 enqueue（入队）阶段的资源超分比。在集群资源不足时，
// 调度器允许一定程度的资源超额承诺，即允许进入队列的 Pod 总资源需求超过集群实际可用资源。
// 这样可以在资源紧张时仍保持一定的调度弹性，避免过多 Pod 因资源不足而被阻塞在队列外。
//
// 【核心概念】
//   - overcommit-factor（超分因子）：决定集群资源可被超额承诺的倍数，默认值为 1.2，
//     即允许入队 Pod 的资源需求达到集群总资源的 120%。
//   - idleResource（空闲资源）：集群总资源 × 超分因子 - 已使用资源，
//     表示当前还能容纳多少新的入队资源需求。
//   - inqueueResource（已入队资源）：已经获准入进入队列的 Job 所占用的资源总量，
//     用于在后续 Job 入队判断时做资源扣减。
//
// 【生效阶段】
// 该插件在 enqueue 动作中生效，通过注册 JobEnqueueableFn 和 JobEnqueuedFn 两个扩展点来：
//  1. 判断一个 Job 是否可以入队（JobEnqueueableFn）
//  2. 在 Job 入队后更新已入队资源统计（JobEnqueuedFn）
package overcommit

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// PluginName 是插件的名称，用于在调度器配置中标识该插件
	PluginName = "overcommit"
	// overCommitFactor 是资源配置项的键名，用于从插件参数中获取超分因子
	// 它决定了当集群资源不足时，调度器允许多少个 pending 状态的 Pod 进入队列
	overCommitFactor = "overcommit-factor"
	// defaultOverCommitFactor 定义了默认的超分因子为 1.2
	// 即允许入队 Pod 的资源需求总量达到集群总资源的 120%
	defaultOverCommitFactor = 1.2
)

// overcommitPlugin 是超分插件的结构体，保存插件运行所需的状态和配置
type overcommitPlugin struct {
	// pluginArguments 存储插件的配置参数，从调度器配置中传入
	pluginArguments framework.Arguments
	// totalResource 记录集群的总资源量（所有节点资源之和）
	totalResource *api.Resource
	// idleResource 记录集群的可用空闲资源量
	// 计算方式：集群总资源 × 超分因子 - 所有节点已使用资源
	// 该值代表了集群在超分策略下还能额外接纳多少资源需求
	idleResource *api.Resource
	// inqueueResource 记录当前已获准入队列的 Job 所占用的资源总量
	// 每当有新 Job 入队时，会累加该 Job 的最小资源需求
	// 在判断后续 Job 能否入队时，会将此值作为已占用资源参与比较
	inqueueResource *api.Resource
	// overCommitFactor 超分因子，控制资源超额承诺的倍数
	// 值越大，允许入队的 Pod 越多，但资源争抢风险也越高
	overCommitFactor float64
}

// New 创建并返回一个新的 overcommit 插件实例
// 初始化时所有资源统计置为零值，超分因子使用默认值 1.2
func New(arguments framework.Arguments) framework.Plugin {
	return &overcommitPlugin{
		pluginArguments:  arguments,
		totalResource:    api.EmptyResource(),
		idleResource:     api.EmptyResource(),
		inqueueResource:  api.EmptyResource(),
		overCommitFactor: defaultOverCommitFactor,
	}
}

// Name 返回插件名称
func (op *overcommitPlugin) Name() string {
	return PluginName
}

/*
OnSessionOpen 用户需要通过 overcommit 插件参数配置超分因子，格式如下：

actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: overcommit
    arguments:
    overcommit-factor: 1.0

【配置说明】
  - overcommit-factor 必须 >= 1.0，否则会被重置为默认值 1.2
  - 设为 1.0 表示不允许超分，入队 Pod 的资源需求不能超过集群实际可用资源
  - 设为 1.2（默认值）表示允许入队 Pod 的资源需求达到集群总资源的 120%
*/
func (op *overcommitPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter overcommit plugin ...")
	defer klog.V(5).Infof("Leaving overcommit plugin.")

	// 从插件参数中读取用户配置的超分因子
	op.pluginArguments.GetFloat64(&op.overCommitFactor, overCommitFactor)
	// 校验超分因子的合法性：不允许小于 1.0（小于 1.0 意味着比实际资源还少，没有意义）
	if op.overCommitFactor < 1.0 {
		klog.Warningf("Invalid input %f for overcommit-factor, reason: overcommit-factor cannot be less than 1,"+
			" using default value: %f.", op.overCommitFactor, defaultOverCommitFactor)
		op.overCommitFactor = defaultOverCommitFactor
	}

	// 【第一步】统计集群总资源：将 Session 中所有节点的资源汇总
	op.totalResource.Add(ssn.TotalResource)

	// 【第二步】计算集群的可用空闲资源（考虑超分因子）
	// 先遍历所有节点，累加已使用的资源
	used := api.EmptyResource()
	for _, node := range ssn.Nodes {
		used.Add(node.Used)
	}
	// 空闲资源 = 集群总资源 × 超分因子 - 已使用资源
	// Multi(op.overCommitFactor) 将总资源放大超分倍数，相当于扩大了资源上限
	// SubWithoutAssert 执行减法且不断言结果非负（允许结果为负，表示已超用）
	op.idleResource = op.totalResource.Clone().Multi(op.overCommitFactor).SubWithoutAssert(used)

	// 【第三步】统计已入队 Job 的资源占用量
	for _, job := range ssn.Jobs {
		// 情况一：Job 处于 Inqueue(已入队)状态
		// 将该 Job 的最小资源需求累加到 inqueueResource 中
		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue && job.PodGroup.Spec.MinResources != nil {
			// 扣除调度门控(scheduling gated)任务占用的资源
			// 因为被门控的任务暂时不会调度，不应阻塞其他 Job 入队
			op.inqueueResource.Add(job.DeductSchGatedResources(job.GetMinResources()))
			continue
		}
		/* 情况二：Job 处于 Running(运行中)状态
		这个判断避免了对已完成 Job(如 Spark driver 已完成但 PodGroup 仍为 Running)重复预留资源
		核心问题：为什么要单独处理 Running Job?
		allocatedTaskNum >= MinMember ---> 已分配(绑定到节点)的 Task 数 ≥ 最小运行成员数
			因为 Running 状态的 Job 已经有一部分资源被实际占用了，不能像 Inqueue 状态的 Job 那样把 MinResources 整个算进去，否则会重复计算
		*/
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			// GetInqueueResource 计算运行中 Job 还需预留的资源 = MinResources - 已分配资源
			inqueued := util.GetInqueueResource(job, job.Allocated)
			// 同样扣除调度门控任务的资源
			op.inqueueResource.Add(job.DeductSchGatedResources(inqueued))
		}
	}

	// 【第四步】注册 JobEnqueueableFn 扩展点
	// 该函数在 enqueue 阶段被调用，用于判断一个 Job 是否有足够的资源可以入队
	// 返回 util.Permit 表示允许入队，util.Reject 表示拒绝入队
	ssn.AddJobEnqueueableFn(op.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)
		idle := op.idleResource
		// 拷贝一份已入队资源，避免修改原始数据
		inqueue := api.EmptyResource()
		inqueue.Add(op.inqueueResource)

		// 特殊情况：BestEffort 类型的 Job（没有设置 MinResources）
		// 这类 Job 不保证资源，直接允许入队
		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("Job <%s/%s> is bestEffort, permit to be inqueue.", job.Namespace, job.Name)
			return util.Permit
		}

		//TODO: if allow 1 more job to be inqueue beyond overcommit-factor, large job may be inqueue and create pods
		// 获取该 Job 的最小资源需求
		jobMinReq := job.GetMinResources()
		// 核心判断：已入队资源 + 当前 Job 最小资源需求 <= 空闲资源（按请求的维度逐一比较）
		// LessEqualWithDimensionAndResourcesName 只比较 jobMinReq 中请求了的资源维度
		// 返回值 couldInqueue 表示是否可以入队，resourceNames 记录超用的资源名称列表
		couldInqueue, resourceNames := inqueue.Add(jobMinReq).LessEqualWithDimensionAndResourcesName(idle, jobMinReq)
		if couldInqueue { // 资源充足，允许入队
			klog.V(4).Infof("Sufficient resources, permit job <%s/%s> to be inqueue", job.Namespace, job.Name)
			return util.Permit
		}
		// 资源不足，拒绝入队
		klog.V(4).Infof("Resource in cluster is overused, reject job <%s/%s> to be inqueue",
			job.Namespace, job.Name)

		// 记录 PodGroup 不可调度的事件，方便用户排查问题
		// 事件中包含具体超用的资源名称列表
		ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), util.FormatResourceNames("resource in cluster is overused", "overused", resourceNames))
		return util.Reject
	})

	// 【第五步】注册 JobEnqueuedFn 扩展点
	// 该函数在 Job 成功入队后被调用，用于更新 inqueueResource 统计
	// 这样后续 Job 的入队判断就能感知到本次入队带来的资源变化
	ssn.AddJobEnqueuedFn(op.Name(), func(obj interface{}) {
		job := obj.(*api.JobInfo)
		// BestEffort Job 不占用资源配额，跳过
		if job.PodGroup.Spec.MinResources == nil {
			return
		}
		// 将该 Job 的最小资源需求（扣除调度门控任务资源后）累加到已入队资源中
		jobMinReq := job.GetMinResources()
		op.inqueueResource.Add(job.DeductSchGatedResources(jobMinReq))
	})
}

// OnSessionClose 在调度会话关闭时清理资源引用，释放内存
// 每次调度周期结束后调用，确保下一轮调度使用全新的状态
func (op *overcommitPlugin) OnSessionClose(ssn *framework.Session) {
	op.totalResource = nil
	op.idleResource = nil
	op.inqueueResource = nil
}
