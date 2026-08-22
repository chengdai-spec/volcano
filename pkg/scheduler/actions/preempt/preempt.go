/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added topology-aware preemption
- Enhanced with predicate error caching and BestEffort constraints
- Added victim selection algorithms with scoring and ordering

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

package preempt

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// 抢占 Action 的配置参数键名，用于从调度器配置中读取对应的参数值。
const (
	// EnableTopologyAwarePreemptionKey 是否启用拓扑感知抢占的配置键。
	// 拓扑感知抢占会并行模拟驱逐多个候选节点，再从中选出最优节点，而非逐个节点尝试。
	EnableTopologyAwarePreemptionKey = "enableTopologyAwarePreemption"

	// TopologyAwarePreemptWorkerNumKey 拓扑感知抢占并行 worker 数量的配置键。
	// 控制 DryRunPreemption 并行模拟驱逐时的并发度。
	TopologyAwarePreemptWorkerNumKey = "topologyAwarePreemptWorkerNum"

	// MinCandidateNodesPercentageKey 候选节点最小百分比的配置键。
	// DryRunPreemption 评估的候选节点数 = 总节点数 × 此百分比 / 100。
	MinCandidateNodesPercentageKey = "minCandidateNodesPercentage"
	// MinCandidateNodesAbsoluteKey 候选节点最小绝对数的配置键。
	// 即使百分比计算结果更小，也至少评估此数量的候选节点。
	MinCandidateNodesAbsoluteKey = "minCandidateNodesAbsolute"
	// MaxCandidateNodesAbsoluteKey 候选节点最大绝对数的配置键。
	// 限制 DryRunPreemption 最多评估的候选节点数量，避免大规模集群下评估开销过大。
	MaxCandidateNodesAbsoluteKey = "maxCandidateNodesAbsolute"
)

// Action 定义了抢占（preempt）调度动作的结构体。
// 抢占是指高优先级任务驱逐低优先级任务以获取资源的调度行为。
// 支持两种抢占模式：普通抢占（normalPreempt）和拓扑感知抢占（topologyAwarePreempt）。
type Action struct {
	ssn *framework.Session // 调度会话，包含当前调度周期的所有状态和插件回调函数

	enablePredicateErrorCache bool // 是否启用谓词错误缓存，避免对同一节点重复执行已知会失败的谓词检查

	enableTopologyAwarePreemption bool // 是否启用拓扑感知抢占模式

	topologyAwarePreemptWorkerNum int // 拓扑感知抢占的并行 worker 数量（默认 16）
	minCandidateNodesPercentage   int // 候选节点最小百分比（默认 10），用于计算需要评估的节点数量
	minCandidateNodesAbsolute     int // 候选节点最小绝对数（默认 1）
	maxCandidateNodesAbsolute     int // 候选节点最大绝对数（默认 100）
}

// New 创建并返回一个抢占 Action 实例，所有参数使用默认值。
// 默认配置：启用谓词错误缓存、关闭拓扑感知抢占、16 个并行 worker、
// 候选节点百分比 10%、最小绝对数 1、最大绝对数 100。
func New() *Action {
	return &Action{
		enablePredicateErrorCache:     true,
		enableTopologyAwarePreemption: false,
		topologyAwarePreemptWorkerNum: 16,
		minCandidateNodesPercentage:   10,
		minCandidateNodesAbsolute:     1,
		maxCandidateNodesAbsolute:     100,
	}
}

// Name 返回抢占动作的名称 "preempt"，用于在调度框架中标识此 Action。
func (pmpt *Action) Name() string {
	return "preempt"
}

// Initialize 初始化抢占动作（当前为空实现，预留扩展）。
func (pmpt *Action) Initialize() {}

// parseArguments 从调度器配置中解析抢占动作的各项参数。
// 依次读取谓词错误缓存、拓扑感知抢占开关、并行 worker 数量、
// 候选节点百分比/最小绝对数/最大绝对数等配置项，并保存 Session 引用。
func (pmpt *Action) parseArguments(ssn *framework.Session) {
	arguments := framework.GetArgOfActionFromConf(ssn.Configurations, pmpt.Name())
	arguments.GetBool(&pmpt.enablePredicateErrorCache, conf.EnablePredicateErrCacheKey)
	arguments.GetBool(&pmpt.enableTopologyAwarePreemption, EnableTopologyAwarePreemptionKey)
	arguments.GetInt(&pmpt.topologyAwarePreemptWorkerNum, TopologyAwarePreemptWorkerNumKey)
	arguments.GetInt(&pmpt.minCandidateNodesPercentage, MinCandidateNodesPercentageKey)
	arguments.GetInt(&pmpt.minCandidateNodesAbsolute, MinCandidateNodesAbsoluteKey)
	arguments.GetInt(&pmpt.maxCandidateNodesAbsolute, MaxCandidateNodesAbsoluteKey)
	pmpt.ssn = ssn
}

// Execute 执行抢占动作的完整流程。
//
// 抢占是指高优先级任务驱逐低优先级任务以获取资源的调度行为。
// 抢占分为两个阶段：队列内抢占（Job 间 + Task 间）。
//
// 步骤：
//  1. 解析配置参数；
//  2. 遍历所有 Job，收集“饥饿”(Starving)的抢占者 Job 及其 Pending 任务；
//  3. 按队列优先级排序，依次处理每个队列的抢占；
//  4. 队列内 Job 间抢占：按 Job 优先级依次尝试抢占同队列中其他 Job 的资源；
//  5. 队列内 Task 间抢占：对每个 Job 尝试抢占同 Job 内其他 Task 的资源；
//  6. 使用 Statement 事务机制确保抢占成功时提交、失败时回滚。
func (pmpt *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Preempt ...")
	defer klog.V(5).Infof("Leaving Preempt ...")

	// 步骤 1：解析配置参数
	pmpt.parseArguments(ssn)

	// preemptorsMap: 队列 ID → 抢占者 Job 优先级队列（按 JobOrderFn 排序）
	preemptorsMap := map[api.QueueID]*util.PriorityQueue{}
	// preemptorTasks: Job ID → 该 Job 中待抢占的 Task 优先级队列（按 TaskOrderFn 排序）
	preemptorTasks := map[api.JobID]*util.PriorityQueue{}

	// underRequestByQueue: 队列 ID → 该队列下所有“饥饿”Job 列表，用于后续 Job 内 Task 间抢占
	underRequestByQueue := map[api.QueueID][]*api.JobInfo{}

	// 步骤 2：遍历所有 Job，收集符合条件的抢占者
	for _, job := range ssn.Jobs {
		// 跳过 Pending 状态的 Job（尚未进入调度流程）
		if job.IsPending() {
			continue
		}

		// 跳过未通过有效性检查的 Job
		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip preemption, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		// 跳过所属队列不存在的 Job
		if _, found := ssn.Queues[job.Queue]; !found {
			klog.V(3).Infof("Queue <%s> not found for Job <%s/%s>, skip preemption", job.Queue, job.Namespace, job.Name)
			continue
		}

		// 只处理“饥饿”的 Job（资源需求未满足，需要更多资源）
		if !ssn.JobStarving(job) {
			continue
		}

		// TODO: 当前包含 NetworkTopology 约束的 Job 不支持抢占
		// 原因：抢占可能破坏 NCCL 通信拓扑完整性，待 issue #4374 解决后移除此限制
		if job.ContainsNetworkTopology() {
			klog.V(3).Infof("Job <%s/%s> Queue <%s> skip preemption, reason: jobs containing networkTopology do not support preemption",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		// 将抢占者 Job 加入对应队列的优先级队列
		if _, found := preemptorsMap[job.Queue]; !found {
			preemptorsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
		}
		preemptorsMap[job.Queue].Push(job)
		underRequestByQueue[job.Queue] = append(underRequestByQueue[job.Queue], job)
		// 收集该 Job 中所有 Pending 且未被调度门控的 Task 作为抢占者任务。
		preemptorTasks[job.UID] = util.NewPriorityQueue(ssn.TaskOrderFn)
		for _, task := range job.TaskStatusIndex[api.Pending] {
			if task.SchGated {
				continue
			}
			preemptorTasks[job.UID].Push(task)
		}
	}

	// 步骤 3：按队列优先级排序，依次处理每个队列。
	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	for queueID := range preemptorsMap {
		if queue, found := ssn.Queues[queueID]; found {
			queues.Push(queue)
		}
	}

	ph := util.NewPredicateHelper()
	// 步骤 4：队列内 Job 间抢占
	// 按队列优先级依次处理，每个队列内按 Job 优先级依次尝试抢占同队列中其他 Job 的资源
	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)
		for {
			preemptors := preemptorsMap[queue.UID]

			// 如果队列为空或没有抢占者，跳出当前队列的处理循环
			if preemptors == nil || preemptors.Empty() {
				klog.V(4).Infof("No preemptors in Queue <%s>, break.", queue.Name)
				break
			}

			preemptorJob := preemptors.Pop().(*api.JobInfo)

			// 为当前 Job 的抢占创建临时 Statement，隔离驱逐操作
			stmt := framework.NewStatement(ssn)
			var assigned bool
			var err error
			for {
				// 如果 Job 不再“饥饿”（资源已满足），停止抢占
				if !ssn.JobStarving(preemptorJob) {
					break
				}

				// 如果没有待抢占的 Task，跳出当前 Job 的处理循环
				if preemptorTasks[preemptorJob.UID].Empty() {
					klog.V(3).Infof("No preemptor task in job <%s/%s>.",
						preemptorJob.Namespace, preemptorJob.Name)
					break
				}

				// 弹出最高优先级的待抢占 Task
				preemptor := preemptorTasks[preemptorJob.UID].Pop().(*api.TaskInfo)

				// 执行抢占，filter 函数限定可被抢占的目标：
				// 必须是同队列内其他 Job 的、可抢占状态的、符合 BestEffort 约束的任务
				assigned, err = pmpt.preempt(ssn, stmt, preemptor, func(task *api.TaskInfo) bool {
					// 忽略非运行状态的任务
					if !api.PreemptableStatus(task.Status) {
						return false
					}
					// BestEffort Pod 不能抢占非 BestEffort Pod
					if preemptor.BestEffort && !task.BestEffort {
						return false
					}
					// 跳过未标记为可抢占的任务（通过 volcano.sh/preemptable 注解控制）
					if !task.Preemptable {
						return false
					}
					job, found := ssn.Jobs[task.Job]
					if !found {
						return false
					}
					// 只能抢占同队列内其他 Job 的任务。
					return job.Queue == preemptorJob.Queue && preemptor.Job != task.Job
				}, ph)
				if err != nil {
					klog.V(3).Infof("Preemptor <%s/%s> failed to preempt Task , err: %s", preemptor.Namespace, preemptor.Name, err)
				}
			}

			// 仅当 Job 已被管线（Pipelined）时才提交变更，否则回滚并尝试下一个 Job。
			if ssn.JobPipelined(preemptorJob) {
				stmt.Commit()
			} else {
				stmt.Discard()
				continue
			}

			// 如果抢占成功分配了资源，将 Job 重新加入优先级队列，以便继续抢占。
			if assigned {
				preemptors.Push(preemptorJob)
			}
		}

		// 步骤 5：队列内 Job 内 Task 间抢占
		// 对当前队列下的每个 Job，尝试让高优先级 Pending Task 抢占同 Job 内低优先级运行中 Task 的资源
		for _, job := range underRequestByQueue[queue.UID] {
			// 使用独立的 intraJobPreemptors 优先级队列，避免覆盖 preemptorTasks map
			// preemptorTasks 在上方 Job 发现阶段填充，被队列间抢占循环消费；
			// 如果在此处覆盖，多队列场景下会因 Go map 迭代顺序不确定性导致其他队列的抢占者丢失
			intraJobPreemptors := util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				// 跳过调度门控的 Task。
				if task.SchGated {
					continue
				}
				intraJobPreemptors.Push(task)
			}
			for {
				if intraJobPreemptors.Empty() {
					break
				}

				preemptor := intraJobPreemptors.Pop().(*api.TaskInfo)

				// 为每个 Task 的抢占创建独立的 Statement
				stmt := framework.NewStatement(ssn)
				// 执行抢占，filter 函数限定可被抢占的目标为同 Job 内的任务
				assigned, err := pmpt.preempt(ssn, stmt, preemptor, func(task *api.TaskInfo) bool {
					// 忽略非运行状态的任务
					if !api.PreemptableStatus(task.Status) {
						return false
					}
					// BestEffort Pod 不能抢占非 BestEffort Pod
					if preemptor.BestEffort && !task.BestEffort {
						return false
					}
					// 跳过未标记为可抢占的任务
					if !task.Preemptable {
						return false
					}

					// 只能抢占同 Job 内的任务
					return preemptor.Job == task.Job
				}, ph)
				if err != nil {
					klog.V(3).Infof("Preemptor <%s/%s> failed to preempt Task , err: %s", preemptor.Namespace, preemptor.Name, err)
				}

				// 仅当抢占成功时才提交变更，否则回滚驱逐操作并跳出当前 Task 的循环。
				// 这与队列间抢占检查 JobPipelined 后才提交的逻辑一致。
				if !assigned {
					stmt.Discard()
					break
				}
				stmt.Commit()
			}
		}
	}
}

// UnInitialize 反初始化抢占动作（当前为空实现，预留扩展）。
func (pmpt *Action) UnInitialize() {}

// preempt 执行单个任务的抢占流程。
//
// 步骤：
//
//	1.检查抢占资格(preemptionPolicy 是否为 Never/nominatedNode 状态检查等)
//	2.运行 PrePredicateFn(PreFilter 插件)
//	3.过滤不可调度节点,然后执行谓词过滤得到可行节点列表
//	4.按 Shard 分组(本 Shard 优先,其他 Shard 次之)
//	5.根据配置选择 topologyAwarePreempt 或 normalPreempt 执行抢占
func (pmpt *Action) preempt(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateHelper util.PredicateHelper,
) (bool, error) {
	// 步骤 1：检查抢占资格。
	if err := pmpt.taskEligibleToPreempt(preemptor); err != nil {
		return false, err
	}

	// 步骤 2：运行 PreFilter 插件，生成 CycleState。
	if err := ssn.PrePredicateFn(preemptor); err != nil {
		return false, fmt.Errorf("PrePredicate for task %s/%s failed for: %v", preemptor.Namespace, preemptor.Name, err)
	}

	// 步骤 3：过滤不可调度节点，执行谓词过滤。
	// PredicateForPreemptAction 会保留状态为 Unschedulable（可解析）的节点，过滤掉 UnschedulableAndUnresolvable 的节点。
	/*
		┌──────────────────────────────────────────────────────────┐
		│ Node-1: 资源不足（CPU 已用 95%）                            │
		│   谓词结果: Unschedulable                                  │
		│   原因: Insufficient CPU                                  │
		│   驱逐低优先级Pod? → 释放CPU → A可以调度 ✅                  │
		├──────────────────────────────────────────────────────────┤
		│ Node-2: Pod要求 nodeAffinity: zone=us-east                │
		│   但 Node-2 的标签是 zone=eu-west                          │
		│   谓词结果: UnschedulableAndUnresolvable                   │
		│   原因: node(s) didn't match Pod's node affinity/selector │
		│   驱逐低优先级Pod? → 标签还是不对 → A仍然无法调度 ❌            │
		├──────────────────────────────────────────────────────────-┤
		│ Node-3: 节点有污点 gpu=true:NoSchedule                      │
		│   Pod A 没有对应容忍                                        │
		│   谓词结果: UnschedulableAndUnresolvable                   │
		│   原因: node(s) had taint {gpu=true:NoSchedule}           │
		│   驱逐低优先级Pod? → 污点还在 → A仍然无法调度 ❌               │
		└──────────────────────────────────────────────────────────┘
	*/

	allNodes := ssn.FilterOutUnschedulableAndUnresolvableNodesForTask(preemptor)
	predicateNodes, _ := predicateHelper.PredicateNodes(preemptor, allNodes, ssn.PredicateForPreemptAction, pmpt.enablePredicateErrorCache, ssn.NodesInShard)

	// 步骤 4：按 Shard 分组，返回 [本Shard节点, 其他Shard节点]
	candidateNodes := util.GetPredicatedNodeByShard(predicateNodes, ssn.NodesInShard)
	var preemptSuccess bool
	var err error
	// 步骤 5：按优先级依次尝试每组节点，抢占成功则立即返回。
	for _, nodes := range candidateNodes {
		if pmpt.enableTopologyAwarePreemption {
			if preemptSuccess, err = pmpt.topologyAwarePreempt(ssn, stmt, preemptor, filter, nodes); preemptSuccess {
				break
			}
		} else if preemptSuccess, err = pmpt.normalPreempt(ssn, stmt, preemptor, filter, nodes); preemptSuccess {
			break
		}
	}
	return preemptSuccess, err
}

// normalPreempt 执行普通（非拓扑感知）的抢占流程。
//
// 与 topologyAwarePreempt 不同，normalPreempt 先对所有候选节点打分排序，
// 然后从最高分节点开始逐个尝试驱逐，直到找到一个能容纳抢占者的节点。
//
// 步骤：
//  1. 调用 PrioritizeNodes 对所有候选节点打分，按分数降序排列；
//  2. 依次遍历每个节点，收集可驱逐任务并通过 ValidateVictims 验证；
//  3. 使用临时 Statement 按优先级从低到高驱逐牺牲者，直到资源足够；
//  4. 调用 Pipeline 将抢占者加入节点管线；成功则合并到调用方 Statement，失败则尝试下一个节点。
func (pmpt *Action) normalPreempt(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateNodes []*api.NodeInfo,
) (bool, error) {
	// 步骤 1：对所有候选节点打分并排序。
	nodeScores := util.PrioritizeNodes(preemptor, predicateNodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)
	selectedNodes := util.SortNodes(nodeScores)

	job, found := ssn.Jobs[preemptor.Job]
	if !found {
		return false, fmt.Errorf("not found Job %s in Session", preemptor.Job)
	}

	currentQueue := ssn.Queues[job.Queue]

	assigned := false

	// 步骤 2：依次遍历每个节点，尝试驱逐牺牲者。
	for _, node := range selectedNodes {
		klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.",
			preemptor.Namespace, preemptor.Name, node.Name)

		// 收集节点上符合 filter 条件的可驱逐任务。
		var preemptees []*api.TaskInfo
		for _, task := range node.Tasks {
			if filter == nil {
				preemptees = append(preemptees, task.Clone())
			} else if filter(task) {
				preemptees = append(preemptees, task.Clone())
			}
		}
		victims := ssn.Preemptable(preemptor, preemptees)
		metrics.UpdatePreemptionVictimsCount(len(victims))

		if err := util.ValidateVictims(preemptor, node, victims); err != nil {
			klog.V(3).Infof("No validated victims on Node <%s>: %v", node.Name, err)
			continue
		}

		// 使用临时 Statement 隔离每个节点的驱逐操作。
		// 成功时合并到调用方 Statement；失败时丢弃，避免产生副作用。
		nodeStmt := framework.NewStatement(ssn)

		// 步骤 3：按优先级从低到高驱逐牺牲者，直到资源足够。
		victimsQueue := ssn.BuildVictimsPriorityQueue(victims, preemptor)
		preempted := api.EmptyResource()

		for !victimsQueue.Empty() {
			// 资源足够时停止驱逐。
			// 抢占发生在同一 Queue 内：高优先级 Job/Task 抢占低优先级 Job/Task 的资源。
			// 需要同时满足：队列可分配 + 节点空闲资源足够。
			if ssn.Allocatable(currentQueue, preemptor) && preemptor.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
				break
			}
			preemptee := victimsQueue.Pop().(*api.TaskInfo)
			klog.V(3).Infof("Try to preempt Task <%s/%s> for Task <%s/%s>",
				preemptee.Namespace, preemptee.Name, preemptor.Namespace, preemptor.Name)
			nodeStmt.Evict(preemptee, "preempt")
			preempted.Add(preemptee.Resreq)
		}

		evictionOccurred := false
		if !preempted.IsEmpty() {
			evictionOccurred = true
		}

		metrics.RegisterPreemptionAttempts()
		klog.V(3).Infof("Preempted <%v> for Task <%s/%s> requested <%v>.",
			preempted, preemptor.Namespace, preemptor.Name, preemptor.InitResreq)

		// 步骤 4：将抢占者管线到目标节点。
		if ssn.Allocatable(currentQueue, preemptor) && preemptor.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
			if err := nodeStmt.Pipeline(preemptor, node.Name, evictionOccurred); err != nil {
				klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
					preemptor.Namespace, preemptor.Name, node.Name)
				// Pipeline 失败：丢弃该节点的驱逐操作，尝试下一个节点。
				nodeStmt.Discard()
				continue
			}

			// Pipeline 成功：将该节点的操作合并到调用方 Statement。
			stmt.Merge(nodeStmt)
			assigned = true
			break
		}

		// 资源不足：丢弃该节点的操作，尝试下一个节点。
		nodeStmt.Discard()
	}

	return assigned, nil
}

// taskEligibleToPreempt 检查任务是否有资格抢占其他任务。
//
// 检查规则：
//  1. 如果 Pod 的 preemptionPolicy 为 Never，则不允许抢占；
//  2. 如果 Pod 有 nominatedNodeName，检查该节点是否已经可调度（是则不需抢占）；
//  3. 如果 nominatedNode 上已有低优先级 Pod 因抢占而终止（DisruptionTarget=True），
//     说明上一轮抢占正在生效中，应等待完成而非重复抢占。
func (pmpt *Action) taskEligibleToPreempt(preemptor *api.TaskInfo) error {
	if preemptor.Pod.Spec.PreemptionPolicy != nil && *preemptor.Pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return fmt.Errorf("not eligible to preempt other tasks due to preemptionPolicy is Never")
	}

	nomNodeName := preemptor.Pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		nodeInfo, ok := pmpt.ssn.Nodes[nomNodeName]
		if !ok {
			return fmt.Errorf("not eligible due to the pod's nominated node is not found in the session")
		}

		err := pmpt.ssn.PredicateFn(preemptor, nodeInfo)
		if err == nil {
			return fmt.Errorf("not eligible due to the pod's nominated node is already schedulable, which should not happen as preemption means no node is schedulable")
		}

		fitError, ok := err.(*api.FitError)
		if !ok {
			return fmt.Errorf("not eligible due to the predicate returned a non-FitError error, the error is: %v", err)
		}

		// 如果 nominatedNode 被谓词判定为 UnschedulableAndUnresolvable
		// 说明该节点的问题无法通过抢占解决，应重新进入抢占流程
		if fitError.Status.ContainsUnschedulableAndUnresolvable() {
			return nil
		}

		// 检查 nominatedNode 上是否已有低优先级 Pod 因抢占而终止
		// 如果有，说明上一轮抢占正在生效中，应等待完成而非重复抢占
		preemptorPodPriority := PodPriority(preemptor.Pod)
		for _, p := range nodeInfo.Pods() {
			if PodPriority(p) < preemptorPodPriority && podTerminatingByPreemption(p) {
				return fmt.Errorf("not eligible due to a terminating pod caused by preemption on the nominated node")
			}
		}
	}
	return nil
}

// topologyAwarePreempt 执行拓扑感知的抢占流程。
//
// 与 normalPreempt 不同，topologyAwarePreempt 不会简单地按节点分数排序后逐个尝试，
// 而是通过并行模拟驱逐（DryRunPreemption）在所有候选节点上同时寻找可行的牺牲方案，
// 然后从所有成功的候选节点中选出最优的一个执行真实驱逐。
//
// 步骤：
//  1. 调用 findCandidates 在所有候选节点上并行模拟驱逐，收集可行的候选节点列表
//  2. 调用 SelectCandidate 从候选节点中选出最优节点(基于牺牲者优先级、数量等多维度评分)
//  3. 使用临时 Statement 执行 prepareCandidate 真实驱逐牺牲者；
//  4. 调用 Pipeline 将抢占者加入目标节点的管线；成功则合并到调用方 Statement，失败则回滚
func (pmpt *Action) topologyAwarePreempt(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateNodes []*api.NodeInfo,
) (bool, error) {
	// 步骤 1：在所有候选节点上并行模拟驱逐(dry-run)，收集可行的候选节点
	// 此阶段不会对 stmt 产生任何副作用，所有操作都在克隆的节点快照上执行
	candidates, nodeToStatusMap, err := pmpt.findCandidates(preemptor, filter, predicateNodes, stmt)
	if err != nil && len(candidates) == 0 {
		return false, err
	}

	// 如果没有找到任何可行候选节点，返回错误。
	// nodeToStatusMap 包含每个节点的失败原因，用于日志诊断。
	if len(candidates) == 0 {
		return false, fmt.Errorf("no candidates that fit the pod, the status of the nodes are %v", nodeToStatusMap)
	}

	// 步骤 2：从所有候选节点中选出最优节点。
	// 评分维度依次为：最小最高优先级牺牲者 → 最小优先级总和 → 最少牺牲者数量 → 最晚启动时间。
	bestCandidate := SelectCandidate(candidates)
	if bestCandidate == nil || len(bestCandidate.Name()) == 0 {
		return false, fmt.Errorf("no candidate node for preemption")
	}

	// 步骤 3：使用临时 Statement 执行真实驱逐。
	// 使用临时 stmt 的目的是：只有当整个抢占流程（驱逐 + Pipeline）都成功时，
	// 才将操作合并到调用方的 Statement 中；否则回滚所有驱逐操作，避免产生副作用。
	tmpStmt := framework.NewStatement(ssn)

	// 步骤 3a：对最优候选节点上的所有牺牲者执行真实驱逐（调用 stmt.Evict）。
	prepareCandidate(bestCandidate, preemptor.Pod, tmpStmt)
	// 步骤 3b：将抢占者管线到目标节点。
	if err := tmpStmt.Pipeline(preemptor, bestCandidate.Name(), true); err != nil {
		klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
			preemptor.Namespace, preemptor.Name, bestCandidate.Name())
		// Pipeline 失败：回滚所有驱逐操作，防止产生副作用。
		tmpStmt.Discard()
		return false, err
	}

	// 步骤 4：抢占成功，将临时 Statement 中的所有操作合并到调用方的 Statement。
	stmt.Merge(tmpStmt)
	return true, nil
}

// findCandidates 在所有候选节点上查找抢占候选节点。
//
// 步骤：
//  1. 根据候选节点总数计算需要评估的节点数量（由 minCandidateNodesPercentage 等参数控制）；
//  2. 调用 DryRunPreemption 并行模拟驱逐，收集可行候选节点和各节点的状态信息。
func (pmpt *Action) findCandidates(
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateNodes []*api.NodeInfo,
	stmt *framework.Statement,
) ([]*candidate, map[string]api.Status, error) {
	if len(predicateNodes) == 0 {
		klog.V(3).Infof("No nodes are eligible to preempt task %s/%s", preemptor.Namespace, preemptor.Name)
		return nil, nil, nil
	}
	klog.Infof("the predicateNodes number is %d", len(predicateNodes))

	nodeToStatusMap := make(map[string]api.Status)

	// 步骤 1：计算随机偏移量(用于均匀分布起始节点)和候选节点数量
	// 模拟驱逐多个 Pod/重新计算资源/运行插件校验等
	offset, numCandidates := pmpt.GetOffsetAndNumCandidates(len(predicateNodes))

	// 步骤 2：并行模拟驱逐，收集可行候选节点
	candidates, nodeStatuses, err := pmpt.DryRunPreemption(preemptor, predicateNodes, offset, numCandidates, filter, stmt)
	for node, nodeStatus := range nodeStatuses {
		nodeToStatusMap[node] = nodeStatus
	}

	return candidates, nodeToStatusMap, err
}

// prepareCandidate 对选中的最优候选节点执行真实驱逐操作。
//
// 遍历候选节点上的所有牺牲者（victims），依次调用 stmt.Evict 将其驱逐，
// 为抢占者腾出资源。此函数会在 Statement 中产生真实的副作用，
// 因此调用方需确保在临时 Statement 上执行，以便失败时能回滚。
func prepareCandidate(c *candidate, pod *v1.Pod, stmt *framework.Statement) {
	for _, victim := range c.Victims() {
		klog.V(3).Infof("Try to preempt Task <%s/%s> for Task <%s/%s>",
			victim.Namespace, victim.Name, pod.Namespace, pod.Name)
		stmt.Evict(victim, "preempt")
	}

	metrics.RegisterPreemptionAttempts()
}

// podTerminatingByPreemption 判断 Pod 是否处于因抢占而导致的终止状态
// 检查条件：DeletionTimestamp 不为空 + 存在 DisruptionTarget 条件且 Reason 为 PreemptionByScheduler。
func podTerminatingByPreemption(p *v1.Pod) bool {
	if p.DeletionTimestamp == nil {
		return false
	}

	for _, condition := range p.Status.Conditions {
		if condition.Type == v1.DisruptionTarget {
			return condition.Status == v1.ConditionTrue && condition.Reason == v1.PodReasonPreemptionByScheduler
		}
	}
	return false
}

// PodPriority 返回 Pod 的优先级值。
// 如果 Pod 未设置 Priority，则默认为 0（静态默认优先级）。
func PodPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	// 当运行中的 Pod 的 Priority 为 nil 时，说明创建时还没有全局默认优先级类，
	// 因此使用静态默认优先级 0。
	return 0
}

// calculateNumCandidates 计算 DryRunPreemption 需要评估的候选节点数量。
//
// 计算规则：
//
//	n = numNodes * minCandidateNodesPercentage / 100
//	然后限制在 [minCandidateNodesAbsolute, maxCandidateNodesAbsolute] 范围内，
//	且不超过 numNodes。
func (pmpt *Action) calculateNumCandidates(numNodes int) int {
	n := (numNodes * pmpt.minCandidateNodesPercentage) / 100

	if n < pmpt.minCandidateNodesAbsolute {
		n = pmpt.minCandidateNodesAbsolute
	}

	if n > pmpt.maxCandidateNodesAbsolute {
		n = pmpt.maxCandidateNodesAbsolute
	}

	if n > numNodes {
		n = numNodes
	}

	return n
}

// GetOffsetAndNumCandidates 返回随机偏移量和候选节点数量。
//
// offset 用于在节点列表中错开起始位置，避免总是从同一节点开始评估，
// 从而实现抢占尝试在集群节点上的均匀分布。
func (pmpt *Action) GetOffsetAndNumCandidates(numNodes int) (int, int) {
	return rand.Intn(numNodes), pmpt.calculateNumCandidates(numNodes)
}

// DryRunPreemption 在所有候选节点上并行模拟驱逐操作(dry-run)，查找可行的抢占候选节点
//
// 该函数使用 workqueue.ParallelizeUntil 并行处理所有节点，每个节点的处理逻辑为：
//  1. 克隆节点快照和 CycleState，避免影响真实状态；
//  2. 调用 SelectVictimsOnNode 在克隆的节点上模拟驱逐，获取牺牲者列表；
//  3. 如果找到可行牺牲者，将该节点加入候选列表；
//  4. 当候选数量达到 numCandidates 时，通过 context cancel 提前终止并行处理。
//
// 参数：
//   - offset: 随机偏移量，用于在节点列表中错开起始位置，避免总是从同一节点开始评估；
//   - numCandidates: 最多收集的候选节点数量。
func (pmpt *Action) DryRunPreemption(
	preemptor *api.TaskInfo,
	potentialNodes []*api.NodeInfo,
	offset int,
	numCandidates int,
	filter func(*api.TaskInfo) bool,
	stmt *framework.Statement,
) ([]*candidate, map[string]api.Status, error) {
	candidates := newCandidateList(numCandidates)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeStatuses := make(map[string]api.Status)
	var statusesLock sync.Mutex
	var errs []error

	job, found := pmpt.ssn.Jobs[preemptor.Job]
	if !found {
		return nil, nil, fmt.Errorf("not found Job %s in Session", preemptor.Job)
	}

	currentQueue := pmpt.ssn.Queues[job.Queue]

	state := pmpt.ssn.GetCycleState(preemptor.UID)

	// checkNode 是并行执行的单节点检查函数。
	// i 为节点索引，通过 (offset + i) % len 实现环形遍历，均匀分布起始位置。
	checkNode := func(i int) {
		// 克隆节点快照和状态，确保模拟操作不影响真实数据。
		nodeInfoCopy := potentialNodes[(int(offset)+i)%len(potentialNodes)].Clone()
		stateCopy := state.Clone()

		// 在克隆的节点上模拟驱逐，获取牺牲者列表。
		victims, status := SelectVictimsOnNode(ctx, stateCopy, preemptor, currentQueue, nodeInfoCopy, pmpt.ssn, filter, stmt)
		if status.IsSuccess() && len(victims) != 0 {
			// 找到可行牺牲者，将该节点加入候选列表。
			c := &candidate{
				victims: victims,
				name:    nodeInfoCopy.Name,
			}
			candidates.add(c)
			// 候选数量已达上限，取消剩余并行任务。
			if candidates.size() >= numCandidates {
				cancel()
			}
			return
		}
		if status.IsSuccess() && len(victims) == 0 {
			status = api.AsStatus(fmt.Errorf("expected at least one victim pod on node %q", nodeInfoCopy.Name))
		}
		statusesLock.Lock()
		if status.Code == api.Error {
			errs = append(errs, status.AsError())
		}
		nodeStatuses[nodeInfoCopy.Name] = *status
		statusesLock.Unlock()
	}

	// 使用 topologyAwarePreemptWorkerNum 个 worker 并行处理所有节点。
	workqueue.ParallelizeUntil(ctx, pmpt.topologyAwarePreemptWorkerNum, len(potentialNodes), checkNode)
	return candidates.get(), nodeStatuses, utilerrors.NewAggregate(errs)
}

// candidate 表示一个抢占候选节点，包含节点名称和该节点上需要驱逐的牺牲者列表。
type candidate struct {
	victims []*api.TaskInfo // 该节点上需要驱逐的任务列表
	name    string          // 候选节点名称
}

// Victims 返回候选节点上的牺牲者列表。
func (s *candidate) Victims() []*api.TaskInfo {
	return s.victims
}

// Name 返回候选节点名称。
func (s *candidate) Name() string {
	return s.name
}

// candidateList 是一个并发安全的候选节点列表。
// 多个 goroutine 通过 add() 原子地添加候选节点，
// 最终通过 get() 获取所有已添加的候选节点。
type candidateList struct {
	idx   int32        // 原子计数器，指向下一个可写入的位置
	items []*candidate // 候选节点数组，初始化时分配固定大小
}

// newCandidateList 创建指定容量的候选节点列表。
func newCandidateList(size int) *candidateList {
	return &candidateList{idx: -1, items: make([]*candidate, size)}
}

// add 原子地将候选节点添加到列表中。
// 如果列表已满（idx >= len(items)），则静默丢弃。
func (cl *candidateList) add(c *candidate) {
	if idx := atomic.AddInt32(&cl.idx, 1); idx < int32(len(cl.items)) {
		cl.items[idx] = c
	}
}

// size 返回当前已添加的候选节点数量。
// 注意：此方法可能在 add() 仍在执行时被调用，因此返回值可能不是最终值。
func (cl *candidateList) size() int {
	n := int(atomic.LoadInt32(&cl.idx) + 1)
	if n >= len(cl.items) {
		n = len(cl.items)
	}
	return n
}

// get 返回所有已添加的候选节点。
// 此方法非原子，必须在所有 add() 完成后调用。
func (cl *candidateList) get() []*candidate {
	return cl.items[:cl.size()]
}

// SelectVictimsOnNode 在指定节点上查找最小的抢占牺牲者集合。
//
// 该函数在克隆的节点快照上执行（dry-run），通过模拟移除/添加 Pod 来判断抢占是否可行。
//
// 步骤：
//  1. 收集节点上所有符合 filter 条件的可驱逐任务（preemptees）；
//  2. 调用 ssn.Preemptable 插件链筛选合法的牺牲者候选（allVictims）；
//  3. 调用 ValidateVictims 验证牺牲者是否满足抢占约束；
//  4. 构建牺牲者优先级队列，按优先级从低到高逐个弹出并模拟移除；
//  5. 每次移除后检查：队列是否可分配 + 节点空闲资源是否足够 + 模拟谓词是否通过；
//     如果全部满足，停止移除，当前已移除的 Pod 即为“潜在牺牲者”（potentialVictims）；
//  6. 反向遍历 potentialVictims，尝试“赦免”(reprieve)高优先级 Pod：
//     将被移除的 Pod 重新加回节点，如果抢占者仍然能调度，则不驱逐该 Pod；
//     否则确认驱逐。这样可以最小化实际驱逐的 Pod 数量。
func SelectVictimsOnNode(
	ctx context.Context,
	state fwk.CycleState,
	preemptor *api.TaskInfo,
	currentQueue *api.QueueInfo,
	nodeInfo *api.NodeInfo,
	ssn *framework.Session,
	filter func(*api.TaskInfo) bool,
	stmt *framework.Statement,
) ([]*api.TaskInfo, *api.Status) {
	var potentialVictims []*api.TaskInfo

	// removeTask 模拟从节点上移除一个牺牲者任务（dry-run），分为两个阶段：
	//
	// 阶段 1：调用 SimulateRemoveTaskFn 插件链，通知各插件更新其内部状态快照。
	//   当前注册了该插件的有：
	//   - predicates 插件：调用 InterPodAffinity.RemovePod，更新 Pod 亲和性拓扑计数，
	//     移除牺牲者 Pod 后，可能释放亲和性约束，使抢占者 Pod 满足调度条件。
	//   - capacity 插件：从队列的 allocated 中扣除牺牲者资源，重新计算队列 share，
	//     同步更新层次化队列的祖先节点 allocated，使 SimulateAllocatableFn 能正确判断队列是否可分配。
	//   - proportion 插件：与 capacity 类似，从队列的 allocated 中扣除资源并更新 share。
	//
	// 阶段 2：调用 nodeInfo.RemoveTask，更新节点级别的资源跟踪：
	//   - Idle += 牺牲者资源（释放空闲资源）
	//   - Used -= 牺牲者资源（减少已用资源）
	//   - 从 nodeInfo.Tasks 中删除该任务
	//   - 更新 NUMA 拓扑资源计数
	//
	// 这两个阶段必须同时执行，因为后续的 SimulateAllocatableFn 依赖队列级别的 allocated 状态，
	// 而 nodeInfo.FutureIdle() 依赖节点级别的 Idle 状态，SimulatePredicateFn 依赖 Pod 亲和性状态。
	removeTask := func(rti *api.TaskInfo) error {
		// 阶段 1：通知插件链更新内部状态快照（亲和性、队列分配等）。
		err := ssn.SimulateRemoveTaskFn(ctx, state, preemptor, rti, nodeInfo)
		if err != nil {
			return err
		}
		// 阶段 2：更新节点资源跟踪（Idle/Used/Tasks/NUMA）。
		nodeInfo.RemoveTask(rti)
		return nil
	}

	// addTask 模拟向节点上添加一个任务（dry-run），用于赦免（reprieve）阶段。
	// 与 removeTask 对称，同样分为两个阶段：
	//
	// 阶段 1：调用 SimulateAddTaskFn 插件链，通知各插件更新内部状态快照：
	//   - predicates 插件：调用 InterPodAffinity.AddPod，更新 Pod 亲和性拓扑计数。
	//   - capacity 插件：将任务资源加回队列的 allocated，重新计算 share 和祖先队列。
	//   - proportion 插件：将任务资源加回队列的 allocated 并更新 share。
	//
	// 阶段 2：调用 nodeInfo.AddTask，更新节点级别的资源跟踪：
	//   - Idle -= 任务资源（占用空闲资源）
	//   - Used += 任务资源（增加已用资源）
	//   - 将任务加入 nodeInfo.Tasks
	//   - 更新 NUMA 拓扑资源计数
	//
	// 在 reprievePod 中，addTask 用于将潜在牺牲者加回节点，
	// 然后检查抢占者是否仍能调度：如果能，说明该 Pod 不需要驱逐（赦免成功）。
	addTask := func(ati *api.TaskInfo) error {
		// 阶段 1：通知插件链更新内部状态快照（亲和性、队列分配等）。
		err := ssn.SimulateAddTaskFn(ctx, state, preemptor, ati, nodeInfo)
		if err != nil {
			return err
		}

		// 阶段 2：更新节点资源跟踪（Idle/Used/Tasks/NUMA）。
		if err := nodeInfo.AddTask(ati); err != nil {
			return err
		}
		return nil
	}

	// 步骤 1：收集节点上所有符合 filter 条件的可驱逐任务
	var preemptees []*api.TaskInfo
	for _, task := range nodeInfo.Tasks {
		if filter == nil {
			preemptees = append(preemptees, task.Clone())
		} else if filter(task) {
			preemptees = append(preemptees, task.Clone())
		}
	}

	klog.V(3).Infof("all preemptees: %v", preemptees)

	// 步骤 2：通过插件链筛选合法的牺牲者候选。
	allVictims := ssn.Preemptable(preemptor, preemptees)
	metrics.UpdatePreemptionVictimsCount(len(allVictims))

	// 步骤 3：验证牺牲者是否满足抢占约束。
	if err := util.ValidateVictims(preemptor, nodeInfo, allVictims); err != nil {
		klog.V(3).Infof("No validated victims on Node <%s>: %v", nodeInfo.Name, err)
		return nil, api.AsStatus(fmt.Errorf("no validated victims on Node <%s>: %v", nodeInfo.Name, err))
	}

	klog.V(3).Infof("allVictims: %v", allVictims)

	// 步骤 4：构建牺牲者优先级队列（低优先级先弹出）。
	victimsQueue := ssn.BuildVictimsPriorityQueue(allVictims, preemptor)

	// 步骤 5：逐个弹出最低优先级的牺牲者并模拟移除，直到资源足够。
	for !victimsQueue.Empty() {
		task := victimsQueue.Pop().(*api.TaskInfo)
		potentialVictims = append(potentialVictims, task)
		if err := removeTask(task); err != nil {
			return nil, api.AsStatus(err)
		}

		// 检查三个条件：
		// 1) 队列可分配（SimulateAllocatableFn）
		// 2) 节点空闲资源足够（FutureIdle >= preemptor 请求）
		// 3) 模拟谓词通过（SimulatePredicateFn，检查亲和性、端口等约束）
		if ssn.SimulateAllocatableFn(ctx, state, currentQueue, preemptor) && preemptor.InitResreq.LessEqual(nodeInfo.FutureIdle(), api.Zero) {
			if err := ssn.SimulatePredicateFn(ctx, state, preemptor, nodeInfo); err == nil {
				klog.V(3).Infof("Pod %v/%v can be scheduled on node %v after preempt %v/%v, stop evicting more pods", preemptor.Namespace, preemptor.Name, nodeInfo.Name, task.Namespace, task.Name)
				break
			}
		}
	}

	// 如果没有找到任何潜在牺牲者，该节点不适合抢占。
	if len(potentialVictims) == 0 {
		return nil, api.AsStatus(fmt.Errorf("no preemption victims found for incoming pod"))
	}

	// 即使移除了所有潜在牺牲者后抢占者仍无法调度，则该节点不适合抢占。
	// 唯一的例外是 Pod 间亲和性导致的失败，但出于性能考虑不支持此场景。
	if err := ssn.SimulatePredicateFn(ctx, state, preemptor, nodeInfo); err != nil {
		return nil, api.AsStatus(fmt.Errorf("failed to predicate pod %s/%s on node %s: %v", preemptor.Namespace, preemptor.Name, nodeInfo.Name, err))
	}

	var victims []*api.TaskInfo

	klog.V(3).Infof("potentialVictims---: %v, nodeInfo: %v", potentialVictims, nodeInfo.Name)

	// TODO: consider the PDB violation here

	// reprievePod 尝试“赦免”一个潜在牺牲者：
	// 将其重新加回节点，如果抢占者仍然能调度，说明该 Pod 不需要被驱逐（返回 true）；
	// 否则确认驱逐（返回 false），将其加入最终 victims 列表。
	reprievePod := func(pi *api.TaskInfo) (bool, error) {
		if err := addTask(pi); err != nil {
			klog.ErrorS(err, "Failed to add task", "task", klog.KObj(pi.Pod))
			return false, err
		}

		var fits bool
		if ssn.SimulateAllocatableFn(ctx, state, currentQueue, preemptor) && preemptor.InitResreq.LessEqual(nodeInfo.FutureIdle(), api.Zero) {
			err := ssn.SimulatePredicateFn(ctx, state, preemptor, nodeInfo)
			fits = err == nil
		}

		if !fits {
			// 加回该 Pod 后抢占者无法调度，确认驱逐。
			if err := removeTask(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi)
			klog.V(3).Info("Pod is a potential preemption victim on node", "pod", klog.KObj(pi.Pod), "node", klog.KObj(nodeInfo.Node))
		}
		klog.Infof("reprievePod for task: %v, fits: %v", pi.Name, fits)
		return fits, nil
	}

	// 步骤 6：反向遍历 potentialVictims，优先赦免高优先级 Pod。
	// potentialVictims 是按优先级从低到高弹出的，反转后高优先级排在前面，
	// 优先尝试赦免高优先级 Pod，最小化抢占影响。
	for i, j := 0, len(potentialVictims)-1; i < j; i, j = i+1, j-1 {
		potentialVictims[i], potentialVictims[j] = potentialVictims[j], potentialVictims[i]
	}

	// 依次尝试赦免每个潜在牺牲者。
	for _, p := range potentialVictims {
		if _, err := reprievePod(p); err != nil {
			return nil, api.AsStatus(err)
		}
	}

	klog.Infof("victims: %v", victims)

	return victims, &api.Status{
		Reason: "",
	}
}

// SelectCandidate 从候选节点列表中选出最优节点。
//
// 步骤：
//  1. 将候选列表转换为 victimsMap（节点名 → 牺牲者列表）；
//  2. 调用 pickOneNodeForPreemption 通过多级评分函数选出最优节点；
//  3. 根据选中节点名构建并返回最终 candidate。
//
// 注意：此方法导出是为了便于测试。
func SelectCandidate(candidates []*candidate) *candidate {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	// 步骤 1：构建节点名 → 牺牲者列表的映射。
	victimsMap := CandidatesToVictimsMap(candidates)
	scoreFuncs := OrderedScoreFuncs(victimsMap)
	// 步骤 2：通过多级评分函数选出最优节点。
	candidateNode := pickOneNodeForPreemption(victimsMap, scoreFuncs)

	if victims := victimsMap[candidateNode]; victims != nil {
		return &candidate{
			victims: victims,
			name:    candidateNode,
		}
	}

	// 理论上不应到达这里，作为容错返回第一个候选。
	klog.Error(errors.New("no candidate selected"), "Should not reach here", "candidates", candidates)
	return candidates[0]
}

// CandidatesToVictimsMap 将候选列表转换为节点名 → 牺牲者列表的映射。
func CandidatesToVictimsMap(candidates []*candidate) map[string][]*api.TaskInfo {
	m := make(map[string][]*api.TaskInfo, len(candidates))
	for _, c := range candidates {
		m[c.Name()] = c.Victims()
	}
	return m
}

// OrderedScoreFuncs 返回自定义的有序评分函数列表。
// 当前返回 nil，表示使用 pickOneNodeForPreemption 内置的默认四级评分规则。
// TODO: 未来考虑将评分函数开放给插件自定义。
func OrderedScoreFuncs(nodesToVictims map[string][]*api.TaskInfo) []func(node string) int64 {
	return nil
}

// pickOneNodeForPreemption 从候选节点中选出一个最优节点。
//
// 当 scoreFuncs 为空时，使用以下多级评分规则依次筛选（前一级能分出胜负则不再执行后续规则）：
//  1. 最小最高优先级牺牲者：牺牲者中最高优先级越低越好（避免驱逐高优先级 Pod）；
//  2. 最小优先级总和：牺牲者优先级总和越小越好；
//  3. 最少牺牲者数量：牺牲者数量越少越好；
//  4. 最晚启动时间：牺牲者中最高优先级 Pod 的启动时间越晚越好（减少资源浪费）。
//
// 如果 scoreFuncs 不为空，则使用自定义评分函数。
// 每级评分取得最高分的节点如果唯一，则直接返回；否则进入下一级评分。
func pickOneNodeForPreemption(nodesToVictims map[string][]*api.TaskInfo, scoreFuncs []func(node string) int64) string {
	if len(nodesToVictims) == 0 {
		return ""
	}

	// 收集所有候选节点名。
	allCandidates := make([]string, 0, len(nodesToVictims))
	for node := range nodesToVictims {
		allCandidates = append(allCandidates, node)
	}

	if len(scoreFuncs) == 0 {
		// 评分规则 1：最小最高优先级牺牲者（牺牲者中最高优先级越低，得分越高）。
		minHighestPriorityScoreFunc := func(node string) int64 {
			highestPodPriority := PodPriority(nodesToVictims[node][0].Pod)
			return -int64(highestPodPriority)
		}
		// 评分规则 2：最小优先级总和（牺牲者优先级总和越小，得分越高）。
		// 加上 MaxInt32+1 使所有优先级为非负数，避免负优先级导致的计算偏差。
		minSumPrioritiesScoreFunc := func(node string) int64 {
			var sumPriorities int64
			for _, task := range nodesToVictims[node] {
				sumPriorities += int64(PodPriority(task.Pod)) + int64(math.MaxInt32+1)
			}
			return -sumPriorities
		}
		// 评分规则 3：最少牺牲者数量（牺牲者越少，得分越高）。
		minNumPodsScoreFunc := func(node string) int64 {
			return -int64(len(nodesToVictims[node]))
		}
		// 评分规则 4：最晚启动时间（牺牲者中最高优先级 Pod 启动越晚，得分越高）。
		latestStartTimeScoreFunc := func(node string) int64 {
			earliestStartTimeOnNode := GetEarliestPodStartTime(nodesToVictims[node])
			if earliestStartTimeOnNode == nil {
				klog.Error(errors.New("earliestStartTime is nil for node"), "Should not reach here", "node", node)
				return int64(math.MinInt64)
			}
			return earliestStartTimeOnNode.UnixNano()
		}

		// 按优先级顺序执行评分函数，前一级能分出胜负则不再执行后续规则。
		scoreFuncs = []func(string) int64{
			minHighestPriorityScoreFunc,
			minSumPrioritiesScoreFunc,
			minNumPodsScoreFunc,
			latestStartTimeScoreFunc,
		}
	}

	// 依次执行每级评分函数，每级筛选出得分最高的节点集合。
	for _, f := range scoreFuncs {
		selectedNodes := []string{}
		maxScore := int64(math.MinInt64)
		for _, node := range allCandidates {
			score := f(node)
			if score > maxScore {
				maxScore = score
				selectedNodes = []string{}
			}
			if score == maxScore {
				selectedNodes = append(selectedNodes, node)
			}
		}
		// 如果该级评分能唯一确定一个节点，直接返回。
		if len(selectedNodes) == 1 {
			return selectedNodes[0]
		}
		// 否则用筛选后的节点集合进入下一级评分。
		allCandidates = selectedNodes
	}

	// 所有评分函数都无法分出胜负，返回第一个节点。
	return allCandidates[0]
}

// GetEarliestPodStartTime 返回所有牺牲者中最高优先级 Pod 的最早启动时间。
// 用于 pickOneNodeForPreemption 的启动时间评分规则。
func GetEarliestPodStartTime(tasks []*api.TaskInfo) *metav1.Time {
	if len(tasks) == 0 {
		// should not reach here.
		klog.Background().Error(nil, "victims.Pods is empty. Should not reach here")
		return nil
	}

	earliestPodStartTime := GetPodStartTime(tasks[0].Pod)
	maxPriority := PodPriority(tasks[0].Pod)

	for _, task := range tasks {
		if podPriority := PodPriority(task.Pod); podPriority == maxPriority {
			if podStartTime := GetPodStartTime(task.Pod); podStartTime.Before(earliestPodStartTime) {
				earliestPodStartTime = podStartTime
			}
		} else if podPriority > maxPriority {
			maxPriority = podPriority
			earliestPodStartTime = GetPodStartTime(task.Pod)
		}
	}

	return earliestPodStartTime
}

// GetPodStartTime 返回 Pod 的启动时间。
// 如果 Pod 尚未启动（StartTime 为 nil），则返回当前时间。
func GetPodStartTime(pod *v1.Pod) *metav1.Time {
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime
	}
	// 被 Assume 或已绑定但尚未启动的 Pod 没有 StartTime。
	return &metav1.Time{Time: time.Now()}
}
