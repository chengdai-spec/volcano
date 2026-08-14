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

// Package allocate 实现了 Volcano 调度器的「资源分配」动作(Action)
//
// 整体调度流程(从宏观到微观)：
//  1. 队列排序 → 选出优先级最高的队列
//  2. 作业排序 → 从该队列中选出优先级最高的作业(Job)
//  3. 子作业排序 → 从作业中选出需要调度的子作业(SubJob)
//  4. 任务排序 → 从子作业中选出待调度的任务(Task/Pod)
//  5. 节点过滤 → 用谓词函数(Predicate)过滤掉不满足要求的节点
//  6. 节点打分 → 用打分函数(NodeOrderFn)选出最优节点，将任务绑定到该节点
//
// 核心概念说明：
//   - HyperNode(超级节点)：Volcano 的网络拓扑层次结构，可以理解为机架、交换机等物理分组。
//     HyperNode 构成一棵树，叶子节点是真实的 Kubernetes 节点，非叶子节点代表交换机/机架等。
//   - Gradient(梯度)：当在低层 HyperNode 找不到足够资源时，逐层向上扩大搜索范围。
//     例如：先在同一个机架内找 → 找不到就扩大到同一排机架 → 再找不到就扩大到整个集群。
//   - SubJob(子作业)：一个 Job 可以按标签选择器拆分成多个 SubJob，每个 SubJob 可以有独立的
//     网络拓扑策略(硬性/软性模式)和最小可用副本数要求。
//   - Nomination(提名)：抢占动作(preempt/reclaim)可以给 SubJob 指定一个「提名 HyperNode」，
//     分配时优先尝试该 HyperNode，跳过耗时的梯度搜索，这是快速路径。
//   - Worksheet(工作表)：调度过程中用于跟踪剩余待调度任务的临时数据结构，
//     每次尝试不同 HyperNode 时会克隆工作表，互不影响。
package allocate

import (
	"fmt"
	"math"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
	commonutil "volcano.sh/volcano/pkg/util"
)

// allocateContext 是整个分配动作的上下文数据，保存了调度循环中需要的所有中间状态。
// 它把队列、作业、子作业、任务分层组织起来，方便逐层遍历调度。
type allocateContext struct {
	queues              *util.PriorityQueue                 // 按优先级排列的队列(Queue)优先队列，每次弹出优先级最高的队列
	jobsByQueue         map[api.QueueID]*util.PriorityQueue // 每个队列下的作业优先队列，key 是队列 ID
	jobWorksheet        map[api.JobID]*JobWorksheet         // 每个作业的工作表，记录该作业下还有哪些 SubJob 和 Task 待调度
	tasksNoHardTopology map[api.JobID]*util.PriorityQueue   // 没有硬性网络拓扑策略的作业的任务队列(直接分配，不走拓扑搜索)
}

// JobWorksheet 作业工作表，跟踪一个 Job 下还有哪些 SubJob 需要调度，
// 以及每个 SubJob 内还有哪些 Task 需要调度。
// 在尝试不同 HyperNode 时会克隆此工作表，确保每次尝试都是干净的初始状态。
type JobWorksheet struct {
	subJobs          *util.PriorityQueue               // 待调度的 SubJob 优先队列
	subJobWorksheets map[api.SubJobID]*SubJobWorksheet // 每个 SubJob 对应的工作表（包含该 SubJob 下待调度的 Task）
}

// ShallowCopyFrom 浅拷贝：直接引用另一个工作表的 subJobs 和 subJobWorksheets。
// 用途：在梯度搜索中选出了最优 HyperNode 后，用最优方案对应的剩余工作表来覆盖当前工作表，
// 这样后续调度就知道哪些任务已经被分配了，不需要重新计算。
func (w *JobWorksheet) ShallowCopyFrom(another *JobWorksheet) {
	if another == nil {
		return
	}
	w.subJobs = another.subJobs
	w.subJobWorksheets = another.subJobWorksheets
}

// Empty 判断工作表是否为空(没有待调度的 SubJob 了)
func (w *JobWorksheet) Empty() bool {
	return w.subJobs == nil || w.subJobs.Empty()
}

// Clone 深拷贝：完整复制工作表的所有数据（包括优先队列和子工作表）。
// 用途：在梯度搜索中，每次尝试一个新的 HyperNode 前，都克隆一份工作表，
// 这样不同 HyperNode 之间的调度尝试互不影响——一个 HyperNode 失败了不会污染另一个的中间状态。
func (w *JobWorksheet) Clone() *JobWorksheet {
	subJobWorksheets := make(map[api.SubJobID]*SubJobWorksheet)
	for subJobID, tasks := range w.subJobWorksheets {
		subJobWorksheets[subJobID] = tasks.Clone()
	}
	return &JobWorksheet{
		subJobs:          w.subJobs.Clone(),
		subJobWorksheets: subJobWorksheets,
	}
}

// SubJobWorksheet 子作业工作表，跟踪一个 SubJob 下还有哪些 Task 需要调度。
// 和 JobWorksheet 类似，在梯度搜索中也会被克隆以保证独立性。
type SubJobWorksheet struct {
	tasks *util.PriorityQueue // 待调度的 Task 优先队列
}

// ShallowCopyFrom 浅拷贝：直接引用另一个子工作表的 tasks。
// 用途：同 JobWorksheet.ShallowCopyFrom，选中最优 HyperNode 后继承其剩余任务列表。
func (w *SubJobWorksheet) ShallowCopyFrom(another *SubJobWorksheet) {
	if another == nil {
		return
	}
	w.tasks = another.tasks
}

// Empty 判断子工作表是否为空（没有待调度的 Task 了）
func (w *SubJobWorksheet) Empty() bool {
	return w.tasks == nil || w.tasks.Empty()
}

// Clone 深拷贝：完整复制子工作表的任务队列。
// 用途：在梯度搜索中，每次尝试新的 HyperNode 前克隆，确保不同尝试之间互不影响。
func (w *SubJobWorksheet) Clone() *SubJobWorksheet {
	return &SubJobWorksheet{
		tasks: w.tasks.Clone(),
	}
}

// Action 是 allocate 动作的主体，负责将待调度的 Pod 分配到合适的节点上。
// 它实现了 framework.Action 接口（Initialize/Execute/UnInitialize）。
type Action struct {
	session *framework.Session
	// 是否开启谓词错误缓存。
	// 开启后，同一个 Task 在同一轮调度中对同一 Node 的谓词失败结果会被缓存，
	// 避免重复计算，提升性能。默认开启。
	enablePredicateErrorCache bool

	recorder *Recorder // 记录调度决策，用于调试和追踪
}

// New 创建一个新的 allocate Action 实例，默认开启谓词错误缓存
func New() *Action {
	return &Action{
		enablePredicateErrorCache: true, // 默认开启谓词错误缓存
	}
}

// Name 返回动作名称 "allocate"，用于在配置中查找该动作的参数
func (alloc *Action) Name() string {
	return "allocate"
}

// Initialize 初始化动作（当前无需额外初始化操作）
func (alloc *Action) Initialize() {}

// parseArguments 从调度器配置中读取 allocate 动作的参数
func (alloc *Action) parseArguments(ssn *framework.Session) {
	arguments := framework.GetArgOfActionFromConf(ssn.Configurations, alloc.Name())
	arguments.GetBool(&alloc.enablePredicateErrorCache, conf.EnablePredicateErrCacheKey)
}

// Execute 是 allocate 动作的入口方法，整个资源分配的调度循环从这里开始。
//
// 核心流程：
//  1. 解析配置参数（如是否开启谓词错误缓存）
//  2. 构建分配上下文（收集所有队列、作业、子作业、待调度任务）
//  3. 执行资源分配循环（队列 → 作业 → 子作业 → 任务 → 节点）
func (alloc *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Allocate ...")
	defer klog.V(5).Infof("Leaving Allocate ...")

	alloc.parseArguments(ssn)

	// Pod 的分配可能有多个阶段：
	// 1. 选择一个名为 Q 的队列(使用 ssn.QueueOrderFn)
	// 2. 从队列 Q 中选择一个名为 J 的作业(使用 ssn.JobOrderFn)
	// 3. 从作业 J 中选择一个名为 T 的任务(使用 ssn.TaskOrderFn)
	// 4. 使用 predicateFn 过滤掉 T 无法分配的节点
	// 5. 使用 ssn.NodeOrderFn 判断最佳节点，并将其分配给 T
	alloc.session = ssn
	alloc.recorder = NewRecorder()

	// 构建分配上下文：把所有待调度的队列、作业、任务按优先级组织好
	actx := alloc.buildAllocateContext()
	klog.V(3).Infof("Try to allocate resource to %d Queues", actx.queues.Len())

	// 开始逐队列、逐作业、逐任务地分配资源
	alloc.allocateResources(actx)
}

// buildAllocateContext 构建分配上下文，遍历所有作业，过滤掉不符合条件的作业，
// 将符合条件作业的队列、子作业、任务按优先级组织到上下文数据结构中。
//
// 过滤条件：
//   - 作业状态为 Pending 且配置了 enqueue 动作 → 跳过(由 enqueue 动作负责入队)
//   - 作业未通过合法性校验 → 跳过
//   - 作业所属队列不存在 → 跳过
//   - 作业包含网络拓扑需求但 HyperNode 尚未就绪 → 跳过
//   - 作业的工作表为空(没有待调度的 SubJob/Task) → 跳过
func (alloc *Action) buildAllocateContext() *allocateContext {
	ssn := alloc.session

	actx := &allocateContext{
		queues:              util.NewPriorityQueue(ssn.QueueOrderFn), // 队列优先队列，按 QueueOrderFn 排序
		jobsByQueue:         make(map[api.QueueID]*util.PriorityQueue),
		jobWorksheet:        make(map[api.JobID]*JobWorksheet),
		tasksNoHardTopology: make(map[api.JobID]*util.PriorityQueue),
	}

	// 遍历所有作业，过滤并加入上下文
	for _, job := range ssn.Jobs {
		// 处理 Pending 状态的作业：
		//   - 如果配置了 enqueue 动作，则跳过(由 enqueue 动作负责将作业从 Pending 变为 Inqueue)
		//   - 如果没有配置 enqueue 动作，则直接将状态改为 Inqueue，避免作业永远无法被调度
		if job.IsPending() {
			if conf.EnabledActionMap["enqueue"] {
				klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: job status is pending.",
					job.Namespace, job.Name, job.Queue)
				continue
			} else {
				klog.V(4).Infof("Job <%s/%s> Queue <%s> status update from pending to inqueue, reason: no enqueue action is configured.",
					job.Namespace, job.Name, job.Queue)
				job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			}
		}

		// 作业合法性校验(比如队列是否超过配额等)，不通过则跳过
		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		// 作业所属队列必须在当前 session 中存在
		if _, found := ssn.Queues[job.Queue]; !found {
			klog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		// 如果作业包含网络拓扑需求但 HyperNode 尚未就绪，跳过
		// 这是因为 HyperNode 是网络拓扑感知调度的前提
		if !ssn.HyperNodesReadyToSchedule && job.ContainsNetworkTopology() {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: hyperNodes are not ready for scheduling",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		// 组织作业的工作表(把 SubJob 和 Task 按优先级排列好)
		worksheet := alloc.organizeJobWorksheet(job)
		if worksheet.Empty() {
			continue
		}

		// 如果该队列第一次出现，创建队列的作业优先队列，并将队列加入全局队列优先队列
		if _, found := actx.jobsByQueue[job.Queue]; !found {
			actx.jobsByQueue[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			actx.queues.Push(ssn.Queues[job.Queue])
		}

		klog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		actx.jobsByQueue[job.Queue].Push(job)
		actx.jobWorksheet[job.UID] = worksheet

		// 没有硬性网络拓扑策略的作业，把它的待调度任务直接存到 tasksNoHardTopology
		// 这样后续可以直接分配，不需要走复杂的拓扑搜索流程
		if !job.ContainsHardTopology() {
			if subJobWorksheet, exist := worksheet.subJobWorksheets[job.DefaultSubJobID()]; exist {
				actx.tasksNoHardTopology[job.UID] = subJobWorksheet.tasks
			}
		}
	}

	return actx
}

// organizeJobWorksheet 为一个作业组织工作表，确定哪些 SubJob 需要调度，
// 哪些 SubJob 是「必须」的(满足作业运行的最小子集)，并把待调度的 Task 加入到各 SubJob 的工作表中。
//
// 关键逻辑：
//   - 已就绪的 SubJob 会被跳过（不需要再分配资源）
//   - 根据子组策略（SubGroupPolicy）的最小成员数，计算出满足作业运行所需的最少 SubJob 集合
//   - 「必须」的 SubJob 在优先队列中会被优先调度（排在前面），确保作业能尽快满足最小运行条件
//   - 有外部调度门（SchedulingGate）的 Task 和零资源请求（BestEffort）的 Task 会被跳过

/*
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: distributed-training
spec:
  minAvailable: 6
  schedulerName: volcano
  tasks:
    - name: "worker"
      replicas: 8                 # 共 8 个 worker Pod
      partitionPolicy:
        partitionSize: 2         # 每 2 个 Pod 组成一个 SubJob（分区）
        minPartitions: 3          # 至少需要 3 个分区就绪，作业才能运行
        networkTopology:
          mode: hard              # 硬性拓扑：每个分区必须分配到同一个机架内
      template:
        spec:
          containers:
            - name: worker
              image: training:latest
              resources:
                requests:
                  cpu: "2"
    - name: "ps"
      replicas: 4                 # 共 4 个 ps Pod
      partitionPolicy:
        partitionSize: 1          # 每 1 个 Pod 组成一个 SubJob
        minPartitions: 2          # 至少需要 2 个分区就绪
      template:
        spec:
          containers:
            - name: ps
              image: parameter-server:latest
              resources:
                requests:
                  cpu: "4"
-------------------------------------------------------------------------------
第 1 步：从 YAML 到 Pod 标签
控制器在为每个 Pod 打标签时，核心逻辑在 job_controller_util.go：

```go
// 关键代码：
partitionID = ix / partitionSize
if ts.PartitionPolicy != nil {
    partitionID = ix / int(ts.PartitionPolicy.PartitionSize)
    pod.Labels[batch.TaskPartitionID] = strconv.Itoa(partitionID)
}
```
其中 ix 是该 task 下 Pod 的序号（从 0 开始），batch.TaskPartitionID = "volcano.sh/partition-id"。
具体推导：
| Pod 名称 | ix (序号)  | partitionSize | partitionID = ix/2 | Pod 上的标签                   |
|----------|-----------|---------------|--------------------|--------------------------------|
| worker-0 | 0         | 2             | 0/2 = 0            | `volcano.sh/partition-id: "0"` |
| worker-1 | 1         | 2             | 1/2 = 0            | `volcano.sh/partition-id: "0"` |
| worker-2 | 2         | 2             | 2/2 = 1            | `volcano.sh/partition-id: "1"` |
| worker-3 | 3         | 2             | 3/2 = 1            | `volcano.sh/partition-id: "1"` |
| worker-4 | 4         | 2             | 4/2 = 2            | `volcano.sh/partition-id: "2"` |
| worker-5 | 5         | 2             | 5/2 = 2            | `volcano.sh/partition-id: "2"` |
| worker-6 | 6         | 2             | 6/2 = 3            | `volcano.sh/partition-id: "3"` |
| worker-7 | 7         | 2             | 7/2 = 3            | `volcano.sh/partition-id: "3"` |
同理，ps 任务(partitionSize=1):
| Pod 名称 | ix | partitionSize | partitionID = ix/1 | Pod 上的标签 -------------------|
|----------|----|---------------|--------------------|--------------------------------|
| ps-0     | 0  | 1             | 0/1 = 0            | `volcano.sh/partition-id: "0"` |
| ps-1     | 1  | 1             | 1/1 = 1            | `volcano.sh/partition-id: "1"` |
| ps-2     | 2  | 1             | 2/1 = 2            | `volcano.sh/partition-id: "2"` |
| ps-3     | 3  | 1             | 3/1 = 3            | `volcano.sh/partition-id: "3"` |
----------------------------------------------------------------------------------------------

第 2 步：控制器转换 partitionPolicy → SubGroupPolicy
在 job_controller_actions.go：
```go
subGroupPolicy := scheduling.SubGroupPolicySpec{
    Name:         "worker",                    // ← task 名称
    SubGroupSize: &partitionSize,              // ← 2
    MinSubGroups: &minPartitions,              // ← 3
    LabelSelector: &metav1.LabelSelector{
        MatchLabels: map[string]string{
            "volcano.sh/task-spec": "worker",  // ← 匹配 worker 类型的 Pod
        },
    },
    MatchLabelKeys: []string{"volcano.sh/partition-id"},  // ← 按 partition-id 分组！
}
```
关键字段:
1.LabelSelector：先筛出属于 "worker" 的 Pod
2.MatchLabelKeys：再按 volcano.sh/partition-id 这个标签的值进一步分组
----------------------------------------------------------------------------------------------------------------
第 3 步：调度器中 Pod → SubJob 的匹配过程
调度器在 getOrCreateSubJob 中为每个 Pod 找到它所属的 SubJob：
```go
func (ji *JobInfo) getOrCreateSubJob(ti *TaskInfo) *SubJobInfo {
    for _, policy := range ji.PodGroup.Spec.SubGroupPolicy {
        // 第一步：检查 Pod 是否匹配 LabelSelector
        if matchValues := getSubJobMatchValues(policy, ti.Pod); len(matchValues) > 0 {
            // 第二步：用 policy 名称 + matchValues 生成 SubJobID
            groupID := getSubJobGID(ji.UID, policy.Name)
            subJobID := getSubJobID(ji.UID, policy.Name, matchValues)
            // 第三步：创建或获取 SubJob
            if _, found := ji.SubJobs[subJobID]; !found {
                ji.SubJobs[subJobID] = NewSubJobInfo(groupID, subJobID, ji.UID, &policy, matchValues)
            }
            return ji.SubJobs[subJobID]
        }
    }
    return ji.getOrCreateDefaultSubJob()
}
```
getSubJobMatchValues 做了两件事：s
检查 LabelSelector：Pod 是否有 volcano.sh/task-spec: worker 标签
提取 MatchLabelKeys 的值：从 Pod 上读 volcano.sh/partition-id 标签的值
getSubJobID 生成 ID：
```go
func getSubJobID(job JobID, policy string, matchValues []string) SubJobID {
    id := strings.Join(matchValues, "-")  // matchValues 就是 ["0"] 或 ["1"] 等
    return SubJobID(fmt.Sprintf("%s/%s-%s", job, policy, id))
}

```
逐个 Pod 推导:
| Pod       | task-spec 标签 | partition-id 标签 | 匹配哪个 policy? | matchValues | GID                         | SubJobID                        |
|-----------|----------------|------------------|------------------|-------------|-----------------------------|---------------------------------|
| worker-0  | worker         | "0"              | worker policy    | ["0"]       | distributed-training/worker | distributed-training/worker-0   |
| worker-1  | worker         | "0"              | worker policy    | ["0"]       | distributed-training/worker | distributed-training/worker-0   |
| worker-2  | worker         | "1"              | worker policy    | ["1"]       | distributed-training/worker | distributed-training/worker-1   |
| worker-3  | worker         | "1"              | worker policy    | ["1"]       | distributed-training/worker | distributed-training/worker-1   |
| worker-4  | worker         | "2"              | worker policy    | ["2"]       | distributed-training/worker | distributed-training/worker-2   |
| worker-5  | worker         | "2"              | worker policy    | ["2"]       | distributed-training/worker | distributed-training/worker-2   |
| worker-6  | worker         | "3"              | worker policy    | ["3"]       | distributed-training/worker | distributed-training/worker-3   |
| worker-7  | worker         | "3"              | worker policy    | ["3"]       | distributed-training/worker | distributed-training/worker-3   |
| ps-0      | ps             | "0"              | ps policy        | ["0"]       | distributed-training/ps     | distributed-training/ps-0       |
| ps-1      | ps             | "1"              | ps policy        | ["1"]       | distributed-training/ps     | distributed-training/ps-1       |
| ps-2      | ps             | "2"              | ps policy        | ["2"]       | distributed-training/ps     | distributed-training/ps-2       |
| ps-3      | ps             | "3"              | ps policy        | ["3"]       | distributed-training/ps     | distributed-training/ps-3       |
-----------------------------------------------------------------------------------------------------------------------------------------------------
第 4 步：最终数据结构
job.SubJobs = {
  // ──────────── GID = "distributed-training/worker" ────────────
  "distributed-training/worker-0": SubJobInfo{
      GID = "distributed-training/worker",   // 同组的标识
      UID = "distributed-training/worker-0",
      MinAvailable = 2,                      // = SubGroupSize = 2（该 SubJob 必须有2个Pod就绪）
      Tasks = {worker-0, worker-1},          // partition-id="0" 的2个Pod
  },
  "distributed-training/worker-1": SubJobInfo{
      GID = "distributed-training/worker",
      UID = "distributed-training/worker-1",
      MinAvailable = 2,
      Tasks = {worker-2, worker-3},          // partition-id="1" 的2个Pod
  },
  "distributed-training/worker-2": SubJobInfo{
      GID = "distributed-training/worker",
      UID = "distributed-training/worker-2",
      MinAvailable = 2,
      Tasks = {worker-4, worker-5},          // partition-id="2" 的2个Pod
  },
  "distributed-training/worker-3": SubJobInfo{
      GID = "distributed-training/worker",
      UID = "distributed-training/worker-3",
      MinAvailable = 2,
      Tasks = {worker-6, worker-7},          // partition-id="3" 的2个Pod
  },

  // ──────────── GID = "distributed-training/ps" ────────────
  "distributed-training/ps-0": SubJobInfo{
      GID = "distributed-training/ps",
      UID = "distributed-training/ps-0",
      MinAvailable = 1,                       // = SubGroupSize = 1
      Tasks = {ps-0},                         // partition-id="0" 的1个Pod
  },
  "distributed-training/ps-1": SubJobInfo{
      GID = "distributed-training/ps",
      UID = "distributed-training/ps-1",
      MinAvailable = 1,
      Tasks = {ps-1},                         // partition-id="1" 的1个Pod
  },
  "distributed-training/ps-2": SubJobInfo{
      GID = "distributed-training/ps",
      UID = "distributed-training/ps-2",
      MinAvailable = 1,
      Tasks = {ps-2},
  },
  "distributed-training/ps-3": SubJobInfo{
      GID = "distributed-training/ps",
      UID = "distributed-training/ps-3",
      MinAvailable = 1,
      Tasks = {ps-3},
  },
}

job.MinSubJobs = {
  "distributed-training/worker": 3,   // 来自 minPartitions=3
  "distributed-training/ps":     2,   // 来自 minPartitions=2
}
-----------------------------------------------------------------------------------------------------------
总结：三个层级的对应关系
YAML 配置              Pod 标签                    调度器内部结构
─────────────────     ──────────────────────     ──────────────────────────────────
partitionSize=2  →    volcano.sh/partition-id   → SubJob.MinAvailable = 2
                       (每2个Pod一个分区)          (每个SubJob必须2个Pod就绪)

minPartitions=3  →    (4个分区：0,1,2,3)        → job.MinSubJobs[worker_GID] = 3
                                                   (worker组至少需要3个SubJob就绪)

task name=worker →    volcano.sh/task-spec:     → GID = "distributed-training/worker"
                       worker                    (同GID的SubJob属于同一组)

简而言之：
GID（组 ID）= 按 task name 分的大组，同一 task 的所有分区共享一个 GID
SubJobID（子作业 ID）= 按 partition-id 细分的小分区，partition-id 相同的 Pod 归入同一个 SubJob
MinAvailable（每个 SubJob 的最小就绪 Pod 数）= partitionSize
MinSubJobs（每个 GID 组至少需要几个 SubJob 就绪）= minPartitions

*/

func (alloc *Action) organizeJobWorksheet(job *api.JobInfo) *JobWorksheet {
	ssn := alloc.session

	// 收集还未就绪的 SubJob，并统计每个子组中已就绪的数量
	subJobs := make([]*api.SubJobInfo, 0, len(job.SubJobs))
	subJobCountMap := map[api.SubJobGID]int32{} // 每个子组（GID）中已就绪的 SubJob 数量
	for _, subJob := range job.SubJobs {
		if ssn.SubJobReady(job, subJob) {
			// 已就绪的 SubJob 计入统计，但不需要再分配资源
			subJobCountMap[subJob.GID]++
		} else {
			// 未就绪的 SubJob 加入待调度列表
			subJobs = append(subJobs, subJob)
		}
	}

	// 按优先级对待调度的 SubJob 排序
	slices.SortFunc(subJobs, func(l, r *api.SubJobInfo) int {
		if !ssn.SubJobOrderFn(l, r) {
			return 1
		}
		return -1
	})

	// 计算满足作业运行条件所需的最小 SubJob 集合（requireSubJobs）
	// 例如：一个子组要求至少 3 个 SubJob 就绪，已有 1 个就绪，则还需要 2 个
	/*
		案例：
			假设 job.MinSubJobs = {
				"distributed-training/worker": 3,   // worker 组至少需要 3 个 SubJob 就绪
				"distributed-training/ps":     2,   // ps 组至少需要 2 个 SubJob 就绪
			}

			当前状态：
				worker/0 已就绪 ✅（它下面的 worker-pod-0 和 worker-pod-1 都跑起来了）
				ps/0 已就绪 ✅（它下面的 ps-pod-0 跑起来了）
				其余 SubJob 都未就绪 ❌

			所以循环前：
			subJobCountMap = {
				"distributed-training/worker": 1,   ← 已有1个就绪（worker/0）
				"distributed-training/ps":     1,   ← 已有1个就绪（ps/0）
				}
			未就绪的 SubJob 按优先级排列：
			subJobs = [worker/1, worker/2, worker/3, ps/1, ps/2, ps/3]

	*/

	requireSubJobs := sets.Set[api.SubJobID]{}
	for _, subJob := range subJobs {
		if subJobCountMap[subJob.GID] < job.MinSubJobs[subJob.GID] {
			requireSubJobs.Insert(subJob.UID)
			subJobCountMap[subJob.GID]++
		}
	}

	// 创建工作表，SubJob 优先队列的排序规则：
	//   1. 「必须」的 SubJob 优先（requireSubJobs 中的排在前面）
	//   2. 同等优先级下按 SubJobOrderFn 排序
	// 这样可以确保先调度那些满足最小运行条件的 SubJob
	jWorksheet := &JobWorksheet{
		subJobs: util.NewPriorityQueue(func(l, r interface{}) bool {
			lv := l.(*api.SubJobInfo)
			rv := r.(*api.SubJobInfo)

			lreq := requireSubJobs.Has(lv.UID)
			rreq := requireSubJobs.Has(rv.UID)
			if lreq != rreq {
				return lreq // 必须（required）的 SubJob 优先
			}
			return ssn.SubJobOrderFn(l, r)
		}),
		subJobWorksheets: make(map[api.SubJobID]*SubJobWorksheet),
	}

	// 遍历所有 SubJob，为每个 SubJob 创建子工作表，收集待调度的 Pending 状态 Task
	for subJobID, subJob := range job.SubJobs {
		sjWorksheet := &SubJobWorksheet{
			tasks: util.NewPriorityQueue(ssn.TaskOrderFn),
		}

		for _, task := range subJob.TaskStatusIndex[api.Pending] {
			// 跳过有外部（非 Volcano 管理）调度门的 Task
			// Volcano 管理的调度门会被 capacity 插件处理
			if task.SchGated && !api.HasOnlyVolcanoSchedulingGate(task.Pod) {
				klog.V(4).Infof("Task <%v/%v> has external scheduling gate, skip it.",
					task.Namespace, task.Name)
				continue
			}

			// 跳过零资源请求（BestEffort）的 Task，allocate 动作不处理这类 Task
			if task.Resreq.IsEmpty() {
				klog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
					task.Namespace, task.Name)
				continue
			}
			sjWorksheet.tasks.Push(task)
		}

		// 只有有待调度 Task 的 SubJob 才加入工作表
		if !sjWorksheet.Empty() {
			jWorksheet.subJobs.Push(subJob)
			jWorksheet.subJobWorksheets[subJobID] = sjWorksheet
		}
	}

	return jWorksheet
}

// allocateResources 是资源分配的主循环，按照「队列 → 作业 → 子作业 → 任务」的层次依次调度。
//
// 调度策略分两种：
//  1. 带有硬性网络拓扑策略或子作业策略的作业 → 调用 allocateForJob（需要梯度搜索找最优 HyperNode）
//  2. 普通作业 → 调用 allocateResourcesForTasks（直接在集群范围分配，不走拓扑搜索）
//
// 注意：每处理完一个作业后，会将队列重新放回优先队列，
// 这样下一轮循环时队列的优先级会根据最新的资源分配情况重新计算，实现公平调度。
func (alloc *Action) allocateResources(actx *allocateContext) {
	ssn := alloc.session

	queues := actx.queues
	for {
		if queues.Empty() {
			break
		}

		// 弹出优先级最高的队列
		queue := queues.Pop().(*api.QueueInfo)

		// 如果队列资源已超用，跳过该队列
		if ssn.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		// 获取该队列下的作业优先队列
		jobs, found := actx.jobsByQueue[queue.UID]
		if !found || jobs.Empty() {
			klog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		// 弹出该队列下优先级最高的作业
		job := jobs.Pop().(*api.JobInfo)

		// 分支一：作业包含硬性网络拓扑策略或子作业策略
		// 这种情况下需要走 allocateForJob 流程，进行梯度搜索来找到最优的 HyperNode
		// TODO: 未来可能需要统一网络拓扑感知调度和普通调度的逻辑
		if job.ContainsHardTopology() || job.ContainsSubJobPolicy() {
			jobWorksheet := actx.jobWorksheet[job.UID]

			klog.V(3).InfoS("Try to allocate resource for job contains hard topology or subjob policy", "queue", queue.Name, "job", job.UID,
				"allocatedHyperNode", job.AllocatedHyperNode, "subJobNum", jobWorksheet.subJobs.Len())
			// 从集群顶层 HyperNode 开始，逐梯度向下搜索
			stmt := alloc.allocateForJob(job, jobWorksheet, ssn.HyperNodes[framework.ClusterTopHyperNode])
			if stmt != nil && ssn.JobReady(job) { // 作业就绪时才提交，流水线状态不提交
				stmt.Commit()
				ssn.MarkJobDirty(job.UID)
				alloc.recorder.UpdateDecisionToJob(job, ssn.HyperNodes)

				// 如果作业还有剩余任务（比如 minAvailable < replicas），把作业放回队列继续调度
				if !jobWorksheet.Empty() {
					jobs.Push(job)
				}
			}
		} else {
			// 分支二：普通作业（无硬性网络拓扑策略）
			// 直接在集群范围分配，不需要梯度搜索
			subJob, sjExist := job.SubJobs[job.DefaultSubJobID()]
			tasks, tasksExist := actx.tasksNoHardTopology[job.UID]
			if sjExist && tasksExist {
				klog.V(3).InfoS("Try to allocate resource", "queue", queue.Name, "job", job.UID, "taskNum", tasks.Len())
				// 在集群顶层(所有节点范围)分配
				stmt := alloc.allocateResourcesForTasks(subJob, tasks, framework.ClusterTopHyperNode)
				if stmt != nil && ssn.JobReady(job) { // 作业就绪时才提交
					stmt.Commit()

					// 如果还有剩余任务，把作业放回队列继续调度
					if tasks.Len() > 0 {
						jobs.Push(job)
					}
				}
			} else {
				klog.ErrorS(nil, "Can not find default subJob or tasks for job", "job", job.UID,
					"subJobExist", sjExist, "tasksExist", tasksExist)
			}
		}

		// 将队列重新放回优先队列，确保下一轮循环时队列优先级基于最新的资源分配情况重新计算
		queues.Push(queue)
	}
}

// allocateForJob 为包含硬性网络拓扑策略或子作业策略的作业分配资源。
//
// 核心算法——梯度搜索(Dry-Run + 选择最优)：
//  1. 获取该作业可用的 HyperNode 梯度列表(从最底层的机架级到顶层的集群级)
//  2. 对于每个梯度内的每个 HyperNode，做一次「干跑」(Dry-Run)：
//     a. 克隆工作表，确保不同 HyperNode 之间互不影响
//     b. 逐个 SubJob 调用 allocateForSubJob 尝试分配
//     c. 记录分配结果(Statement)和分配得分
//     d. 丢弃(Discard)本次分配，因为这只是试探
//  3. 从所有干跑结果中选择得分最高的 HyperNode
//  4. 用最优 HyperNode 对应的备份 Statement 恢复(RecoverOperations)，返回真正的分配结果
//  5. 用最优 HyperNode 对应的备份工作表浅拷贝覆盖当前工作表，继承剩余未调度的任务
//
// 为什么需要干跑? 因为同一个作业分配到不同的 HyperNode 上效果可能不同，
// 我们需要先「试一试」看哪个 HyperNode 效果最好，然后才真正提交分配。
func (alloc *Action) allocateForJob(job *api.JobInfo, jobWorksheet *JobWorksheet, hyperNodeToAllocate *api.HyperNodeInfo) *framework.Statement {
	ssn := alloc.session

	if jobWorksheet == nil || jobWorksheet.Empty() {
		klog.V(4).InfoS("Empty job worksheet", "job", job.UID)
		return nil
	}

	// 保存子作业初始状态，用于每次干跑后恢复
	alloc.recorder.SnapshotSubJobStatus(job, jobWorksheet)

	// 获取该作业可用的 HyperNode 梯度列表
	// 梯度按从低层(如机架级)到高层(如集群级)排列
	hyperNodeGradients := ssn.HyperNodeGradientForJobFn(job, hyperNodeToAllocate, api.PurposeAllocate)
	for gradient, hyperNodes := range hyperNodeGradients {
		// 备份数据：记录每个 HyperNode 的分配方案，用于最终选择最优
		stmtBackup := make(map[string]*framework.Statement)   // 每个 HyperNode 的分配结果备份
		jobWorksheetsBackup := make(map[string]*JobWorksheet) // 每个 HyperNode 分配后的剩余工作表备份
		subJobsAllocationScores := make(map[string]float64)   // 每个 HyperNode 的分配得分

		// 遍历该梯度下的所有 HyperNode，每个都做一次干跑
		for _, hyperNode := range hyperNodes {
			var stmtList []*framework.Statement
			var subJobsAllocationScore float64

			// 克隆工作表并重置谓词错误缓存，确保每次干跑都是干净的初始状态
			job.ResetFitErr()
			jobWorksheetCopy := jobWorksheet.Clone()
			klog.V(3).InfoS("Try to allocate resource for job in hyperNode", "job", job.UID, "hyperNode", hyperNode.Name)

			// 逐个 SubJob 尝试分配到当前 HyperNode
			for !jobWorksheetCopy.subJobs.Empty() {
				subJob := jobWorksheetCopy.subJobs.Pop().(*api.SubJobInfo)
				subJobWorksheet := jobWorksheetCopy.subJobWorksheets[subJob.UID]

				stmt, allocationScore := alloc.allocateForSubJob(subJob, subJobWorksheet, hyperNode)

				if stmt != nil && len(stmt.Operations()) > 0 {
					stmtList = append(stmtList, stmt)
					subJobsAllocationScore += allocationScore
					// SubJob 有分配但还有剩余任务，放回队列继续调度
					if !subJobWorksheet.Empty() {
						jobWorksheetCopy.subJobs.Push(subJob)
					}

					// 作业已就绪，提前结束（不需要继续分配更多 SubJob）
					if ssn.JobReady(job) {
						break
					}
				}
			}
			// 恢复子作业状态到干跑前的状态
			alloc.recorder.RecoverSubJobStatus(job)

			// 合并该 HyperNode 下所有 SubJob 的分配结果
			mergedStmt := framework.SaveOperations(stmtList...)
			if len(mergedStmt.Operations()) == 0 {
				continue // 没有分配到任何任务，跳过
			}
			// 如果作业已就绪或已流水线化，保存该 HyperNode 的分配方案
			if ssn.JobReady(job) || ssn.JobPipelined(job) {
				stmtBackup[hyperNode.Name] = mergedStmt                          // 备份分配结果
				jobWorksheetsBackup[hyperNode.Name] = jobWorksheetCopy           // 备份剩余工作表
				subJobsAllocationScores[hyperNode.Name] = subJobsAllocationScore // 记录得分
			}

			// 干跑：丢弃所有分配操作，不真正执行
			for _, stmt := range stmtList {
				stmt.Discard()
			}
		}

		// 该梯度下没有找到任何可行的分配方案，尝试下一个梯度
		if len(subJobsAllocationScores) == 0 {
			klog.V(5).InfoS("Find solution for job fail", "job", job.UID, "gradient", gradient)
			continue
		}

		// 从所有可行的 HyperNode 中选择得分最高的
		bestHyperNode, err := alloc.selectBestHyperNodeForJob(subJobsAllocationScores, job)
		if err != nil {
			klog.ErrorS(err, "Cannot find best hyper node for job", "job", job.UID, "gradient", gradient)
			return nil
		}

		// 用最优 HyperNode 的备份 Statement 恢复分配操作
		bestStmt := stmtBackup[bestHyperNode]
		finalStmt := framework.NewStatement(ssn)
		if err = finalStmt.RecoverOperations(bestStmt); err != nil {
			klog.ErrorS(err, "Failed to recover operations", "job", job.UID, "hyperNode", bestHyperNode)
			return nil
		}

		// 用最优 HyperNode 的剩余工作表覆盖当前工作表（浅拷贝）
		// 这样外层调用者知道哪些任务已经被分配了
		jobWorksheet.ShallowCopyFrom(jobWorksheetsBackup[bestHyperNode])

		alloc.recorder.SaveJobDecision(job.UID, bestHyperNode)
		klog.V(3).InfoS("Allocate job to hyperNode success", "job", job.UID, "hyperNode", bestHyperNode)

		return finalStmt
	}

	klog.V(5).InfoS("Cannot find any solution for job", "job", job.UID)
	return nil
}

// allocateForSubJob 为一个子作业分配资源，返回分配结果和得分。
//
// 流程：
//  1. 快速路径：如果子作业有提名 HyperNode（由抢占动作设置），尝试直接用提名分配
//  2. 正常路径：梯度搜索，与 allocateForJob 类似，对每个 HyperNode 做干跑，选择最优
func (alloc *Action) allocateForSubJob(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet, hyperNodeForJob *api.HyperNodeInfo) (*framework.Statement, float64) {
	ssn := alloc.session
	job := ssn.Jobs[subJob.Job]

	if subJobWorksheet == nil || subJobWorksheet.Empty() {
		klog.V(4).InfoS("Empty subJob worksheet", "job", subJob.Job, "subJob", subJob.UID)
		return nil, 0
	}

	klog.V(3).InfoS("Try to allocate resource for subJob", "job", subJob.Job, "subJob", subJob.UID,
		"allocatedHyperNode", subJob.AllocatedHyperNode, "nominatedHyperNode", subJob.NominatedHyperNode,
		"taskNum", subJobWorksheet.tasks.Len())

	// 快速路径：如果子作业有提名 HyperNode，尝试直接用提名分配
	// 这是由抢占动作（gangpreempt/gangreclaim）设置的，跳过耗时的梯度搜索
	if subJob.NominatedHyperNode != "" {
		if stmt, score, ok := alloc.allocateFromNomination(subJob, subJobWorksheet, hyperNodeForJob); ok {
			return stmt, score
		}
		// 提名分配失败，继续走正常路径
	}

	// 正常路径：获取子作业可用的 HyperNode 梯度列表
	hyperNodeGradients := ssn.HyperNodeGradientForSubJobFn(subJob, hyperNodeForJob, api.PurposeAllocate)
	for gradient, hyperNodes := range hyperNodeGradients {
		// 备份数据：记录每个 HyperNode 的分配方案
		stmtBackup := make(map[string]*framework.Statement)         // 每个 HyperNode 的分配结果备份
		subJobWorksheetsBackup := make(map[string]*SubJobWorksheet) // 每个 HyperNode 分配后的剩余工作表备份

		// 遍历该梯度下的所有 HyperNode，每个都做一次干跑
		for _, hyperNode := range hyperNodes {
			// 克隆子工作表并重置谓词错误缓存，确保每次干跑都是干净的初始状态
			job.ResetSubJobFitErr(subJob.UID)
			subJobWorksheetCopy := subJobWorksheet.Clone()

			klog.V(3).InfoS("Try to allocate resource for tasks in subJob", "job", subJob.Job,
				"subJob", subJob.UID, "taskNum", subJobWorksheetCopy.tasks.Len(), "hyperNode", hyperNode.Name)
			stmt := alloc.allocateResourcesForTasks(subJob, subJobWorksheetCopy.tasks, hyperNode.Name)

			if stmt != nil && len(stmt.Operations()) > 0 {
				stmtBackup[hyperNode.Name] = framework.SaveOperations(stmt)  // 备份分配结果
				subJobWorksheetsBackup[hyperNode.Name] = subJobWorksheetCopy // 备份剩余任务
				stmt.Discard()                                               // 干跑：丢弃分配，不真正执行
			}
		}

		// 该梯度下没有找到任何可行的分配方案
		if len(stmtBackup) == 0 {
			klog.V(5).InfoS("Find solution for subJob fail", "subJob", subJob.UID, "gradient", gradient)
			continue // 尝试下一个梯度
		}

		// 从所有可行的 HyperNode 中选择得分最高的
		bestHyperNode, bestScore, err := alloc.selectBestHyperNodeForSubJob(stmtBackup, subJob)
		if err != nil {
			klog.ErrorS(err, "Cannot find best hyper node for subJob", "subJob", subJob.UID, "gradient", gradient)
			return nil, 0
		}

		// 恢复最优 HyperNode 的分配操作，并更新子作业的已分配 HyperNode
		bestStmt := stmtBackup[bestHyperNode]
		finalStmt := framework.NewStatement(ssn)
		if err = finalStmt.RecoverOperations(bestStmt); err != nil {
			klog.ErrorS(err, "Failed to recover operations", "subJob", subJob.UID, "hyperNode", bestHyperNode)
			return nil, 0
		}
		// 计算新的已分配 HyperNode：取当前已分配 HyperNode 和最优 HyperNode 的最近公共祖先（LCA）
		// 这样可以知道任务实际分布在哪个层级的拓扑域内
		newAllocatedHyperNode := ssn.HyperNodes.GetLCAHyperNode(subJob.AllocatedHyperNode, bestHyperNode)
		subJob.AllocatedHyperNode = newAllocatedHyperNode

		// 用最优 HyperNode 的剩余工作表覆盖当前工作表（浅拷贝）
		subJobWorksheet.ShallowCopyFrom(subJobWorksheetsBackup[bestHyperNode])

		alloc.recorder.SaveSubJobDecision(subJob.Job, hyperNodeForJob.Name, subJob.UID, newAllocatedHyperNode)
		klog.V(3).InfoS("Allocate subJob to hyperNode success", "subJob", subJob.UID,
			"hyperNode", bestHyperNode, "score", bestScore, "newAllocatedHyperNode", newAllocatedHyperNode)

		return finalStmt, bestScore
	}

	klog.V(5).InfoS("Cannot find any solution for subJob", "subJob", subJob.UID)
	return nil, 0
}

// selectBestHyperNodeForJob 从作业的各 HyperNode 分配得分中选出最高分的 HyperNode。
// 对于作业级别，直接比较各 HyperNode 的 SubJob 分配总分即可。
func (alloc *Action) selectBestHyperNodeForJob(subJobsAllocationScores map[string]float64, job *api.JobInfo) (string, error) {
	highestScore := math.Inf(-1)
	bestHyperNode := ""
	for hyperNode, score := range subJobsAllocationScores {
		if score > highestScore {
			highestScore = score
			bestHyperNode = hyperNode
		}
	}

	if bestHyperNode == "" {
		return "", fmt.Errorf("no solution found for job %s", job.UID)
	}

	return bestHyperNode, nil
}

// selectBestHyperNodeForSubJob 从子作业的各 HyperNode 分配结果中选出最优的 HyperNode。
// 与 selectBestHyperNodeForJob 不同，这里使用 HyperNodeOrderMapFn 对 HyperNode 进行打分，
// 不仅考虑分配得分，还考虑 HyperNode 的拓扑特性（如负载均衡、亲和性等）。
func (alloc *Action) selectBestHyperNodeForSubJob(stmts map[string]*framework.Statement, subJob *api.SubJobInfo) (string, float64, error) {
	if len(stmts) <= 0 {
		return "", 0, fmt.Errorf("no solution found for subJob %s", subJob.UID)
	}

	ssn := alloc.session
	// 收集候选 HyperNode 下的真实节点列表，用于打分
	candidateHyperNodeGroups := make(map[string][]*api.NodeInfo)
	for hyperNode := range stmts {
		candidateHyperNodeGroups[hyperNode] = ssn.RealNodesList[hyperNode]
	}

	// 使用 HyperNodeOrderMapFn 对候选 HyperNode 打分
	hyperNodeScores, err := util.PrioritizeHyperNodes(candidateHyperNodeGroups, subJob, ssn.HyperNodeOrderMapFn)
	if err != nil {
		return "", 0, fmt.Errorf("prioritize hyperNodes for subJob %s fail: %w", subJob.UID, err)
	}

	bestHyperNode, bestScore := util.SelectBestHyperNodeAndScore(hyperNodeScores)
	if bestHyperNode == "" {
		return "", 0, fmt.Errorf("cannot find best hyperNode for subJob %s", subJob.UID)
	}
	return bestHyperNode, bestScore, nil
}

// nominationPlanEntry 提名计划条目，配对一个待调度任务和它被提名的叶子节点
// 当 SubJob 有提名 HyperNode 时，会为每个 Task 匹配一个提名节点
// 如果验证都通过，就直接按这个计划分配，跳过梯度搜索
type nominationPlanEntry struct {
	task *api.TaskInfo
	node *api.NodeInfo
}

// allocateFromNomination 是子作业分配的快速路径。
//
// 当抢占动作（gangpreempt/gangreclaim）为 SubJob 指定了提名 HyperNode 时，
// 不需要做耗时的梯度搜索，直接验证提名 HyperNode 下的叶子节点是否满足条件即可。
//
// 如果验证失败（比如节点资源不够、谓词不通过），会清除提名信息，
// 让调用者回退到正常的梯度搜索路径。
func (alloc *Action) allocateFromNomination(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet, hyperNodeForJob *api.HyperNodeInfo) (stmt *framework.Statement, score float64, ok bool) {
	ssn := alloc.session
	job := ssn.Jobs[subJob.Job]
	queue := ssn.Queues[job.Queue]
	pinned := subJob.NominatedHyperNode

	// 如果提名分配失败，清除提名信息，确保后续回退到正常的梯度搜索
	defer func() {
		if !ok {
			invalidateSubJobNomination(subJob, subJobWorksheet)
		}
	}()

	// 获取提名 HyperNode 下的真实叶子节点列表
	leafNodes, exist := ssn.RealNodesList[pinned]
	if !exist || len(leafNodes) == 0 {
		klog.V(3).InfoS("NominatedHyperNode no longer in topology, falling back to normal allocation process",
			"subJob", subJob.UID, "nominatedHyperNode", pinned)
		return nil, 0, false
	}
	// 构建叶子节点名称集合，用于后续验证 Task 的提名节点是否属于该 HyperNode
	leafNodeNames := sets.New[string]()
	for _, n := range leafNodes {
		if n != nil {
			leafNodeNames.Insert(n.Name)
		}
	}

	// 验证所有待调度 Task 的提名节点是否有效
	plan, validated := alloc.validateNomination(subJob, subJobWorksheet, queue, leafNodeNames)
	if !validated {
		return nil, 0, false
	}

	// 验证通过，按计划执行分配
	stmt = framework.NewStatement(ssn)
	for _, p := range plan {
		// 如果 SubJob 有网络拓扑需求，设置任务的已分配 HyperNode
		if subJob.WithNetworkTopology() {
			p.task.JobAllocatedHyperNode = pinned
		}
		if err := alloc.allocateResourcesForTask(stmt, p.task, p.node, job); err != nil {
			klog.ErrorS(err, "Allocate from nomination fail, falling back to normal allocation process",
				"subJob", subJob.UID, "task", p.task.UID, "node", p.node.Name)
			stmt.Discard()
			return nil, 0, false
		}
	}

	// 清空工作表中的任务（因为已经全部分配完了），
	// 这样调用者（allocateForSubJob）看到工作表为空，不会将该 SubJob 重新加入梯度搜索
	for !subJobWorksheet.tasks.Empty() {
		subJobWorksheet.tasks.Pop()
	}
	// 更新子作业的已分配 HyperNode（取最近公共祖先）
	newAllocatedHyperNode := ssn.HyperNodes.GetLCAHyperNode(subJob.AllocatedHyperNode, pinned)
	subJob.AllocatedHyperNode = newAllocatedHyperNode
	alloc.recorder.SaveSubJobDecision(subJob.Job, hyperNodeForJob.Name, subJob.UID, newAllocatedHyperNode)
	klog.V(3).InfoS("Allocate subJob from nomination success", "subJob", subJob.UID,
		"nominatedHyperNode", pinned, "newAllocatedHyperNode", newAllocatedHyperNode)
	return stmt, 0, true
}

// validateNomination 验证提名方案是否有效。
// 逐个检查待调度 Task 的提名节点（NominatedNodeName），验证：
//  1. Task 在队列中是否可分配（Allocatable）
//  2. Task 是否有提名节点（NominatedNodeName 不为空）
//  3. 提名节点是否属于提名 HyperNode 的叶子节点集合
//  4. 提名节点是否存在于当前 session 中
//  5. 预谓词（PrePredicate）是否通过
//  6. 谓词（Predicate）是否通过
//
// 任何一步失败都返回 false，调用者会清除提名信息并回退到正常路径。
//
// TODO: 这里的每个 Task 检查流程与 allocateResourcesForTasks 中的流程有重叠，
// 未来考虑统一。
func (alloc *Action) validateNomination(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet, queue *api.QueueInfo, leafNodeNames sets.Set[string]) ([]nominationPlanEntry, bool) {
	ssn := alloc.session
	pinned := subJob.NominatedHyperNode
	ph := util.NewPredicateHelper()
	plan := make([]nominationPlanEntry, 0, subJobWorksheet.tasks.Len())
	// 克隆任务队列来遍历，不修改原始数据
	preview := subJobWorksheet.tasks.Clone()
	for !preview.Empty() {
		task := preview.Pop().(*api.TaskInfo)

		// 检查 1：Task 在队列中是否可分配
		if !ssn.Allocatable(queue, task) {
			klog.V(3).InfoS("Task with nominated node is not allocatable, falling back to normal allocation process",
				"queue", queue.Name, "subJob", subJob.UID, "task", task.UID)
			return nil, false
		}
		nominated := task.Pod.Status.NominatedNodeName

		// 检查 2：Task 是否有提名节点
		if nominated == "" {
			klog.V(3).InfoS("Task missing NominatedNodeName under NominatedHyperNode, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "nominatedHyperNode", pinned)
			return nil, false
		}
		// 检查 3：提名节点是否属于提名 HyperNode 的叶子节点集合
		if !leafNodeNames.Has(nominated) {
			klog.V(3).InfoS("Task NominatedNodeName outside NominatedHyperNode leaf set, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "nominated", nominated, "nominatedHyperNode", pinned)
			return nil, false
		}
		// 检查 4：提名节点是否存在于当前 session 中
		nodeInfo, ok := ssn.Nodes[nominated]
		if !ok || nodeInfo == nil {
			klog.V(3).InfoS("NominatedNodeName not found in session nodes, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "nominated", nominated)
			return nil, false
		}
		// 检查 5：预谓词是否通过
		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).InfoS("PrePredicate failed against nominated node, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "node", nominated, "err", err)
			return nil, false
		}
		// 检查 6：谓词是否通过（对提名节点进行谓词检查）
		predicateNodes, _ := ph.PredicateNodes(task, []*api.NodeInfo{nodeInfo}, alloc.predicate, alloc.enablePredicateErrorCache, ssn.NodesInShard)
		if len(predicateNodes) == 0 {
			klog.V(3).InfoS("Predicate failed against nominated node, falling back to normal allocation process",
				"subJob", subJob.UID, "task", task.UID, "node", nominated)
			return nil, false
		}
		// 所有检查通过，将 (Task, Node) 对加入分配计划
		plan = append(plan, nominationPlanEntry{task: task, node: nodeInfo})
	}
	return plan, true
}

// invalidateSubJobNomination 清除子作业的提名信息。
// 当提名分配失败时调用，清除 SubJob 的 NominatedHyperNode 和每个 Task 的 NominatedNodeName，
// 确保后续调度不会再次尝试无效的提名。
func invalidateSubJobNomination(subJob *api.SubJobInfo, subJobWorksheet *SubJobWorksheet) {
	subJob.NominatedHyperNode = ""
	if subJobWorksheet == nil {
		return
	}
	// 克隆任务队列遍历，清空每个 Task 的提名节点
	preview := subJobWorksheet.tasks.Clone()
	for !preview.Empty() {
		task := preview.Pop().(*api.TaskInfo)
		if task.Pod != nil && task.Pod.Status.NominatedNodeName != "" {
			task.Pod.Status.NominatedNodeName = ""
		}
	}
}

// allocateResourcesForTasks 在指定的 HyperNode 范围内，逐个 Task 进行资源分配。
// 这是分配的核心方法，负责：
//  1. 检查队列是否可分配、Task 是否被调度门阻挡
//  2. 预谓词(PrePredicate)过滤
//  3. 谓词(Predicate)过滤——找出所有满足条件的节点
//  4. 打分(Prioritize)选择最优节点
//  5. 执行分配(Allocate 或 Pipeline)
//
// 分配结果：
//   - 如果 SubJob 已就绪 → 返回 Statement
//   - 如果 SubJob 已流水线化 → 返回 Statement
//   - 否则 → 丢弃 Statement 并返回 nil（说明没有成功分配到足够的 Task）
//
// allocateResourcesForTasks 为指定子作业（SubJob）的一批 Task 分配节点资源。
//
// 该函数是 allocate 动作的核心分配逻辑，负责将优先级队列中的 Task 逐个分配到
// HyperNode 下的真实节点上。整个流程遵循"检查 → 过滤 → 打分 → 分配"的调度管线模式。
//
// 整体流程：
//  1. 获取 HyperNode 下的真实节点列表，构建节点名称集合
//  2. 创建 Statement 事务容器，收集所有分配操作
//  3. 逐个弹出 Task，依次执行以下检查：
//     a. 队列可分配性检查（Allocatable）：队列是否超额使用
//     b. 调度门处理（SchedulingGates）：异步移除队列准入调度门
//     c. 谓词失败缓存检查：该角色是否已有失败记录，避免重复计算
//     d. 预谓词检查（PrePredicateFn）：快速排除全局不可调度的 Task
//     e. 谓词过滤（PredicateNodes）：在所有节点上执行详细过滤
//     - 优先尝试提名节点（NominatedNodeName，来自抢占结果）
//     - 提名节点不可用时，回退到全量节点过滤
//     f. 节点打分选择（prioritizeNodes）：从通过过滤的节点中选最优
//     g. 资源分配（allocateResourcesForTask）：直接分配或流水线分配
//  4. 每次分配后检查 SubJob 是否已就绪（满足 minAvailable），就绪则提前结束
//  5. 最终判定：
//     - SubJob 就绪（Ready）→ 返回 Statement，等待 Commit
//     - SubJob 流水线化（Pipelined）→ 返回 Statement，等待资源释放
//     - 两者都不满足 → Discard 回滚所有操作，返回 nil
//
// 参数：
//   - subJob: 子作业信息，包含 Job ID、minAvailable 等
//   - tasks: 按优先级排序的待分配 Task 队列
//   - hyperNode: 目标 HyperNode 名称，限定分配的节点范围
//
// 返回：分配成功时返回 Statement（包含所有分配操作），失败时返回 nil
func (alloc *Action) allocateResourcesForTasks(subJob *api.SubJobInfo, tasks *util.PriorityQueue, hyperNode string) *framework.Statement {
	ssn := alloc.session

	// 通过子作业的 Job ID 获取完整的 JobInfo 和 QueueInfo
	job := ssn.Jobs[subJob.Job]
	queue := ssn.Queues[job.Queue]

	// ── 步骤 1：获取 HyperNode 下的真实节点列表 ──────────────────────────
	// HyperNode 是 Volcano 的拓扑抽象，一个 HyperNode 下包含多个真实节点。
	// 例如：HyperNode "rack-1" 可能包含 node-1、node-2 两个真实节点。
	nodes, exist := ssn.RealNodesList[hyperNode]
	if !exist || len(nodes) == 0 {
		klog.V(4).InfoS("There is no node in hyperNode", "job", job.UID, "hyperNode", hyperNode)
		return nil
	}

	// 构建节点名称集合，用于后续验证提名节点(NominatedNodeName)是否属于当前 HyperNode
	// 这是一种安全校验，防止跨拓扑域的节点被错误选用
	nodeNameSet := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n != nil {
			nodeNameSet[n.Name] = struct{}{}
		}
	}

	// ── 步骤 2：创建 Statement 事务容器 ──────────────────────────────────
	// Statement 收集本次分配周期内的所有操作(Allocate/Pipeline)
	// 最终统一 Commit(提交到 apiserver)或 Discard(回滚所有变更)
	stmt := framework.NewStatement(ssn)

	// PredicateHelper 封装了谓词过滤逻辑，支持谓词错误缓存以提高性能
	ph := util.NewPredicateHelper()

	// 记录子作业在当前分配轮次中已分配 Task 所在的 HyperNode，
	// 用于软性网络拓扑模式：尽量让 Task 集中在同一拓扑域内。
	allocatedHyperNode := subJob.AllocatedHyperNode

	// ── 步骤 3：逐个 Task 尝试分配 ───────────────────────────────────────
	for !tasks.Empty() {
		// 按优先级弹出 Task（优先级高的先分配）
		task := tasks.Pop().(*api.TaskInfo)

		// ── 3a. 队列可分配性检查 ─────────────────────────────────────────
		// 检查队列的资源配额是否已超额使用
		// 如果队列已超配额，跳过当前 Task（但不终止循环，因为后续 Task 可能更小）
		if !ssn.Allocatable(queue, task) {
			klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
			continue
		}

		// ── 3b. 调度门（SchedulingGates）处理 ─────────────────────────────
		// 如果启用了 SchedulingGates 特性，且 Task 带有队列准入调度门注解，
		// 则异步将 Task 加入调度门管理器的移除队列。
		// 实际移除由后台工作器完成（尽力而为，不阻塞当前调度流程）。
		if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
			task.SchGated && api.HasQueueAllocationGateAnnotation(task.Pod) {
			klog.V(3).Infof("Task %s/%s has the QueueAllocationGate, queue async gate removal", task.Namespace, task.Name)
			ssn.SchGateManager().Enqueue(task)
		}

		// 如果 Task 仍有调度门（gate 尚未被移除），跳过本次分配。
		// 同时检查：如果 Task 只有 Volcano 调度门但没有准入注解，发出警告
		// （这种情况意味着调度门永远不会被自动移除，可能是配置错误）。
		if task.SchGated {
			if api.HasOnlyVolcanoSchedulingGate(task.Pod) && !api.HasQueueAllocationGateAnnotation(task.Pod) {
				klog.Warningf("Task %s/%s has Volcano scheduling gate but missing the opt-in annotation %q; gate will not be removed automatically",
					task.Namespace, task.Name, schedulingv1beta1.QueueAllocationGateKey)
			}
			continue
		}

		// ── 3c. 谓词失败缓存检查 ─────────────────────────────────────────
		// 如果该 Task 的角色规格（TaskRole）在本轮调度中已有谓词失败记录，
		// 直接跳过，避免对相同角色规格的 Task 重复执行耗时的谓词检查。
		if job.TaskHasFitErrors(subJob.UID, task) {
			msg := fmt.Sprintf("Task %s with role spec %s has already predicated failed, skip", task.Name, task.TaskRole)
			klog.V(5).Info(msg)
			fitErrors := api.NewFitErrors()
			fitErrors.SetError(msg)
			job.NodesFitErrors[task.UID] = fitErrors
			continue
		}

		klog.V(3).Infof("There are <%d> nodes for Job <%v/%v>", len(nodes), job.Namespace, job.Name)

		// ── 3d. 预谓词检查（PrePredicateFn）──────────────────────────────
		// 预谓词是轻量级的前置检查（如 PDB 限制、全局资源约束等），
		// 用于快速排除在所有节点上都无法调度的 Task，避免后续昂贵的节点级过滤。
		// 如果预谓词失败，对所有节点标记错误并终止循环
		// （因为预谓词失败是全局性的，后续 Task 大概率也会失败）。
		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
			fitErrors := api.NewFitErrors()
			for _, ni := range nodes {
				fitErrors.SetNodeError(ni.Name, err)
			}
			job.NodesFitErrors[task.UID] = fitErrors
			break // 预谓词失败，终止整个分配循环
		}

		var predicateNodes []*api.NodeInfo
		var fitErrors *api.FitErrors

		// ── 3e. 谓词过滤（PredicateNodes）────────────────────────────────
		// 优化策略：如果 Task 有提名节点（由抢占动作设置），优先尝试该节点。
		// 提名节点是抢占后预留资源的节点，直接使用可以减少不必要的重新过滤。
		// 安全校验：提名节点必须属于当前 HyperNode 的叶子节点集合，防止跨域泄露。
		if nominated := task.Pod.Status.NominatedNodeName; len(nominated) > 0 {
			if _, inLeafSet := nodeNameSet[nominated]; inLeafSet {
				// 进一步检查提名节点的资源是否满足 Task 需求
				if nominatedNodeInfo, ok := ssn.Nodes[nominated]; ok && task.InitResreq.LessEqual(nominatedNodeInfo.FutureIdle(), api.Zero) {
					predicateNodes, fitErrors = ph.PredicateNodes(task, []*api.NodeInfo{nominatedNodeInfo}, alloc.predicate, alloc.enablePredicateErrorCache, ssn.NodesInShard)
				}
			}
		}

		// 如果提名节点不可用、不属于当前 HyperNode、或资源不满足，
		// 回退到从所有 HyperNode 节点中进行全量谓词过滤。
		if len(predicateNodes) == 0 {
			predicateNodes, fitErrors = ph.PredicateNodes(task, nodes, alloc.predicate, alloc.enablePredicateErrorCache, ssn.NodesInShard)
		}

		// ── 3e-2. 谓词过滤结果为空的处理 ─────────────────────────────────
		if len(predicateNodes) == 0 {
			// 记录该 Task 在所有节点上的过滤失败原因
			if fitErrors != nil && hyperNode != framework.ClusterTopHyperNode {
				fitErrors.SetHyperNode(hyperNode)
			}
			job.NodesFitErrors[task.UID] = fitErrors

			// 决策：是否继续尝试后续 Task
			// - NeedContinueAllocating=true：作业还需要更多 Task，跳过当前 Task 继续
			// - NeedContinueAllocating=false：即使分配其他 Task 也无法满足 minAvailable，提前终止
			if job.NeedContinueAllocating(subJob.UID) {
				continue
			} else {
				break
			}
		}

		// ── 3f. 网络拓扑记录 ─────────────────────────────────────────────
		// 如果 SubJob 有网络拓扑需求（如 NCCL 分布式训练需要低延迟互联），
		// 记录当前 Task 被分配到的 HyperNode，用于后续拓扑域收敛计算。
		if subJob.WithNetworkTopology() {
			task.JobAllocatedHyperNode = allocatedHyperNode
		}

		// ── 3g. 节点打分与选择 ───────────────────────────────────────────
		// 从通过谓词过滤的节点中，按资源梯度（本分片空闲 > 其他分片空闲 >
		// 本分片未来空闲 > 其他分片未来空闲）和打分函数选出最优节点。
		bestNode, _ := alloc.prioritizeNodes(ssn, task, predicateNodes)
		if bestNode == nil {
			continue
		}

		// ── 3h. 执行资源分配 ─────────────────────────────────────────────
		// 根据节点资源情况，选择直接分配（Allocate）或流水线分配（Pipeline）：
		// - 节点 Idle 满足需求 → Allocate（Task 立即绑定到节点）
		// - 节点 FutureIdle 满足需求 → Pipeline（等待资源释放后再绑定）
		if err := alloc.allocateResourcesForTask(stmt, task, bestNode, job); err != nil {
			klog.ErrorS(err, "Allocate resources for task fail", "task", task.Name)
			continue
		}

		// ── 3i. 更新已分配 HyperNode（网络拓扑场景）──────────────────────
		// 取当前最优节点所在的 HyperNode 与已分配 HyperNode 的最近公共祖先（LCA），
		// 使得 allocatedHyperNode 始终是所有已分配 Task 的最紧拓扑域。
		// 例如：Task1 在机架A，Task2 在机架B → LCA 为交换机S（覆盖 A 和 B）。
		if subJob.WithNetworkTopology() {
			allocatedHyperNode = getNewAllocatedHyperNode(ssn, bestNode.Name, allocatedHyperNode)
		}

		// ── 3j. 提前结束检查 ─────────────────────────────────────────────
		// 如果 SubJob 已就绪（已分配的 Task 数 >= minAvailable），
		// 无需继续分配更多 Task，提前结束循环。
		if ssn.SubJobReady(job, subJob) {
			break
		}
	}

	// ── 步骤 4：最终判定 ─────────────────────────────────────────────────
	if ssn.SubJobReady(job, subJob) {
		// SubJob 已就绪：足够多的 Task 已被分配到节点上，满足 minAvailable 要求。
		// 返回 Statement，由调用方统一 Commit 提交到 apiserver。
		klog.V(3).InfoS("SubJob ready, return statement", "job", job.UID, "subJob", subJob.UID)
		// 软性拓扑模式下，将计算得到的 allocatedHyperNode 回写到 SubJob，
		// 供后续调度周期参考（确保后续 Task 尽量分配到同一拓扑域）。
		if subJob.IsSoftTopologyMode() {
			subJob.AllocatedHyperNode = allocatedHyperNode
		}
		return stmt
	} else if ssn.SubJobPipelined(job, subJob) {
		// SubJob 已流水线化：虽然节点资源不足无法立即绑定，
		// 但 Task 已被 Pipeline 到即将释放资源的节点上。
		// 返回 Statement，等待资源释放后由后续调度周期处理。
		klog.V(3).InfoS("SubJob pipelined, return statement", "job", job.UID, "subJob", subJob.UID)
		return stmt
	}

	// SubJob 既未就绪也未流水线化，说明本次分配无法满足 minAvailable 要求。
	// 丢弃所有操作（回滚 Session 内存状态），返回 nil 让调用方重试或跳过。
	stmt.Discard()
	return nil
}

// getNewAllocatedHyperNode 在软性拓扑模式下获取作业的新已分配 HyperNode。
// 根据最优节点找到其所属的 HyperNode，然后与当前的 jobAllocatedHyperNode 取最近公共祖先（LCA），
// 这样就能知道所有已分配 Task 的最紧拓扑域是什么。
//
// 例如：第一个 Task 分配到机架 A，第二个 Task 分配到机架 B，
// LCA 可能是交换机 S（机架 A 和 B 的上级），说明作业跨了两个机架。
func getNewAllocatedHyperNode(ssn *framework.Session, bestNode string, jobAllocatedHyperNode string) string {
	// 找到最优节点所属的 HyperNode
	hyperNode := util.FindHyperNodeForNode(bestNode, ssn.RealNodesList, ssn.HyperNodesTiers, ssn.HyperNodesSetByTier)
	if hyperNode != "" {
		if jobAllocatedHyperNode == "" {
			return hyperNode // 第一个分配的 HyperNode，直接返回
		}
		// 取最近公共祖先，确保覆盖所有已分配 Task 的拓扑域
		return ssn.HyperNodes.GetLCAHyperNode(hyperNode, jobAllocatedHyperNode)
	}
	return jobAllocatedHyperNode // 找不到 HyperNode，保持原值
}

// prioritizeNodes 从通过谓词的节点列表中选出得分最高的节点。
//
// 节点按资源充足程度分为四个梯度（优先级从高到低）：
//  1. 本分片内有空闲资源（Idle）的节点 —— 资源立即可用，最优先
//  2. 其他分片内有空闲资源的节点 —— 软分片模式下才考虑
//  3. 本分片内有未来空闲资源（FutureIdle = Idle + Releasing）的节点 —— 需要等释放
//  4. 其他分片内有未来空闲资源的节点 —— 最低优先级
//
// 在高优先级梯度中找到合适节点后，不会再去低优先级梯度中找。
// 这样可以确保：
//   - 优先使用真正空闲的节点，减少不必要的 Pipeline
//   - 优先分配到本分片节点，减少跨分片干扰
func (alloc *Action) prioritizeNodes(ssn *framework.Session, task *api.TaskInfo, predicateNodes []*api.NodeInfo) (*api.NodeInfo, float64) {
	// 将节点分为四个梯度：
	// 梯度 1：本分片内有空闲资源的节点
	// 梯度 2：其他分片内有空闲资源的节点（软分片模式下）
	// 梯度 3：本分片内有未来空闲资源的节点（需要等释放）
	// 梯度 4：其他分片内有未来空闲资源的节点（软分片模式下）
	shardingMode := options.ServerOpts.ShardingMode
	var candidateNodes [][]*api.NodeInfo
	var idleCandidateNodes []*api.NodeInfo
	var futureIdleCandidateNodes []*api.NodeInfo
	var idleCandidateNodesInOtherShards []*api.NodeInfo
	var futureIdleCandidateNodesInOtherShards []*api.NodeInfo
	for _, n := range predicateNodes {
		if task.InitResreq.LessEqual(n.Idle, api.Zero) {
			// 节点有空闲资源满足 Task 需求
			if shardingMode == commonutil.SoftShardingMode && !ssn.NodesInShard.Has(n.Name) {
				idleCandidateNodesInOtherShards = append(idleCandidateNodesInOtherShards, n)
			} else {
				idleCandidateNodes = append(idleCandidateNodes, n)
			}
		} else if task.InitResreq.LessEqual(n.FutureIdle(), api.Zero) {
			// 节点的空闲资源 + 正在释放的资源满足 Task 需求
			if shardingMode == commonutil.SoftShardingMode && !ssn.NodesInShard.Has(n.Name) {
				futureIdleCandidateNodesInOtherShards = append(futureIdleCandidateNodesInOtherShards, n)
			} else {
				futureIdleCandidateNodes = append(futureIdleCandidateNodes, n)
			}
		} else {
			// 谓词通过了但资源不够（理论上不应该出现，防御性代码）
			klog.V(5).Infof("Predicate filtered node %v, idle: %v and future idle: %v do not meet the requirements of task: %v",
				n.Name, n.Idle, n.FutureIdle(), task.Name)
		}
	}

	// 按优先级顺序组装候选节点列表
	// 优先使用有真正空闲资源的节点，其次使用有未来空闲资源的节点
	// 同等条件下优先使用本分片节点，其次使用其他分片节点
	candidateNodes = append(candidateNodes, idleCandidateNodes)
	candidateNodes = append(candidateNodes, idleCandidateNodesInOtherShards)
	candidateNodes = append(candidateNodes, futureIdleCandidateNodes)
	candidateNodes = append(candidateNodes, futureIdleCandidateNodesInOtherShards)

	var bestNode *api.NodeInfo
	var higestScore float64
	// 逐个梯度尝试，找到第一个有合适节点的梯度
	for index, nodes := range candidateNodes {
		if klog.V(5).Enabled() {
			for _, node := range nodes {
				klog.V(5).Infof("node %v, idle: %v, future idle: %v", node.Name, node.Idle, node.FutureIdle())
			}
		}
		switch {
		case len(nodes) == 0:
			klog.V(5).Infof("Task: %v, no matching node is found in the candidateNodes（index: %d） list.", task.Name, index)
		case len(nodes) == 1: // 只有一个节点，直接使用
			bestNode = nodes[0]
		case len(nodes) > 1: // 多个节点，用打分函数选最优
			nodeScores := util.PrioritizeNodes(task, nodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)

			bestNode = ssn.BestNodeFn(task, nodeScores)
			if bestNode == nil {
				bestNode, higestScore = util.SelectBestNodeAndScore(nodeScores)
			}
		}

		// 在高优先级梯度中找到合适节点后，跳过低优先级梯度
		if bestNode != nil {
			break
		}
	}
	return bestNode, higestScore
}

// allocateResourcesForTask 将单个 Task 分配到指定节点上。
//
// 分配策略：
//  1. 如果节点有空闲资源（Idle）满足 Task 需求 → 直接分配（Allocate），Task 绑定到节点
//  2. 如果节点空闲资源不足，但空闲 + 正在释放的资源（FutureIdle）满足需求 → 流水线分配（Pipeline）
//     流水线分配意味着 Task 会等待节点上其他 Pod 释放资源后再真正绑定
//  3. 如果都不满足 → 不做任何操作（调用者会跳过该节点）
func (alloc *Action) allocateResourcesForTask(stmt *framework.Statement, task *api.TaskInfo, node *api.NodeInfo, job *api.JobInfo) (err error) {
	// 情况 1：节点有空闲资源，直接分配
	if task.InitResreq.LessEqual(node.Idle, api.Zero) {
		klog.V(3).Infof("Binding Task <%v/%v> to node <%v>", task.Namespace, task.Name, node.Name)
		if err = stmt.Allocate(task, node); err != nil {
			klog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
				task.UID, node.Name, alloc.session.UID, err)
		} else {
			// 更新端到端调度延迟指标
			metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
			metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
		}
		return
	}

	// 节点空闲资源不足，尝试流水线分配
	klog.V(3).Infof("Predicates failed in allocate for task <%s/%s> on node <%s> with limited resources",
		task.Namespace, task.Name, node.Name)

	// 情况 2：节点的空闲 + 正在释放资源满足 Task 需求，走流水线分配
	// 流水线分配不会立即绑定，而是等资源释放后再处理
	if task.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
		klog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
			task.Namespace, task.Name, node.Name, task.InitResreq, node.Releasing)
		if err = stmt.Pipeline(task, node.Name, false); err != nil {
			klog.Errorf("Failed to pipeline Task %v on %v in Session %v for %v.",
				task.UID, node.Name, alloc.session.UID, err)
		} else {
			// 更新端到端调度延迟指标
			metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
			metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
		}
	}
	return
}

// predicate 是谓词函数，检查 Task 是否可以在指定节点上运行。
//
// 检查步骤：
//  1. 资源谓词：检查节点的未来空闲资源（FutureIdle = Idle + Releasing）是否满足 Task 需求
//     如果不满足，返回 Unschedulable 错误并附带不足的资源名称
//  2. 调用 Session 的 PredicateForAllocateAction，执行各插件注册的谓词检查
//     （如亲和性/反亲和性、数据卷绑定、节点端口冲突等）
func (alloc *Action) predicate(task *api.TaskInfo, node *api.NodeInfo) error {
	// 步骤 1：资源谓词检查
	var statusSets api.StatusSets
	if ok, resources := task.InitResreq.LessEqualWithResourcesName(node.FutureIdle(), api.Zero); !ok {
		statusSets = append(statusSets, &api.Status{Code: api.Unschedulable, Reason: api.WrapInsufficientResourceReason(resources)})
		return api.NewFitErrWithStatus(task, node, statusSets...)
	}
	// 步骤 2：执行插件谓词检查
	return alloc.session.PredicateForAllocateAction(task, node)
}

// UnInitialize 清理动作（当前无需额外清理操作）
func (alloc *Action) UnInitialize() {}
