/*
Copyright 2017 The Kubernetes Authors.
Copyright 2017-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added support for task roles and per-role minimum availability in gang scheduling
- Enhanced job lifecycle management with comprehensive status tracking
- Added resource topology awareness and NUMA support

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

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// subJobCondition 是子作业条件判断函数类型，用于 checkSubJobCondition 中判断子作业是否满足特定条件
type subJobCondition func(*SubJobInfo) bool

// DisruptionBudget 定义作业的中断预算，包含最小可用 Pod 数和最大不可用 Pod 数
// 用于控制作业在驱逐/中断场景下的可用性保障
type DisruptionBudget struct {
	MinAvailable   string // 最小可用 Pod 数量（字符串格式，支持百分比如 "50%" 或绝对值如 "3"）
	MaxUnavailable string // 最大不可用 Pod 数量（字符串格式，同上）
}

// NewDisruptionBudget 为作业创建中断预算对象
// 参数 minAvailable: 最小可用数量字符串
// 参数 maxUnavailable: 最大不可用数量字符串
func NewDisruptionBudget(minAvailable, maxUnavailable string) *DisruptionBudget {
	disruptionBudget := &DisruptionBudget{
		MinAvailable:   minAvailable,
		MaxUnavailable: maxUnavailable,
	}
	return disruptionBudget
}

// Clone 返回 DisruptionBudget 的深拷贝副本
func (db *DisruptionBudget) Clone() *DisruptionBudget {
	return &DisruptionBudget{
		MinAvailable:   db.MinAvailable,
		MaxUnavailable: db.MaxUnavailable,
	}
}

// JobWaitingTime 是 SLA 等待时间的注解键名
// 作业在 Pending 状态等待超过此时间后，应立即入队，集群应为其预留资源
const JobWaitingTime = "sla-waiting-time"

// TaskID 是任务 UID 的类型别名
type TaskID types.UID

// TransactionContext 保存调度事务所需的所有字段
// 每次调度周期（scheduling cycle）中，任务的事务上下文会记录当前调度的中间状态
type TransactionContext struct {
	NodeName              string     // 任务被调度到的节点名称
	EvictionOccurred      bool       // 当前事务中是否发生了驱逐操作
	JobAllocatedHyperNode string     // 作业已分配的超节点(HyperNode，用于网络拓扑感知调度)
	Status                TaskStatus // 当前事务中任务的状态
}

// Clone 返回 TransactionContext 的深拷贝副本
// 如果上下文为 nil 则返回 nil
func (ctx *TransactionContext) Clone() *TransactionContext {
	if ctx == nil {
		return nil
	}
	clone := *ctx
	return &clone
}

// TopologyInfo 记录 NUMA 拓扑感知信息
// 用于支持 CPU 拓扑感知调度，将资源需求绑定到特定的 NUMA 节点
type TopologyInfo struct {
	Policy string                  // 拓扑策略（如 best-effort 或 restricted）
	ResMap map[int]v1.ResourceList // NUMA 资源映射，key 为 NUMA 节点 ID，value 为该 NUMA 节点上的资源列表
}

// Clone 返回 TopologyInfo 的深拷贝副本
// 深拷贝 ResMap 中的每个 ResourceList 以确保修改副本不影响原始对象
func (info *TopologyInfo) Clone() *TopologyInfo {
	copyInfo := &TopologyInfo{
		Policy: info.Policy,
		ResMap: make(map[int]v1.ResourceList),
	}

	for numaID, resList := range info.ResMap {
		copyInfo.ResMap[numaID] = resList.DeepCopy()
	}

	return copyInfo
}

// TaskInfo 包含调度器中一个任务（Pod）的所有信息
// 它是调度器内部对 Pod 的封装，包含资源需求、调度状态、拓扑信息等
type TaskInfo struct {
	UID TaskID // 任务的唯一标识符(对应 Pod 的 UID)
	Job JobID  // 任务所属的作业 ID

	Name      string // 任务名称(Pod 名称)
	Namespace string // 任务所在命名空间
	TaskRole  string // 任务角色，对应 "volcano.sh/task-spec" 注解值，用于区分同一作业中不同角色的任务

	// Resreq 是任务运行时的实际资源需求量
	Resreq *Resource
	// InitResreq 是任务启动时的初始资源需求量（InitContainer 可能需要额外资源）
	InitResreq *Resource
	// DRAResreq aggregates DRA resource requests per DeviceClass
	DRAResreq map[string]*DRAResource
	// ResourceClaimKeys lists namespaced ResourceClaims referenced by this task.
	ResourceClaimKeys []string
	// ResourceClaimDRAResreq stores per-claim DRA resources for shared-claim deduplication.
	ResourceClaimDRAResreq map[string]map[string]*DRAResource

	TransactionContext // 嵌入当前事务上下文（包含 NodeName、Status 等）
	// LastTransaction 保存上一次调度事务的上下文，用于记录调度失败原因
	LastTransaction *TransactionContext

	Priority                    int32 // 任务优先级(影响调度和抢占顺序)
	VolumeReady                 bool  // 卷是否已就绪(用于 Pod 绑定前的检查)
	Preemptable                 bool  // 任务是否可被抢占(对应 volcano.sh/preemptable 注解)
	BestEffort                  bool  // 是否为 BestEffort 任务(即无任何 CPU/内存资源请求)
	HasRestartableInitContainer bool  // Pod 是否包含可重启的 InitContainer(影响资源计算)
	SchGated                    bool  // Pod 是否被 Kubernetes 调度门控(SchedulingGates)阻塞

	// RevocableZone 表示任务可使用的可撤销区域
	// 支持设置 volcano.sh/revocable-zone 注解或标签
	// 空值表示不能使用可撤销节点，"*" 值表示可以使用所有可撤销节点
	RevocableZone string

	NumaInfo *TopologyInfo // NUMA 拓扑信息，用于 NUMA 感知调度
	Pod      *v1.Pod       // 原始 Kubernetes Pod 对象引用

	// CustomBindErrHandler 是自定义的绑定错误回调函数
	// 当任务绑定失败时调用，用于执行自定义的清理或回滚逻辑
	CustomBindErrHandler func() error `json:"-"`
	// CustomBindErrHandlerSucceeded 标识自定义绑定错误处理函数是否执行成功
	CustomBindErrHandlerSucceeded bool
}

// getJobID 根据 Pod 的注解提取其所属的作业 ID
// 通过读取 "scheduling.volcano.sh/group-name" 注解来确定 Pod 属于哪个 PodGroup
// 作业 ID 格式为 "namespace/groupName"
func getJobID(pod *v1.Pod) JobID {
	if gn, found := pod.Annotations[v1beta1.KubeGroupNameAnnotationKey]; found && len(gn) != 0 {
		// Make sure Pod and PodGroup belong to the same namespace.
		jobID := fmt.Sprintf("%s/%s", pod.Namespace, gn)
		return JobID(jobID)
	}

	return ""
}

// getTaskRole 从 Pod 的注解或标签中提取任务角色（task-spec）
// 首先检查注解 "volcano.sh/task-spec"，若不存在则检查标签
// 任务角色用于区分同一作业中不同类型的任务（如 driver/worker）
func getTaskRole(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	if ts, found := pod.Annotations[batch.TaskSpecKey]; found && len(ts) != 0 {
		return ts
	}
	// keep searching pod labels, as it is also added in labels in job controller
	if ts, found := pod.Labels[batch.TaskSpecKey]; found && len(ts) != 0 {
		return ts
	}

	return ""
}

// TaskPriorityAnnotation 是任务优先级注解键
// 可通过此注解为单个任务设置独立于 Pod 优先级的调度优先级
const TaskPriorityAnnotation = "volcano.sh/task-priority"

// NewTaskInfo 根据 Pod 创建新的 TaskInfo 对象
// 该函数是调度器将 Kubernetes Pod 转换为内部 Task 模型的核心入口
// 会提取 Pod 的资源需求、优先级、抢占属性、拓扑信息、调度门控状态等
func NewTaskInfo(pod *v1.Pod) *TaskInfo {
	initResReq := GetPodResourceRequest(pod)
	resReq := initResReq
	bestEffort := initResReq.IsEmpty()
	preemptable := GetPodPreemptable(pod)
	revocableZone := GetPodRevocableZone(pod)
	topologyInfo := GetPodTopologyInfo(pod)
	role := getTaskRole(pod)
	hasRestartableInitContainer := hasRestartableInitContainer(pod)
	// initialize pod scheduling gates info here since it will not change in a scheduling cycle
	schGated := calSchedulingGated(pod)
	jobID := getJobID(pod)

	ti := &TaskInfo{
		UID:                         TaskID(pod.UID),
		Job:                         jobID,
		Name:                        pod.Name,
		Namespace:                   pod.Namespace,
		TaskRole:                    role,
		Priority:                    1,
		Pod:                         pod,
		Resreq:                      resReq,
		InitResreq:                  initResReq,
		Preemptable:                 preemptable,
		BestEffort:                  bestEffort,
		HasRestartableInitContainer: hasRestartableInitContainer,
		RevocableZone:               revocableZone,
		NumaInfo:                    topologyInfo,
		SchGated:                    schGated,
		TransactionContext: TransactionContext{
			NodeName: pod.Spec.NodeName,
			Status:   getTaskStatus(pod),
		},
	}

	// 从 Pod 的 PriorityClass 获取优先级，若未设置则保持默认值 1
	if pod.Spec.Priority != nil {
		ti.Priority = *pod.Spec.Priority
	}

	// 如果 Pod 设置了 "volcano.sh/task-priority" 注解，使用该注解值覆盖优先级
	// 这允许在同一 PriorityClass 下为不同任务设置不同的调度优先级
	if taskPriority, ok := pod.Annotations[TaskPriorityAnnotation]; ok {
		if priority, err := strconv.ParseInt(taskPriority, 10, 32); err == nil {
			ti.Priority = int32(priority)
		}
	}

	return ti
}

// GetTransactionContext 获取任务当前的事务上下文
func (ti *TaskInfo) GetTransactionContext() TransactionContext {
	return ti.TransactionContext
}

// GenerateLastTxContext 生成并设置任务上一次调度事务的上下文
// 在新调度周期开始前调用，将当前事务上下文保存为历史上下文
func (ti *TaskInfo) GenerateLastTxContext() {
	ctx := ti.GetTransactionContext()
	ti.LastTransaction = &ctx
}

// ClearLastTxContext 清除任务的上一次调度事务上下文
func (ti *TaskInfo) ClearLastTxContext() {
	ti.LastTransaction = nil
}

// calSchedulingGated 判断 Pod 是否被 Kubernetes 调度门控（SchedulingGates）阻塞
// 只有当 PodSchedulingReadiness 特性门控启用时才检查
// 如果 Pod 存在 SchedulingGates 且不为空，则认为被门控
func calSchedulingGated(pod *v1.Pod) bool {
	// Only enable if features.PodSchedulingReadiness feature gate is enabled
	if utilfeature.DefaultFeatureGate.Enabled(features.PodSchedulingReadiness) {
		return pod != nil && pod.Spec.SchedulingGates != nil && len(pod.Spec.SchedulingGates) != 0
	}
	return false
}

// SetPodResourceDecision 将 NUMA 资源分配决策写回 Pod 的注解
// 用于拓扑感知调度，将调度器决定的 NUMA 节点资源映射写入 Pod 的 annotation
// 这样节点上的 agent 可以据此执行绑核操作
func (ti *TaskInfo) SetPodResourceDecision() error {
	if ti.NumaInfo == nil || len(ti.NumaInfo.ResMap) == 0 {
		return nil
	}

	klog.V(4).Infof("%v/%v resource decision: %v", ti.Namespace, ti.Name, ti.NumaInfo.ResMap)
	decision := PodResourceDecision{
		NUMAResources: ti.NumaInfo.ResMap,
	}

	layout, err := json.Marshal(&decision)
	if err != nil {
		return err
	}

	metav1.SetMetaDataAnnotation(&ti.Pod.ObjectMeta, topologyDecisionAnnotation, string(layout[:]))
	return nil
}

// UnsetPodResourceDecision 删除 Pod 上的拓扑资源分配决策注解
func (ti *TaskInfo) UnsetPodResourceDecision() {
	delete(ti.Pod.Annotations, topologyDecisionAnnotation)
}

// Clone 返回 TaskInfo 的深拷贝副本
// 克隆所有字段，包括资源需求的深拷贝、NUMA 信息的深拷贝和事务上下文的深拷贝
func (ti *TaskInfo) Clone() *TaskInfo {
	res := &TaskInfo{
		UID:                         ti.UID,
		Job:                         ti.Job,
		Name:                        ti.Name,
		Namespace:                   ti.Namespace,
		TaskRole:                    ti.TaskRole,
		Priority:                    ti.Priority,
		Pod:                         ti.Pod,
		Resreq:                      ti.Resreq.Clone(),
		InitResreq:                  ti.InitResreq.Clone(),
		VolumeReady:                 ti.VolumeReady,
		Preemptable:                 ti.Preemptable,
		BestEffort:                  ti.BestEffort,
		HasRestartableInitContainer: ti.HasRestartableInitContainer,
		RevocableZone:               ti.RevocableZone,
		NumaInfo:                    ti.NumaInfo.Clone(),
		SchGated:                    ti.SchGated,
		TransactionContext: TransactionContext{
			NodeName: ti.NodeName,
			Status:   ti.Status,
		},
		LastTransaction: ti.LastTransaction.Clone(),
	}

	if ti.DRAResreq != nil {
		res.DRAResreq = make(map[string]*DRAResource, len(ti.DRAResreq))
		for k, v := range ti.DRAResreq {
			res.DRAResreq[k] = v.Clone()
		}
	}
	if len(ti.ResourceClaimKeys) > 0 {
		res.ResourceClaimKeys = append([]string(nil), ti.ResourceClaimKeys...)
	}
	if ti.ResourceClaimDRAResreq != nil {
		res.ResourceClaimDRAResreq = cloneResourceClaimDRAResreq(ti.ResourceClaimDRAResreq)
	}

	return res
}

// hasRestartableInitContainer 判断 Pod 是否包含可重启的 InitContainer
// 可重启的 InitContainer（RestartPolicy=Always）在 Pod 整个生命周期内运行
// 其资源需求需要一直被计算，这与普通 InitContainer 不同
func hasRestartableInitContainer(pod *v1.Pod) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			return true
		}
	}
	return false
}

// String 返回 TaskInfo 的可读字符串表示，用于日志输出
func (ti TaskInfo) String() string {
	res := fmt.Sprintf("Task (%v:%v/%v): taskSpec %s, job %v, nodeName %v, status %v, pri %v, "+
		"resreq %v, preemptable %v, revocableZone %v",
		ti.UID, ti.Namespace, ti.Name, ti.TaskRole, ti.Job, ti.NodeName, ti.Status, ti.Priority,
		ti.Resreq, ti.Preemptable, ti.RevocableZone)

	if ti.NumaInfo != nil {
		res += fmt.Sprintf(", numaInfo %v", *ti.NumaInfo)
	}

	return res
}

// JobID 是作业 ID 的类型别名
type JobID types.UID

// TasksMap 是任务映射表类型，key 为 TaskID，value 为 TaskInfo 指针
type TasksMap map[TaskID]*TaskInfo

// NodeResourceMap 存储节点上的资源映射，key 为节点名称
// 用于记录多个节点的资源信息
type NodeResourceMap map[string]*Resource

// JobInfo 包含一个作业（PodGroup）的所有信息
// 它是 Volcano 调度器中对批量作业的完整抽象，包含了作业元数据、任务集合、
// 资源分配状态、调度约束等全部信息
type JobInfo struct {
	UID   JobID     // 作业的唯一标识符
	PgUID types.UID // 对应 PodGroup 的 UID

	Name      string // 作业名称
	Namespace string // 作业所在命名空间

	Queue QueueID // 作业所属的队列 ID

	Priority int32 // 作业优先级

	MinAvailable int32 // 作业最小可用成员数（Gang Scheduling 的核心参数）

	WaitingTime *time.Duration // SLA 等待时间，超过此时间后作业应被优先调度

	JobFitErrors   string                // 作业级别的调度失败原因描述
	NodesFitErrors map[TaskID]*FitErrors // 各任务在各节点上的调度失败详情

	AllocatedHyperNode string
	NetworkTopology    *scheduling.NetworkTopologySpec
	SubJobs            map[SubJobID]*SubJobInfo
	TaskToSubJob       map[TaskID]SubJobID
	MinSubJobs         map[SubJobGID]int32 // key is name of "PodGroup.Spec.SubGroupPolicy", value is minSubGroups

	// All tasks of the Job.
	TaskStatusIndex       map[TaskStatus]TasksMap // 按状态索引的任务映射，用于快速查询某状态下的所有任务
	Tasks                 TasksMap                // 所有任务的映射，key 为 TaskID
	TaskMinAvailable      map[string]int32        // 各角色的最小可用任务数，key 为 "volcano.sh/task-spec" 的值
	TaskMinAvailableTotal int32                   // 所有角色的最小可用任务数之和

	Allocated    *Resource // 已分配的资源总量(包含 Bound/Binding/Running/Allocated 状态的任务资源)
	TotalRequest *Resource // 所有任务的总资源需求量

	CreationTimestamp metav1.Time // 作业创建时间
	PodGroup          *PodGroup   // 关联的 PodGroup 对象引用

	ScheduleStartTimestamp metav1.Time // 调度开始时间戳

	Preemptable bool // 作业是否可被抢占

	// RevocableZone 表示作业可使用的可撤销区域
	// 空值表示不能使用可撤销节点，"*" 表示可使用所有可撤销节点
	// 可撤销节点是指在特定时间段内可被回收的节点资源
	RevocableZone string
	Budget        *DisruptionBudget // 中断预算，控制作业在驱逐场景下的可用性保障
}

// NewJobInfo 创建新的 JobInfo 对象
// uid: 作业唯一标识符
// tasks: 可选的初始任务列表
// 初始化所有内部数据结构（映射、索引等），并将给定任务添加到作业中
func NewJobInfo(uid JobID, tasks ...*TaskInfo) *JobInfo {
	job := &JobInfo{
		UID:              uid,
		MinAvailable:     0,
		NodesFitErrors:   make(map[TaskID]*FitErrors),
		Allocated:        EmptyResource(),
		TotalRequest:     EmptyResource(),
		TaskStatusIndex:  map[TaskStatus]TasksMap{},
		Tasks:            TasksMap{},
		TaskMinAvailable: map[string]int32{},
		SubJobs:          map[SubJobID]*SubJobInfo{},
		TaskToSubJob:     map[TaskID]SubJobID{},
		MinSubJobs:       map[SubJobGID]int32{},
	}

	for _, task := range tasks {
		job.AddTaskInfo(task)
	}

	return job
}

// UnsetPodGroup 从作业中移除 PodGroup 信息
// 清空 SubJobs 并重新将所有任务分配到默认子作业
func cloneNetworkTopology(spec *scheduling.NetworkTopologySpec) *scheduling.NetworkTopologySpec {
	if spec == nil {
		return nil
	}
	return spec.DeepCopy()
}

// UnsetPodGroup removes podGroup details from a job
func (ji *JobInfo) UnsetPodGroup() {
	ji.PodGroup = nil
	ji.NetworkTopology = nil

	clear(ji.SubJobs)
	for _, task := range ji.Tasks {
		ji.addTaskToSubJob(task)
	}
}

// SetPodGroup 设置作业的 PodGroup 信息
// 这是从 PodGroup CRD 同步信息到 JobInfo 的核心方法，会提取并设置：
// 1. 作业名称、命名空间、最小成员数、队列
// 2. SLA 等待时间(从注解解析)
// 3. 抢占属性、可撤销区域、中断预算
// 4. 各角色的最小成员信息(TaskMinAvailable)
// 5. 如果 SubGroupPolicy 变化，重新构建子作业关系
func (ji *JobInfo) SetPodGroup(pg *PodGroup) {
	ji.Name = pg.Name
	ji.Namespace = pg.Namespace
	ji.MinAvailable = pg.Spec.MinMember
	ji.Queue = QueueID(pg.Spec.Queue)
	ji.CreationTimestamp = pg.GetCreationTimestamp()

	// 尝试从注解中解析 SLA 等待时间，优先使用 v1beta1.JobWaitingTime 键
	var err error
	ji.WaitingTime, err = ji.extractWaitingTime(pg, v1beta1.JobWaitingTime)
	if err != nil {
		klog.Warningf("Error occurs in parsing waiting time for job <%s/%s>, err: %s.",
			pg.Namespace, pg.Name, err.Error())
		ji.WaitingTime = nil
	}
	// 如果 v1beta1 键解析失败，回退到 JobWaitingTime 键（"sla-waiting-time"）
	if ji.WaitingTime == nil {
		ji.WaitingTime, err = ji.extractWaitingTime(pg, JobWaitingTime)
		if err != nil {
			klog.Warningf("Error occurs in parsing waiting time for job <%s/%s>, err: %s.",
				pg.Namespace, pg.Name, err.Error())
			ji.WaitingTime = nil
		}
	}

	// 解析抢占属性、可撤销区域和中断预算
	ji.Preemptable = ji.extractPreemptable(pg)
	ji.RevocableZone = ji.extractRevocableZone(pg)
	ji.Budget = ji.extractBudget(pg)

	// 解析各角色的最小成员信息
	ji.ParseMinMemberInfo(pg)

	oldPG := ji.PodGroup
	ji.PgUID = pg.UID
	ji.PodGroup = pg
	ji.NetworkTopology = cloneNetworkTopology(pg.Spec.NetworkTopology)

	// 如果 SubGroupPolicy 发生变化，需要重建子作业映射关系
	if oldPG == nil || !equality.Semantic.DeepEqual(oldPG.Spec.SubGroupPolicy, pg.Spec.SubGroupPolicy) {
		clear(ji.SubJobs)
		for _, task := range ji.Tasks {
			ji.addTaskToSubJob(task)
		}
		clear(ji.MinSubJobs)
		for _, policy := range pg.Spec.SubGroupPolicy {
			groupID := getSubJobGID(ji.UID, policy.Name)
			if policy.MinSubGroups == nil {
				ji.MinSubJobs[groupID] = 0
			} else {
				ji.MinSubJobs[groupID] = *policy.MinSubGroups
			}
		}
	}
}

// extractWaitingTime 从 PodGroup 注解中读取 SLA 等待时间
// 参数 waitingTimeKey: 注解键名（支持 v1beta1.JobWaitingTime 和 JobWaitingTime）
// 返回解析后的等待时间 Duration 和可能的错误
func (ji *JobInfo) extractWaitingTime(pg *PodGroup, waitingTimeKey string) (*time.Duration, error) {
	if _, exist := pg.Annotations[waitingTimeKey]; !exist {
		return nil, nil
	}

	jobWaitingTime, err := time.ParseDuration(pg.Annotations[waitingTimeKey])
	if err != nil {
		return nil, err
	}

	if jobWaitingTime <= 0 {
		return nil, errors.New("invalid sla waiting time")
	}

	return &jobWaitingTime, nil
}

// extractPreemptable 从 PodGroup 的注解或标签中提取 volcano.sh/preemptable 值
// 优先检查注解，若不存在则检查标签
// 返回 true 表示该作业可被抢占
func (ji *JobInfo) extractPreemptable(pg *PodGroup) bool {
	// check annotation first
	if len(pg.Annotations) > 0 {
		if value, found := pg.Annotations[v1beta1.PodPreemptable]; found {
			b, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("invalid %s=%s", v1beta1.PodPreemptable, value)
				return false
			}
			return b
		}
	}

	// 若注解不存在，则检查标签
	if len(pg.Labels) > 0 {
		if value, found := pg.Labels[v1beta1.PodPreemptable]; found {
			b, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("invalid %s=%s", v1beta1.PodPreemptable, value)
				return false
			}
			return b
		}
	}

	return false
}

// extractRevocableZone 从 PodGroup 注解中提取可撤销区域信息
// 支持两种注解方式：
// 1. volcano.sh/revocable-zone: 直接设置可撤销区域（仅支持 "*" 值）
// 2. volcano.sh/preemptable: 若为 true，等价于 revocable-zone="*"
func (ji *JobInfo) extractRevocableZone(pg *PodGroup) string {
	// check annotation first
	if len(pg.Annotations) > 0 {
		if value, found := pg.Annotations[v1beta1.RevocableZone]; found {
			if value != "*" {
				return ""
			}
			return value
		}

		if value, found := pg.Annotations[v1beta1.PodPreemptable]; found {
			if b, err := strconv.ParseBool(value); err == nil && b {
				return "*"
			}
		}
	}

	return ""
}

// extractBudget 从 PodGroup 注解中提取中断预算信息
// 支持 volcano.sh/jdb-min-available 和 volcano.sh/jdb-max-unavailable 两种注解
func (ji *JobInfo) extractBudget(pg *PodGroup) *DisruptionBudget {
	if len(pg.Annotations) > 0 {
		if value, found := pg.Annotations[v1beta1.JDBMinAvailable]; found {
			return NewDisruptionBudget(value, "")
		} else if value, found := pg.Annotations[v1beta1.JDBMaxUnavailable]; found {
			return NewDisruptionBudget("", value)
		}
	}

	return NewDisruptionBudget("", "")
}

// ParseMinMemberInfo 设置作业的最小成员信息
// 1. 将 PodGroup.Spec.MinTaskMember 中的各角色最小成员数写入 TaskMinAvailable
// 2. 计算所有角色的最小成员数之和并写入 TaskMinAvailableTotal
// 这些信息用于 Gang Scheduling 中按角色维度的最小可用性检查
func (ji *JobInfo) ParseMinMemberInfo(pg *PodGroup) {
	taskMinAvailableTotal := int32(0)
	clear(ji.TaskMinAvailable)
	for task, member := range pg.Spec.MinTaskMember {
		ji.TaskMinAvailable[task] = member
		taskMinAvailableTotal += member
	}
	ji.TaskMinAvailableTotal = taskMinAvailableTotal
}

// GetMinResources 返回 PodGroup 中定义的最小资源需求
// 如果未设置 MinResources，返回空资源对象
func (ji *JobInfo) GetMinResources() *Resource {
	if ji.PodGroup.Spec.MinResources == nil {
		return EmptyResource()
	}

	return NewResource(*ji.PodGroup.Spec.MinResources)
}

// GetSchGatedPodResources 获取被 Kubernetes 调度门控阻塞的 Pod 的资源总量
// 注意：仅被 Volcano 调度门控（queue-allocation-gate）阻塞的任务不计入此计算
// 因为这些任务应被计入 inqueue 资源
func (ji *JobInfo) GetSchGatedPodResources() *Resource {
	res := EmptyResource()
	for _, task := range ji.Tasks {
		if task.SchGated {
			// Exclude tasks that are only Volcano scheduling gated
			// These should be counted in inqueue resources, not deducted
			/*
				排除那些仅受 Volcano 调度门控限制的任务
				这些任务应该计入 inqueue 资源，而不是被扣减
			*/
			if HasOnlyVolcanoSchedulingGate(task.Pod) {
				continue
			}
			res.Add(task.Resreq)
		}
	}
	return res
}

// DeductSchGatedResources 从给定资源中扣除被调度门控阻塞的 Pod 的资源
// 如果资源量不足以扣除，返回零值而非负数
// 用途：在计算 inqueue 资源时，需要扣除调度门控任务的资源，
// 以避免这些未真正调度的任务阻塞其他作业的入队
func (ji *JobInfo) DeductSchGatedResources(res *Resource) *Resource {
	schGatedResource := ji.GetSchGatedPodResources()
	// Most jobs do not have any scheduling gated tasks, hence we add this short cut
	if schGatedResource.IsEmpty() {
		return res
	}

	result := res.Clone()
	// schGatedResource can be larger than MinResource because minAvailable of a job can be smaller than number of replica
	result.MilliCPU = max(result.MilliCPU-schGatedResource.MilliCPU, 0)
	result.Memory = max(result.Memory-schGatedResource.Memory, 0)

	// If a scalar resource is present in schGatedResource but not in minResource, skip it
	for name, resource := range res.ScalarResources {
		if schGatedRes, ok := schGatedResource.ScalarResources[name]; ok {
			result.ScalarResources[name] = max(resource-schGatedRes, 0)
		}
	}
	klog.V(3).Infof("Gated resources: %s, MinResource: %s: Result: %s", schGatedResource.String(), res.String(), result.String())
	return result
}

// GetElasticResources 返回作业的弹性资源（即已分配资源中超过最小资源需求的部分）
// 这些资源可以被回收用于其他作业
func (ji *JobInfo) GetElasticResources() *Resource {
	if ji.Allocated == nil {
		return EmptyResource()
	}
	minResource := ji.GetMinResources()
	elastic := ExceededPart(ji.Allocated, minResource)

	return elastic
}

// addTaskIndex 将任务添加到状态索引中
// 相同状态的任务会被分组到同一个 TasksMap 中，便于按状态快速查询
func (ji *JobInfo) addTaskIndex(ti *TaskInfo) {
	if _, found := ji.TaskStatusIndex[ti.Status]; !found {
		ji.TaskStatusIndex[ti.Status] = TasksMap{}
	}
	ji.TaskStatusIndex[ti.Status][ti.UID] = ti
}

// AddTaskInfo 将任务添加到作业中
// 同时更新：任务映射表、状态索引、总资源需求、已分配资源、子作业关系
func (ji *JobInfo) AddTaskInfo(ti *TaskInfo) {
	ji.Tasks[ti.UID] = ti
	ji.addTaskIndex(ti)
	ji.TotalRequest.Add(ti.Resreq)
	if AllocatedStatus(ti.Status) {
		ji.Allocated.Add(ti.Resreq)
	}
	ji.addTaskToSubJob(ti)
}

// UpdateTaskStatus 更新作业中任务的状态
// 先删除旧状态的任务，再以新状态重新添加
// 这保证了任务状态索引的一致性
func (ji *JobInfo) UpdateTaskStatus(task *TaskInfo, status TaskStatus) {
	// First remove the task (if exist) from the task list.
	if _, found := ji.Tasks[task.UID]; found {
		ji.DeleteTaskInfo(task)
	}

	// Update task's status to the target status once task addition is guaranteed to succeed.
	task.Status = status
	ji.AddTaskInfo(task)
}

// deleteTaskIndex 从状态索引中删除任务
// 如果该状态下已无其他任务，则移除整个状态索引项
func (ji *JobInfo) deleteTaskIndex(ti *TaskInfo) {
	if tasks, found := ji.TaskStatusIndex[ti.Status]; found {
		delete(tasks, ti.UID)

		if len(tasks) == 0 {
			delete(ji.TaskStatusIndex, ti.Status)
		}
	}
}

// DeleteTaskInfo 从作业中删除任务
// 同时更新：总资源需求、已分配资源、任务映射表、状态索引、子作业关系
// 如果任务不存在，仅输出警告日志
func (ji *JobInfo) DeleteTaskInfo(ti *TaskInfo) {
	if task, found := ji.Tasks[ti.UID]; found {
		ji.TotalRequest.Sub(task.Resreq)
		if AllocatedStatus(task.Status) {
			ji.Allocated.Sub(task.Resreq)
		}
		delete(ji.Tasks, task.UID)
		ji.deleteTaskIndex(task)
		ji.deleteTaskFromSubJob(ti)
		return
	}

	klog.Warningf("failed to find task <%v/%v> in job <%v/%v>", ti.Namespace, ti.Name, ji.Namespace, ji.Name)
}

// Clone 返回 JobInfo 的深拷贝副本
// 克隆所有字段，包括 PodGroup、资源、任务列表、子作业等
// 注意：NodesFitErrors 会被初始化为空映射，因为调度错误信息不需要在克隆中保留
func (ji *JobInfo) Clone() *JobInfo {
	info := &JobInfo{
		UID:       ji.UID,
		PgUID:     ji.PgUID,
		Name:      ji.Name,
		Namespace: ji.Namespace,
		Queue:     ji.Queue,
		Priority:  ji.Priority,

		MinAvailable:   ji.MinAvailable,
		WaitingTime:    ji.WaitingTime,
		JobFitErrors:   ji.JobFitErrors,
		NodesFitErrors: make(map[TaskID]*FitErrors),
		Allocated:      EmptyResource(),
		TotalRequest:   EmptyResource(),

		PodGroup: ji.PodGroup.Clone(),

		TaskStatusIndex:       map[TaskStatus]TasksMap{},
		TaskMinAvailable:      make(map[string]int32, len(ji.TaskMinAvailable)),
		TaskMinAvailableTotal: ji.TaskMinAvailableTotal,
		Tasks:                 TasksMap{},
		Preemptable:           ji.Preemptable,
		RevocableZone:         ji.RevocableZone,
		Budget:                ji.Budget.Clone(),
		AllocatedHyperNode:    ji.AllocatedHyperNode,
		NetworkTopology:       cloneNetworkTopology(ji.NetworkTopology),
		SubJobs:               map[SubJobID]*SubJobInfo{},
		TaskToSubJob:          map[TaskID]SubJobID{},
		MinSubJobs:            maps.Clone(ji.MinSubJobs),
	}

	ji.CreationTimestamp.DeepCopyInto(&info.CreationTimestamp)

	for task, minAvailable := range ji.TaskMinAvailable {
		info.TaskMinAvailable[task] = minAvailable
	}
	for _, task := range ji.Tasks {
		info.AddTaskInfo(task.Clone())
	}

	for subJobID, subJob := range ji.SubJobs {
		if sji, found := info.SubJobs[subJobID]; found {
			sji.CloneStatusFrom(subJob)
		} else {
			klog.Errorf("Failed to clone subJob %s for job %s", subJob.UID, ji.UID)
		}
	}

	return info
}

// String 返回 JobInfo 的可读字符串表示，用于日志输出
func (ji JobInfo) String() string {
	res := ""

	i := 0
	for _, task := range ji.Tasks {
		res += fmt.Sprintf("\n\t %d: %v", i, task)
		i++
	}

	return fmt.Sprintf("Job (%v): namespace %v (%v), name %v, minAvailable %d, podGroup %+v, preemptable %+v, revocableZone %+v, minAvailable %+v, maxAvailable %+v",
		ji.UID, ji.Namespace, ji.Queue, ji.Name, ji.MinAvailable, ji.PodGroup, ji.Preemptable, ji.RevocableZone, ji.Budget.MinAvailable, ji.Budget.MaxUnavailable) + res
}

// FitError 返回作业任务调度失败的详细信息
// 包括：各状态的任务统计、PodGroup 就绪状态、Pending 任务的详细原因
// 以及原始失败原因（如入队失败或首个 Pod 的谓词失败信息）
func (ji *JobInfo) FitError() string {
	sortReasonsHistogram := func(reasons map[string]int) []string {
		reasonStrings := []string{}
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}

	// Stat histogram for all tasks of the job
	reasons := make(map[string]int)
	for status, taskMap := range ji.TaskStatusIndex {
		reasons[status.String()] += len(taskMap)
	}
	reasons["minAvailable"] = int(ji.MinAvailable)

	podGroupStatus := scheduling.PodGroupNotReady
	if ji.IsReady() {
		podGroupStatus = scheduling.PodGroupReady
	}
	reasonMsg := fmt.Sprintf("%v, %v", podGroupStatus, strings.Join(sortReasonsHistogram(reasons), ", "))

	// Stat histogram for pending tasks only
	reasons = make(map[string]int)
	for uid := range ji.TaskStatusIndex[Pending] {
		reason, _, _ := ji.TaskSchedulingReason(uid)
		reasons[reason]++
	}
	if len(reasons) > 0 {
		reasonMsg += "; " + fmt.Sprintf("%s: %s", Pending.String(), strings.Join(sortReasonsHistogram(reasons), ", "))
	}

	// record the original reason: such as can not enqueue or failed reasons of first pod failed to predicated
	if ji.JobFitErrors != "" {
		reasonMsg += ". Origin reason is: " + ji.JobFitErrors
	} else {
		for _, taskInfo := range ji.Tasks {
			fitError := ji.NodesFitErrors[taskInfo.UID]
			if fitError != nil {
				reasonMsg += fmt.Sprintf(". Origin reason is %v: %v", taskInfo.Name, fitError.Error())
				break
			}
		}
	}

	return reasonMsg
}

// TaskSchedulingReason 获取指定任务的详细调度原因和消息
// 基于上一次调度事务的上下文，返回任务无法调度的具体原因
// 返回值：
//   - reason: 标准化的原因字符串（如 PodReasonSchedulable、PodReasonUnschedulable）
//   - msg: 详细的原因描述信息
//   - nominatedNodeName: 建议节点名称（仅在 Pipelined 且发生驱逐时有效）
func (ji *JobInfo) TaskSchedulingReason(tid TaskID) (reason, msg, nominatedNodeName string) {
	taskInfo, exists := ji.Tasks[tid]
	if !exists {
		return "", "", ""
	}

	// 优先使用上一次事务的上下文（记录了最近一次调度尝试的结果）
	ctx := taskInfo.GetTransactionContext()
	if taskInfo.LastTransaction != nil {
		ctx = *taskInfo.LastTransaction
	}

	msg = ji.JobFitErrors
	switch status := ctx.Status; status {
	case Allocated:
		// 任务可调度，但需要等待 minAvailable 满足后才能绑定
		msg = fmt.Sprintf("Pod %s/%s can possibly be assigned to %s, once minAvailable is satisfied", taskInfo.Namespace, taskInfo.Name, ctx.NodeName)
		return PodReasonSchedulable, msg, ""
	case Pipelined:
		// 任务可调度但需等待资源释放（已被其他任务占用），且 minAvailable 需满足
		msg = fmt.Sprintf("Pod %s/%s can possibly be assigned to %s, once resource is released and minAvailable is satisfied", taskInfo.Namespace, taskInfo.Name, ctx.NodeName)
		if ctx.EvictionOccurred {
			// 如果发生了驱逐，则将驱逐目标节点设为建议节点
			nominatedNodeName = ctx.NodeName
		}
		return PodReasonUnschedulable, msg, nominatedNodeName
	case Pending:
		if fe := ji.NodesFitErrors[tid]; fe != nil {
			// 任务在所有节点上都调度失败（不可调度）
			return PodReasonUnschedulable, fe.Error(), ""
		}
		// 任务尚未被调度尝试，标记为不可调度以支持集群自动扩缩容
		return PodReasonUnschedulable, msg, ""
	default:
		// 其他状态直接返回状态字符串
		return status.String(), msg, ""
	}
}

// ReadyTaskNum 返回就绪任务的数量
// 就绪任务包括：Bound（已绑定）、Binding（绑定中）、Running（运行中）、
// Allocated（已分配）、Succeeded（已成功）状态的任务
func (ji *JobInfo) ReadyTaskNum() int32 {
	occupied := 0
	occupied += len(ji.TaskStatusIndex[Bound])
	occupied += len(ji.TaskStatusIndex[Binding])
	occupied += len(ji.TaskStatusIndex[Running])
	occupied += len(ji.TaskStatusIndex[Allocated])
	occupied += len(ji.TaskStatusIndex[Succeeded])

	return int32(occupied)
}

// WaitingTaskNum 返回等待中（Pipelined）的任务数量
// Pipelined 状态表示任务已找到候选节点但需等待资源释放
func (ji *JobInfo) WaitingTaskNum() int32 {
	return int32(len(ji.TaskStatusIndex[Pipelined]))
}

// PendingBestEffortTaskNum 返回 Pending 状态中的 BestEffort 任务数量
// BestEffort 任务不请求任何 CPU/内存资源，总是被视为可用
func (ji *JobInfo) PendingBestEffortTaskNum() int32 {
	count := 0
	for _, task := range ji.TaskStatusIndex[Pending] {
		if task.BestEffort {
			count++
		}
	}
	return int32(count)
}

// AllocatedTaskNum 返回已分配状态的任务数量
// 已分配状态包括：Bound、Binding、Running、Allocated
func (ji *JobInfo) AllocatedTaskNum() int32 {
	count := 0
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) {
			count += len(tasks)
		}
	}
	return int32(count)
}

// FitFailedRoles 返回指定子作业中调度失败的任务角色集合
// 用于判断某角色的任务是否存在调度失败记录
func (ji *JobInfo) FitFailedRoles(subJob SubJobID) map[string]struct{} {
	failedRoles := map[string]struct{}{}
	for tid := range ji.NodesFitErrors {
		if ji.TaskToSubJob[tid] != subJob {
			continue
		}
		task := ji.Tasks[tid]
		failedRoles[task.TaskRole] = struct{}{}
	}
	return failedRoles
}

// TaskHasFitErrors 检查任务是否有调度失败的记录
// 如果任务未设置 task-spec（TaskRole），则不使用缓存，返回 false
// 否则检查该任务角色是否在子作业的失败记录中
func (ji *JobInfo) TaskHasFitErrors(subJob SubJobID, task *TaskInfo) bool {
	// if the task didn't set the spec key, should not use the cache
	if len(task.TaskRole) == 0 {
		return false
	}

	_, exist := ji.FitFailedRoles(subJob)[task.TaskRole]
	return exist
}

// NeedContinueAllocating 检查当前作业在某个 Pod 调度失败后是否可以继续分配
// 有两种情况可以继续：
//  1. 作业总的可分配数量满足 minAvailable（无独立角色 minMember 设置时）
//     因为某些 Pod 不可调度，但其他 Pod 可调度且数量满足 Gang Scheduling 要求
//  2. 每个角色的可分配数量满足其独立的 minAvailable
//     当某角色调度失败但其已分配数量已满足 minMember 时，其他角色可继续
//
// 性能分析：由于失败角色在出队时已预检，此函数最多被调用次数等于作业中的角色数
func (ji *JobInfo) NeedContinueAllocating(subJobID SubJobID) bool {
	// 如果作业的 MinAvailable 等于任务总数，则所有任务都必须运行，任何一个失败都不能继续
	if int(ji.MinAvailable) == len(ji.Tasks) {
		return false
	}
	// 对子作业做同样的检查：如果子任务的 MinAvailable 大于等于子任务总数，不能继续
	if subJob, found := ji.SubJobs[subJobID]; found {
		if int(subJob.MinAvailable) >= len(subJob.Tasks) {
			return false
		}
	}

	// 包含子作业策略的作业暂不支持以下优化逻辑，直接返回 true
	// todo: 包含子作业策略的作业不支持以下策略
	if ji.ContainsSubJobPolicy() {
		return true
	}

	failedRoles := ji.FitFailedRoles(subJobID)

	pending := map[string]int32{}
	for _, task := range ji.TaskStatusIndex[Pending] {
		pending[task.TaskRole]++
	}
	// 情况1：不考虑每个角色的最小值，仅比较总可分配数与作业的 MinAvailable
	// 当 MinAvailable < TaskMinAvailableTotal 时，使用此逻辑
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		left := int32(0)
		for role, cnt := range pending {
			if _, ok := failedRoles[role]; !ok {
				left += cnt
			}
		}
		return ji.ReadyTaskNum()+left >= ji.MinAvailable
	}

	// 情况2：每个角色有独立的 minMember，逐个检查
	// 如果某角色调度失败且其已分配数量小于 minAvailable，则不能继续
	allocated := ji.getJobAllocatedRoles()
	for role := range failedRoles {
		min := ji.TaskMinAvailable[role]
		if min == 0 {
			continue
		}
		// current role predicated failed and it means the left task with same role can not be allocated,
		// and allocated number less than minAvailable, it can not be ready
		if allocated[role] < min {
			return false
		}
	}

	return true
}

// getJobAllocatedRoles 返回每个角色的已分配任务数量
// 已分配包括：AllocatedStatus（Bound/Binding/Running/Allocated）和 Succeeded 状态的任务
// 以及 Pending 状态中的 BestEffort 任务(无资源需求，视为自动满足)
func (ji *JobInfo) getJobAllocatedRoles() map[string]int32 {
	occupiedMap := map[string]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) ||
			status == Succeeded {
			for _, task := range tasks {
				occupiedMap[task.TaskRole]++
			}
			continue
		}

		if status == Pending {
			for _, task := range tasks {
				if task.InitResreq.IsEmpty() {
					occupiedMap[task.TaskRole]++
				}
			}
		}
	}
	return occupiedMap
}

// CheckTaskValid 检查作业中各角色的任务数量是否有效
// 当 MinAvailable >= TaskMinAvailableTotal 时才进行检查
// 如果某角色的实际任务数(包含 Allocated/Succeeded/Pipelined/Pending 状态)小于其 minAvailable，返回 false
func (ji *JobInfo) CheckTaskValid() bool {
	// 如果 MinAvailable 小于各角色最小成员数之和，跳过此检查
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}

	actual := map[string]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) ||
			status == Succeeded ||
			status == Pipelined ||
			status == Pending {
			for _, task := range tasks {
				actual[task.TaskRole]++
			}
		}
	}

	klog.V(4).Infof("job %s/%s actual: %+v, ji.TaskMinAvailable: %+v", ji.Name, ji.Namespace, actual, ji.TaskMinAvailable)
	for task, minAvailable := range ji.TaskMinAvailable {
		if minAvailable == 0 {
			continue
		}
		if act, ok := actual[task]; !ok || act < minAvailable {
			return false
		}
	}
	return true
}

// CheckTaskReady 检查作业中各角色是否已就绪
// 就绪条件：每个角色的已分配任务数 >= 其 minAvailable
// 与 CheckTaskValid 不同，此方法只统计已分配和已成功的任务
func (ji *JobInfo) CheckTaskReady() bool {
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}
	occupiedMap := ji.getJobAllocatedRoles()
	for taskSpec, minNum := range ji.TaskMinAvailable {
		if occupiedMap[taskSpec] < minNum {
			klog.V(4).Infof("Job %s/%s Task %s occupied %v less than task min available", ji.Namespace, ji.Name, taskSpec, occupiedMap[taskSpec])
			return false
		}
	}
	return true
}

// CheckTaskPipelined 检查作业中各角色是否已 Pipelined
// Pipelined 条件：每个角色的已分配+已成功+已 Pipelined+BestEffort Pending 数 >= 其 minAvailable
func (ji *JobInfo) CheckTaskPipelined() bool {
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}
	occupiedMap := map[string]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) ||
			status == Succeeded ||
			status == Pipelined {
			for _, task := range tasks {
				occupiedMap[task.TaskRole]++
			}
			continue
		}

		if status == Pending {
			for _, task := range tasks {
				if task.InitResreq.IsEmpty() {
					occupiedMap[task.TaskRole]++
				}
			}
		}
	}
	for taskSpec, minNum := range ji.TaskMinAvailable {
		if occupiedMap[taskSpec] < minNum {
			klog.V(4).Infof("Job %s/%s Task %s occupied %v less than task min available", ji.Namespace, ji.Name, taskSpec, occupiedMap[taskSpec])
			return false
		}
	}
	return true
}

// CheckTaskStarving 检查作业中是否有角色处于饥饿状态
// 饥饿条件：某角色的已分配+已成功+已 Pipelined 数 < 其 minAvailable
// 返回 true 表示至少有一个角色需要更多资源
func (ji *JobInfo) CheckTaskStarving() bool {
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}
	occupiedMap := map[string]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) ||
			status == Succeeded ||
			status == Pipelined {
			for _, task := range tasks {
				occupiedMap[task.TaskRole]++
			}
			continue
		}
	}
	for taskSpec, minNum := range ji.TaskMinAvailable {
		if occupiedMap[taskSpec] < minNum {
			klog.V(4).Infof("Job %s/%s Task %s occupied %v less than task min available", ji.Namespace, ji.Name, taskSpec, occupiedMap[taskSpec])
			return true
		}
	}
	return false
}

// ValidTaskNum 返回有效任务的数量
// 有效任务包括：Allocated/Succeeded/Pipelined/Pending 状态的任务
func (ji *JobInfo) ValidTaskNum() int32 {
	occupied := 0
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) ||
			status == Succeeded ||
			status == Pipelined ||
			status == Pending {
			occupied += len(tasks)
		}
	}

	return int32(occupied)
}

// CheckSubJobValid 检查子作业的数量是否满足最小子作业数要求
// 遍历每个子作业组，检查该组中的子作业数量是否 >= MinSubJobs
func (ji *JobInfo) CheckSubJobValid() bool {
	subJobs := map[SubJobGID]int32{}
	for _, subJob := range ji.SubJobs {
		if _, ok := subJobs[subJob.GID]; !ok {
			subJobs[subJob.GID] = 0
		}
		subJobs[subJob.GID]++
	}
	for subJobGID, minSubJobs := range ji.MinSubJobs {
		if subJobs[subJobGID] < minSubJobs {
			return false
		}
	}
	return true
}

// checkSubJobCondition 是子作业条件检查的通用方法
// 遍历所有子作业，对每个子作业应用条件函数，统计满足条件的子作业数
// 然后检查每个子作业组中满足条件的子作业数是否 >= minSubGroups
func (ji *JobInfo) checkSubJobCondition(condition subJobCondition) error {
	allocatedSubJobs := map[SubJobGID]int32{}
	for _, subJob := range ji.SubJobs {
		if _, ok := allocatedSubJobs[subJob.GID]; !ok {
			allocatedSubJobs[subJob.GID] = 0
		}
		if condition(subJob) {
			allocatedSubJobs[subJob.GID]++
		}
	}
	for subJobGID, minSubJobs := range ji.MinSubJobs {
		if minSubJobs == 0 {
			continue
		}
		if allocatedSubJobs[subJobGID] < minSubJobs {
			return fmt.Errorf("the number of allocated subGroups %d is less than the number of subGroups %d with the minSubGroups attribute in subGroupPolicy %s.",
				allocatedSubJobs[subJobGID], minSubJobs, subJobGID)
		}
	}
	return nil
}

// CheckSubJobReady 检查子作业是否已就绪
// 每个 SubGroupPolicy 组中，就绪的子作业数量必须 >= minSubGroups
func (ji *JobInfo) CheckSubJobReady() bool {
	if err := ji.checkSubJobCondition(func(subJob *SubJobInfo) bool {
		return subJob.IsReady()
	}); err != nil {
		klog.V(4).Infof("Job %s/%s SubJob not ready, reason: %v", ji.Namespace, ji.Name, err.Error())
		return false
	}
	return true
}

// CheckSubJobPipelined 检查子作业是否已 Pipelined
// 每个 SubGroupPolicy 组中，Pipelined 的子作业数量必须 >= minSubGroups
func (ji *JobInfo) CheckSubJobPipelined() bool {
	if err := ji.checkSubJobCondition(func(subJob *SubJobInfo) bool {
		return subJob.IsPipelined()
	}); err != nil {
		klog.V(4).Infof("Job %s/%s SubJob not pipelined, reason: %v", ji.Namespace, ji.Name, err.Error())
		return false
	}
	return true
}

// IsReady 检查作业是否就绪（满足 Gang Scheduling 的最小可用成员数）
// 就绪条件：ReadyTaskNum + PendingBestEffortTaskNum >= MinAvailable
func (ji *JobInfo) IsReady() bool {
	return ji.ReadyTaskNum()+ji.PendingBestEffortTaskNum() >= ji.MinAvailable
}

// IsPipelined 检查作业是否已 Pipelined
// Pipelined 条件：WaitingTaskNum + ReadyTaskNum + PendingBestEffortTaskNum >= MinAvailable
// 表示作业已有足够任务找到候选节点（部分在等待资源释放）
func (ji *JobInfo) IsPipelined() bool {
	return ji.WaitingTaskNum()+ji.ReadyTaskNum()+ji.PendingBestEffortTaskNum() >= ji.MinAvailable
}

// IsStarving 检查作业是否处于饥饿状态
// 饥饿条件：WaitingTaskNum + ReadyTaskNum < MinAvailable
// 表示作业还没有足够的任务找到候选节点
func (ji *JobInfo) IsStarving() bool {
	return ji.WaitingTaskNum()+ji.ReadyTaskNum() < ji.MinAvailable
}

// IsPending 返回作业是否处于 Pending 状态
// 当 PodGroup 为 nil、PodGroup 阶段为 Pending 或为空时返回 true
func (ji *JobInfo) IsPending() bool {
	return ji.PodGroup == nil ||
		ji.PodGroup.Status.Phase == scheduling.PodGroupPending ||
		ji.PodGroup.Status.Phase == ""
}

// HasPendingTasks 返回作业是否有 Pending 状态的任务
func (ji *JobInfo) HasPendingTasks() bool {
	return len(ji.TaskStatusIndex[Pending]) != 0
}

// IsHardTopologyMode 返回作业的网络拓扑模式是否为硬性模式，以及允许的最高层级
// 硬性模式表示任务必须部署在满足拓扑约束的节点上，不能降级
func (ji *JobInfo) IsHardTopologyMode() (bool, int) {
	if ji.NetworkTopology == nil || ji.NetworkTopology.HighestTierAllowed == nil {
		return false, 0
	}

	return ji.NetworkTopology.Mode == scheduling.HardNetworkTopologyMode, *ji.NetworkTopology.HighestTierAllowed
}

// IsSoftTopologyMode 返回作业是否配置了软性网络拓扑模式
// 软性模式下，调度器会尽量满足拓扑约束，但允许降级到较低层级的拓扑
func (ji *JobInfo) IsSoftTopologyMode() bool {
	if ji.NetworkTopology == nil {
		return false
	}
	return ji.NetworkTopology.Mode == scheduling.SoftNetworkTopologyMode
}

// WithNetworkTopology 返回作业是否配置了网络拓扑
func (ji *JobInfo) WithNetworkTopology() bool {
	return ji.NetworkTopology != nil
}

// ResetFitErr 重置作业和节点的调度错误信息
// 在新的调度周期开始时调用
func (ji *JobInfo) ResetFitErr() {
	ji.JobFitErrors = ""
	ji.NodesFitErrors = make(map[TaskID]*FitErrors)
}

// ResetSubJobFitErr 重置指定子作业的节点调度错误信息
// 使用 maps.DeleteFunc 过滤掉属于指定子作业的任务错误记录
func (ji *JobInfo) ResetSubJobFitErr(subJob SubJobID) {
	maps.DeleteFunc(ji.NodesFitErrors, func(taskID TaskID, _ *FitErrors) bool {
		return ji.TaskToSubJob[taskID] == subJob
	})
}

// DefaultSubJobGID 返回默认子作业的组 ID（使用作业 UID）
func (ji *JobInfo) DefaultSubJobGID() SubJobGID {
	return SubJobGID(ji.UID)
}

// DefaultSubJobID 返回默认子作业的 ID（使用作业 UID）
func (ji *JobInfo) DefaultSubJobID() SubJobID {
	return SubJobID(ji.UID)
}

// getOrCreateDefaultSubJob 获取或创建默认子作业
// 如果作业没有配置 SubGroupPolicy，则所有任务都属于默认子作业
// 默认子作业的 SubGroupSize 设置为作业的 MinAvailable
func (ji *JobInfo) getOrCreateDefaultSubJob() *SubJobInfo {
	defaultSubJobGID := ji.DefaultSubJobGID()
	defaultSubJob := ji.DefaultSubJobID()
	if _, found := ji.SubJobs[defaultSubJob]; !found {
		policy := &scheduling.SubGroupPolicySpec{}
		if !ji.ContainsSubJobPolicy() {
			policy.SubGroupSize = ptr.To(ji.MinAvailable)
		}
		if ji.NetworkTopology != nil {
			policy.NetworkTopology = cloneNetworkTopology(ji.NetworkTopology)
		}
		ji.SubJobs[defaultSubJob] = NewSubJobInfo(defaultSubJobGID, defaultSubJob, ji.UID, policy, nil)
	}
	return ji.SubJobs[defaultSubJob]
}

// getOrCreateSubJob 根据 Pod 的标签匹配规则获取或创建子作业
// 遍历 PodGroup 的 SubGroupPolicy，找到第一个匹配当前任务的策略
// 如果没有匹配的策略，返回默认子作业
func (ji *JobInfo) getOrCreateSubJob(ti *TaskInfo) *SubJobInfo {
	if ji.PodGroup == nil {
		return ji.getOrCreateDefaultSubJob()
	}

	for _, policy := range ji.PodGroup.Spec.SubGroupPolicy {
		if matchValues := getSubJobMatchValues(policy, ti.Pod); len(matchValues) > 0 {
			groupID := getSubJobGID(ji.UID, policy.Name)
			subJobID := getSubJobID(ji.UID, policy.Name, matchValues)
			if _, found := ji.SubJobs[subJobID]; !found {
				ji.SubJobs[subJobID] = NewSubJobInfo(groupID, subJobID, ji.UID, &policy, matchValues)
			}
			return ji.SubJobs[subJobID]
		}
	}

	return ji.getOrCreateDefaultSubJob()
}

// addTaskToSubJob 将任务添加到对应的子作业中
// 根据任务的标签匹配确定其所属的子作业，并更新 TaskToSubJob 映射
func (ji *JobInfo) addTaskToSubJob(ti *TaskInfo) {
	subJob := ji.getOrCreateSubJob(ti)
	subJob.addTask(ti)

	ji.TaskToSubJob[ti.UID] = subJob.UID
}

// deleteTaskFromSubJob 从子作业中删除任务
// 同时清理 TaskToSubJob 映射中的记录
func (ji *JobInfo) deleteTaskFromSubJob(ti *TaskInfo) {
	subJobID := ji.TaskToSubJob[ti.UID]
	if subJob, found := ji.SubJobs[subJobID]; found {
		subJob.deleteTask(ti)
	}

	delete(ji.TaskToSubJob, ti.UID)
}

// ContainsSubJobPolicy 返回作业是否配置了子作业策略（SubGroupPolicy）
// 如果配置了，则作业中的任务会根据策略被分配到不同的子作业
func (ji *JobInfo) ContainsSubJobPolicy() bool {
	if ji.PodGroup == nil {
		return false
	}
	return len(ji.PodGroup.Spec.SubGroupPolicy) > 0
}

// ContainsHardTopologyInSubJob 返回子作业中是否包含硬性网络拓扑约束
// 遍历所有 SubGroupPolicy，检查是否有策略配置了 HardNetworkTopologyMode
func (ji *JobInfo) ContainsHardTopologyInSubJob() bool {
	for _, subJob := range ji.SubJobs {
		if hard, _ := subJob.IsHardTopologyMode(); hard {
			return true
		}
	}
	return false
}

// ContainsHardTopology 返回作业或其子作业中是否包含硬性网络拓扑约束
func (ji *JobInfo) ContainsHardTopology() bool {
	if hard, _ := ji.IsHardTopologyMode(); hard || ji.ContainsHardTopologyInSubJob() {
		return true
	}
	return false
}

// ContainsNetworkTopologyInSubJob 返回子作业中是否配置了网络拓扑
func (ji *JobInfo) ContainsNetworkTopologyInSubJob() bool {
	for _, subJob := range ji.SubJobs {
		if subJob.WithNetworkTopology() {
			return true
		}
	}
	return false
}

// ContainsNetworkTopology 返回作业或其子作业中是否配置了网络拓扑
func (ji *JobInfo) ContainsNetworkTopology() bool {
	return ji.WithNetworkTopology() || ji.ContainsNetworkTopologyInSubJob()
}

// DRAResource represents aggregated DRA resource request for a single DeviceClass
type DRAResource struct {
	// Count is the total number of devices requested
	Count int64
	// Capacity maps dimension name to total requested quantity
	Capacity map[string]resource.Quantity
}

func cloneResourceClaimDRAResreq(in map[string]map[string]*DRAResource) map[string]map[string]*DRAResource {
	if in == nil {
		return nil
	}
	out := make(map[string]map[string]*DRAResource, len(in))
	for claimKey, resources := range in {
		out[claimKey] = make(map[string]*DRAResource, len(resources))
		for deviceClass, res := range resources {
			out[claimKey][deviceClass] = res.Clone()
		}
	}
	return out
}

// Clone returns a deep copy of DRAResource
func (d *DRAResource) Clone() *DRAResource {
	if d == nil {
		return nil
	}
	out := &DRAResource{
		Count: d.Count,
	}
	if d.Capacity != nil {
		out.Capacity = make(map[string]resource.Quantity, len(d.Capacity))
		for k, v := range d.Capacity {
			out.Capacity[k] = v.DeepCopy()
		}
	}
	return out
}

// Add adds another DRAResource into this one
func (d *DRAResource) Add(other *DRAResource) {
	if other == nil {
		return
	}
	d.Count += other.Count
	if other.Capacity != nil {
		if d.Capacity == nil {
			d.Capacity = make(map[string]resource.Quantity)
		}
		for k, v := range other.Capacity {
			if existing, ok := d.Capacity[k]; ok {
				existing.Add(v)
				d.Capacity[k] = existing
			} else {
				d.Capacity[k] = v.DeepCopy()
			}
		}
	}
}

// Sub subtracts another DRAResource from this one, ensuring values do not drop below zero
func (d *DRAResource) Sub(other *DRAResource) {
	if other == nil {
		return
	}
	d.Count -= other.Count
	if d.Count < 0 {
		d.Count = 0
	}
	if other.Capacity != nil && d.Capacity != nil {
		zeroQuantity := resource.MustParse("0")
		for k, v := range other.Capacity {
			if existing, ok := d.Capacity[k]; ok {
				existing.Sub(v)
				if existing.Cmp(zeroQuantity) < 0 {
					existing = zeroQuantity.DeepCopy()
				}
				d.Capacity[k] = existing
			}
		}
	}
}

// GetMinDRAResources returns the minimum DRA resources required by the job based on TaskMinAvailable
func (ji *JobInfo) GetMinDRAResources() map[string]*DRAResource {
	if len(ji.Tasks) == 0 {
		return nil
	}

	result := make(map[string]*DRAResource)
	addResource := func(res map[string]*DRAResource, times int32) {
		for deviceClass, request := range res {
			if _, exists := result[deviceClass]; !exists {
				result[deviceClass] = &DRAResource{
					Count:    0,
					Capacity: make(map[string]resource.Quantity),
				}
			}

			result[deviceClass].Count += request.Count * int64(times)
			for dim, cap := range request.Capacity {
				totalCap := cap.DeepCopy()
				for i := int32(0); i < times-1; i++ {
					totalCap.Add(cap)
				}

				if existing, exists := result[deviceClass].Capacity[dim]; exists {
					existing.Add(totalCap)
					result[deviceClass].Capacity[dim] = existing
				} else {
					result[deviceClass].Capacity[dim] = totalCap
				}
			}
		}
	}

	if len(ji.TaskMinAvailable) == 0 {
		minAvailable := ji.MinAvailable
		if minAvailable <= 0 {
			return nil
		}

		tasks := make([]*TaskInfo, 0, len(ji.Tasks))
		for _, task := range ji.Tasks {
			if task.DRAResreq != nil {
				tasks = append(tasks, task)
			}
		}
		sort.Slice(tasks, func(i, j int) bool {
			if tasks[i].Namespace != tasks[j].Namespace {
				return tasks[i].Namespace < tasks[j].Namespace
			}
			if tasks[i].Name != tasks[j].Name {
				return tasks[i].Name < tasks[j].Name
			}
			return tasks[i].UID < tasks[j].UID
		})

		for i, task := range tasks {
			if int32(i) >= minAvailable {
				break
			}
			addResource(task.DRAResreq, 1)
		}

		if len(result) == 0 {
			return nil
		}
		return result
	}

	// Since DRA requests can vary per task/pod, we aggregate them based on TaskMinAvailable
	processedRoles := make(map[string]struct{})
	for _, task := range ji.Tasks {
		if task.DRAResreq == nil {
			continue
		}

		// Calculate how many times this task type needs to run
		taskType := task.TaskRole
		minNum, ok := ji.TaskMinAvailable[taskType]
		if !ok || minNum <= 0 {
			continue
		}
		if _, seen := processedRoles[taskType]; seen {
			continue
		}
		processedRoles[taskType] = struct{}{}

		addResource(task.DRAResreq, minNum)
	}

	if len(result) == 0 {
		return nil
	}
	return result
}
