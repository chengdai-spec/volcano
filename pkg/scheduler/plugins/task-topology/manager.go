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

package tasktopology

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// ============================================================================
// topologyType 拓扑类型定义
// ============================================================================
// topologyType 枚举定义了四种拓扑关系类型，用于区分不同任务类型之间的
// 亲和/反亲和关系。这些类型决定了任务在分桶和排序时的优先级。
// ============================================================================

// topologyType 表示拓扑关系的类型
type topologyType int

const (
	// selfAntiAffinity 自身反亲和: 同一任务类型的多个副本之间应分散到不同节点
	// 例如: "ps" 自身反亲和 → 多个 ps 副本不应放在同一节点
	// 优先级最高(4)，因为自身反亲和通常是硬性需求（如避免单点故障）
	selfAntiAffinity topologyType = iota

	// interAntiAffinity 交叉反亲和: 不同任务类型之间应分散到不同节点
	// 例如: "worker" 与 "chief" 反亲和 → worker 和 chief 不应放在同一节点
	// 优先级最低(1)
	interAntiAffinity

	// selfAffinity 自身亲和: 同一任务类型的多个副本应尽量调度到同一节点
	// 例如: "worker" 自身亲和 → 多个 worker 副本尽量放在同一节点
	// 优先级(2)
	selfAffinity

	// interAffinity 交叉亲和: 不同任务类型之间应尽量调度到同一节点
	// 例如: "ps" 与 "worker" 亲和 → ps 和 worker 尽量放在同一节点
	// 优先级(3)
	interAffinity
)

// affinityPriority 定义了各拓扑类型的优先级，数值越大优先级越高
// 优先级排序: selfAntiAffinity(4) > interAffinity(3) > selfAffinity(2) > interAntiAffinity(1)
//
// 这个优先级影响:
//   - taskAffinityOrder: 在同桶内任务排序时，优先级高的任务类型排在前面
//   - taskAffinityPriority: 记录每个任务类型的最高拓扑优先级，用于分桶排序
var affinityPriority = map[topologyType]int{
	selfAntiAffinity:  4,
	interAffinity:     3,
	selfAffinity:      2,
	interAntiAffinity: 1,
}

// ============================================================================
// JobManager 作业管理器
// ============================================================================
// JobManager 是 task-topology 插件中每个作业的管理器，负责:
//   1. 解析和应用用户定义的拓扑配置（亲和/反亲和/任务排序）
//   2. 将作业中的任务划分为若干桶（Bucket）
//   3. 提供桶查询、任务排序、亲和性检查等接口供插件调用
//
// 每个有拓扑配置的作业在 OnSessionOpen 阶段创建一个 JobManager，
// 在 OnSessionClose 阶段被销毁。
// ============================================================================

// JobManager 是用于保存作业亲和性和桶信息的结构体
type JobManager struct {
	// jobID 是作业的唯一标识
	jobID api.JobID

	// buckets 保存该作业的所有桶
	// 每个桶包含一组具有亲和关系的任务
	buckets []*Bucket

	// podInBucket 记录每个 Pod 属于哪个桶
	// 键为 Pod UID，值为桶索引（或 OutOfBucket=-1 表示不属于任何桶）
	podInBucket map[types.UID]int

	// podInTask 记录每个 Pod 的任务类型名称
	// 键为 Pod UID，值为任务类型名称（如 "ps"、"worker"）
	podInTask map[types.UID]string

	// taskAffinityPriority 记录每个任务类型的最高拓扑优先级
	// 键为任务类型名称，值为该任务类型在所有拓扑配置中的最高优先级
	// 用于在 TaskOrderFn 中比较不同任务类型的调度优先级
	taskAffinityPriority map[string]int

	// taskExistOrder 记录用户自定义的任务排序
	// 键为任务类型名称，值为排序权重（排在 TaskOrder 列表越前面的值越大）
	// 例如 TaskOrder=["ps","worker","chief"] → ps=3, worker=2, chief=1
	taskExistOrder map[string]int

	// interAffinity 记录交叉亲和关系
	// 结构: [任务类型A] -> [任务类型B, 任务类型C, ...]
	// 表示任务类型A 与 B、C 之间存在亲和关系，应尽量调度到同一桶
	interAffinity map[string]map[string]struct{}

	// selfAffinity 记录自身亲和关系
	// 包含的任务类型，其多个副本应尽量调度到同一桶
	selfAffinity map[string]struct{}

	// interAntiAffinity 记录交叉反亲和关系
	// 结构: [任务类型A] -> [任务类型B, 任务类型C, ...]
	// 表示任务类型A 与 B、C 之间存在反亲和关系，应分散到不同桶
	interAntiAffinity map[string]map[string]struct{}

	// selfAntiAffinity 记录自身反亲和关系
	// 包含的任务类型，其多个副本应分散到不同桶
	selfAntiAffinity map[string]struct{}

	// bucketMaxSize 记录所有桶中的最大任务数（已绑定 + 待调度）
	// 用于在 NodeOrderFn 中将桶评分归一化到 [0, MaxNodeScore] 范围
	bucketMaxSize int

	// nodeTaskSet 记录每个节点上已绑定的各任务类型数量
	// 结构: [节点名称] -> [任务类型名称] -> 数量
	// 用于在 calcBucketScore 中计算任务在节点上的亲和/反亲和评分
	nodeTaskSet map[string]map[string]int
}

// NewJobManager 为指定作业创建一个新的作业管理器
//
// 参数:
//   - jobID: 作业唯一标识
//
// 返回:
//   - 初始化完成的 JobManager 指针
func NewJobManager(jobID api.JobID) *JobManager {
	return &JobManager{
		jobID: jobID,

		buckets:     make([]*Bucket, 0),
		podInBucket: make(map[types.UID]int),
		podInTask:   make(map[types.UID]string),

		taskAffinityPriority: make(map[string]int),
		taskExistOrder:       make(map[string]int),
		interAffinity:        make(map[string]map[string]struct{}),
		interAntiAffinity:    make(map[string]map[string]struct{}),
		selfAffinity:         make(map[string]struct{}),
		selfAntiAffinity:     make(map[string]struct{}),

		bucketMaxSize: 0,
		nodeTaskSet:   make(map[string]map[string]int),
	}
}

// MarkOutOfBucket 将任务标记为不属于任何桶
// 当任务没有拓扑配置或无法识别任务类型时调用
//
// 参数:
//   - uid: Pod 的唯一标识
func (jm *JobManager) MarkOutOfBucket(uid types.UID) {
	jm.podInBucket[uid] = OutOfBucket
}

// MarkTaskHasTopology 标记任务类型具有拓扑配置，并记录其最高优先级
//
// 如果任务类型已存在更高优先级的拓扑配置，则不更新
// 这确保每个任务类型保留其最高优先级的拓扑类型
//
// 参数:
//   - taskName: 任务类型名称
//   - topoType: 拓扑类型（selfAntiAffinity/interAffinity/selfAffinity/interAntiAffinity）
func (jm *JobManager) MarkTaskHasTopology(taskName string, topoType topologyType) {
	priority := affinityPriority[topoType]
	if priority > jm.taskAffinityPriority[taskName] {
		jm.taskAffinityPriority[taskName] = priority
	}
}

// ============================================================================
// ApplyTaskTopology 将 TaskTopology 转换为内部矩阵表示
// ============================================================================
// 该方法将用户配置的亲和性/反亲和性分组列表转换为 JobManager 内部的
// 邻接矩阵表示，便于后续分桶和评分时快速查询。
//
// 转换示例:
//
//	输入 TaskTopology:
//	  Affinity:     [["ps", "worker"], ["ps", "chief"]]
//	  AntiAffinity: [["ps"], ["worker", "chief"]]
//	  TaskOrder:    ["ps", "worker", "chief", "evaluator"]
//
//	转换结果:
//	  interAffinity:
//	    ps     -> {worker, chief}
//	    worker -> {ps}
//	    chief  -> {ps}
//	  selfAffinity: (无，因为没有单元素亲和组)
//
//	  interAntiAffinity:
//	    worker -> {chief}
//	    chief  -> {worker}
//	  selfAntiAffinity: {ps}  (ps 自身反亲和)
//
//	  taskExistOrder: ps=4, worker=3, chief=2, evaluator=1
//	  taskAffinityPriority: ps=4(selfAntiAffinity), worker=3(interAffinity),
//	                        chief=3(interAffinity), evaluator=0(无拓扑配置)
//
// 规则:
//   - 单元素亲和组 ["evaluator"] → selfAffinity，表示该任务类型的副本应聚在一起
//   - 多元素亲和组 ["ps","worker"] → interAffinity，表示这些任务类型应聚在一起
//   - 单元素反亲和组 ["ps"] → selfAntiAffinity，表示该任务类型的副本应分散
//   - 多元素反亲和组 ["worker","chief"] → interAntiAffinity，表示这些任务类型应分散
// ============================================================================

// ApplyTaskTopology 将 TaskTopology 转换为内部矩阵表示形式
func (jm *JobManager) ApplyTaskTopology(topo *TaskTopology) {
	// ---- 处理亲和性配置 ----
	for _, aff := range topo.Affinity {
		if len(aff) == 1 {
			// 单元素亲和组: 自身亲和
			// 例如 ["worker"] 表示 worker 副本之间应尽量在同一节点
			taskName := aff[0]
			jm.selfAffinity[taskName] = struct{}{}
			jm.MarkTaskHasTopology(taskName, selfAffinity)
			continue
		}
		// 多元素亲和组: 交叉亲和
		// 例如 ["ps", "worker"] 表示 ps 和 worker 应尽量在同一节点
		// 建立双向关系: ps→worker 和 worker→ps
		for index, src := range aff {
			for _, dst := range aff[:index] {
				addAffinity(jm.interAffinity, src, dst)
				addAffinity(jm.interAffinity, dst, src)
			}
			jm.MarkTaskHasTopology(src, interAffinity)
		}
	}

	// ---- 处理反亲和性配置 ----
	for _, aff := range topo.AntiAffinity {
		if len(aff) == 1 {
			// 单元素反亲和组: 自身反亲和
			// 例如 ["ps"] 表示 ps 副本之间应分散到不同节点
			taskName := aff[0]
			jm.selfAntiAffinity[taskName] = struct{}{}
			jm.MarkTaskHasTopology(taskName, selfAntiAffinity)
			continue
		}
		// 多元素反亲和组: 交叉反亲和
		// 例如 ["worker", "chief"] 表示 worker 和 chief 应分散到不同节点
		// 建立双向关系: worker→chief 和 chief→worker
		for index, src := range aff {
			for _, dst := range aff[:index] {
				addAffinity(jm.interAntiAffinity, src, dst)
				addAffinity(jm.interAntiAffinity, dst, src)
			}
			jm.MarkTaskHasTopology(src, interAntiAffinity)
		}
	}

	// ---- 处理任务排序配置 ----
	// 将 TaskOrder 列表转换为权重映射，排在前面的任务类型权重更大
	// 例如 TaskOrder=["ps","worker","chief"] (length=3):
	//   ps=3, worker=2, chief=1
	// 权重越大表示调度优先级越高
	length := len(topo.TaskOrder)
	for index, taskName := range topo.TaskOrder {
		jm.taskExistOrder[taskName] = length - index
	}
}

// NewBucket 在 JobManager 中创建一个新桶
// 新桶的 index 为当前桶列表的长度，并追加到 buckets 切片中
//
// 返回:
//   - 新创建的桶指针
func (jm *JobManager) NewBucket() *Bucket {
	bucket := NewBucket()
	bucket.index = len(jm.buckets)
	jm.buckets = append(jm.buckets, bucket)
	return bucket
}

// AddTaskToBucket 将任务添加到指定索引的桶中
//
// 该方法同时更新:
//   - podInBucket: 记录 Pod 所属的桶索引
//   - 桶的 taskNameSet、tasks、资源请求等（通过 bucket.AddTask）
//   - bucketMaxSize: 如果该桶大小超过历史最大值则更新
//
// 参数:
//   - bucketIndex: 目标桶索引
//   - taskName:    任务类型名称
//   - task:        任务信息
func (jm *JobManager) AddTaskToBucket(bucketIndex int, taskName string, task *api.TaskInfo) {
	bucket := jm.buckets[bucketIndex]
	// 记录 Pod 所属的桶
	jm.podInBucket[task.Pod.UID] = bucketIndex
	// 将任务添加到桶中
	bucket.AddTask(taskName, task)
	// 更新最大桶大小（待调度 + 已绑定）
	if size := len(bucket.tasks) + bucket.boundTask; size > jm.bucketMaxSize {
		jm.bucketMaxSize = size
	}
}

// ============================================================================
// taskAffinityOrder 任务亲和性排序
// ============================================================================
// 比较两个任务的调度优先级，用于在同一桶内或分桶排序时确定任务的处理顺序。
//
// 排序规则（优先级从高到低）:
//  1. 用户自定义任务排序（taskExistOrder），值大的优先
//  2. 拓扑优先级（taskAffinityPriority），值大的优先
//  3. 如果两者都相同，则认为两个任务优先级相等
//
// 返回值:
//   -1: L 优先级低于 R
//    0: L 和 R 优先级相同
//    1: L 优先级高于 R
// ============================================================================

// taskAffinityOrder 比较两个任务的亲和性排序
// L 与 R 比较: -1 表示 L < R，0 表示 L == R，1 表示 L > R
func (jm *JobManager) taskAffinityOrder(L, R *api.TaskInfo) int {
	LTaskName := jm.podInTask[L.Pod.UID]
	RTaskName := jm.podInTask[R.Pod.UID]

	// 同一任务类型的任务优先级相同
	if LTaskName == RTaskName {
		return 0
	}

	// 1. 首先比较用户自定义的任务排序
	LOrder := jm.taskExistOrder[LTaskName]
	ROrder := jm.taskExistOrder[RTaskName]
	if LOrder != ROrder {
		if LOrder > ROrder {
			return 1
		}
		return -1
	}

	// 2. 其次比较拓扑优先级
	LPriority := jm.taskAffinityPriority[LTaskName]
	RPriority := jm.taskAffinityPriority[RTaskName]
	if LPriority != RPriority {
		if LPriority > RPriority {
			return 1
		}
		return -1
	}

	// 3. 所有亲和性配置相同，两个任务优先级相等
	return 0
}

// ============================================================================
// buildTaskInfo 构建待分桶任务列表
// ============================================================================
// 遍历作业的所有任务，筛选出需要参与分桶的任务:
//   - 无任务类型名称（taskName 为空）→ 标记为 OutOfBucket，不参与分桶
//   - 无拓扑配置（taskAffinityPriority 中不存在）→ 标记为 OutOfBucket，不参与分桶
//   - 有拓扑配置 → 记录 podInTask 映射，加入待分桶列表
//
// 参数:
//   - tasks: 作业的所有任务
//
// 返回:
//   - 待分桶的任务列表
// ============================================================================

// buildTaskInfo 筛选并返回需要参与分桶的任务列表
func (jm *JobManager) buildTaskInfo(tasks map[api.TaskID]*api.TaskInfo) []*api.TaskInfo {
	taskWithoutBucket := make([]*api.TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		pod := task.Pod

		// 获取任务类型名称
		taskName := getTaskName(task)
		if taskName == "" {
			// 无法识别任务类型，标记为桶外任务
			jm.MarkOutOfBucket(pod.UID)
			continue
		}
		if _, hasTopology := jm.taskAffinityPriority[taskName]; !hasTopology {
			// 任务类型没有拓扑配置，标记为桶外任务
			jm.MarkOutOfBucket(pod.UID)
			continue
		}

		// 记录 Pod 的任务类型名称
		jm.podInTask[pod.UID] = taskName
		taskWithoutBucket = append(taskWithoutBucket, task)
	}
	return taskWithoutBucket
}

// ============================================================================
// checkTaskSetAffinity 计算任务与桶内任务集合的亲和性评分
// ============================================================================
// 遍历桶内所有任务类型，计算指定任务类型与它们的亲和/反亲和总分。
//
// 评分规则:
//   - 亲和关系（self/inter）: 每个匹配的任务 +count 分
//   - 反亲和关系（self/inter）: 每个匹配的任务 -count 分
//   - count 为桶内该任务类型的数量
//
// 参数:
//   - taskName:     待评估的任务类型名称
//   - taskNameSet:  桶内任务类型集合（类型名 -> 数量）
//   - onlyAnti:     是否只计算反亲和评分（在 calcBucketScore 中用于惩罚）
//
// 返回:
//   - 亲和性评分（正数表示亲和，负数表示反亲和）
//
// 示例:
//   桶内: {ps: 2, worker: 1}
//   评估任务: worker
//   配置: ps 与 worker 亲和, worker 自身反亲和
//   结果: ps亲和(+2) + worker自身反亲和(-1) = +1
// ============================================================================

// checkTaskSetAffinity 计算任务类型与给定任务集合的亲和性评分
func (jm *JobManager) checkTaskSetAffinity(taskName string, taskNameSet map[string]int, onlyAnti bool) int {
	bucketPodAff := 0

	// 无任务类型名称，返回0分
	if taskName == "" {
		return bucketPodAff
	}

	for taskNameInBucket, count := range taskNameSet {
		// 判断是否为同一任务类型
		theSameTask := taskNameInBucket == taskName

		// 计算亲和性评分（仅在 onlyAnti=false 时计算）
		if !onlyAnti {
			affinity := false
			if theSameTask {
				// 同一任务类型: 检查自身亲和
				_, affinity = jm.selfAffinity[taskName]
			} else {
				// 不同任务类型: 检查交叉亲和
				_, affinity = jm.interAffinity[taskName][taskNameInBucket]
			}
			if affinity {
				// 亲和: 加分（按桶内该类型任务数量）
				bucketPodAff += count
			}
		}

		// 计算反亲和性评分（始终计算）
		antiAffinity := false
		if theSameTask {
			// 同一任务类型: 检查自身反亲和
			_, antiAffinity = jm.selfAntiAffinity[taskName]
		} else {
			// 不同任务类型: 检查交叉反亲和
			_, antiAffinity = jm.interAntiAffinity[taskName][taskNameInBucket]
		}
		if antiAffinity {
			// 反亲和: 减分（按桶内该类型任务数量）
			bucketPodAff -= count
		}
	}

	return bucketPodAff
}

// ============================================================================
// buildBucket 分桶核心算法
// ============================================================================
// 将排序后的任务逐一分配到最合适的桶中。
//
// 分桶策略:
//  1. 已绑定节点的任务:
//     - 优先放入该节点对应的桶（通过 nodeBucketMapping 缓存）
//     - 如果该节点还没有对应桶，则新建一个桶
//  2. 未绑定节点的任务:
//     - 遍历所有现有桶，计算任务与每个桶的亲和性评分（checkTaskSetAffinity）
//     - 选择亲和性评分最高的桶
//     - 如果评分相同，选择资源评分（reqScore）更小的桶（负载均衡）
//  3. 如果所有桶的亲和性评分都为负（说明该任务与所有桶都反亲和），
//     或没有可用桶，则新建一个桶
//
// 参数:
//   - taskWithOrder: 已排序的待分桶任务列表
// ============================================================================

// buildBucket 将排序后的任务分配到桶中
func (jm *JobManager) buildBucket(taskWithOrder []*api.TaskInfo) {
	// nodeBucketMapping 缓存节点名到桶的映射，用于已绑定任务快速找到对应桶
	nodeBucketMapping := make(map[string]*Bucket)

	for _, task := range taskWithOrder {
		klog.V(5).Infof("jobID %s task with order task %s/%s", jm.jobID, task.Namespace, task.Name)

		var selectedBucket *Bucket
		// 初始化为最小整数，确保任何亲和性评分都能被选中
		maxAffinity := math.MinInt32

		taskName := getTaskName(task)

		if task.NodeName != "" {
			// 已绑定节点的任务: 直接使用该节点对应的桶
			// 这样可以将同节点的已绑定任务归入同一桶，
			// 为后续未绑定任务的分桶提供参考基准
			maxAffinity = 0
			selectedBucket = nodeBucketMapping[task.NodeName]
		} else {
			// 未绑定节点的任务: 遍历所有桶寻找最佳匹配
			for _, bucket := range jm.buckets {
				// 计算任务与该桶的亲和性评分
				bucketPodAff := jm.checkTaskSetAffinity(taskName, bucket.taskNameSet, false)

				// 选择亲和性评分最高的桶
				if bucketPodAff > maxAffinity {
					maxAffinity = bucketPodAff
					selectedBucket = bucket
				} else if bucketPodAff == maxAffinity && selectedBucket != nil &&
					bucket.reqScore < selectedBucket.reqScore {
					// 亲和性评分相同，选择资源评分更小的桶（负载均衡）
					selectedBucket = bucket
				}
			}
		}

		// 如果与所有桶都反亲和（maxAffinity < 0），或没有可用桶，则新建桶
		if maxAffinity < 0 || selectedBucket == nil {
			selectedBucket = jm.NewBucket()
			if task.NodeName != "" {
				// 记录节点到新桶的映射
				nodeBucketMapping[task.NodeName] = selectedBucket
			}
		}

		// 将任务添加到选中的桶中
		jm.AddTaskToBucket(selectedBucket.index, taskName, task)
	}
}

// ============================================================================
// ConstructBucket 分桶入口方法
// ============================================================================
// 该方法是 JobManager 分桶流程的入口，由 initBucket 在 OnSessionOpen 阶段调用。
//
// 流程:
//  1. buildTaskInfo: 筛选有拓扑配置的任务，过滤掉无拓扑配置的任务
//  2. 使用 TaskOrder 对任务进行排序（已绑定的任务优先，其次按拓扑优先级和用户排序）
//  3. buildBucket: 将排序后的任务逐一分配到最合适的桶中
//
// 参数:
//   - tasks: 作业的所有任务
// ============================================================================

// ConstructBucket 为作业的任务构建桶
func (jm *JobManager) ConstructBucket(tasks map[api.TaskID]*api.TaskInfo) {
	// 1. 筛选有拓扑配置的任务
	taskWithoutBucket := jm.buildTaskInfo(tasks)

	// 2. 使用 TaskOrder 排序器对任务排序
	o := TaskOrder{
		tasks: taskWithoutBucket,

		manager: jm,
	}
	// sort.Reverse 使高优先级任务排在前面
	sort.Sort(sort.Reverse(&o))

	// 3. 将排序后的任务分配到桶中
	jm.buildBucket(o.tasks)
}

// ============================================================================
// TaskBound 任务绑定回调
// ============================================================================
// 当任务被调度器成功分配到节点后，由 AllocateFunc 回调调用。
// 更新两个数据结构:
//  1. nodeTaskSet: 记录节点上各任务类型的数量（用于后续 calcBucketScore）
//  2. Bucket.TaskBound: 更新桶内任务状态
//
// 参数:
//   - task: 已被绑定到节点的任务信息
// ============================================================================

// TaskBound 处理任务绑定到节点后的状态更新
func (jm *JobManager) TaskBound(task *api.TaskInfo) {
	// 1. 更新 nodeTaskSet，记录节点上的任务类型分布
	if taskName := getTaskName(task); taskName != "" {
		set, ok := jm.nodeTaskSet[task.NodeName]
		if !ok {
			set = make(map[string]int)
			jm.nodeTaskSet[task.NodeName] = set
		}
		set[taskName]++
	}

	// 2. 更新桶内任务状态
	bucket := jm.GetBucket(task)
	if bucket != nil {
		bucket.TaskBound(task)
	}
}

// ============================================================================
// GetBucket 桶查询
// ============================================================================
// 根据 Pod UID 查询其所属的桶。
//
// 返回:
//   - 如果 Pod 属于某个桶，返回该桶指针
//   - 如果 Pod 不属于任何桶（OutOfBucket 或未记录），返回 nil
// ============================================================================

// GetBucket 获取任务所属的桶
func (jm *JobManager) GetBucket(task *api.TaskInfo) *Bucket {
	index, ok := jm.podInBucket[task.Pod.UID]
	if !ok || index == OutOfBucket {
		return nil
	}

	bucket := jm.buckets[index]
	return bucket
}

// ============================================================================
// String 调试输出
// ============================================================================
// 生成 JobManager 的可读字符串，用于 klog 调试日志。
// 输出格式包含:
//   - 插件名、作业ID、最大桶大小
//   - 各拓扑配置（saa/iaa/sa/ia 分别对应四种拓扑类型）
//   - 优先级和排序配置
//   - 每个桶的详细信息（待调度任务名 + 已绑定节点分布）
// ============================================================================

// String 返回 JobManager 的可读字符串表示
func (jm *JobManager) String() string {
	// saa: selfAntiAffinity（自身反亲和）
	// iaa: interAntiAffinity（交叉反亲和）
	// sa:  selfAffinity（自身亲和）
	// ia:  interAffinity（交叉亲和）
	msg := []string{
		fmt.Sprintf("%s - job %s max %d || saa: %v - iaa: %v - sa: %v - ia: %v || priority: %v - order: %v || ",
			PluginName, jm.jobID, jm.bucketMaxSize,
			jm.selfAntiAffinity, jm.interAntiAffinity,
			jm.selfAffinity, jm.interAffinity,
			jm.taskAffinityPriority, jm.taskExistOrder,
		),
	}

	// 输出每个桶的详细信息
	for _, bucket := range jm.buckets {
		bucketMsg := fmt.Sprintf("b:%d -- ", bucket.index)

		// 桶内待调度任务名称
		var info []string
		for _, task := range bucket.tasks {
			info = append(info, task.Pod.Name)
		}
		bucketMsg += strings.Join(info, ", ")
		bucketMsg += "|"

		// 桶内已绑定任务的节点分布
		info = nil
		for nodeName, count := range bucket.node {
			info = append(info, fmt.Sprintf("n%s-%d", nodeName, count))
		}
		bucketMsg += strings.Join(info, ", ")

		msg = append(msg, "["+bucketMsg+"]")
	}
	return strings.Join(msg, " ")
}
