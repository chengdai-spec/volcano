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
	"strings"
	"time"

	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

// ============================================================================
// task-topology 插件主结构
// ============================================================================
// taskTopologyPlugin 是 Volcano 调度器的 task-topology 插件实现。
//
// 插件功能概述:
//   task-topology 插件通过分析用户在 PodGroup annotations 中定义的
//   亲和性（affinity）、反亲和性（anti-affinity）和任务排序（taskOrder），
//   将同一作业中的任务划分为若干"桶"（Bucket），并基于桶信息实现:
//     1. TaskOrderFn: 调整任务调度顺序，使有拓扑关系的任务优先调度
//     2. NodeOrderFn: 调整节点评分，使具有亲和关系的任务尽量调度到同一节点
//
// 插件生命周期:
//   - OnSessionOpen: 初始化桶(读取拓扑配置、构建桶、注册回调函数)
//   - 调度循环:     TaskOrderFn 和 NodeOrderFn 被反复调用
//   - OnSessionClose: 清理资源
// ============================================================================

// taskTopologyPlugin 是 task-topology 插件的主结构体
type taskTopologyPlugin struct {
	// arguments 保存调度器配置中传入的插件参数
	arguments framework.Arguments

	// weight 是插件在节点评分中的权重因子
	// 通过调度器配置的 task-topology.weight 参数设置，默认为 1
	// 在 NodeOrderFn 中，最终评分 = 桶评分 * weight * MaxNodeScore / bucketMaxSize
	weight int

	// managers 保存每个作业的 JobManager
	// 键为 JobID，值为该作业的拓扑管理器
	// 在 OnSessionOpen 阶段初始化，在 OnSessionClose 阶段清空
	managers map[api.JobID]*JobManager
}

// New 是 task-topology 插件的工厂函数
// 由调度器框架在加载插件时调用，创建插件实例
//
// 参数:
//   - arguments: 调度器配置中该插件的参数
//
// 返回:
//   - 实现了 framework.Plugin 接口的插件实例
func New(arguments framework.Arguments) framework.Plugin {
	return &taskTopologyPlugin{
		arguments: arguments,

		weight:   calculateWeight(arguments),
		managers: make(map[api.JobID]*JobManager),
	}
}

// Name 返回插件名称
func (p *taskTopologyPlugin) Name() string {
	return PluginName
}

// ============================================================================
// TaskOrderFn 任务排序函数
// ============================================================================
// 该函数决定同一作业内两个任务的调度先后顺序。
// 排序规则（从高到低优先级）:
//
//  1. 在桶内的任务优先于桶外任务
//     - 桶内任务有拓扑配置，应优先调度以建立亲和关系
//     - 桶外任务无拓扑配置，优先级较低
//
//  2. 不同作业的任务不比较（返回0，交由其他插件/框架决定）
//
//  3. 桶外任务之间不排序（返回0）
//
//  4. 桶大的优先于桶小的
//     - 桶内任务数更多的桶优先调度，尽快完成大桶的调度
//
//  5. 同一桶内的任务按亲和性顺序排序
//     - 调用 taskAffinityOrder 比较拓扑优先级
//
//  6. 不同桶之间，老桶（index小）优先于新桶（index大）
//
// 示例:
//
//	作业A有两个桶:
//	  bucket1: {a1, a3}    (2个任务)
//	  bucket2: {a2}        (1个任务)
//	  桶外: {a4}
//
//	作业B有一个桶:
//	  bucket1: {b1, b2}    (2个任务)
//	  桶外: {b3}
//
//	正确的任务调度顺序:
//	  a1 a3 a2 b1 b2 a4 b3
//	（桶内任务先于桶外，大桶先于小桶，老桶先于新桶）
// ============================================================================

// TaskOrderFn 返回 -1 使 l 排在 r 之前
//
// 例如:
// A:
//
//	| bucket1   | bucket2   | out of bucket
//	| a1 a3     | a2        | a4
//
// B:
//
//	| bucket1   | out of bucket
//	| b1 b2     | b3
//
// 正确的任务顺序应为:
//
//	a1 a3 a2 b1 b2 a4 b3
func (p *taskTopologyPlugin) TaskOrderFn(l interface{}, r interface{}) int {
	lv, ok := l.(*api.TaskInfo)
	if !ok {
		klog.Errorf("Object is not a taskinfo")
		return 0
	}
	rv, ok := r.(*api.TaskInfo)
	if !ok {
		klog.Errorf("Object is not a taskinfo")
		return 0
	}

	// 获取两个任务各自所属作业的 JobManager
	lvJobManager := p.managers[lv.Job]
	rvJobManager := p.managers[rv.Job]
	if lvJobManager == nil {
		klog.V(4).Infof("No job manager for job <ID: %s>, do not return task order.", lv.Job)
		return 0
	}
	if rvJobManager == nil {
		klog.V(4).Infof("No job manager for job <ID: %s>, do not return task order.", rv.Job)
		return 0
	}

	// 获取两个任务所属的桶
	lvBucket := lvJobManager.GetBucket(lv)
	rvBucket := rvJobManager.GetBucket(rv)

	// 规则1: 在桶内的任务优先于桶外任务
	lvInBucket := lvBucket != nil
	rvInBucket := rvBucket != nil
	if lvInBucket != rvInBucket {
		if lvInBucket {
			// lv 在桶内，rv 在桶外，lv 优先
			return -1
		}
		// rv 在桶内，lv 在桶外，rv 优先
		return 1
	}

	// 规则2: 不同作业的任务不比较（交由其他插件/框架处理）
	if lv.Job != rv.Job {
		return 0
	}

	// 规则3: 桶外任务之间不排序
	if !lvInBucket && !rvInBucket {
		return 0
	}

	// 规则4: 桶大的优先于桶小的（更快完成大桶调度）
	lvHasTask := len(lvBucket.tasks)
	rvHasTask := len(rvBucket.tasks)
	if lvHasTask != rvHasTask {
		if lvHasTask > rvHasTask {
			return -1
		}
		return 1
	}

	// 获取桶索引用于比较
	lvBucketIndex := lvBucket.index
	rvBucketIndex := rvBucket.index

	// 规则5: 同一桶内，按亲和性顺序排序
	if lvBucketIndex == rvBucketIndex {
		affinityOrder := lvJobManager.taskAffinityOrder(lv, rv)
		// 注意: 返回 -affinityOrder 是因为 taskAffinityOrder 返回1表示L优先级高，
		// 而此处需要返回-1表示L排在前面
		return -affinityOrder
	}

	// 规则6: 不同桶之间，老桶（index小）优先于新桶（index大）
	if lvBucketIndex < rvBucketIndex {
		return -1
	}
	return 1
}

// ============================================================================
// calcBucketScore 计算桶评分
// ============================================================================
// 计算指定任务在指定节点上的桶评分。该评分用于 NodeOrderFn 中调整节点排序。
//
// 评分计算逻辑:
//
//  1. 基础评分 = bucket.node[node.Name]
//     该节点上已绑定的桶内任务数量。已有任务越多，评分越高，
//     因为将新任务调度到该节点可以增强亲和关系。
//
//  2. 反亲和惩罚
//     如果节点上已绑定的任务与当前任务存在反亲和关系，则减分。
//     这使得反亲和任务尽量分散到不同节点。
//
//  3. 桶内其他任务加成
//     评分 += len(bucket.tasks)，表示将当前任务调度到该节点后，
//     桶内其他待调度任务也可能被调度到该节点。
//
//  4. 资源适配调整
//     如果桶的总资源请求超过节点可用资源，则逐步移除桶内其他任务，
//     每移除一个任务评分-1，直到桶的剩余请求能放入节点。
//     这确保评分反映节点实际能容纳的桶内任务数。
//
// 参数:
//   - task: 待调度的任务
//   - node: 候选节点
//
// 返回:
//   - score: 桶评分
//   - jobManager: 任务所属的 JobManager(可能为nil)
//   - error: 错误信息
// ============================================================================

// calcBucketScore 计算任务在节点上的桶评分
//
// 该函数是 NodeOrderFn 的核心，用于评估"把某个任务调度到某个节点"的优劣。
// 评分越高，说明该节点越适合放这个任务（从拓扑亲和性角度）。
//
// 评分由四个部分组成:
//   1. 基础评分: 该节点上已有多少同桶任务(越多越好，说明亲和关系强)
//   2. 反亲和惩罚: 节点上已有任务与当前任务反亲和时扣分
//   3. 桶内加成: 桶内待调度任务数量(假设它们也能跟来)
//   4. 资源适配调整: 如果节点放不下整个桶，逐步扣分直到能放下
func (p *taskTopologyPlugin) calcBucketScore(task *api.TaskInfo, node *api.NodeInfo) (int, *JobManager, error) {
	// ================================================================
	// 前置检查 1: 节点资源能否容纳当前任务本身
	// ================================================================
	// maxResource = 空闲资源 + 释放中资源（即将回收的资源）
	// 例如: node-A.Idle=4CPU/8Gi, Releasing=0 → maxResource=4CPU/8Gi
	// 如果 ps1 请求 1CPU/2Gi，4CPU/8Gi >= 1CPU/2Gi → 通过
	maxResource := node.Idle.Clone().Add(node.Releasing)
	if req := task.Resreq; req != nil && maxResource.LessPartly(req, api.Zero) {
		// 节点连当前这一个任务都放不下，直接返回0分
		// 例如: node-C 只剩 0.5CPU，ps1 需要 1CPU → 0分
		return 0, nil, nil
	}

	// ================================================================
	// 前置检查 2: 获取任务的 JobManager 和 Bucket
	// ================================================================
	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		// 该作业没有拓扑配置，不参与评分
		return 0, nil, nil
	}

	bucket := jobManager.GetBucket(task)
	if bucket == nil {
		// 该任务是桶外任务（无拓扑配置或被 MarkOutOfBucket），返回0分
		// 桶外任务不受 task-topology 插件影响
		return 0, jobManager, nil
	}

	// ================================================================
	// 评分第 1 部分: 基础评分
	// ================================================================
	// bucket.node[node.Name] 表示该节点上已绑定的桶内任务数量
	// 例如: ps1 属于 Bucket1，Bucket1.node["node-A"]=0 → 基础分=0
	//       如果 ps0 属于 Bucket0，Bucket0.node["node-A"]=2 → 基础分=2
	// 含义: 节点上已有越多同桶任务，亲和关系越强，评分越高
	score := bucket.node[node.Name]

	// ================================================================
	// 评分第 2 部分: 反亲和惩罚
	// ================================================================
	// nodeTaskSet 记录了该节点上所有作业的所有任务类型分布
	// 例如: node-A 上有 {ps:1, worker:1}
	//       checkTaskSetAffinity("ps", {ps:1, worker:1}, onlyAnti=true)
	//       → ps 自身反亲和 → -1
	//       → score += (-1) → score = 0 + (-1) = -1
	// 含义: 节点上有与当前任务反亲和的任务时扣分，促使任务分散
	if nodeTaskSet := jobManager.nodeTaskSet[node.Name]; nodeTaskSet != nil {
		taskName := getTaskName(task)
		// onlyAnti=true: 只计算反亲和评分（负分），不计算亲和加分
		// 因为亲和加分已经通过 bucket.node 在第1部分体现了
		affinityScore := jobManager.checkTaskSetAffinity(taskName, nodeTaskSet, true)
		if affinityScore < 0 {
			// 反亲和惩罚只有负分才生效（正分说明无反亲和，不需要调整）
			score += affinityScore
		}
	}
	klog.V(4).Infof("task %s/%s, node %s, additional score %d, task %d",
		task.Namespace, task.Name, node.Name, score, len(bucket.tasks))

	// ================================================================
	// 评分第 3 部分: 桶内待调度任务加成
	// ================================================================
	// len(bucket.tasks) 是桶内尚未绑定的待调度任务数量（含当前任务）
	// 例如: Bucket1 有 {ps1, worker1} → len=2 → score += 2
	// 含义: 桶内还有这么多任务可能被调度到这个节点，
	//       桶越大、潜在收益越高
	score += len(bucket.tasks)

	// ================================================================
	// 评分第 4 部分: 资源适配调整
	// ================================================================
	// 检查节点能否容纳整个桶的资源请求
	// 例如: bucket.request = 3CPU/6Gi (ps1 + worker1)
	//       maxResource(node-A) = 4CPU/8Gi → 3CPU/6Gi <= 4CPU/8Gi → 直接返回
	//       maxResource(node-A) = 2CPU/4Gi → 3CPU/6Gi > 2CPU/4Gi → 需要削减
	if bucket.request == nil || bucket.request.LessEqual(maxResource, api.Zero) {
		// 桶的总请求能放入节点，无需削减，直接返回
		// 例如: node-A 4CPU/8Gi >= 3CPU/6Gi → return score=1, jobManager, nil
		return score, jobManager, nil
	}

	// 桶的总请求超过节点可用资源，需要逐步"放弃"桶内其他任务
	// 每放弃一个任务，评分-1，直到剩余请求能放入节点
	// 例如: node-A 只剩 2CPU/4Gi，bucket.request=3CPU/6Gi
	//       遍历 bucket.tasks:
	//         - 遇到 worker1 (2CPU/4Gi): remains -= 2CPU/4Gi → remains=1CPU/2Gi, score-- → 0
	//           检查 1CPU/2Gi <= 2CPU/4Gi → 满足，break
	//       最终 score=0，含义: node-A 只能放 ps1 自己，worker1 放不下
	remains := bucket.request.Clone()
	// 注意: map 遍历顺序是随机的，因此削减哪些任务不完全确定
	// 但最终保证 remains 能放入节点
	for bucketTaskID, bucketTask := range bucket.tasks {
		// 当前任务不能被移除（它一定放在这个节点上）
		if bucketTaskID == task.Pod.UID || bucketTask.Resreq == nil {
			continue
		}
		// 从剩余请求中减去该任务的资源
		remains.Sub(bucketTask.Resreq)
		// 评分-1: 桶内少一个任务能跟来
		score--
		// 检查剩余请求是否能放入节点
		if remains.LessEqual(maxResource, api.Zero) {
			break
		}
	}
	// 此时桶的剩余请求一定能放入节点（至少当前任务本身能放下，前面已校验）
	return score, jobManager, nil
}

// ============================================================================
// NodeOrderFn 节点评分函数
// ============================================================================
// 基于桶评分计算节点在该任务上的得分。分数越高，节点越优先被选中。
//
// 评分计算公式:
//   fScore = score * weight * MaxNodeScore / bucketMaxSize
//
// 其中:
//   - score: calcBucketScore 返回的原始桶评分
//   - weight: 插件权重（默认1，可通过 task-topology.weight 配置）
//   - MaxNodeScore: kube-scheduler 的最大节点评分（通常为100）
//   - bucketMaxSize: 该作业最大桶的任务数，用于归一化
//
// 归一化的目的:
//   使不同作业、不同桶的评分具有可比性，最终评分落在 [0, MaxNodeScore] 范围内
//
// 参数:
//   - task: 待调度任务
//   - node: 候选节点
//
// 返回:
//   - 节点评分（float64）
//   - 错误信息
// ============================================================================

// NodeOrderFn 基于桶评分计算节点得分
func (p *taskTopologyPlugin) NodeOrderFn(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
	// 计算原始桶评分
	score, jobManager, err := p.calcBucketScore(task, node)
	if err != nil {
		return 0, err
	}
	// 应用权重
	fScore := float64(score * p.weight)
	// 归一化到 [0, MaxNodeScore] 范围
	if jobManager != nil && jobManager.bucketMaxSize != 0 {
		fScore = fScore * float64(fwk.MaxNodeScore) / float64(jobManager.bucketMaxSize)
	}
	klog.V(4).Infof("task %s/%s at node %s has bucket score %d, score %f",
		task.Namespace, task.Name, node.Name, score, fScore)
	return fScore, nil
}

// ============================================================================
// AllocateFunc 任务分配回调
// ============================================================================
// 当任务被调度器成功分配到节点后调用，更新 JobManager 的状态。
// 主要调用 JobManager.TaskBound 来:
//   1. 更新 nodeTaskSet（记录节点上的任务类型分布）
//   2. 更新桶内任务状态（从待调度列表移到已绑定）
// ============================================================================

// AllocateFunc 是任务分配事件回调
func (p *taskTopologyPlugin) AllocateFunc(event *framework.Event) {
	task := event.Task

	// 获取任务所属作业的 JobManager
	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		return
	}
	// 更新桶和节点任务分布状态
	jobManager.TaskBound(task)
}

// ============================================================================
// initBucket 桶初始化
// ============================================================================
// 在 OnSessionOpen 阶段遍历所有作业，为有拓扑配置的作业创建 JobManager
// 并构建桶。
//
// 流程:
//   1. 遍历调度会话中的所有作业
//   2. 跳过没有待调度任务的作业
//   3. 从 PodGroup annotations 读取拓扑配置
//   4. 创建 JobManager 并应用拓扑配置
//   5. 构建桶(将任务按亲和性分配到桶中)
//   6. 将 JobManager 保存到插件实例的 managers map 中
// ============================================================================

// initBucket 初始化所有作业的桶
func (p *taskTopologyPlugin) initBucket(ssn *framework.Session) {
	for jobID, job := range ssn.Jobs {
		// 跳过没有待调度任务的作业
		if !job.HasPendingTasks() {
			klog.V(4).Infof("No pending tasks in job <%s/%s> by plugin %s.",
				job.Namespace, job.Name, PluginName)
			continue
		}

		// 从 PodGroup annotations 读取拓扑配置
		/*
			annotations:
			  volcano.sh/task-topology-affinity: "ps,worker;ps,chief"
			  volcano.sh/task-topology-anti-affinity: "ps;worker,chief"
			  volcano.sh/task-topology-task-order: "ps,worker,chief,evaluator"
		*/
		jobTopology, err := readTopologyFromPgAnnotations(job)
		if err != nil {
			klog.V(4).Infof("Failed to read task topology from job <%s/%s> annotations, error: %s.",
				job.Namespace, job.Name, err.Error())
			continue
		}
		// 没有拓扑配置的作业跳过
		if jobTopology == nil {
			continue
		}

		// 创建 JobManager 并构建桶
		manager := NewJobManager(jobID)
		manager.ApplyTaskTopology(jobTopology)
		manager.ConstructBucket(job.Tasks)

		// 保存到插件实例
		p.managers[job.UID] = manager
	}
}

// ============================================================================
// affinityCheck 亲和性配置校验
// ============================================================================
// 校验亲和性配置中引用的任务类型是否存在于作业中，以及是否有重复。
//
// 校验规则:
//   1. 作业和亲和性配置不能为空
//   2. 收集作业中所有存在的任务类型
//   3. 遍历亲和性配置中的每个任务类型名称:
//      - 跳过空字符串（处理冗余分隔符的情况）
//      - 检查任务类型是否存在于作业中
//      - 检查同一亲和组内是否有重复的任务类型
//
// 参数:
//   - job:      作业信息
//   - affinity: 亲和性分组配置（二维数组）
//
// 返回:
//   - 错误信息，nil表示校验通过
// ============================================================================

// affinityCheck 校验亲和性配置的合法性
func affinityCheck(job *api.JobInfo, affinity [][]string) error {
	if job == nil || affinity == nil {
		return fmt.Errorf("empty input, job: %v, affinity: %v", job, affinity)
	}

	// 收集作业中所有存在的任务类型
	var taskNumber = len(job.Tasks)
	var taskRef = make(map[string]bool, taskNumber)
	for _, task := range job.Tasks {
		if _, exist := taskRef[task.TaskRole]; !exist {
			taskRef[task.TaskRole] = true
		}
	}

	// 校验亲和性配置中的每个任务类型
	for _, aff := range affinity {
		affTasks := make(map[string]bool, len(aff))
		for _, task := range aff {
			// 跳过空字符串（处理 "ps,,worker" 这样的冗余分隔符）
			if len(task) == 0 {
				continue
			}
			// 检查任务类型是否存在于作业中
			if _, exist := taskRef[task]; !exist {
				return fmt.Errorf("task %s do not exist in job <%s/%s>", task, job.Namespace, job.Name)
			}
			// 检查同一亲和组内是否有重复
			if _, exist := affTasks[task]; exist {
				return fmt.Errorf("task %s is duplicate in job <%s/%s>", task, job.Namespace, job.Name)
			}
			affTasks[task] = true
		}
	}
	return nil
}

// ============================================================================
// splitAnnotations 解析 annotation 字符串
// ============================================================================
// 将 annotation 字符串解析为二维字符串数组。
//
// 解析格式:
//   - 分号 ";" 分隔不同的亲和组
//   - 逗号 "," 分隔同一组内的不同任务类型
//
// 示例:
//   输入: "ps,worker;ps,chief"
//   输出: [["ps", "worker"], ["ps", "chief"]]
//
// 参数:
//   - job:       作业信息（用于校验）
//   - annotation: 原始 annotation 字符串
//
// 返回:
//   - 解析后的二维字符串数组
//   - 错误信息
// ============================================================================

// splitAnnotations 将 annotation 字符串解析为二维任务类型数组
func splitAnnotations(job *api.JobInfo, annotation string) ([][]string, error) {
	// 按分号分隔不同的亲和组
	affinityStr := strings.Split(annotation, ";")
	if len(affinityStr) == 0 {
		return nil, nil
	}
	// 按逗号分隔每组内的任务类型
	var affinity = make([][]string, len(affinityStr))
	for i, str := range affinityStr {
		affinity[i] = strings.Split(str, ",")
	}
	// 校验解析结果
	if err := affinityCheck(job, affinity); err != nil {
		klog.V(4).Infof("Job <%s/%s> affinity key invalid: %s.",
			job.Namespace, job.Name, err.Error())
		return nil, err
	}
	return affinity, nil
}

// ============================================================================
// readTopologyFromPgAnnotations 从 PodGroup annotations 读取拓扑配置
// ============================================================================
// 从 PodGroup 的 annotations 中读取三类拓扑配置:
//   - 亲和性 (volcano.sh/task-topology-affinity)
//   - 反亲和性 (volcano.sh/task-topology-anti-affinity)
//   - 任务排序 (volcano.sh/task-topology-task-order)
//
// 如果三个 annotation 都不存在，返回 nil（表示该作业没有拓扑配置）。
// 如果任一 annotation 解析失败，返回错误。
//
// 参数:
//   - job: 作业信息
//
// 返回:
//   - TaskTopology 指针（包含解析后的拓扑配置）
//   - 错误信息
//
// 配置示例:
//   annotations:
//     volcano.sh/task-topology-affinity: "ps,worker;ps,chief"
//     volcano.sh/task-topology-anti-affinity: "ps;worker,chief"
//     volcano.sh/task-topology-task-order: "ps,worker,chief,evaluator"
// ============================================================================

// readTopologyFromPgAnnotations 从 PodGroup annotations 读取拓扑配置
func readTopologyFromPgAnnotations(job *api.JobInfo) (*TaskTopology, error) {
	// 读取三个 annotation
	jobAffinityStr, affinityExist := job.PodGroup.Annotations[JobAffinityAnnotations]
	jobAntiAffinityStr, antiAffinityExist := job.PodGroup.Annotations[JobAntiAffinityAnnotations]
	taskOrderStr, taskOrderExist := job.PodGroup.Annotations[TaskOrderAnnotations]

	// 如果三个 annotation 都不存在，返回 nil
	if !(affinityExist || antiAffinityExist || taskOrderExist) {
		return nil, nil
	}

	// 初始化 TaskTopology
	var jobTopology = TaskTopology{
		Affinity:     nil,
		AntiAffinity: nil,
		TaskOrder:    nil,
	}

	// 解析亲和性配置
	if affinityExist {
		affinities, err := splitAnnotations(job, jobAffinityStr)
		if err != nil {
			klog.V(4).Infof("Job <%s/%s> affinity key invalid: %s.",
				job.Namespace, job.Name, err.Error())
			return nil, err
		}
		jobTopology.Affinity = affinities
	}

	// 解析反亲和性配置
	if antiAffinityExist {
		affinities, err := splitAnnotations(job, jobAntiAffinityStr)
		if err != nil {
			klog.V(4).Infof("Job <%s/%s> anti affinity key invalid: %s.",
				job.Namespace, job.Name, err.Error())
			return nil, err
		}
		jobTopology.AntiAffinity = affinities
	}

	// 解析任务排序配置
	if taskOrderExist {
		// 任务排序按逗号分隔
		jobTopology.TaskOrder = strings.Split(taskOrderStr, ",")
		// 校验任务排序中引用的任务类型是否合法
		if err := affinityCheck(job, [][]string{jobTopology.TaskOrder}); err != nil {
			klog.V(4).Infof("Job <%s/%s> task order key invalid: %s.",
				job.Namespace, job.Name, err.Error())
			return nil, err
		}
	}

	return &jobTopology, nil
}

// ============================================================================
// OnSessionOpen / OnSessionClose 会话生命周期回调
// ============================================================================

// OnSessionOpen 在调度会话开始时调用
// 负责初始化插件的桶数据结构，并注册回调函数
//
// 流程:
//  1. 记录开始时间
//  2. 调用 initBucket 初始化所有作业的桶
//  3. 注册 TaskOrderFn(任务排序回调)
//  4. 注册 NodeOrderFn(节点评分回调)
//  5. 注册 AllocateFunc(任务分配事件回调)
//  6. 记录耗时
func (p *taskTopologyPlugin) OnSessionOpen(ssn *framework.Session) {
	start := time.Now()
	klog.V(3).Infof("start to init task topology plugin, weight[%d], defined order %v", p.weight, affinityPriority)

	// 初始化桶
	p.initBucket(ssn)

	// 注册任务排序函数
	ssn.AddTaskOrderFn(p.Name(), p.TaskOrderFn)

	// 注册节点评分函数
	ssn.AddNodeOrderFn(p.Name(), p.NodeOrderFn)

	// 注册事件处理器（任务分配回调）
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: p.AllocateFunc,
	})

	klog.V(3).Infof("finished to init task topology plugin, using time %v", time.Since(start))
}

// OnSessionClose 在调度会话结束时调用
// 清理所有作业管理器，释放内存
func (p *taskTopologyPlugin) OnSessionClose(ssn *framework.Session) {
	p.managers = nil
}
