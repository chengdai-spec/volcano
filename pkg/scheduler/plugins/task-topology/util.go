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
	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

// ============================================================================
// task-topology 插件常量定义
// ============================================================================
// 本文件定义了 task-topology 插件所需的所有常量、基础数据结构以及辅助工具函数。
// task-topology 插件的核心目标是：根据用户在 PodGroup annotations 中配置的
// 亲和性（affinity）、反亲和性（anti-affinity）和任务排序（taskOrder），
// 将同一作业中的任务划分为若干"桶"（Bucket），使得具有亲和关系的任务
// 尽量调度到同一节点，具有反亲和关系的任务尽量分散到不同节点。
// ============================================================================

const (
	// PluginName 表示 task-topology 调度插件的名称
	PluginName = "task-topology"

	// PluginWeight 是在调度器配置中指定 task-topology 插件权重的参数键名
	// 用户通过该参数控制 task-topology 在节点评分中的影响力
	PluginWeight = "task-topology.weight"

	// JobAffinityKey 是从作业 annotations 中读取 task-topology 配置的键名
	// （主要供 job plugin 使用，将 topology 信息序列化后写入作业 annotation）
	JobAffinityKey = "volcano.sh/task-topology"

	// OutOfBucket 表示任务不属于任何桶，即在分桶过程中被排除在外
	// 当任务没有拓扑配置或无法匹配到任何桶时，会被标记为该值
	OutOfBucket = -1

	// JobAffinityAnnotations 是从 PodGroup annotations 中读取亲和性配置的键名
	// 格式示例: "ps,worker;ps,chief" 表示两组亲和关系:
	//   - ps 与 worker 亲和（应尽量调度到同一节点）
	//   - ps 与 chief 亲和（应尽量调度到同一节点）
	JobAffinityAnnotations = "volcano.sh/task-topology-affinity"

	// JobAntiAffinityAnnotations 是从 PodGroup annotations 中读取反亲和性配置的键名
	// 格式示例: "ps;worker,chief" 表示两组反亲和关系:
	//   - ps 自身反亲和（ps 副本之间应分散到不同节点）
	//   - worker 与 chief 反亲和（应分散到不同节点）
	JobAntiAffinityAnnotations = "volcano.sh/task-topology-anti-affinity"

	// TaskOrderAnnotations 是从 PodGroup annotations 中读取任务排序配置的键名
	// 格式示例: "ps,worker,chief,evaluator" 表示调度优先级从高到低
	TaskOrderAnnotations = "volcano.sh/task-topology-task-order"
)

// TaskTopology 是用于保存作业亲和性信息的结构体
// 该结构体从 PodGroup annotations 解析得到，包含了用户定义的三类拓扑配置：
//   - Affinity:      亲和性分组，同一组内的任务类型应尽量调度到同一节点
//   - AntiAffinity:  反亲和性分组，同一组内的任务类型应尽量分散到不同节点
//   - TaskOrder:     任务调度优先级排序，排在前面的任务类型优先调度
//
// 示例配置（对应一个 TF 训练作业）:
//
//	Affinity:     [["ps", "worker"], ["ps", "chief"]]
//	AntiAffinity: [["ps"], ["worker", "chief"]]
//	TaskOrder:    ["ps", "worker", "chief", "evaluator"]
//
// 含义:
//   - ps 与 worker 亲和 → ps 和 worker 尽量在同一节点
//   - ps 与 chief 亲和  → ps 和 chief 尽量在同一节点
//   - ps 自身反亲和     → 多个 ps 副本分散到不同节点
//   - worker 与 chief 反亲和 → worker 和 chief 分散到不同节点
//   - 调度顺序: ps > worker > chief > evaluator
type TaskTopology struct {
	Affinity     [][]string `json:"affinity,omitempty"`
	AntiAffinity [][]string `json:"antiAffinity,omitempty"`
	TaskOrder    []string   `json:"taskOrder,omitempty"`
}

// calculateWeight 从调度器配置参数中读取 task-topology 插件的权重值
//
// 用户应在调度器配置中按以下格式指定权重:
//
//	actions: "enqueue, reclaim, allocate, backfill, preempt"
//	tiers:
//	- plugins:
//	  - name: task-topology
//	    arguments:
//	      task-topology.weight: 10
//
// 权重值用于在 NodeOrderFn 中将桶评分缩放到标准分数范围 [0, MaxNodeScore]
// 权重越大，task-topology 的评分在综合节点评分中的占比越高
//
// 参数:
//   - args: 框架传入的插件参数map
//
// 返回:
//   - 权重值，默认为 1
func calculateWeight(args framework.Arguments) int {
	// 默认权重为 1，即 task-topology 的评分以原始比例参与节点排序
	weight := 1

	// 从参数中读取 task-topology.weight 配置，如果未配置则保持默认值 1
	args.GetInt(&weight, PluginWeight)

	return weight
}

// getTaskName 从任务的 Pod annotations 中提取任务类型名称（TaskRole）
//
// 在 Volcano 作业中，每个任务都有一个 TaskRole（如 "ps"、"worker"、"chief" 等），
// 该值存储在 Pod 的 annotation 中，键为 v1alpha1.TaskSpecKey
// task-topology 插件通过 TaskRole 来识别任务类型，从而应用对应的亲和/反亲和规则
//
// 参数:
//   - task: 任务信息
//
// 返回:
//   - 任务类型名称字符串，如果 Pod 没有该 annotation 则返回空字符串
func getTaskName(task *api.TaskInfo) string {
	return task.Pod.Annotations[v1alpha1.TaskSpecKey]
}

// addAffinity 向亲和性映射表中添加一条双向亲和关系
//
// 亲和性映射表的结构为 map[源任务类型][目标任务类型]struct{}
// 用于表示"源任务类型"与"目标任务类型"之间存在亲和/反亲和关系
// 该函数会建立双向关系（src→dst），调用方需同时调用 addAffinity(m, dst, src)
// 来确保双向关系完整
//
// 参数:
//   - m:   亲和性映射表，可以是 interAffinity 或 interAntiAffinity
//   - src: 源任务类型名称
//   - dst: 目标任务类型名称
func addAffinity(m map[string]map[string]struct{}, src, dst string) {
	srcMap, ok := m[src]
	if !ok {
		srcMap = make(map[string]struct{})
		m[src] = srcMap
	}
	srcMap[dst] = struct{}{}
}

// ============================================================================
// TaskOrder 排序结构体
// ============================================================================
// TaskOrder 实现了 sort.Interface 接口，用于在 ConstructBucket 阶段
// 对待分桶的任务进行排序。排序规则决定了任务被分配到桶的先后顺序，
// 从而影响最终的桶划分结果。
//
// 排序优先级（从高到低）:
//  1. 已绑定节点的任务优先（因为需要按节点归入对应的桶）
//  2. 拓扑优先级更高的任务优先（selfAntiAffinity > interAffinity > selfAffinity > interAntiAffinity）
//  3. 用户自定义的任务排序（TaskOrder 中靠前的任务优先）
//  4. 同等条件下按任务名称排序（保证排序的确定性）
// ============================================================================

// TaskOrder 是用于保存任务排序信息的结构体
type TaskOrder struct {
	tasks   []*api.TaskInfo // 待排序的任务列表
	manager *JobManager     // 关联的作业管理器，提供拓扑优先级查询
}

// Len 返回待排序任务的数量
func (p *TaskOrder) Len() int { return len(p.tasks) }

// Swap 交换任务列表中两个位置的任务
func (p *TaskOrder) Swap(l, r int) {
	p.tasks[l], p.tasks[r] = p.tasks[r], p.tasks[l]
}

// Less 判断位置 l 的任务是否应该排在位置 r 的任务之前
//
// 排序逻辑:
//  1. 已绑定节点的任务排在前面（优先处理），因为已绑定的任务需要按节点
//     归入对应桶，作为后续未绑定任务分桶的参考基准
//  2. 如果两个任务都已绑定节点，按节点名称降序排列（仅保证确定性）
//  3. 如果两个任务都未绑定节点，通过 taskAffinityOrder 比较拓扑优先级
//  4. 拓扑优先级相同时，按任务名称降序排列（保证确定性）
//
// 注意: 使用 sort.Reverse 包装后，Less 返回 true 表示"排在后面"，
// 即实际效果是优先级高的任务排在切片前面
func (p *TaskOrder) Less(l, r int) bool {
	L := p.tasks[l]
	R := p.tasks[r]

	LHasNode := L.NodeName != ""
	RHasNode := R.NodeName != ""
	if LHasNode || RHasNode {
		// 已绑定节点的任务优先级更高，排在前面
		if LHasNode != RHasNode {
			return !LHasNode
		}
		// 两个任务都已绑定节点，按节点名降序排列保证确定性
		return L.NodeName > R.NodeName
	}

	// 两个任务都未绑定节点，比较拓扑优先级
	result := p.manager.taskAffinityOrder(L, R)
	// 拓扑优先级相同，按任务名称降序排列保证确定性
	if result == 0 {
		return L.Name > R.Name
	}
	return result < 0
}
