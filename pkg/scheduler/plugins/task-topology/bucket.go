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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// ============================================================================
// reqAction 资源操作类型
// ============================================================================
// reqAction 用于标识对桶内资源请求的操作类型：增加或减少。
// 当任务加入桶时使用 reqAdd 增加资源请求，当任务从桶中移除（绑定到节点）时
// 使用 reqSub 减少资源请求。
// ============================================================================

// reqAction 表示对桶资源请求的操作类型
type reqAction int

const (
	// reqSub 表示从桶的资源请求中减去指定资源（任务被绑定到节点时调用）
	reqSub reqAction = iota
	// reqAdd 表示向桶的资源请求中增加指定资源（新任务加入桶时调用）
	reqAdd
)

// ============================================================================
// Bucket 桶结构体
// ============================================================================
// Bucket（桶）是 task-topology 插件的核心数据结构，用于将同一作业中
// 具有亲和关系的任务分组到一起。
//
// 核心思想:
//   - 将作业中的任务按照亲和性/反亲和性规则划分为若干桶
//   - 同一桶内的任务应尽量调度到同一节点（亲和）
//   - 不同桶的任务应尽量分散到不同节点（反亲和）
//   - 调度时以桶为单位计算节点评分，优先选择能容纳更多桶内任务的节点
//
// 桶的生命周期:
//   1. ConstructBucket 阶段创建桶，并将任务按亲和性分配到桶中
//   2. 调度过程中，NodeOrderFn 以桶为单位计算节点评分
//   3. 任务被绑定到节点后，通过 TaskBound 更新桶状态
// ============================================================================

// Bucket 是用于按亲和性和反亲和性对任务进行分类的结构体
type Bucket struct {
	// index 是桶在 JobManager.buckets 切片中的索引位置
	// 用于唯一标识桶，并在分桶和评分过程中快速定位
	index int

	// tasks 保存桶内尚未绑定到节点的待调度任务
	// 键为 Pod UID，值为任务信息
	// 任务被绑定到节点后会从此 map 中删除
	tasks map[types.UID]*api.TaskInfo

	// taskNameSet 统计桶内各任务类型的数量
	// 键为任务类型名称（如 "ps"、"worker"），值为该类型的任务数量
	// 用于在 checkTaskSetAffinity 中计算亲和/反亲和评分
	taskNameSet map[string]int

	// reqScore 是桶内资源请求的评分值
	// 用于在多个桶之间进行负载均衡（选择 reqScore 更小的桶）
	// 评分计算规则: 1m CPU = 1分，1Mi 内存 = 1分，1个标量资源 = 1分
	reqScore float64

	// request 是桶内所有未绑定任务的资源请求总量
	// 用于判断节点是否能容纳整个桶，以及在评分时计算需要移除多少任务才能放下
	request *api.Resource

	// boundTask 记录桶内已绑定到节点的任务数量
	// 用于计算 bucketMaxSize 和评估桶的实际分布情况
	boundTask int

	// node 记录桶内任务在各节点上的分布情况
	// 键为节点名称，值为该节点上绑定的桶内任务数量
	// 在 NodeOrderFn 中，node[node.Name] 作为基础评分，
	// 表示该节点上已有多少桶内任务（已有任务越多，评分越高）
	node map[string]int
}

// NewBucket 创建一个新的空桶
// 所有字段初始化为空值或零值
func NewBucket() *Bucket {
	return &Bucket{
		index:       0,
		tasks:       make(map[types.UID]*api.TaskInfo),
		taskNameSet: make(map[string]int),

		reqScore: 0,
		request:  api.EmptyResource(),

		boundTask: 0,
		node:      make(map[string]int),
	}
}

// CalcResReq 计算任务的资源请求并更新桶的资源评分和总请求量
//
// 评分规则（reqScore）:
//   - CPU: 以 millicore 为单位，1m CPU = 1分
//   - 内存: 转换为 MiB，1Mi 内存 = 1分（原始单位为字节，除以 1024*1024）
//   - 标量资源（如 GPU）: 每个单位 = 1分
//   - 最终评分 = CPU + 内存 + 所有标量资源之和
//
// 该评分用于在 buildBucket 阶段进行桶间负载均衡:
// 当多个桶的亲和性评分相同时，优先选择 reqScore 更小的桶，
// 使任务分布更加均匀
//
// 参数:
//   - req:    任务的资源请求
//   - action: 资源操作类型（reqAdd 增加或 reqSub 减少）
func (b *Bucket) CalcResReq(req *api.Resource, action reqAction) {
	if req == nil {
		return
	}

	// CPU 请求量（单位: millicore）
	cpu := req.MilliCPU
	// 内存请求量转换为 MiB（原始单位为字节，1Mi = 1024*1024 字节）
	// 将 1Mi 内存等同于 1m CPU 和 1m GPU 的评分
	mem := req.Memory / 1024 / 1024
	// 基础评分 = CPU + 内存
	score := cpu + mem
	// 加上所有标量资源（如 GPU、RDMA 等）的请求量
	for _, request := range req.ScalarResources {
		score += request
	}

	switch action {
	case reqSub:
		// 任务从桶中移除（绑定到节点），减少资源评分和总请求量
		b.reqScore -= score
		b.request.Sub(req)
	case reqAdd:
		// 新任务加入桶，增加资源评分和总请求量
		b.reqScore += score
		b.request.Add(req)
	default:
		klog.V(3).Infof("Invalid action <%v> for resource <%v>", action, req)
	}
}

// AddTask 将任务添加到桶中
//
// 添加逻辑:
//   - 始终更新 taskNameSet，记录桶内各任务类型的数量
//   - 如果任务已绑定到节点（NodeName 非空），说明是已调度的任务:
//   - 更新 node 映射，记录该节点上的桶内任务数
//   - 增加 boundTask 计数
//   - 不计入待调度任务列表和资源请求（因为已分配资源）
//   - 如果任务未绑定节点，说明是待调度任务:
//   - 加入 tasks 待调度列表
//   - 调用 CalcResReq 增加桶的资源请求总量
//
// 参数:
//   - taskName: 任务类型名称（如 "ps"、"worker"）
//   - task:     任务信息
func (b *Bucket) AddTask(taskName string, task *api.TaskInfo) {
	// 更新桶内任务类型计数
	b.taskNameSet[taskName]++
	if task.NodeName != "" {
		// 任务已绑定到节点，记录节点分布
		b.node[task.NodeName]++
		b.boundTask++
		return
	}

	// 任务未绑定节点，加入待调度列表并累加资源请求
	b.tasks[task.Pod.UID] = task
	b.CalcResReq(task.Resreq, reqAdd)
}

// TaskBound 处理任务被绑定到节点后的桶状态更新
//
// 当任务被调度器成功分配到节点后，调用此方法更新桶状态:
//  1. 更新 node 映射，记录该节点上新绑定的桶内任务
//  2. 增加 boundTask 计数
//  3. 从待调度任务列表中删除该任务
//  4. 从桶资源请求中减去该任务的资源（因为已分配）
//
// 参数:
//   - task: 已被绑定到节点的任务信息
func (b *Bucket) TaskBound(task *api.TaskInfo) {
	// 记录任务绑定的节点
	b.node[task.NodeName]++
	b.boundTask++

	// 从待调度列表中移除
	delete(b.tasks, task.Pod.UID)
	// 从桶资源请求中减去该任务的资源
	b.CalcResReq(task.Resreq, reqSub)
}
