/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2024 The Volcano Authors.

Modifications made by Volcano authors:
- Added Err field to Event structure for error handling

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

package framework

import (
	"volcano.sh/volcano/pkg/scheduler/api"
)

// Event structure
type Event struct {
	Task *api.TaskInfo
	Err  error
}

/*
EventHandler structure
这两个回调函数是 Volcano 调度器框架的资源分配事件通知机制
用于在各个调度插件中响应 Task 的资
*/

type EventHandler struct {
	/*
		AllocateFunc Task 被分配资源时触发
			在以下场景被调用:
			Allocate 操作: Task 成功分配到节点，状态变为 Allocated
			Pipeline 操作: Task 流水线分配(等资源释放), 状态变为 Pipelined
			Evict 后重新分配: Task 被驱逐后触发分配回调
			典型用途: 各插件在 Task 获得资源后更新自身的统计数据。例如：
			capacity 插件 在 AllocateFunc 中累加已分配资源量
			drf 插件 更新 Dominant Resource Fairness 的份额计算
	*/
	AllocateFunc func(event *Event)
	/*
		DeallocateFunc Task 被释放资源时触发
			在以下场景被调用:
			Task 从节点上释放资源，状态变为 Unallocated
			典型用途: 各插件在 Task 释放资源后更新自身的统计数据。例如：
			capacity 插件 在 DeallocateFunc 中减去已释放资源量
			drf 插件 更新 Dominant Resource Fairness 的份额计算
	*/
	DeallocateFunc func(event *Event)
}
