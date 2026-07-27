/*
Copyright 2022 The Volcano Authors.

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

package state

import (
	"volcano.sh/apis/pkg/apis/flow/v1alpha1"
)

// State 状态机接口，每种状态（Pending/Running/Succeed/Failed/Terminating）都实现该接口
// Execute 方法根据当前状态和接收到的 Action 执行对应的处理逻辑
type State interface {
	// Execute 根据当前状态执行指定的 Action
	Execute(action v1alpha1.Action) error
}

// UpdateJobFlowStatusFn 更新 JobFlow 状态的回调函数类型
// 由各状态机在 Execute 中传入，在 syncJobFlow 收集完子 Job 状态后调用
// 参数：status - 当前 JobFlow 状态；allJobList - JobFlow 中定义的 Job 总数
type UpdateJobFlowStatusFn func(status *v1alpha1.JobFlowStatus, allJobList int)

// JobFlowActionFn JobFlow 同步动作的函数类型
// 由控制器层注入到 state 包，供状态机调用以执行实际的同步逻辑
type JobFlowActionFn func(jobflow *v1alpha1.JobFlow, fn UpdateJobFlowStatusFn) error

var (
	// SyncJobFlow 同步 JobFlow 状态的函数引用
	// 由控制器层的 Initialize 方法注入为 syncJobFlow，
	// 状态机通过此变量调用控制器层的同步逻辑
	SyncJobFlow JobFlowActionFn
)

// NewState 根据 JobFlow 当前的 Phase 创建对应的状态机实例
// 状态流转：Pending → Running → Succeed / Failed / Terminating
// 如果 Phase 不匹配任何已知状态，返回 nil
func NewState(jobFlow *v1alpha1.JobFlow) State {
	switch jobFlow.Status.State.Phase {
	case "", v1alpha1.Pending:
		return &pendingState{jobFlow: jobFlow}
	case v1alpha1.Running:
		return &runningState{jobFlow: jobFlow}
	case v1alpha1.Succeed:
		return &succeedState{jobFlow: jobFlow}
	case v1alpha1.Terminating:
		return &terminatingState{jobFlow: jobFlow}
	case v1alpha1.Failed:
		return &failedState{jobFlow: jobFlow}
	}

	return nil
}
