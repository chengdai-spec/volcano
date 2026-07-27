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
	jobflowv1alpha1 "volcano.sh/apis/pkg/apis/flow/v1alpha1"
)

// pendingState 待处理状态
// 表示 JobFlow 刚创建或子 Job 尚未开始运行的状态
type pendingState struct {
	jobFlow *jobflowv1alpha1.JobFlow
}

// Execute 在 Pending 状态下执行同步动作
// 调用 SyncJobFlow 同步子 Job 状态，并根据结果决定状态流转：
//   - 有 Job 在运行或已完成且无失败 → 转为 Running
//   - 有 Job 失败或被终止 → 转为 Failed，并更新失败指标
//   - 其他情况 → 保持 Pending
func (p *pendingState) Execute(action jobflowv1alpha1.Action) error {
	switch action {
	case jobflowv1alpha1.SyncJobFlowAction:
		return SyncJobFlow(p.jobFlow, func(status *jobflowv1alpha1.JobFlowStatus, allJobList int) {
			// 有 Job 在运行或已完成，且没有失败的 → 转为 Running
			if (len(status.RunningJobs) > 0 || len(status.CompletedJobs) > 0) && len(status.FailedJobs) <= 0 {
				status.State.Phase = jobflowv1alpha1.Running
			} else if len(status.FailedJobs) > 0 || len(status.TerminatedJobs) > 0 { // TODO(dongjiang1989) 待完善失败条件判断
				UpdateJobFlowFailed(p.jobFlow.Namespace)
				status.State.Phase = jobflowv1alpha1.Failed
			} else {
				// 无子 Job 在运行，保持 Pending
				status.State.Phase = jobflowv1alpha1.Pending
			}
		})
	}
	return nil
}
