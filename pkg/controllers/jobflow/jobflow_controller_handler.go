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

package jobflow

import (
	"k8s.io/klog/v2"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	jobflowv1alpha1 "volcano.sh/apis/pkg/apis/flow/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	"volcano.sh/volcano/pkg/controllers/apis"
)

// enqueue 将 FlowRequest 放入工作队列
// 是控制器内部统一的入队函数，由各个事件处理器调用
func (jf *jobflowcontroller) enqueue(req apis.FlowRequest) {
	jf.queue.Add(req)
}

// addJobFlow JobFlow 创建事件的处理函数
// 当新的 JobFlow 被创建时，生成 SyncJobFlowAction 请求并放入工作队列，
// 触发控制器开始按依赖顺序部署 Job
func (jf *jobflowcontroller) addJobFlow(obj interface{}) {
	jobFlow, ok := obj.(*jobflowv1alpha1.JobFlow)
	if !ok {
		klog.Errorf("Failed to convert %v to jobFlow", obj)
		return
	}

	// 使用值类型而非指针，确保工作队列中的对象不会被意外修改
	req := apis.FlowRequest{
		Namespace:   jobFlow.Namespace,
		JobFlowName: jobFlow.Name,

		Action: jobflowv1alpha1.SyncJobFlowAction,
		Event:  jobflowv1alpha1.OutOfSyncEvent,
	}

	jf.enqueueJobFlow(req)
}

// updateJobFlow JobFlow 更新事件的处理函数
// 仅在 JobFlow 已成功(Succeed)且保留策略为 Delete 时才触发同步，
// 用于处理 JobFlow 完成后删除子 Job 的场景
// TODO: 当前 JobFlow 的更新操作预留为未来使用，普通的更新不会影响 JobFlow 流程
func (jf *jobflowcontroller) updateJobFlow(oldObj, newObj interface{}) {
	oldJobFlow, ok := oldObj.(*jobflowv1alpha1.JobFlow)
	if !ok {
		klog.Errorf("Failed to convert %v to jobflow", oldJobFlow)
		return
	}

	newJobFlow, ok := newObj.(*jobflowv1alpha1.JobFlow)
	if !ok {
		klog.Errorf("Failed to convert %v to jobflow", newJobFlow)
		return
	}

	// ResourceVersion 相同说明是重复事件，跳过
	if newJobFlow.ResourceVersion == oldJobFlow.ResourceVersion {
		return
	}

	// 仅在 JobFlow 已成功且保留策略为 Delete 时才触发同步(用于清理子 Job)
	if newJobFlow.Status.State.Phase != jobflowv1alpha1.Succeed || newJobFlow.Spec.JobRetainPolicy != jobflowv1alpha1.Delete {
		return
	}

	req := apis.FlowRequest{
		Namespace:   newJobFlow.Namespace,
		JobFlowName: newJobFlow.Name,

		Action: jobflowv1alpha1.SyncJobFlowAction,
		Event:  jobflowv1alpha1.OutOfSyncEvent,
	}

	jf.enqueueJobFlow(req)
}

// updateJob VCJob 更新事件的处理函数
// 当子 Job 状态发生变化时，触发其父 JobFlow 的同步，以更新 JobFlow 的整体状态
// 仅处理由 JobFlow 创建的 Job（通过 OwnerReference 判断）
func (jf *jobflowcontroller) updateJob(oldObj, newObj interface{}) {
	oldJob, ok := oldObj.(*batch.Job)
	if !ok {
		klog.Errorf("Failed to convert %v to vcjob", oldObj)
		return
	}

	newJob, ok := newObj.(*batch.Job)
	if !ok {
		klog.Errorf("Failed to convert %v to vcjob", newObj)
		return
	}

	// 过滤非 JobFlow 创建的 Job（检查 OwnerReference 是否指向 JobFlow）
	if !isControlledBy(newJob, helpers.JobFlowKind) {
		return
	}

	// ResourceVersion 相同说明是重复事件，跳过
	if newJob.ResourceVersion == oldJob.ResourceVersion {
		return
	}

	// 从 Job 的 OwnerReference 中提取父 JobFlow 的名称
	jobFlowName := getJobFlowNameByJob(newJob)
	if jobFlowName == "" {
		return
	}

	req := apis.FlowRequest{
		Namespace:   newJob.Namespace,
		JobFlowName: jobFlowName,
		Action:      jobflowv1alpha1.SyncJobFlowAction,
		Event:       jobflowv1alpha1.OutOfSyncEvent,
	}

	jf.enqueueJobFlow(req)
}
