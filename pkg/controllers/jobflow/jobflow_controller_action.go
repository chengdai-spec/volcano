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
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
	v1alpha1flow "volcano.sh/apis/pkg/apis/flow/v1alpha1"
	"volcano.sh/apis/pkg/client/clientset/versioned/scheme"
	"volcano.sh/volcano/pkg/controllers/jobflow/state"
)

// syncJobFlow 同步 JobFlow 的核心业务逻辑
// 由状态机（state 包）通过 SyncJobFlow 变量调用
// 流程：
//  1. 如果 JobFlow 已成功且保留策略为 Delete，则删除所有子 Job
//  2. 按依赖顺序部署 Job(从 JobTemplate 创建 VCJob)
//  3. 收集所有子 Job 的状态，构建 JobFlowStatus
//  4. 调用 updateStateFn 根据子 Job 状态计算 JobFlow 的新 Phase
//  5. 将更新后的 Status 写入 API Server
func (jf *jobflowcontroller) syncJobFlow(jobFlow *v1alpha1flow.JobFlow, updateStateFn state.UpdateJobFlowStatusFn) error {
	klog.V(4).Infof("Begin to sync JobFlow %s.", jobFlow.Name)
	defer klog.V(4).Infof("End sync JobFlow %s.", jobFlow.Name)

	// 如果 JobFlow 已成功完成且保留策略为 Delete，则清理所有子 Job
	// JobRetainPolicy 决定 JobFlow 成功后的行为：Delete 删除子 Job，Retain 保留
	if jobFlow.Spec.JobRetainPolicy == v1alpha1flow.Delete && jobFlow.Status.State.Phase == v1alpha1flow.Succeed {
		if err := jf.deleteAllJobsCreatedByJobFlow(jobFlow); err != nil {
			klog.Errorf("Failed to delete jobs of JobFlow %v/%v: %v",
				jobFlow.Namespace, jobFlow.Name, err)
			return err
		}
		return nil
	}

	// 按依赖顺序部署 Job(检查每个 flow 的依赖是否满足，满足则从 JobTemplate 创建 VCJob)
	if err := jf.deployJob(jobFlow); err != nil {
		klog.Errorf("Failed to create jobs of JobFlow %v/%v: %v",
			jobFlow.Namespace, jobFlow.Name, err)
		return err
	}

	// 收集所有子 Job 的状态信息，构建 JobFlowStatus
	jobFlowStatus, err := jf.getAllJobStatus(jobFlow)
	if err != nil {
		return err
	}
	jobFlow.Status = *jobFlowStatus
	// 调用状态机传入的回调函数，根据子 Job 状态计算 JobFlow 的新 Phase
	// 例如：所有 Job 完成 → Succeed，有 Job 失败 → Failed
	updateStateFn(&jobFlow.Status, len(jobFlow.Spec.Flows))
	_, err = jf.vcClient.FlowV1alpha1().JobFlows(jobFlow.Namespace).UpdateStatus(context.Background(), jobFlow, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of JobFlow %v/%v: %v",
			jobFlow.Namespace, jobFlow.Name, err)
		return err
	}

	return nil
}

// deployJob 按依赖顺序部署 JobFlow 中定义的所有 Job
// 遍历 Spec.Flows 列表，对每个 flow 检查：
//   - Job 是否已存在(已存在则跳过)
//   - 如果没有依赖(DependsOn 为空)，直接创建
//   - 如果有依赖，先判断依赖是否满足(所有 target Job 都已完成)再创建
func (jf *jobflowcontroller) deployJob(jobFlow *v1alpha1flow.JobFlow) error {
	for _, flow := range jobFlow.Spec.Flows {
		jobName := getJobName(jobFlow.Name, flow.Name)
		// 检查该 Job 是否已经存在
		if _, err := jf.jobLister.Jobs(jobFlow.Namespace).Get(jobName); err != nil {
			if errors.IsNotFound(err) {
				// Job 不存在，需要创建
				if flow.DependsOn == nil || flow.DependsOn.Targets == nil {
					// 无依赖，直接创建
					if err := jf.createJob(jobFlow, flow); err != nil {
						return err
					}
				} else {
					// 有依赖，先判断依赖是否都已满足
					flag, err := jf.judge(jobFlow, flow)
					if err != nil {
						return err
					}
					if flag {
						if err := jf.createJob(jobFlow, flow); err != nil {
							return err
						}
					}
				}
				continue
			}
			return err
		}
		// Job 已存在，跳过
	}
	return nil
}

// judge 判断指定 flow 的所有依赖是否都已满足
// 依赖满足条件：所有 target 对应的 VCJob 都已处于 Completed 状态
// 返回 (true, nil) 表示依赖满足，可以创建该 Job
func (jf *jobflowcontroller) judge(jobFlow *v1alpha1flow.JobFlow, flow v1alpha1flow.Flow) (bool, error) {
	for _, targetName := range flow.DependsOn.Targets {
		targetJobName := getJobName(jobFlow.Name, targetName)
		job, err := jf.jobLister.Jobs(jobFlow.Namespace).Get(targetJobName)
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Info(fmt.Sprintf("No %v Job found！", targetJobName))
				return false, nil
			}
			return false, err
		}
		// 依赖的 Job 尚未完成，当前 flow 还不能创建
		if job.Status.State.Phase != v1alpha1.Completed {
			return false, nil
		}
	}
	return true, nil
}

// createJob 从 JobTemplate 创建一个新的 VCJob
// 流程：加载 JobTemplate → 填充 Job 元信息和 Spec → 设置 OwnerReference → 调用 API 创建
// 创建成功后会在 JobFlow 上记录一个 Kubernetes 事件
func (jf *jobflowcontroller) createJob(jobFlow *v1alpha1flow.JobFlow, flow v1alpha1flow.Flow) error {
	job := new(v1alpha1.Job)
	if err := jf.loadJobTemplateAndSetJob(jobFlow, flow.Name, getJobName(jobFlow.Name, flow.Name), job); err != nil {
		return err
	}
	// 如果 Job 已经存在(并发创建场景), 忽略错误
	if _, err := jf.vcClient.BatchV1alpha1().Jobs(jobFlow.Namespace).Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	// 记录创建事件
	jf.recorder.Eventf(jobFlow, corev1.EventTypeNormal, "Created", fmt.Sprintf("create a job named %v!", job.Name))
	return nil
}

// getAllJobStatus 收集 JobFlow 下所有子 Job 的状态信息，构建 JobFlowStatus
// 返回的状态包含：
//   - 各阶段 Job 名称列表（PendingJobs, RunningJobs, FailedJobs 等）
//   - 每个 Job 的详细状态（包含运行历史记录）
//   - Job 的条件信息（Phase、创建时间、运行时长、任务状态统计）
func (jf *jobflowcontroller) getAllJobStatus(jobFlow *v1alpha1flow.JobFlow) (*v1alpha1flow.JobFlowStatus, error) {
	jobList, err := jf.getAllJobsCreatedByJobFlow(jobFlow)
	if err != nil {
		klog.Error(err, "get jobList error")
		return nil, err
	}

	// 按 Job 状态分类收集 Job 名称
	statusListJobMap := map[v1alpha1.JobPhase][]string{
		v1alpha1.Pending:     make([]string, 0),
		v1alpha1.Running:     make([]string, 0),
		v1alpha1.Completing:  make([]string, 0),
		v1alpha1.Completed:   make([]string, 0),
		v1alpha1.Terminating: make([]string, 0),
		v1alpha1.Terminated:  make([]string, 0),
		v1alpha1.Failed:      make([]string, 0),
	}

	UnKnowJobs := make([]string, 0)
	// conditions: 每个 Job 的条件信息（Phase、创建时间、运行时长、任务状态统计）
	conditions := make(map[string]v1alpha1flow.Condition)
	for _, job := range jobList {
		if _, ok := statusListJobMap[job.Status.State.Phase]; ok {
			statusListJobMap[job.Status.State.Phase] = append(statusListJobMap[job.Status.State.Phase], job.Name)
		} else {
			UnKnowJobs = append(UnKnowJobs, job.Name)
		}
		conditions[job.Name] = v1alpha1flow.Condition{
			Phase:           job.Status.State.Phase,
			CreateTimestamp: job.CreationTimestamp,
			RunningDuration: job.Status.RunningDuration,
			TaskStatusCount: job.Status.TaskStatusCount,
		}
	}
	// 构建 JobStatusList（包含每个 Job 的运行历史记录）
	// 复用已有的 jobStatusList 以保持历史记录的连续性
	jobStatusList := make([]v1alpha1flow.JobStatus, 0)
	if jobFlow.Status.JobStatusList != nil {
		jobStatusList = jobFlow.Status.JobStatusList
	}
	for _, job := range jobList {
		runningHistories := getRunningHistories(jobStatusList, job)
		endTimeStamp := metav1.Time{}
		if job.Status.RunningDuration != nil {
			endTimeStamp = metav1.Time{Time: job.CreationTimestamp.Add(job.Status.RunningDuration.Duration)}
		}
		jobStatus := v1alpha1flow.JobStatus{
			Name:             job.Name,
			State:            job.Status.State.Phase,
			StartTimestamp:   job.CreationTimestamp,
			EndTimestamp:     endTimeStamp,
			RestartCount:     job.Status.RetryCount,
			RunningHistories: runningHistories,
		}
		jobFlag := true
		// 更新已有记录或追加新记录
		for i := range jobStatusList {
			if jobStatusList[i].Name == jobStatus.Name {
				jobFlag = false
				jobStatusList[i] = jobStatus
			}
		}
		if jobFlag {
			jobStatusList = append(jobStatusList, jobStatus)
		}
	}

	jobFlowStatus := v1alpha1flow.JobFlowStatus{
		PendingJobs:    statusListJobMap[v1alpha1.Pending],
		RunningJobs:    statusListJobMap[v1alpha1.Running],
		FailedJobs:     statusListJobMap[v1alpha1.Failed],
		CompletedJobs:  statusListJobMap[v1alpha1.Completed],
		TerminatedJobs: statusListJobMap[v1alpha1.Terminated],
		UnKnowJobs:     UnKnowJobs,
		JobStatusList:  jobStatusList,
		Conditions:     conditions,
		State:          jobFlow.Status.State,
	}
	return &jobFlowStatus, nil
}

// getRunningHistories 获取或更新 Job 的运行历史记录
// 如果 Job 状态发生变化，会记录历史状态的结束时间并新增一条当前状态的记录
// 如果 Job 是首次出现，则新增一条初始记录
func getRunningHistories(jobStatusList []v1alpha1flow.JobStatus, job *v1alpha1.Job) []v1alpha1flow.JobRunningHistory {
	runningHistories := make([]v1alpha1flow.JobRunningHistory, 0)
	flag := true
	for _, jobStatusGet := range jobStatusList {
		if jobStatusGet.Name == job.Name && jobStatusGet.RunningHistories != nil {
			flag = false
			runningHistories = jobStatusGet.RunningHistories
			// State change
			if len(runningHistories) == 0 {
				continue
			}
			// 状态发生变化时，结束上一条历史记录并新增当前状态记录
			if runningHistories[len(runningHistories)-1].State != job.Status.State.Phase {
				runningHistories[len(runningHistories)-1].EndTimestamp = metav1.Time{
					Time: time.Now(),
				}
				runningHistories = append(runningHistories, v1alpha1flow.JobRunningHistory{
					StartTimestamp: metav1.Time{Time: time.Now()},
					EndTimestamp:   metav1.Time{},
					State:          job.Status.State.Phase,
				})
			}
		}
	}
	// 首次创建运行历史记录（之前没有该 Job 的状态记录）
	if flag && job.Status.State.Phase != "" {
		runningHistories = append(runningHistories, v1alpha1flow.JobRunningHistory{
			StartTimestamp: metav1.Time{
				Time: time.Now(),
			},
			EndTimestamp: metav1.Time{},
			State:        job.Status.State.Phase,
		})
	}
	return runningHistories
}

// loadJobTemplateAndSetJob 从 JobTemplate 加载模板并填充到 Job 对象中
// 流程：从缓存获取 JobTemplate → 复制 Spec 到新 Job → 设置 Labels/Annotations → 设置 OwnerReference
// OwnerReference 确保 JobFlow 删除时级联删除所有子 Job
func (jf *jobflowcontroller) loadJobTemplateAndSetJob(jobFlow *v1alpha1flow.JobFlow, flowName string, jobName string, job *v1alpha1.Job) error {
	// 从本地缓存获取 JobTemplate
	jobTemplate, err := jf.jobTemplateLister.JobTemplates(jobFlow.Namespace).Get(flowName)
	if err != nil {
		return err
	}

	// 将 JobTemplate 的 Spec 复制到新 Job 中，并设置元信息
	*job = v1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobFlow.Namespace,
			Labels: map[string]string{
				CreatedByJobTemplate: GenerateObjectString(jobFlow.Namespace, flowName),
				CreatedByJobFlow:     GenerateObjectString(jobFlow.Namespace, jobFlow.Name),
			},
			Annotations: map[string]string{
				CreatedByJobTemplate: GenerateObjectString(jobFlow.Namespace, flowName),
				CreatedByJobFlow:     GenerateObjectString(jobFlow.Namespace, jobFlow.Name),
			},
		},
		Spec:   jobTemplate.Spec,
		Status: v1alpha1.JobStatus{},
	}

	// 设置 OwnerReference，使 JobFlow 成为该 Job 的控制器（级联删除）
	return controllerutil.SetControllerReference(jobFlow, job, scheme.Scheme)
}

// deleteAllJobsCreatedByJobFlow 删除 JobFlow 创建的所有子 Job
// 用于 JobRetainPolicy=Delete 且 JobFlow 成功时的清理操作
func (jf *jobflowcontroller) deleteAllJobsCreatedByJobFlow(jobFlow *v1alpha1flow.JobFlow) error {
	jobList, err := jf.getAllJobsCreatedByJobFlow(jobFlow)
	if err != nil {
		return err
	}

	for _, job := range jobList {
		err := jf.vcClient.BatchV1alpha1().Jobs(jobFlow.Namespace).Delete(context.Background(), job.Name, metav1.DeleteOptions{})
		if err != nil {
			klog.Errorf("Failed to delete job of JobFlow %v/%v: %v",
				jobFlow.Namespace, jobFlow.Name, err)
			return err
		}
	}
	return nil
}

// getAllJobsCreatedByJobFlow 通过 Label Selector 查询 JobFlow 创建的所有子 Job
// 使用 CreatedByJobFlow 标签进行过滤，标签值格式为 "<namespace>.<jobFlowName>"
func (jf *jobflowcontroller) getAllJobsCreatedByJobFlow(jobFlow *v1alpha1flow.JobFlow) ([]*v1alpha1.Job, error) {
	selector := labels.NewSelector()
	r, err := labels.NewRequirement(CreatedByJobFlow, selection.In, []string{GenerateObjectString(jobFlow.Namespace, jobFlow.Name)})
	if err != nil {
		return nil, err
	}
	selector = selector.Add(*r)
	return jf.jobLister.Jobs(jobFlow.Namespace).List(selector)
}
