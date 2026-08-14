/*
Copyright 2019 The Volcano Authors.

Licensed under the Apache License, Version 2.0.

本文件是 Volcano Job Controller 中 Job 同步与 Pod 管理的核心逻辑之一。
主要职责包括：
1. 初始化 Job 状态；
2. 创建或更新 PodGroup；
3. 创建 Job 需要的 PVC；
4. 根据 Job Spec 创建 Pod；
5. 删除多余 Pod、过期 Pod、out-of-sync Pod；
6. kill Job / Task / Pod / Partition；
7. 统计 Job 下 Pod 的状态；
8. 更新 Job Status；
9. 记录 PodGroup、Pod、Job 相关事件。
*/

package job

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/apis"
	jobhelpers "volcano.sh/volcano/pkg/controllers/job/helpers"
	"volcano.sh/volcano/pkg/controllers/job/state"
	"volcano.sh/volcano/pkg/controllers/metrics"
)

// calMutex 用于保护 calcPodStatus 中 taskStatusCount 这个 map。
// 因为 syncJob 中会并发创建 Pod，并发统计 Pod 状态时 map 写入不是线程安全的，所以需要加锁。
var calMutex sync.Mutex

// getPodGroupByJob 根据 Job 获取对应的 PodGroup。
// 新版本 PodGroup 名称格式为：job.Name-job.UID。
// 老版本 PodGroup 名称格式为：job.Name。
// 所以这里先查新格式，如果不存在，再查老格式，以兼容历史版本。
func (cc *jobcontroller) getPodGroupByJob(job *batch.Job) (*scheduling.PodGroup, error) {
	pgName := cc.generateRelatedPodGroupName(job)
	pg, err := cc.pgLister.PodGroups(job.Namespace).Get(pgName)
	if err == nil {
		return pg, nil
	}
	if apierrors.IsNotFound(err) {
		pg, err := cc.pgLister.PodGroups(job.Namespace).Get(job.Name)
		if err != nil {
			return nil, err
		}
		return pg, nil
	}
	return nil, err
}

// generateRelatedPodGroupName 生成 Job 对应的 PodGroup 名称。
// 使用 UID 是为了避免同名 Job 删除后重建时，新的 Job 复用旧 PodGroup。
func (cc *jobcontroller) generateRelatedPodGroupName(job *batch.Job) string {
	return fmt.Sprintf("%s-%s", job.Name, string(job.UID))
}

// killTarget 用于删除指定目标。
// target 可以是：
// 1. 某个 Task；
// 2. 某个 Pod；
// 3. 某个 Partition。
func (cc *jobcontroller) killTarget(jobInfo *apis.JobInfo, target state.Target, updateStatus state.UpdateStatusFn) error {
	switch target.Type {
	case state.TargetTypeTask:
		klog.V(3).Infof("Killing task <%s> of Job <%s/%s>, current version %d", target.TaskName, jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
		defer klog.V(3).Infof("Finished task <%s> of Job <%s/%s> killing, current version %d", target.TaskName, jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
	case state.TargetTypePod:
		klog.V(3).Infof("Killing pod <%s> of Job <%s/%s>, current version %d", target.PodName, jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
		defer klog.V(3).Infof("Finished pod <%s> of Job <%s/%s> killing, current version %d", target.PodName, jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
	case state.TargetTypePartition:
		klog.V(3).Infof("Killing partition <%s> partition of Job <%s/%s>, current version %d", target.PartitionName, jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
		defer klog.V(3).Infof("Finished partition <%s> of Job <%s/%s> killing, current version %d", target.PartitionName, jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
	default:
	}
	return cc.killPods(jobInfo, nil, &target, updateStatus)
}

// killJob 用于 kill 整个 Job。
// podRetainPhase 表示哪些 Pod 状态需要保留。
// updateStatus 是状态机回调函数，用来更新 Job 状态。
func (cc *jobcontroller) killJob(jobInfo *apis.JobInfo, podRetainPhase state.PhaseMap, updateStatus state.UpdateStatusFn) error {
	klog.V(3).Infof("Killing Job <%s/%s>, current version %d", jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> killing, current version %d", jobInfo.Namespace, jobInfo.Name, jobInfo.Job.Status.Version)
	return cc.killPods(jobInfo, podRetainPhase, nil, updateStatus)
}

// killPods 是删除 Pod 的核心函数。
// 它既可以删除整个 Job 的 Pod，也可以删除指定 Task、Pod 或 Partition。
// 如果 target == nil，表示 kill 整个 Job。
// 如果 target != nil，表示 kill 指定目标。
func (cc *jobcontroller) killPods(jobInfo *apis.JobInfo, podRetainPhase state.PhaseMap, target *state.Target, updateStatus state.UpdateStatusFn) error {
	job := jobInfo.Job
	if job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.", job.Namespace, job.Name)
		return nil
	}

	var pending, running, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batch.TaskState)
	var errs []error
	var total int
	podsToKill := make(map[string]*v1.Pod)

	if target != nil {
		switch target.Type {
		case state.TargetTypeTask:
			// 删除某个 Task 下的所有 Pod。
			if targetPods, found := jobInfo.Pods[target.TaskName]; found {
				podsToKill = targetPods
			}
		case state.TargetTypePod:
			// 删除某个 Task 下指定名称的 Pod。
			if targetPods, found := jobInfo.Pods[target.TaskName]; found {
				if pod, found := targetPods[target.PodName]; found {
					podsToKill[target.PodName] = pod
				}
			}
		case state.TargetTypePartition:
			// 删除某个 Partition 下的 Pod。
			partitionInfo, found := jobInfo.Partitions[target.TaskName]
			if !found || partitionInfo == nil || partitionInfo.Partition == nil {
				klog.Infof("Job <%s/%s> has not partition group in task %s, skip management process.", job.Namespace, job.Name, target.TaskName)
				return nil
			}
			podsInPartition, found := partitionInfo.Partition[target.PartitionName]
			if !found || podsInPartition == nil {
				klog.Infof("Job <%s/%s> has not partition group %s in task %s, skip management process.", job.Namespace, job.Name, target.PartitionName, target.TaskName)
				return nil
			}
			if targetPods, found := jobInfo.Pods[target.TaskName]; found {
				for _, pod := range podsInPartition {
					if pod, found := targetPods[pod.Name]; found {
						podsToKill[pod.Name] = pod
					}
				}
			}
			if len(podsToKill) == 0 {
				klog.Infof("Job <%s/%s> has not partition group in task %s, skip management process.", job.Namespace, job.Name, target.TaskName)
				return nil
			}
		default:
		}
		total += len(podsToKill)
	} else {
		// kill 整个 Job 时递增 Version。
		// Version 用来标记 Job 生命周期中的一次重启或重建，避免旧 Pod 被误认为新 Pod。
		job.Status.Version++
		for _, pods := range jobInfo.Pods {
			for _, pod := range pods {
				total++
				if pod.DeletionTimestamp != nil {
					klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
					terminating++
					continue
				}

				maxRetry := job.Spec.MaxRetry
				lastRetry := false
				if job.Status.RetryCount >= maxRetry-1 {
					lastRetry = true
				}

				// 如果是最后一次重试，只保留 Failed 和 Succeeded Pod。
				// 如果不是最后一次重试，则根据 podRetainPhase 判断是否保留。
				retainPhase := podRetainPhase
				if lastRetry {
					retainPhase = state.PodRetainPhaseSoft
				}
				_, retain := retainPhase[pod.Status.Phase]
				if !retain {
					podsToKill[pod.Name] = pod
				}
			}
		}
	}

	// 删除 Pod 前，先将 Pod 标记为 out-of-sync。
	// 这样可以避免 controller 误把这些旧 Pod 当作当前有效 Pod。
	for podName, pod := range podsToKill {
		_, err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, jobhelpers.OutOfSyncJSONPatch(), metav1.PatchOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
			delete(podsToKill, podName)
		} else {
			klog.V(3).InfoS("Marked Pod as out-of-sync", "Pod", klog.KObj(pod), "UID", pod.UID)
		}
	}

	// 真正删除 Pod。
	for _, pod := range podsToKill {
		if pod.DeletionTimestamp != nil {
			klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
			terminating++
			continue
		}
		err := cc.deleteJobPod(job.Name, pod)
		if err == nil {
			klog.V(3).InfoS("Deleted Pod of Job", "Job", klog.KObj(job), "Pod", klog.KObj(pod), "UID", pod.UID)
			terminating++
			continue
		}
		errs = append(errs, err)
		cc.resyncTask(pod)
		classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
		calcPodStatus(pod, taskStatusCount)
	}

	if len(errs) != 0 {
		klog.Errorf("failed to kill pods for job %s/%s, with err %+v", job.Namespace, job.Name, errs)
		cc.recorder.Event(job, v1.EventTypeWarning, FailedDeletePodReason, fmt.Sprintf("Error deleting pods: %+v", errs))
		return fmt.Errorf("failed to kill %d pods of %d", len(errs), total)
	}

	job = job.DeepCopy()
	job.Status.Pending = pending
	job.Status.Running = running
	job.Status.Succeeded = succeeded
	job.Status.Failed = failed
	job.Status.Terminating = terminating
	job.Status.Unknown = unknown
	job.Status.TaskStatusCount = taskStatusCount

	if updateStatus != nil {
		if updateStatus(&job.Status) {
			job.Status.State.LastTransitionTime = metav1.Now()
			jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
			job.Status.Conditions = append(job.Status.Conditions, jobCondition)
		}
	}

	runningDuration := metav1.Duration{Duration: job.Status.State.LastTransitionTime.Sub(jobInfo.Job.CreationTimestamp.Time)}
	klog.V(3).Infof("Running duration is %s", runningDuration.ToUnstructured())
	job.Status.RunningDuration = &runningDuration

	// pluginOnJobDelete 必须在更新 Job Status 之前执行。
	if err := cc.pluginOnJobDelete(job); err != nil {
		return err
	}

	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		klog.Errorf("Job %v/%v was not found", job.Namespace, job.Name)
		return nil
	}
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v", job.Namespace, job.Name, err)
		return err
	}
	if e := cc.cache.Update(newJob); e != nil {
		klog.Errorf("KillJob - Failed to update Job %v/%v in cache:  %v", newJob.Namespace, newJob.Name, e)
		return e
	}

	// 删除 Job 对应的 PodGroup。
	pg, err := cc.getPodGroupByJob(job)
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to find PodGroup of Job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
		return err
	}
	if pg != nil {
		if err := cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Delete(context.TODO(), pg.Name, metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to delete PodGroup of Job %s/%s: %v", job.Namespace, job.Name, err)
				return err
			}
		}
	}

	// 注意：这里不删除 input/output，等 Job 真正被删除时再处理。
	return nil
}

// initiateJob 初始化 Job。
// 包括初始化状态、执行插件、创建 PVC、创建或更新 PodGroup。
func (cc *jobcontroller) initiateJob(job *batch.Job) (*batch.Job, error) {
	klog.V(3).Infof("Starting to initiate Job <%s/%s>", job.Namespace, job.Name)
	jobInstance, err := cc.initJobStatus(job)
	if err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.JobStatusError), fmt.Sprintf("Failed to initialize job status, err: %v", err))
		return nil, err
	}
	if err := cc.pluginOnJobAdd(jobInstance); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PluginError), fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return nil, err
	}
	newJob, err := cc.createJobIOIfNotExist(jobInstance)
	if err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PVCError), fmt.Sprintf("Failed to create PVC, err: %v", err))
		return nil, err
	}
	if err := cc.createOrUpdatePodGroup(newJob); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PodGroupError), fmt.Sprintf("Failed to create PodGroup, err: %v", err))
		return nil, err
	}
	return newJob, nil
}

// initOnJobUpdate 在 Job 更新时执行。
// 与首次初始化不同，这里主要执行插件更新逻辑和 PodGroup 同步。
func (cc *jobcontroller) initOnJobUpdate(job *batch.Job) error {
	klog.V(3).Infof("Starting to initiate Job <%s/%s> on update", job.Namespace, job.Name)
	if err := cc.pluginOnJobUpdate(job); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PluginError), fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return err
	}
	if err := cc.createOrUpdatePodGroup(job); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PodGroupError), fmt.Sprintf("Failed to create PodGroup, err: %v", err))
		return err
	}
	return nil
}

// GetQueueInfo 获取 Job 所属 Queue。
func (cc *jobcontroller) GetQueueInfo(queue string) (*scheduling.Queue, error) {
	queueInfo, err := cc.queueLister.Get(queue)
	if err != nil {
		klog.Errorf("Failed to get queue from queueLister, error: %s", err.Error())
	}
	return queueInfo, err
}

// syncJob 是 Job Controller 的核心同步函数。
// 它负责初始化 Job、创建 PodGroup、等待 PodGroup 调度条件、创建 Pod、删除多余 Pod、统计状态、更新 Job Status。
func (cc *jobcontroller) syncJob(jobInfo *apis.JobInfo, updateStatus state.UpdateStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Starting to sync up Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> sync up, current version %d", job.Namespace, job.Name, job.Status.Version)

	if jobInfo.Job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.", jobInfo.Job.Namespace, jobInfo.Job.Name)
		return nil
	}

	// 深拷贝，避免直接修改 informer cache 中的对象。
	job = job.DeepCopy()

	queueInfo, err := cc.GetQueueInfo(job.Spec.Queue)
	if err != nil {
		return err
	}

	// 如果 Queue 配置了 ExtendClusters，说明该 Job 需要跨集群转发。
	var jobForwarding bool
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		if len(job.Annotations) == 0 {
			job.Annotations = make(map[string]string)
		}
		job.Annotations[batch.JobForwardingKey] = "true"
		job, err = cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	if !isInitiated(job) {
		if job, err = cc.initiateJob(job); err != nil {
			return err
		}
	} else {
		if err = cc.initOnJobUpdate(job); err != nil {
			return err
		}
	}

	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		job.Annotations[batch.JobForwardingKey] = "true"
		_, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	var syncTask bool
	pg, err := cc.getPodGroupByJob(job)
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to find PodGroup of Job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
		return err
	}
	if pg != nil {
		// 只有 PodGroup 状态不为空且不为 Pending，才开始同步 Pod。
		// 这体现了 Volcano gang scheduling 的设计：先等 PodGroup 被调度器确认，再创建具体 Pod。
		if pg.Status.Phase != "" && pg.Status.Phase != scheduling.PodGroupPending {
			syncTask = true
		}
		cc.recordPodGroupEvent(job, pg)
	}

	var jobCondition batch.JobCondition
	oldStatus := job.Status

	if !syncTask {
		if updateStatus != nil {
			updateStatus(&job.Status)
		}
		if equality.Semantic.DeepEqual(job.Status, oldStatus) {
			klog.V(4).Infof("Job <%s/%s> has not updated for no changing", job.Namespace, job.Name)
			return nil
		}
		job.Status.State.LastTransitionTime = metav1.Now()
		jobCondition = newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
		job.Status.Conditions = append(job.Status.Conditions, jobCondition)
		newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update status of Job %v/%v: %v", job.Namespace, job.Name, err)
			return err
		}
		if e := cc.cache.Update(newJob); e != nil {
			klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v", newJob.Namespace, newJob.Name, e)
			return e
		}
		return nil
	}

	var running, pending, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batch.TaskState)
	podToCreate := make(map[string][]*v1.Pod)
	var podToDelete []*v1.Pod
	var creationErrs []error
	var deletionErrs []error
	appendMutex := sync.Mutex{}

	appendError := func(container *[]error, err error) {
		appendMutex.Lock()
		defer appendMutex.Unlock()
		*container = append(*container, err)
	}

	waitCreationGroup := sync.WaitGroup{}

	for _, ts := range job.Spec.Tasks {
		ts.Template.Name = ts.Name
		tc := ts.Template.DeepCopy()
		name := ts.Template.Name
		pods, found := jobInfo.Pods[name]
		if !found {
			pods = map[string]*v1.Pod{}
		}

		var podToCreateEachTask []*v1.Pod
		for i := 0; i < int(ts.Replicas); i++ {
			podName := fmt.Sprintf(jobhelpers.PodNameFmt, job.Name, name, i)
			if pod, found := pods[podName]; !found {
				// 如果期望的 Pod 不存在，则创建。
				newPod := createJobPod(job, tc, i, jobForwarding, pg, &ts)
				if err := cc.pluginOnPodCreate(job, newPod); err != nil {
					return err
				}
				podToCreateEachTask = append(podToCreateEachTask, newPod)
				waitCreationGroup.Add(1)
			} else {
				// 已存在的 Pod 从 map 中删除。
				// 循环结束后，map 中剩余的 Pod 就是超出 replicas 的多余 Pod。
				delete(pods, podName)
				if pod.DeletionTimestamp != nil {
					klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
					atomic.AddInt32(&terminating, 1)
					continue
				}
				if jobhelpers.IsOutOfSyncPod(pod) {
					podToDelete = append(podToDelete, pod)
				}
				classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
				calcPodStatus(pod, taskStatusCount)
			}
		}

		podToCreate[ts.Name] = podToCreateEachTask

		// 剩余 Pod 是多余 Pod，需要删除，例如缩容场景。
		for _, pod := range pods {
			podToDelete = append(podToDelete, pod)
		}
	}

	for taskName, podToCreateEachTask := range podToCreate {
		if len(podToCreateEachTask) == 0 {
			continue
		}
		go func(taskName string, podToCreateEachTask []*v1.Pod) {
			taskIndex := jobhelpers.GetTaskIndexUnderJob(taskName, job)
			if job.Spec.Tasks[taskIndex].DependsOn != nil {
				if !cc.waitDependsOnTaskMeetCondition(taskIndex, job) {
					klog.V(3).Infof("Job %s/%s depends on task not ready", job.Name, job.Namespace)
					for _, pod := range podToCreateEachTask {
						go func(pod *v1.Pod) {
							defer waitCreationGroup.Done()
						}(pod)
					}
					return
				}
			}

			for _, pod := range podToCreateEachTask {
				go func(pod *v1.Pod) {
					defer waitCreationGroup.Done()
					newPod, err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
					if err != nil {
						if apierrors.IsAlreadyExists(err) {
							klog.V(4).Infof("Pod %s for Job %s already exists, skipping", pod.Name, job.Name)
						} else {
							klog.Errorf("Failed to create pod %s for Job %s, err %#v", pod.Name, job.Name, err)
							appendError(&creationErrs, fmt.Errorf("failed to create pod %s, err: %#v", pod.Name, err))
						}
					} else {
						classifyAndAddUpPodBaseOnPhase(newPod, &pending, &running, &succeeded, &failed, &unknown)
						calcPodStatus(newPod, taskStatusCount)
						metrics.ObserveJobToPodCreationLatency(newPod.CreationTimestamp.Sub(job.CreationTimestamp.Time))
						klog.V(5).InfoS("Created Pod for Job", "Job", klog.KObj(job), "Pod", klog.KObj(pod))
					}
				}(pod)
			}
		}(taskName, podToCreateEachTask)
	}

	waitCreationGroup.Wait()

	if len(creationErrs) != 0 {
		cc.recorder.Event(job, v1.EventTypeWarning, FailedCreatePodReason, fmt.Sprintf("Error creating pods: %+v", creationErrs))
		return fmt.Errorf("failed to create %d pods of %d", len(creationErrs), len(podToCreate))
	}

	waitDeletionGroup := sync.WaitGroup{}
	waitDeletionGroup.Add(len(podToDelete))
	for _, pod := range podToDelete {
		go func(pod *v1.Pod) {
			defer waitDeletionGroup.Done()
			err := cc.deleteJobPod(job.Name, pod)
			if err != nil {
				klog.Errorf("Failed to delete pod %s for Job %s, err %#v", pod.Name, job.Name, err)
				appendError(&deletionErrs, err)
				cc.resyncTask(pod)
			} else {
				klog.V(3).InfoS("Deleted Pod of Job", "Job", klog.KObj(job), "Pod", klog.KObj(pod), "UID", pod.UID)
				atomic.AddInt32(&terminating, 1)
			}
		}(pod)
	}
	waitDeletionGroup.Wait()

	if len(deletionErrs) != 0 {
		cc.recorder.Event(job, v1.EventTypeWarning, FailedDeletePodReason, fmt.Sprintf("Error deleting pods: %+v", deletionErrs))
		return fmt.Errorf("failed to delete %d pods of %d", len(deletionErrs), len(podToDelete))
	}

	newStatus := batch.JobStatus{
		State:               job.Status.State,
		Pending:             pending,
		Running:             running,
		Succeeded:           succeeded,
		Failed:              failed,
		Terminating:         terminating,
		Unknown:             unknown,
		Version:             job.Status.Version,
		MinAvailable:        job.Spec.MinAvailable,
		TaskStatusCount:     taskStatusCount,
		ControlledResources: job.Status.ControlledResources,
		Conditions:          job.Status.Conditions,
		RetryCount:          job.Status.RetryCount,
	}

	if updateStatus != nil {
		updateStatus(&newStatus)
	}

	if reflect.DeepEqual(job.Status, newStatus) {
		klog.V(3).Infof("Job <%s/%s> has not updated for no changing", job.Namespace, job.Name)
		return nil
	}

	job.Status = newStatus
	job.Status.State.LastTransitionTime = metav1.Now()
	jobCondition = newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
	job.Status.Conditions = append(job.Status.Conditions, jobCondition)

	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v", job.Namespace, job.Name, err)
		return err
	}
	if e := cc.cache.Update(newJob); e != nil {
		klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v", newJob.Namespace, newJob.Name, e)
		return e
	}

	return nil
}

// waitDependsOnTaskMeetCondition 判断当前 Task 依赖的 Task 是否满足启动条件。
// 如果当前 Task 没有 DependsOn，直接返回 true。
// 如果配置了 IterationAny，则任意一个依赖 Task 满足即可。
// 否则需要所有依赖 Task 都满足。
func (cc *jobcontroller) waitDependsOnTaskMeetCondition(taskIndex int, job *batch.Job) bool {
	if job.Spec.Tasks[taskIndex].DependsOn == nil {
		return true
	}
	dependsOn := *job.Spec.Tasks[taskIndex].DependsOn
	if len(dependsOn.Name) > 1 && dependsOn.Iteration == batch.IterationAny {
		for _, task := range dependsOn.Name {
			if cc.isDependsOnPodsReady(task, job) {
				return true
			}
		}
		return false
	}
	for _, dependsOnTask := range dependsOn.Name {
		if !cc.isDependsOnPodsReady(dependsOnTask, job) {
			return false
		}
	}
	return true
}

// isDependsOnPodsReady 判断依赖 Task 下的 Pod 是否 ready。
// 判断标准：
// 1. Pod Phase 是 Running 或 Succeeded；
// 2. 所有 Container 都 Ready；
// 3. 如果依赖 Task 设置了 MinAvailable，则 ready 数量不能小于 MinAvailable。
func (cc *jobcontroller) isDependsOnPodsReady(task string, job *batch.Job) bool {
	dependsOnPods := jobhelpers.GetPodsNameUnderTask(task, job)
	dependsOnTaskIndex := jobhelpers.GetTaskIndexUnderJob(task, job)
	runningPodCount := 0
	for _, podName := range dependsOnPods {
		pod, err := cc.podLister.Pods(job.Namespace).Get(podName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				_, errGetJob := cc.jobLister.Jobs(job.Namespace).Get(job.Name)
				if errGetJob != nil {
					return apierrors.IsNotFound(errGetJob)
				}
			}
			klog.Errorf("Failed to get pod %v/%v %v", job.Namespace, podName, err)
			continue
		}

		if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodSucceeded {
			klog.V(5).Infof("Sequential state, pod %v/%v of depends on tasks is not running", pod.Namespace, pod.Name)
			continue
		}

		allContainerReady := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if !containerStatus.Ready {
				allContainerReady = false
				break
			}
		}
		if allContainerReady {
			runningPodCount++
		}
	}

	dependsOnTaskMinReplicas := job.Spec.Tasks[dependsOnTaskIndex].MinAvailable
	if dependsOnTaskMinReplicas != nil {
		if runningPodCount < int(*dependsOnTaskMinReplicas) {
			klog.V(5).Infof("In a depends on startup state, there are already %d pods running, which is less than the minimum number of runs", runningPodCount)
			return false
		}
	}
	return true
}

// createJobIOIfNotExist 创建 Job 需要的 PVC。
// 如果 Job Spec 中 volume 没有指定 VolumeClaimName，则生成一个 PVC 名称并创建 PVC。
// 如果指定了 VolumeClaimName，则检查 PVC 是否存在。
// 同时会把受控资源写入 job.Status.ControlledResources。
func (cc *jobcontroller) createJobIOIfNotExist(job *batch.Job) (*batch.Job, error) {
	var needUpdate bool
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = make(map[string]string)
	}
	for index, volume := range job.Spec.Volumes {
		vcName := volume.VolumeClaimName
		if len(vcName) == 0 {
			for {
				vcName = jobhelpers.GenPVCName(job.Name)
				exist, err := cc.checkPVCExist(job, vcName)
				if err != nil {
					return job, err
				}
				if exist {
					continue
				}
				job.Spec.Volumes[index].VolumeClaimName = vcName
				needUpdate = true
				break
			}
			if volume.VolumeClaim != nil {
				if err := cc.createPVC(job, vcName, volume.VolumeClaim); err != nil {
					return job, err
				}
			}
		} else {
			exist, err := cc.checkPVCExist(job, vcName)
			if err != nil {
				return job, err
			}
			if !exist {
				return job, fmt.Errorf("pvc %s is not found, the job will be in the Pending state until the PVC is created", vcName)
			}
		}
		job.Status.ControlledResources["volume-pvc-"+vcName] = vcName
	}

	if needUpdate {
		newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update Job %v/%v for volume claim name: %v ", job.Namespace, job.Name, err)
			return job, err
		}
		newJob.Status = job.Status
		return newJob, err
	}
	return job, nil
}

// checkPVCExist 检查 PVC 是否存在。
// NotFound 返回 false,nil；其他错误返回 false,error。
func (cc *jobcontroller) checkPVCExist(job *batch.Job, pvc string) (bool, error) {
	if _, err := cc.pvcLister.PersistentVolumeClaims(job.Namespace).Get(pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		klog.V(3).Infof("Failed to get PVC %s for job <%s/%s>: %v", pvc, job.Namespace, job.Name, err)
		return false, err
	}
	return true, nil
}

// createPVC 创建 PVC，并设置 Job 为 OwnerReference。
// 这样 Job 删除后，Kubernetes 可以根据 owner reference 做级联清理。
func (cc *jobcontroller) createPVC(job *batch.Job, vcName string, volumeClaim *v1.PersistentVolumeClaimSpec) error {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      vcName,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, helpers.JobKind),
			},
		},
		Spec: *volumeClaim,
	}
	klog.V(3).Infof("Try to create PVC: %v", pvc)
	if _, e := cc.kubeClient.CoreV1().PersistentVolumeClaims(job.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{}); e != nil {
		klog.V(3).Infof("Failed to create PVC for Job <%s/%s>: %v", job.Namespace, job.Name, e)
		return e
	}
	return nil
}

// createOrUpdatePodGroup 创建或更新 Job 对应的 PodGroup。
// PodGroup 是 Volcano gang scheduling 的核心资源。
// 它描述了该 Job 至少需要多少 Pod、多少资源才能整体调度。
func (cc *jobcontroller) createOrUpdatePodGroup(job *batch.Job) error {
	pg, err := cc.getPodGroupByJob(job)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to get PodGroup for Job <%s/%s>: %v", job.Namespace, job.Name, err)
			return err
		}

		minTaskMember := map[string]int32{}
		for _, task := range job.Spec.Tasks {
			minTaskMember[task.Name] = cc.getMinTaskMember(task)
		}

		pg := &scheduling.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   job.Namespace,
				Name:        cc.generateRelatedPodGroupName(job),
				Annotations: job.Annotations,
				Labels:      job.Labels,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(job, helpers.JobKind),
				},
			},
			Spec: scheduling.PodGroupSpec{
				MinMember:         job.Spec.MinAvailable,
				MinTaskMember:     minTaskMember,
				Queue:             job.Spec.Queue,
				MinResources:      cc.calcPGMinResources(job),
				PriorityClassName: job.Spec.PriorityClassName,
			},
		}

		if job.Spec.NetworkTopology != nil {
			nt := &scheduling.NetworkTopologySpec{
				Mode: scheduling.NetworkTopologyMode(job.Spec.NetworkTopology.Mode),
			}
			if job.Spec.NetworkTopology.HighestTierAllowed != nil {
				nt.HighestTierAllowed = job.Spec.NetworkTopology.HighestTierAllowed
			} else if job.Spec.NetworkTopology.HighestTierName != "" {
				nt.HighestTierName = job.Spec.NetworkTopology.HighestTierName
			}
			pg.Spec.NetworkTopology = nt
		}

		setPgSubGroupPolicy(pg, job.Spec.Tasks)

		if _, err = cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				klog.Errorf("Failed to create PodGroup for Job <%s/%s>: %v", job.Namespace, job.Name, err)
				return err
			}
		}
		return nil
	}

	podGroupToUpdate := pg.DeepCopy()
	pgShouldUpdate := cc.shouldUpdateExistingPodGroup(podGroupToUpdate, job)
	if !pgShouldUpdate {
		return nil
	}

	_, err = cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Update(context.TODO(), podGroupToUpdate, metav1.UpdateOptions{})
	if err != nil {
		klog.V(3).Infof("Failed to update PodGroup for Job <%s/%s>: %v", job.Namespace, job.Name, err)
	}
	return err
}

// getMinTaskMember 获取某个 Task 在 PodGroup 中的最小成员数。
// 优先级：
// 1. task.MinAvailable；
// 2. PartitionPolicy 中的 MinPartitions * PartitionSize；
// 3. task.Replicas。
func (cc *jobcontroller) getMinTaskMember(task batch.TaskSpec) int32 {
	if task.MinAvailable != nil {
		return *task.MinAvailable
	}
	if task.PartitionPolicy != nil && task.PartitionPolicy.MinPartitions != 0 && task.PartitionPolicy.PartitionSize != 0 {
		return task.PartitionPolicy.MinPartitions * task.PartitionPolicy.PartitionSize
	}
	return task.Replicas
}

// shouldUpdateExistingPodGroup 判断已有 PodGroup 是否需要更新。
// 会比较 PriorityClassName、MinMember、MinResources、MinTaskMember、SubGroupPolicy 等字段。
func (cc *jobcontroller) shouldUpdateExistingPodGroup(pg *scheduling.PodGroup, job *batch.Job) bool {
	pgShouldUpdate := false

	if pg.Spec.PriorityClassName != job.Spec.PriorityClassName {
		pg.Spec.PriorityClassName = job.Spec.PriorityClassName
		pgShouldUpdate = true
	}

	minResources := cc.calcPGMinResources(job)
	if pg.Spec.MinMember != job.Spec.MinAvailable || !equality.Semantic.DeepEqual(pg.Spec.MinResources, minResources) {
		pg.Spec.MinMember = job.Spec.MinAvailable
		pg.Spec.MinResources = minResources
		pgShouldUpdate = true
	}

	if pg.Spec.MinTaskMember == nil {
		pgShouldUpdate = true
		pg.Spec.MinTaskMember = make(map[string]int32)
	}

	for _, task := range job.Spec.Tasks {
		cnt := cc.getMinTaskMember(task)
		if taskMember, ok := pg.Spec.MinTaskMember[task.Name]; !ok {
			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = cnt
		} else {
			if taskMember == cnt {
				continue
			}
			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = cnt
		}
	}

	if updatePgSubGroupPolicy(pg, job.Spec.Tasks) {
		pgShouldUpdate = true
	}

	return pgShouldUpdate
}

// deleteJobPod 删除 Job 下的 Pod。
// NotFound 不认为是错误，因为目标 Pod 已经不存在，符合删除预期。
func (cc *jobcontroller) deleteJobPod(jobName string, pod *v1.Pod) error {
	err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete pod %s/%s for Job %s, err %#v", pod.Namespace, pod.Name, jobName, err)
		return fmt.Errorf("failed to delete pod %s, err %#v", pod.Name, err)
	}
	return nil
}

// calcPGMinResources 计算 PodGroup 的最小资源需求。
// 这里会考虑 Task 优先级和 MinAvailable。
// 如果 job.MinAvailable 小于所有 task.MinAvailable 之和，则只计算前 job.MinAvailable 个 Pod 的资源。
// 否则计算满足所有 task 最小需求时的资源。
func (cc *jobcontroller) calcPGMinResources(job *batch.Job) *v1.ResourceList {
	var tasksPriority TasksPriority
	totalMinAvailable := int32(0)

	for _, task := range job.Spec.Tasks {
		tp := TaskPriority{0, task}
		pc := task.Template.Spec.PriorityClassName
		if pc != "" {
			priorityClass, err := cc.pcLister.Get(pc)
			if err != nil || priorityClass == nil {
				klog.Warningf("Ignore task %s priority class %s: %v", task.Name, pc, err)
			} else {
				tp.priority = priorityClass.Value
			}
		}
		tasksPriority = append(tasksPriority, tp)

		if task.MinAvailable != nil {
			totalMinAvailable += *task.MinAvailable
		} else {
			totalMinAvailable += task.Replicas
		}
	}

	if job.Spec.MinAvailable < totalMinAvailable {
		minReq := tasksPriority.CalcFirstCountResources(job.Spec.MinAvailable)
		return &minReq
	}

	minReq := tasksPriority.CalcPGMinResources(job.Spec.MinAvailable)
	return &minReq
}

// initJobStatus 初始化 Job 状态。
// 如果 Job 状态已经存在，则直接返回。
// 否则设置为 Pending，并写入 MinAvailable、Condition 等信息。
func (cc *jobcontroller) initJobStatus(job *batch.Job) (*batch.Job, error) {
	if job.Status.State.Phase != "" {
		return job, nil
	}

	job.Status.State.Phase = batch.Pending
	job.Status.State.LastTransitionTime = metav1.Now()
	job.Status.MinAvailable = job.Spec.MinAvailable
	jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
	job.Status.Conditions = append(job.Status.Conditions, jobCondition)

	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v", job.Namespace, job.Name, err)
		return nil, err
	}
	if err := cc.cache.Update(newJob); err != nil {
		klog.Errorf("CreateJob - Failed to update Job %v/%v in cache:  %v", newJob.Namespace, newJob.Name, err)
		return nil, err
	}
	return newJob, nil
}

// recordPodGroupEvent 根据 PodGroup 最新 Condition 记录事件。
// 如果最新 Condition 不是 Scheduled，则记录 Warning 事件，帮助用户了解 Pending 原因。
func (cc *jobcontroller) recordPodGroupEvent(job *batch.Job, podGroup *scheduling.PodGroup) {
	var latestCondition *scheduling.PodGroupCondition

	for _, condition := range podGroup.Status.Conditions {
		if condition.Status == v1.ConditionTrue {
			if latestCondition == nil || condition.LastTransitionTime.Time.After(latestCondition.LastTransitionTime.Time) {
				latestCondition = &condition
			}
		}
	}

	if latestCondition != nil && latestCondition.Type != scheduling.PodGroupScheduled {
		cc.recorder.Eventf(job, v1.EventTypeWarning, string(batch.PodGroupPending), fmt.Sprintf("PodGroup %s:%s %s, reason: %s", job.Namespace, job.Name, strings.ToLower(string(latestCondition.Type)), latestCondition.Message))
	}
}

// classifyAndAddUpPodBaseOnPhase 根据 Pod Phase 统计数量。
// 使用 atomic 是因为该函数可能在并发创建 Pod 的 goroutine 中被调用。
func classifyAndAddUpPodBaseOnPhase(pod *v1.Pod, pending, running, succeeded, failed, unknown *int32) {
	switch pod.Status.Phase {
	case v1.PodPending:
		atomic.AddInt32(pending, 1)
	case v1.PodRunning:
		atomic.AddInt32(running, 1)
	case v1.PodSucceeded:
		atomic.AddInt32(succeeded, 1)
	case v1.PodFailed:
		atomic.AddInt32(failed, 1)
	default:
		atomic.AddInt32(unknown, 1)
	}
}

// calcPodStatus 按 Task 维度统计 Pod 状态。
// taskStatusCount 的结构大致是：
// taskName -> PodPhase -> count。
// 因为 map 并发写不安全，所以这里使用 calMutex。
func calcPodStatus(pod *v1.Pod, taskStatusCount map[string]batch.TaskState) {
	taskName, found := pod.Annotations[batch.TaskSpecKey]
	if !found {
		return
	}

	calMutex.Lock()
	defer calMutex.Unlock()

	if _, ok := taskStatusCount[taskName]; !ok {
		taskStatusCount[taskName] = batch.TaskState{
			Phase: make(map[v1.PodPhase]int32),
		}
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		taskStatusCount[taskName].Phase[v1.PodPending]++
	case v1.PodRunning:
		taskStatusCount[taskName].Phase[v1.PodRunning]++
	case v1.PodSucceeded:
		taskStatusCount[taskName].Phase[v1.PodSucceeded]++
	case v1.PodFailed:
		taskStatusCount[taskName].Phase[v1.PodFailed]++
	default:
		taskStatusCount[taskName].Phase[v1.PodUnknown]++
	}
}

// isInitiated 判断 Job 是否已经初始化
// 如果 Phase 为空或者 Pending，认为还没有真正完成初始化
func isInitiated(job *batch.Job) bool {
	if job.Status.State.Phase == "" || job.Status.State.Phase == batch.Pending {
		return false
	}
	return true
}

// newCondition 构造一个 JobCondition。
func newCondition(status batch.JobPhase, lastTransitionTime *metav1.Time) batch.JobCondition {
	return batch.JobCondition{
		Status:             status,
		LastTransitionTime: lastTransitionTime,
	}
}

// setPgSubGroupPolicy 根据 Task 的 PartitionPolicy 初始化 PodGroup 的 SubGroupPolicy
// SubGroupPolicy 用于描述分区调度策略
func setPgSubGroupPolicy(pg *scheduling.PodGroup, tasks []batch.TaskSpec) {
	pg.Spec.SubGroupPolicy = make([]scheduling.SubGroupPolicySpec, 0)
	for _, taskSpec := range tasks {
		if taskSpec.PartitionPolicy == nil {
			continue
		}
		subGroupPolicy := getSubGroupPolicy(taskSpec)
		pg.Spec.SubGroupPolicy = append(pg.Spec.SubGroupPolicy, subGroupPolicy)
	}
}

// updatePgSubGroupPolicy 更新 PodGroup 的 SubGroupPolicy
// 如果新旧 SubGroupPolicy 不一致，则返回 true，表示需要更新 PodGroup
func updatePgSubGroupPolicy(pg *scheduling.PodGroup, tasks []batch.TaskSpec) bool {
	subGroupPolicyShouldUpdate := false

	oldSubGroupPolicyMap := make(map[string]scheduling.SubGroupPolicySpec)
	for _, subGroupPolicy := range pg.Spec.SubGroupPolicy {
		oldSubGroupPolicyMap[subGroupPolicy.Name] = subGroupPolicy
	}

	newSubGroupPolicyList := make([]scheduling.SubGroupPolicySpec, 0)
	for _, taskSpec := range tasks {
		if taskSpec.PartitionPolicy == nil {
			if _, ok := oldSubGroupPolicyMap[taskSpec.Name]; ok {
				subGroupPolicyShouldUpdate = true
			}
			continue
		}

		newSubGroupPolicy := getSubGroupPolicy(taskSpec)
		newSubGroupPolicyList = append(newSubGroupPolicyList, newSubGroupPolicy)

		if !equality.Semantic.DeepEqual(newSubGroupPolicy, oldSubGroupPolicyMap[taskSpec.Name]) {
			subGroupPolicyShouldUpdate = true
		}
	}

	if subGroupPolicyShouldUpdate {
		pg.Spec.SubGroupPolicy = newSubGroupPolicyList
	}

	return subGroupPolicyShouldUpdate
}

// getSubGroupPolicy 根据 TaskSpec 生成 SubGroupPolicy。
// 主要包括：
// 1. SubGroup 名称；
// 2. 分组大小；
// 3. 最小分组数；
// 4. LabelSelector；
// 5. MatchLabelKeys；
// 6. NetworkTopology。
func getSubGroupPolicy(taskSpec batch.TaskSpec) scheduling.SubGroupPolicySpec {
	subGroupPolicy := scheduling.SubGroupPolicySpec{
		Name:         taskSpec.Name,
		SubGroupSize: &taskSpec.PartitionPolicy.PartitionSize,
		MinSubGroups: &taskSpec.PartitionPolicy.MinPartitions,
	}

	if taskSpec.PartitionPolicy != nil {
		subGroupPolicy.LabelSelector = &metav1.LabelSelector{
			MatchLabels: map[string]string{
				batch.TaskSpecKey: taskSpec.Name,
			},
		}
	}

	subGroupPolicy.MatchLabelKeys = []string{batch.TaskPartitionID}

	if taskSpec.PartitionPolicy.NetworkTopology != nil {
		nt := &scheduling.NetworkTopologySpec{
			Mode:               scheduling.NetworkTopologyMode(taskSpec.PartitionPolicy.NetworkTopology.Mode),
			HighestTierAllowed: taskSpec.PartitionPolicy.NetworkTopology.HighestTierAllowed,
			HighestTierName:    taskSpec.PartitionPolicy.NetworkTopology.HighestTierName,
		}
		subGroupPolicy.NetworkTopology = nt
	}

	return subGroupPolicy
}
