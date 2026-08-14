/*
Copyright 2017 The Volcano Authors.

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

package job

import (
	"fmt"
	"sort"
	"strconv"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog/v2"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	schedulingv2 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/apis"
	jobcache "volcano.sh/volcano/pkg/controllers/cache"
	jobhelpers "volcano.sh/volcano/pkg/controllers/job/helpers"
	"volcano.sh/volcano/pkg/controllers/job/state"
	"volcano.sh/volcano/pkg/controllers/util"
)

// MakePodName 根据 Job 名称、Task 名称和副本索引生成 Pod 名称。
//
// 命名格式由 jobhelpers.PodNameFmt 定义，通常为 "<jobName>-<taskName>-<index>"。
// 例如：Job "training"，Task "worker"，Index 0 → "training-worker-0"。
func MakePodName(jobName string, taskName string, index int) string {
	return fmt.Sprintf(jobhelpers.PodNameFmt, jobName, taskName, index)
}

// createJobPod 根据 Job 的 Task 模板创建一个 Pod 对象。
//
// 该函数不会真正向 Kubernetes 提交 Pod，而是构造一个内存中的 Pod 对象，
// 后续由 controller 真正创建。
//
// 参数：
//   - job: 所属的 Volcano Job；
//   - template: Task 的 Pod 模板；
//   - ix: 该 Pod 在 Task 副本中的索引（从 0 开始）；
//   - jobForwarding: 是否启用 Job 转发（跨集群场景）；
//   - pg: 关联的 PodGroup（用于 gang scheduling）；
//   - ts: Task 规格定义。
//
// 核心流程：
//  1. 深拷贝 Pod 模板，避免修改原始模板；
//  2. 构造 Pod 基础元数据（名称、命名空间、OwnerReference）；
//  3. 继承 Job 的 SchedulerName 和 PriorityClassName；
//  4. 挂载 Job 声明的 Volume（PVC）；
//  5. 设置 Pod Annotations（TaskIndex、TaskSpec、PodGroup、Queue 等调度关键信息）；
//  6. 设置 Pod Labels（用于 Service 发现和调度匹配）；
//  7. 从 Job 继承调度相关 Annotations（PodPreemptable、CooldownTime 等）；
//  8. 设置 Partition 标签（用于分布式训练的分组调度）。
func createJobPod(job *batch.Job, template *v1.PodTemplateSpec, ix int, jobForwarding bool, pg *schedulingv2.PodGroup, ts *batch.TaskSpec) *v1.Pod {
	// 步骤 1：深拷贝模板，防止后续修改影响原始模板。
	templateCopy := template.DeepCopy()

	// 步骤 2：构造 Pod 基础元数据。
	// OwnerReference 指向 Job，确保 Job 删除时 Pod 被级联删除。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobhelpers.MakePodName(job.Name, template.Name, ix),
			Namespace: job.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, helpers.JobKind),
			},
			Labels:      templateCopy.Labels,
			Annotations: templateCopy.Annotations,
		},
		Spec: templateCopy.Spec,
	}

	// 步骤 3：如果 Pod 模板没有指定 SchedulerName，则继承 Job 的 SchedulerName。
	// 这确保了 Pod 会被 Volcano 调度器而非默认调度器处理。
	if len(pod.Spec.SchedulerName) == 0 {
		pod.Spec.SchedulerName = job.Spec.SchedulerName
	}

	// 如果 Pod 模板没有指定 PriorityClassName，则继承 Job 的 PriorityClassName。
	if len(pod.Spec.PriorityClassName) == 0 && len(job.Spec.PriorityClassName) != 0 {
		pod.Spec.PriorityClassName = job.Spec.PriorityClassName
	}

	// 步骤 4：挂载 Job 声明的 Volume。
	//
	// Job 可以在 Spec.Volumes 中声明共享存储（PVC）。
	// 同一个 VolumeClaimName 只创建一次 Volume，避免重复挂载。
	// 每个 Container 都会挂载该 Volume 到指定路径。
	volumeMap := make(map[string]string)
	for _, volume := range job.Spec.Volumes {
		vcName := volume.VolumeClaimName
		// 为 Volume 生成唯一名称，避免同名冲突。
		name := fmt.Sprintf("%s-%s", job.Name, jobhelpers.GenRandomStr(12))
		if _, ok := volumeMap[vcName]; !ok {
			volume := v1.Volume{
				Name: name,
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: vcName,
					},
				},
			}
			pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
			volumeMap[vcName] = name
		} else {
			// 同一个 PVC 已经挂载过，跳过重复 Volume。
			continue
		}

		// 将该 Volume 挂载到每个 Container 的指定路径。
		for i, c := range pod.Spec.Containers {
			vm := v1.VolumeMount{
				MountPath: volume.MountPath,
				Name:      name,
			}
			pod.Spec.Containers[i].VolumeMounts = append(c.VolumeMounts, vm)
		}
	}

	// 步骤 5：设置 Task 相关 Annotations。
	//
	// 这些 Annotations 是 Volcano 调度器和 controller 识别 Pod 归属的关键信息。
	tsKey := templateCopy.Name
	if len(tsKey) == 0 {
		tsKey = batch.DefaultTaskSpec
	}

	if len(pod.Annotations) == 0 {
		pod.Annotations = make(map[string]string)
	}

	index := strconv.Itoa(ix)
	// TaskIndex：该 Pod 在 Task 中的副本索引。
	pod.Annotations[batch.TaskIndex] = index
	// TaskSpecKey：该 Pod 属于哪个 Task（如 "worker"、"ps"）。
	pod.Annotations[batch.TaskSpecKey] = tsKey
	// KubeGroupNameAnnotationKey：关联的 PodGroup 名称，用于 gang scheduling。
	pgName := job.Name + "-" + string(job.UID)
	pod.Annotations[schedulingv2.KubeGroupNameAnnotationKey] = pgName
	// JobNameKey：所属 Job 名称。
	pod.Annotations[batch.JobNameKey] = job.Name
	// QueueNameKey：所属 Queue 名称，用于队列资源管理。
	pod.Annotations[batch.QueueNameKey] = job.Spec.Queue
	// JobVersion：Job 版本号，用于判断请求是否过期。
	pod.Annotations[batch.JobVersion] = fmt.Sprintf("%d", job.Status.Version)
	// PodTemplateKey：Job 名称 + 模板名称，用于标识 Pod 来源模板。
	pod.Annotations[batch.PodTemplateKey] = fmt.Sprintf("%s-%s", job.Name, template.Name)
	// JobRetryCountKey：Job 当前重试次数。
	pod.Annotations[batch.JobRetryCountKey] = strconv.Itoa(int(job.Status.RetryCount))

	// 如果 Task 指定了 NUMA 拓扑策略，则设置到 Pod Annotations 中。
	if ts.TopologyPolicy != "" {
		pod.Annotations[schedulingv2.NumaPolicyKey] = string(ts.TopologyPolicy)
	}

	// 步骤 6：从 Job Annotations 继承调度相关配置。
	//
	// 这些配置影响调度器的抢占、冷却、可撤销区域等行为。
	if len(job.Annotations) > 0 {
		// PodPreemptable：是否允许该 Pod 被抢占。
		if value, found := job.Annotations[schedulingv2.PodPreemptable]; found {
			pod.Annotations[schedulingv2.PodPreemptable] = value
		}
		// CooldownTime：Pod 释放资源后的冷却时间，避免资源频繁震荡。
		if value, found := job.Annotations[schedulingv2.CooldownTime]; found {
			pod.Annotations[schedulingv2.CooldownTime] = value
		}
		// RevocableZone：可撤销区域，标识该 Pod 可以被调度到哪些区域。
		if value, found := job.Annotations[schedulingv2.RevocableZone]; found {
			pod.Annotations[schedulingv2.RevocableZone] = value
		}

		// JobDisruptionBudget：Job 的 disruptions 预算，类似 PodDisruptionBudget。
		// JDBMinAvailable 和 JDBMaxUnavailable 二选一。
		if value, found := job.Annotations[schedulingv2.JDBMinAvailable]; found {
			pod.Annotations[schedulingv2.JDBMinAvailable] = value
		} else if value, found := job.Annotations[schedulingv2.JDBMaxUnavailable]; found {
			pod.Annotations[schedulingv2.JDBMaxUnavailable] = value
		}
	}

	// 步骤 7：设置 Pod Labels。
	//
	// Labels 主要用于 Service 发现和调度匹配。
	if len(pod.Labels) == 0 {
		pod.Labels = make(map[string]string)
	}

	// 设置与 Service 关联的 Labels。
	pod.Labels[batch.TaskIndex] = index
	pod.Labels[batch.JobNameKey] = job.Name
	pod.Labels[batch.TaskSpecKey] = tsKey
	pod.Labels[batch.JobNamespaceKey] = job.Namespace
	pod.Labels[batch.QueueNameKey] = job.Spec.Queue
	if len(job.Labels) > 0 {
		if value, found := job.Labels[schedulingv2.PodPreemptable]; found {
			pod.Labels[schedulingv2.PodPreemptable] = value
		}
		if value, found := job.Labels[schedulingv2.CooldownTime]; found {
			pod.Labels[schedulingv2.CooldownTime] = value
		}
	}

	// 步骤 8：设置 Job 转发标记。
	//
	// 跨集群调度场景中，jobForwarding 为 true 表示该 Pod 需要被转发到远端集群。
	if jobForwarding {
		pod.Annotations[batch.JobForwardingKey] = "true"
		pod.Labels[batch.JobForwardingKey] = "true"
	}

	// 步骤 9：设置 Partition 标签。
	//
	// 当 Task 配置了 PartitionPolicy 时，Pod 会被分配到不同的 Partition 分组。
	// Partition 用于分布式训练中的分组调度，确保同一组 Pod 调度到同一拓扑域。
	//
	// 分配规则：partitionID = podIndex / partitionSize。
	// 例如：10 个 Pod，partitionSize=5，则 Pod 0-4 属于 Partition 0，Pod 5-9 属于 Partition 1。
	if ts.PartitionPolicy != nil {
		var partitionID int
		// 非法 partitionSize 兜底处理，默认归入 Partition 0。
		if ts.PartitionPolicy.PartitionSize <= 0 {
			partitionID = 0
		} else {
			partitionID = ix / int(ts.PartitionPolicy.PartitionSize)
		}
		pod.Labels[batch.TaskPartitionID] = strconv.Itoa(partitionID)
	}

	return pod
}

// applyPolicies 根据 Job 的生命周期策略（LifecyclePolicy）计算当前事件应该执行的动作。
//
// 该函数是 Volcano Job Controller 的核心决策逻辑。
// 每当 Job/Pod 发生事件（PodPending、PodFailed、CommandIssued 等）时，
// controller 会调用该函数决定下一步应该做什么。
//
// 参数：
//   - job: 当前 Volcano Job 对象；
//   - req: 触发本次计算的事件请求。
//
// 返回值：
//   - delayAct: 包含要执行的动作、延迟时间、关联 Pod 等信息。
//     如果 delay == 0，表示立即执行；否则表示需要延迟执行。
//
// 核心流程：
//  1. 初始化默认动作（SyncJobAction）；
//  2. 如果 Request 已指定 Action，直接返回（Command 场景）；
//  3. 如果是内部事件，不做任何动作；
//  4. 校验 Job UID，防止同名 Job 的事件混淆；
//  5. 校验 Job Version，丢弃过期请求；
//  6. 匹配 Task 级别策略（优先级高于 Job 级别）；
//  7. 匹配 Job 级别策略；
//  8. 如果都不匹配，返回默认的 SyncJobAction。
//
// 策略匹配规则：
//   - 先匹配 Event（事件类型）或 AnyEvent（通配）；
//   - 再匹配 ExitCode（容器退出码）；
//   - 对于 PodPendingEvent，需要检查是否配置了 Timeout：
//     - 有 Timeout：设置 delay，延迟执行；
//     - 无 Timeout：跳过该策略，继续匹配下一条（相当于“不处理”）。
func applyPolicies(job *batch.Job, req *apis.Request) (delayAct *delayAction) {
	// 步骤 1：初始化 delayAction，默认动作为 SyncJobAction。
	//
	// SyncJobAction 是“安全默认值”，表示仅同步 Job 状态，不做额外操作。
	// 如果后续没有匹配到任何策略，就会执行这个默认动作。
	delayAct = &delayAction{
		jobKey:    jobcache.JobKeyByReq(req),
		event:     req.Event,
		taskName:  req.TaskName,
		podName:   req.PodName,
		podUID:    req.PodUID,
		partition: req.PartitionID,
		action:    v1alpha1.SyncJobAction,
	}

	// 步骤 2：如果 Request 已经指定了 Action，直接返回。
	//
	// 这种情况通常来自 Command 资源（用户通过 Command 显式触发某个动作）。
	// Command 指定的 Action 优先级最高，不需要再匹配策略。
	if len(req.Action) != 0 {
		delayAct.action = req.Action
		return
	}

	// 步骤 3：内部事件不触发任何动作。
	//
	// 内部事件包括：OutOfSyncEvent、CommandIssuedEvent、PodRunningEvent。
	// 这些事件是 controller 内部同步行为产生的，不应该触发策略执行。
	if isInternalEvent(req.Event) {
		return
	}

	// 步骤 4：校验 Job UID，防止同名 Job 事件混淆。
	//
	// 场景：
	//   - 旧 Job 被删除，新 Job 使用相同名称创建；
	//   - 旧 Job 的 Pod 事件延迟到达，此时 cache 中已经是新 Job；
	//   - 如果不校验 UID，旧 Pod 的事件可能会影响新 Job 的状态。
	//
	// 解决：如果 UID 不匹配，回退到 SyncJobAction，不做额外操作。
	if len(req.JobUid) != 0 && job != nil && req.JobUid != job.UID {
		klog.V(2).Infof("The req belongs to job(%s/%s) and job uid is %v, but the uid of job(%s/%s) is %v in cache, perform %v action",
			req.Namespace, req.JobName, req.JobUid, job.Namespace, job.Name, job.UID, v1alpha1.SyncJobAction)
		return
	}

	// 步骤 5：校验 Job Version，丢弃过期请求。
	//
	// Job 每次状态变更都会递增 Version。
	// 如果请求的 Version 小于 cache 中的 Version，说明该请求是基于旧状态产生的，
	// 已经过时，应该回退到 SyncJobAction 重新同步。
	if req.JobVersion < job.Status.Version {
		klog.Infof("Request %s is outdated, will perform sync instead.", req)
		return
	}

	// 步骤 6：匹配 Task 级别策略。
	//
	// Task 级别策略优先级高于 Job 级别。
	// 如果请求来自某个具体 Task（req.TaskName != ""），先尝试匹配该 Task 的策略。
	// 匹配成功后立即返回，不再检查 Job 级别策略。
	if len(req.TaskName) != 0 {
		// 遍历 Job 的所有 Task，找到对应的 Task。
		for _, task := range job.Spec.Tasks {
			if task.Name == req.TaskName {
				// 遍历该 Task 的所有生命周期策略。
				for _, policy := range task.Policies {
					policyEvents := getEventlist(policy)

					// 步骤 6.1：按事件类型匹配。
					//
					// 如果策略声明了 Events（列表）或 Event（单个），
					// 检查当前请求的事件是否在策略的事件列表中，或者策略是否声明了 AnyEvent（通配）。
					if len(policyEvents) > 0 && len(req.Event) > 0 {
						if checkEventExist(policyEvents, req.Event) || checkEventExist(policyEvents, v1alpha1.AnyEvent) {
							// 检查是否需要配置超时。
							//
							// shouldConfigureTimeout 只对 PodPendingEvent 返回 true。
							// 对于 PodPendingEvent：
							//   - 如果 policy.Timeout != nil：设置 delay，延迟执行；
							//   - 如果 policy.Timeout == nil：跳过该策略，继续匹配下一条。
							//     这是因为 PodPending 没有 timeout 时，不应该立即执行动作，
							//     而是等待用户显式配置超时时间。
							// 对于其他事件（PodFailed、PodEvicted 等）：
							//   - 不需要 timeout 配置，直接执行动作。
							if !shouldConfigureTimeout(req.Event) || policy.Timeout != nil {
								delayAct.action = policy.Action
								if policy.Timeout != nil {
									delayAct.delay = policy.Timeout.Duration
								}
								return
							}
						}
					}

					// 步骤 6.2：按容器退出码匹配。
					//
					// 用户可以根据容器退出码定义不同的处理策略。
					// 例如：退出码 137（OOMKilled）时重启任务，退出码 1 时终止 Job。
					// 注意：退出码 0 在 validation webhook 中已被阻止，不会到达这里。
					if policy.ExitCode != nil && *policy.ExitCode == req.ExitCode {
						delayAct.action = policy.Action
						if policy.Timeout != nil {
							delayAct.delay = policy.Timeout.Duration
						}
						return
					}
				}
				// 找到对应 Task 后，无论是否匹配到策略，都停止遍历其他 Task。
				break
			}
		}
	}

	// 步骤 7：匹配 Job 级别策略。
	//
	// 如果 Task 级别没有匹配到任何策略（或请求不属于任何 Task），
	// 则尝试匹配 Job 级别的策略。
	// Job 级别策略适用于整个 Job，不区分 Task。
	for _, policy := range job.Spec.Policies {
		policyEvents := getEventlist(policy)

		// 步骤 7.1：按事件类型匹配。
		if len(policyEvents) > 0 && len(req.Event) > 0 {
			if checkEventExist(policyEvents, req.Event) || checkEventExist(policyEvents, v1alpha1.AnyEvent) {
				// 与 Task 级别相同的超时逻辑。
				// PodPendingEvent 且无 Timeout 时跳过，其他事件直接执行。
				if !(shouldConfigureTimeout(req.Event) && policy.Timeout == nil) {
					delayAct.action = policy.Action
					if policy.Timeout != nil {
						delayAct.delay = policy.Timeout.Duration
					}
					return
				}
			}
		}

		// 步骤 7.2：按容器退出码匹配。
		if policy.ExitCode != nil && *policy.ExitCode == req.ExitCode {
			delayAct.action = policy.Action
			if policy.Timeout != nil {
				delayAct.delay = policy.Timeout.Duration
			}
			return
		}
	}

	// 步骤 8：没有匹配到任何策略，返回默认的 SyncJobAction。
	//
	// SyncJobAction 会触发 Job 状态同步，但不会执行重启、终止等副作用动作。
	return
}

// shouldConfigureTimeout 判断某个事件是否需要超时配置。
//
// 目前只有 PodPendingEvent 支持超时配置。
// 这是因为 Pod Pending 是一个“等待中”状态，用户可能希望等一段时间后再决定是否重启。
// 而 PodFailed、PodEvicted 等事件通常表示已经确定的异常，不需要等待。
func shouldConfigureTimeout(event v1alpha1.Event) bool {
	return event == v1alpha1.PodPendingEvent
}

// getEventlist 从 LifecyclePolicy 中提取所有事件。
//
// LifecyclePolicy 支持两种事件声明方式：
//   - Event: 单个事件（如 event: PodPending）；
//   - Events: 事件列表（如 events: [PodPending, PodFailed]）。
//
// 该函数将两种方式合并为一个事件列表，方便统一匹配。
func getEventlist(policy batch.LifecyclePolicy) []v1alpha1.Event {
	policyEventsList := policy.Events
	if len(policy.Event) > 0 {
		policyEventsList = append(policyEventsList, policy.Event)
	}
	return policyEventsList
}

// checkEventExist 检查请求事件是否存在于策略的事件列表中。
//
// 用于 applyPolicies 中判断当前事件是否匹配某条策略。
func checkEventExist(policyEvents []v1alpha1.Event, reqEvent v1alpha1.Event) bool {
	for _, event := range policyEvents {
		if event == reqEvent {
			return true
		}
	}
	return false
}

// TaskPriority 表示一个 Task 及其优先级。
//
// 用于在计算 PodGroup 最小资源时，按优先级排序 Task，
// 优先保障高优先级任务的资源需求。
type TaskPriority struct {
	priority int32

	batch.TaskSpec
}

// TasksPriority 是 TaskPriority 的切片，实现了 sort.Interface 接口。
type TasksPriority []TaskPriority

// Len 返回 Task 数量，实现 sort.Interface。
func (p TasksPriority) Len() int { return len(p) }

// Less 比较两个 Task 的优先级，优先级高的排在前面（降序）。
//
// 注意：这里是 p[i].priority > p[j].priority，即优先级数值越大越靠前。
func (p TasksPriority) Less(i, j int) bool {
	return p[i].priority > p[j].priority
}

// Swap 交换两个 Task 的位置，实现 sort.Interface。
func (p TasksPriority) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// isControlledBy 判断一个 Kubernetes 对象是否被指定 GVK 的资源控制。
//
// 通过检查 OwnerReferences 中的 ControllerRef 来判断。
// 用于确认某个资源（如 Pod）是否属于某个 Volcano Job。
func isControlledBy(obj metav1.Object, gvk schema.GroupVersionKind) bool {
	controllerRef := metav1.GetControllerOf(obj)
	if controllerRef == nil {
		return false
	}
	if controllerRef.APIVersion == gvk.GroupVersion().String() && controllerRef.Kind == gvk.Kind {
		return true
	}
	return false
}

// CalcFirstCountResources 计算前 count 个 Pod 所需的资源总量。
//
// 按优先级从高到低遍历 Task，累计计算前 count 个 Pod 的资源请求。
// 用于在 PodGroup 最小资源计算中，优先保障高优先级任务的资源。
//
// 计算逻辑：
//  1. 按优先级降序排序；
//  2. 遍历 Task，如果当前 Task 的副本数 >= 剩余 count，则只取 count 个副本的资源，然后结束；
//  3. 如果副本数 < 剩余 count，则取全部副本的资源，继续下一个 Task。
func (p TasksPriority) CalcFirstCountResources(count int32) v1.ResourceList {
	sort.Sort(p)
	minReq := v1.ResourceList{}

	for _, task := range p {
		if count <= task.Replicas {
			// 当前 Task 的副本数足够满足剩余 count，只取 count 个副本的资源。
			minReq = quotav1.Add(minReq, util.CalTaskRequests(&v1.Pod{Spec: task.Template.Spec}, count))
			break
		} else {
			// 当前 Task 的副本数不足，取全部副本，继续下一个 Task。
			minReq = quotav1.Add(minReq, util.CalTaskRequests(&v1.Pod{Spec: task.Template.Spec}, task.Replicas))
			count -= task.Replicas
		}
	}
	return minReq
}

// CalcPGMinResources 计算 PodGroup 的最小资源需求。
//
// PodGroup 是 Volcano gang scheduling 的核心资源，
// 调度器需要知道 PodGroup 至少需要多少资源才能做出调度决策。
//
// 计算逻辑：
//  1. 按优先级降序排序 Task；
//  2. 第一轮：累加每个 Task 的 MinAvailable 个副本的资源，直到达到 jobMinAvailable；
//  3. 第二轮：如果第一轮未达到 jobMinAvailable，则用 Task 的剩余副本（Replicas - MinAvailable）填充。
//
// 为什么要分两轮：
//
//	MinAvailable 是 Task 必须保障的最小副本数，优先级最高。
//	如果所有 Task 的 MinAvailable 之和小于 jobMinAvailable，
//	需要用额外的副本（非必须但可用）来填充，以确保 PodGroup 的最小资源足够。
func (p TasksPriority) CalcPGMinResources(jobMinAvailable int32) v1.ResourceList {
	sort.Sort(p)
	minReq := v1.ResourceList{}
	podCnt := int32(0)

	// 第一轮：累加每个 Task 的 MinAvailable 个副本的资源。
	//
	// 优先处理高优先级 Task，确保它们的最小副本数被保障。
	for _, task := range p {
		if task.MinAvailable == nil {
			// 实际上 webhook 会为所有 Task 设置 MinAvailable，这里是兑底处理。
			continue
		}

		// 取 MinAvailable 和剩余所需 Pod 数的较小值。
		validReplics := *task.MinAvailable
		if left := jobMinAvailable - podCnt; left < validReplics {
			validReplics = left
		}
		minReq = quotav1.Add(minReq, util.CalTaskRequests(&v1.Pod{Spec: task.Template.Spec}, validReplics))
		podCnt += validReplics
		if podCnt >= jobMinAvailable {
			break
		}
	}

	// 如果第一轮已经达到 jobMinAvailable，直接返回。
	if podCnt >= jobMinAvailable {
		return minReq
	}

	// 第二轮：用 Task 的剩余副本填充。
	//
	// 剩余副本 = Replicas - MinAvailable。
	// 这些副本不是必须运行的，但如果需要凑够 jobMinAvailable，可以用它们填充。
	leftCnt := jobMinAvailable - podCnt
	for _, task := range p {
		// 计算当前 Task 还有多少剩余副本可用。
		left := task.Replicas
		if task.MinAvailable != nil {
			if *task.MinAvailable == task.Replicas {
				// MinAvailable == Replicas，没有剩余副本可用。
				continue
			} else {
				left = task.Replicas - *task.MinAvailable
			}
		}

		// 取剩余副本和还需要的 Pod 数的较小值。
		if leftCnt >= left {
			minReq = quotav1.Add(minReq, util.CalTaskRequests(&v1.Pod{Spec: task.Template.Spec}, left))
			leftCnt -= left
		} else {
			minReq = quotav1.Add(minReq, util.CalTaskRequests(&v1.Pod{Spec: task.Template.Spec}, leftCnt))
			leftCnt = 0
		}
		if leftCnt <= 0 {
			break
		}
	}
	return minReq
}

// isInternalEvent 判断事件是否为内部事件。
//
// 内部事件是 controller 内部同步行为产生的事件，不应该触发用户定义的生命周期策略。
//
// 当前内部事件包括：
//   - OutOfSyncEvent：缓存与 apiserver 状态不一致时触发的同步事件；
//   - CommandIssuedEvent：用户通过 Command 资源触发的事件，已在 applyPolicies 步骤 2 单独处理；
//   - PodRunningEvent：Pod 进入 Running 状态，通常不需要触发策略动作。
func isInternalEvent(event v1alpha1.Event) bool {
	switch event {
	case v1alpha1.OutOfSyncEvent,
		v1alpha1.CommandIssuedEvent,
		v1alpha1.PodRunningEvent:
		return true
	default:
		return false
	}
}

// isInternalAction 判断动作是否为内部动作。
//
// 内部动作是 controller 自身的同步行为，不是用户策略触发的副作用动作。
// 在 applyPolicies 执行完成后，只有非内部动作才会触发 cleanupDelayActions（清理同类型延迟动作）。
//
// 当前内部动作包括：
//   - SyncJobAction：同步 Job 状态，默认安全动作；
//   - EnqueueAction：将 Job 加入队列等待调度；
//   - SyncQueueAction：同步 Queue 状态；
//   - OpenQueueAction：打开 Queue，允许接收新 Job；
//   - CloseQueueAction：关闭 Queue，停止接收新 Job。
func isInternalAction(action v1alpha1.Action) bool {
	switch action {
	case v1alpha1.SyncJobAction,
		v1alpha1.EnqueueAction,
		v1alpha1.SyncQueueAction,
		v1alpha1.OpenQueueAction,
		v1alpha1.CloseQueueAction:
		return true
	default:
		return false
	}
}

// GetStateAction 将 delayAction 转换为状态机可执行的 Action。
//
// 状态机（state.State）需要知道动作的目标对象（Task、Pod、Partition），
// 该函数根据 delayAction 中的动作类型设置对应的 Target。
//
// 转换规则：
//   - RestartTaskAction → Target 指向 Task；
//   - RestartPodAction → Target 指向具体的 Pod；
//   - RestartPartitionAction → Target 指向具体的 Partition；
//   - 其他动作（如 RestartJobAction）→ 不需要 Target，作用于整个 Job。
func GetStateAction(delayAct *delayAction) state.Action {
	action := state.Action{Action: delayAct.action}

	if delayAct.action == v1alpha1.RestartTaskAction {
		action.Target = state.Target{TaskName: delayAct.taskName, Type: state.TargetTypeTask}
	} else if delayAct.action == v1alpha1.RestartPodAction {
		action.Target = state.Target{TaskName: delayAct.taskName, PodName: delayAct.podName, Type: state.TargetTypePod}
	} else if delayAct.action == v1alpha1.RestartPartitionAction {
		action.Target = state.Target{TaskName: delayAct.taskName, PodName: delayAct.podName, PartitionName: delayAct.partition, Type: state.TargetTypePartition}
	}

	return action
}

// ActionType 表示动作的作用级别。
//
// 用于 cleanupDelayActions 中判断两个延迟动作是否属于“同类型”，
// 从而决定是否清理。
//
// 级别从大到小：Job > Task > Pod > Partition。
type ActionType int

const (
	// JobAction Job 级别动作，如 RestartJob、TerminateJob、AbortJob 等。
	JobAction ActionType = iota
	// TaskAction Task 级别动作，如 RestartTask。
	TaskAction
	// PodAction Pod 级别动作，如 RestartPod。
	PodAction
	// PartitionAction Partition 级别动作，如 RestartPartition。
	PartitionAction
)

// GetActionType 根据动作名称返回其作用级别。
//
// 用于 cleanupDelayActions 中判断两个延迟动作是否属于同类型。
// 同类型的延迟动作在其中一个执行后会被清理，避免重复执行。
//
// 分类规则：
//   - Job 级别：AbortJob、RestartJob、TerminateJob、CompleteJob、ResumeJob；
//   - Task 级别：RestartTask；
//   - Pod 级别：RestartPod；
//   - Partition 级别：RestartPartition；
//   - 默认：JobAction（未明确分类的动作统一归为 Job 级别）。
func GetActionType(action v1alpha1.Action) ActionType {
	switch action {
	case v1alpha1.AbortJobAction,
		v1alpha1.RestartJobAction,
		v1alpha1.TerminateJobAction,
		v1alpha1.CompleteJobAction,
		v1alpha1.ResumeJobAction:
		return JobAction
	case v1alpha1.RestartTaskAction:
		return TaskAction
	case v1alpha1.RestartPodAction:
		return PodAction
	case v1alpha1.RestartPartitionAction:
		return PartitionAction
	}
	return JobAction
}
