/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added comprehensive operation management with save/recover capabilities
- Enhanced with Allocate/UnAllocate and UnPipeline operations
- Added improved error handling and rollback support

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
	"errors"
	"fmt"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// Operation 定义调度操作中类型的枚举值。
// Statement 在执行 Evict/Pipeline/Allocate 时，会将每次操作记录为一个 operation，
// 后续通过 Commit 提交到 apiserver，或通过 Discard 回滚所有变更。
type Operation int8

const (
	// Evict 驱逐操作：将 Task 从当前节点上驱逐（释放资源）
	Evict = iota
	// Pipeline 流水线操作：将 Task 预分配到节点，等待资源释放后再真正绑定
	Pipeline
	// Allocate 分配操作：将 Task 正式分配到节点，资源已满足可直接绑定
	Allocate
)

// operation 记录单次调度操作的详细信息。
// 每个 operation 会被追加到 Statement 的 operations 切片中，
// 在 Commit 时执行实际操作，在 Discard 时执行回滚操作。
type operation struct {
	name   Operation   // 操作类型：Evict / Pipeline / Allocate
	task   *api.TaskInfo // 被操作的 Task 信息
	reason string      // 驱逐原因（仅 Evict 操作时使用）
}

// Statement 是调度会话（Session）中的事务性操作容器。
// 它收集一次调度周期内对 Task 的所有操作（驱逐、流水线分配、正式分配），
// 并在调度决策完成后统一提交（Commit）或丢弃（Discard），实现类似数据库事务的语义：
//   - Commit：将所有操作落地（如向 apiserver 发送绑定请求、驱逐 Pod）
//   - Discard：逆序回滚所有操作，恢复 Session 中的内存状态
//   - Merge：将多个 Statement 的操作合并到一个 Statement 中统一处理
type Statement struct {
	operations []operation // 操作记录列表，按执行顺序追加
	ssn        *Session    // 所属的调度会话，提供 Jobs/Nodes 等全局上下文
}

// NewStatement 创建并返回一个新的 Statement 对象。
// 传入的 ssn 为当前调度会话，Statement 通过它访问 Job 和 Node 信息。
func NewStatement(ssn *Session) *Statement {
	return &Statement{
		ssn: ssn,
	}
}

// Operations 返回当前 Statement 中收集的所有操作记录。
// 主要用于调试、日志输出或跨 Statement 合并时查看操作内容。
func (s *Statement) Operations() []operation {
	return s.operations
}

// Evict 在 Session 内存中将指定 Task 标记为"正在释放"（Releasing），
// 并触发所有 EventHandler 的 DeallocateFunc 回调，通知各插件更新资源统计。
//
// 注意：此方法仅修改 Session 内存状态，不会立即向 apiserver 发送驱逐请求。
// 真正的驱逐动作在 Commit() 时通过 evict() 方法执行。
//
// 参数：
//   - reclaimee: 被驱逐的 Task 信息
//   - reason: 驱逐原因，用于事件记录和日志
func (s *Statement) Evict(reclaimee *api.TaskInfo, reason string) {
	// 第一步：在 Session 的 Job 索引中，将 Task 状态更新为 Releasing
	if job, found := s.ssn.Jobs[reclaimee.Job]; found {
		job.UpdateTaskStatus(reclaimee, api.Releasing)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when evicting.",
			reclaimee.Job, s.ssn.UID)
	}

	// 第二步：更新节点上该 Task 的状态（节点视角的资源变化）
	if node, found := s.ssn.Nodes[reclaimee.NodeName]; found {
		node.UpdateTask(reclaimee)
	}

	// 第三步：触发所有 EventHandler 的 DeallocateFunc，通知插件（如 drf、proportion）
	// 该 Task 正在释放资源，插件需要相应地减少已分配资源统计
	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eventInfo := &Event{
				Task: reclaimee,
			}
			eh.DeallocateFunc(eventInfo)
		}
	}

	// 第四步：将本次驱逐操作记录到 operations 列表，等待 Commit 或 Discard
	s.operations = append(s.operations, operation{
		name:   Evict,
		task:   reclaimee,
		reason: reason,
	})
}

// evict 执行真正的驱逐操作（小写开头，仅在 Commit 时调用）。
// 通过 cache 向 apiserver 发送驱逐 Pod 的请求。
// 如果驱逐失败，会立即调用 unevict 回滚 Session 内存状态，保证一致性。
//
// 执行步骤：
//  1. 调用 cache 层向 apiserver 发送驱逐请求
//  2. 若驱逐失败，调用 unevict 回滚 Session 内存状态
func (s *Statement) evict(reclaimee *api.TaskInfo, reason string) error {
	// 第一步：调用 cache 层向 apiserver 发送驱逐请求
	if err := s.ssn.cache.Evict(reclaimee, reason); err != nil {
		// 第二步（失败分支）：驱逐失败，回滚 Session 中的状态（将 Task 恢复为 Running）
		if e := s.unevict(reclaimee); e != nil {
			klog.Errorf("Faled to unevict task <%v/%v>: %v.", reclaimee.Namespace, reclaimee.Name, e)
		}
		return err
	}

	return nil
}

// unevict 是 Evict 的回滚方法（小写开头，仅在内部使用）。
// 将 Task 状态恢复为 Running，并触发 AllocateFunc 回调，
// 通知各插件该 Task 仍然占用资源。
// 典型调用场景：Commit 时驱逐失败，或 Discard 时回滚驱逐操作。
//
// 执行步骤：
//  1. 将 Task 状态恢复为 Running
//  2. 更新节点上该 Task 的状态
//  3. 触发 AllocateFunc 回调，通知插件资源仍被占用
func (s *Statement) unevict(reclaimee *api.TaskInfo) error {
	// 第一步：将 Task 状态恢复为 Running（撤销之前的 Releasing 状态）
	job, found := s.ssn.Jobs[reclaimee.Job]
	if found {
		job.UpdateTaskStatus(reclaimee, api.Running)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unevicting.",
			reclaimee.Job, s.ssn.UID)
	}

	// 第二步：更新节点上该 Task 的状态
	if node, found := s.ssn.Nodes[reclaimee.NodeName]; found {
		node.UpdateTask(reclaimee)
	}

	// 第三步：触发 AllocateFunc 回调，通知插件该 Task 资源仍然被占用
	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: reclaimee,
			})
		}
	}

	return nil
}

// Pipeline 将 Task 以"流水线"方式预分配到指定节点。
// 流水线分配适用于节点当前空闲资源不足，但未来可用资源（空闲 + 正在释放）满足需求的场景。
//
// 执行流程：
//  1. 将 Task 状态更新为 Pipelined
//  2. 将 Task 添加到目标节点的 Task 列表中
//  3. 触发所有 EventHandler 的 AllocateFunc 回调
//  4. 将本次操作记录到 operations 列表
//
// 错误处理：如果任何步骤失败，会通过 defer 自动调用 unPipeline 回滚所有变更。
//
// 参数：
//   - task: 待分配的 Task
//   - hostname: 目标节点名称
//   - evictionOccurred: 是否因驱逐而产生的分配（用于后续调度决策参考）
func (s *Statement) Pipeline(task *api.TaskInfo, hostname string, evictionOccurred bool) (err error) {
	// 延迟回滚机制：如果 Pipeline 返回错误，自动执行 unPipeline 撤销所有变更
	defer func() {
		if err == nil {
			return
		}
		if rollbackErr := s.unPipeline(task); rollbackErr != nil {
			klog.Errorf("Failed to rollback pipeline for task <%v/%v> on node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, rollbackErr)
		}
	}()

	errInfos := make([]error, 0)

	// 第一步：在 Session 的 Job 索引中，将 Task 状态更新为 Pipelined
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Pipelined)
	} else {
		err := fmt.Errorf("Failed to find Job <%s> in Session <%s> index when pipeline.",
			task.Job, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	// 第二步：设置 Task 的目标节点和驱逐标记
	task.NodeName = hostname
	task.EvictionOccurred = evictionOccurred

	// 第三步：将 Task 添加到目标节点的 Task 列表中（更新节点资源使用情况）
	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> when pipeline in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
			errInfos = append(errInfos, err)
		}
		klog.V(3).Infof("After pipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		err := fmt.Errorf("Failed to find Node <%s> in Session <%s> index when pipeline.",
			hostname, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	// 第四步：触发所有 EventHandler 的 AllocateFunc 回调
	// 通知各插件（如 drf、proportion、capacity）该 Task 已预分配资源
	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eventInfo := &Event{
				Task: task,
			}
			eh.AllocateFunc(eventInfo)
			if eventInfo.Err != nil {
				klog.Errorf("Failed to exec allocate callback functions for task <%v/%v> to node <%v> when pipeline in Session <%v>: %v",
					task.Namespace, task.Name, hostname, s.ssn.UID, eventInfo.Err)
				errInfos = append(errInfos, eventInfo.Err)
			}
		}
	}

	// 第五步：检查是否有错误，如有则返回错误（触发 defer 回滚），否则记录操作
	if len(errInfos) != 0 {
		return fmt.Errorf("Task(%s/%s) pipeline to node(%s) error and errInfos num is %d, pipeline has been rolled back",
			task.Namespace, task.Name, hostname, len(errInfos))
	} else {
		s.operations = append(s.operations, operation{
			name: Pipeline,
			task: task,
		})
	}

	return nil
}

// pipeline 执行真正的流水线提交动作（小写开头，仅在 Commit 时调用）。
// 当前实现为空，因为 Pipeline 的 Session 内存状态已在 Pipeline() 中完成，
// Commit 时无需额外操作（与 Allocate 不同，Allocate 需要向 apiserver 发送绑定请求）。
func (s *Statement) pipeline(task *api.TaskInfo) {
}

// UnPipeline 是 Pipeline 的公开回滚方法，将 Task 从 Pipelined 状态恢复为 Pending。
// 主要用于 backfill 等场景中取消之前的流水线分配。
func (s *Statement) UnPipeline(task *api.TaskInfo) error {
	return s.unPipeline(task)
}

// unPipeline 将 Task 从 Pipelined 状态回滚为 Pending，并释放节点上的资源占用。
//
// 执行流程：
//  1. 将 Task 状态恢复为 Pending
//  2. 从目标节点的 Task 列表中移除该 Task（释放节点资源）
//  3. 触发 DeallocateFunc 回调，通知插件更新资源统计
//  4. 清空 Task 的 NodeName 和 JobAllocatedHyperNode
func (s *Statement) unPipeline(task *api.TaskInfo) error {
	// 第一步：将 Task 状态恢复为 Pending
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Pending)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unpipeline.", task.Job, s.ssn.UID)
	}

	// 第二步：从目标节点移除该 Task，释放节点上被占用的资源
	if node, found := s.ssn.Nodes[task.NodeName]; found {
		node.RemoveTask(task)
		klog.V(3).Infof("After unpipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when unpipeline.",
			task.NodeName, s.ssn.UID)
	}

	// 第三步：触发 DeallocateFunc 回调，通知插件该 Task 已释放资源
	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eventInfo := &Event{
				Task: task,
			}
			eh.DeallocateFunc(eventInfo)
		}
	}

	// 第四步：清空 Task 的节点关联信息
	task.NodeName = ""
	task.JobAllocatedHyperNode = ""

	return nil
}

// Allocate 将 Task 正式分配到指定节点。
// 分配后 Task 状态变为 Allocated，表示节点资源已满足 Task 需求，可以进入绑定流程。
//
// 执行流程：
//  1. 设置 Pod 的目标节点（Pod.Spec.NodeName）
//  2. 将 Task 状态更新为 Allocated
//  3. 将 Task 添加到目标节点
//  4. 触发所有 EventHandler 的 AllocateFunc 回调
//  5. 将本次操作记录到 operations 列表
//
// 错误处理：如果任何步骤失败，会通过 defer 自动调用 unallocate 回滚所有变更。
//
// 参数：
//   - task: 待分配的 Task
//   - nodeInfo: 目标节点信息
func (s *Statement) Allocate(task *api.TaskInfo, nodeInfo *api.NodeInfo) (err error) {
	// 延迟回滚机制：如果 Allocate 返回错误，自动执行 unallocate 撤销所有变更
	defer func() {
		if err != nil {
			if rollbackErr := s.unallocate(task); rollbackErr != nil {
				klog.Errorf("Failed to rollback allocation for Task <%v/%v> on node <%v> in Session <%v>: %v",
					task.Namespace, task.Name, nodeInfo.Name, s.ssn.UID, rollbackErr)
			}
		}
	}()

	errInfos := make([]error, 0)
	hostname := nodeInfo.Name

	// 第一步：设置 Pod 的目标节点名称（kubelet 将根据此字段决定 Pod 运行在哪个节点）
	task.Pod.Spec.NodeName = hostname

	// 第二步：在 Session 的 Job 索引中，将 Task 状态更新为 Allocated
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Allocated)
	} else {
		err := fmt.Errorf("Failed to find Job <%s> in Session <%s> index when allocating.",
			task.Job, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	// 第三步：设置 Task 的目标节点，并将其添加到节点的 Task 列表中
	task.NodeName = hostname
	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> when allocating in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
			errInfos = append(errInfos, err)
		}
		klog.V(3).Infof("After allocated Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		err := fmt.Errorf("Failed to find Node <%s> in Session <%s> index when allocating.",
			hostname, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	// 第四步：触发所有 EventHandler 的 AllocateFunc 回调
	// 通知各插件（如 drf、proportion、capacity）该 Task 已分配资源
	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eventInfo := &Event{
				Task: task,
			}
			eh.AllocateFunc(eventInfo)
			if eventInfo.Err != nil {
				klog.Errorf("Failed to exec allocate callback functions for task <%v/%v> to node <%v> when allocating in Session <%v>: %v",
					task.Namespace, task.Name, hostname, s.ssn.UID, eventInfo.Err)
				errInfos = append(errInfos, eventInfo.Err)
			}
		}
	}

	// 第五步：检查错误，无错误则记录操作，否则返回错误（触发 defer 回滚）
	if len(errInfos) != 0 {
		err = fmt.Errorf("Task %s/%s allocate to node %s error and errInfos num is %d, allocation has been rolled back",
			task.Namespace, task.Name, hostname, len(errInfos))
		return
	} else {
		// 记录操作到 operations 列表，等待 Commit 或 Discard
		klog.V(3).Info("Allocating operations ...")
		s.operations = append(s.operations, operation{
			name: Allocate,
			task: task,
		})
	}

	return nil
}

// allocate 执行真正的绑定操作（小写开头，仅在 Commit 时调用）。
// 通过 cache 层创建绑定上下文并向 apiserver 发送绑定请求，将 Pod 绑定到目标节点。
// 绑定成功后将 Task 状态更新为 Binding。
// 如果绑定失败，会立即调用 unallocate 回滚 Session 内存状态。
//
// 执行步骤：
//  1. 创建绑定上下文并向 apiserver 发送 Bind 请求
//  2. 将 Task 状态更新为 Binding
//  3. 更新任务调度延迟指标
func (s *Statement) allocate(task *api.TaskInfo) error {
	// 第一步：创建绑定上下文（包含 Pod 和节点信息，用于向 apiserver 发送 Bind 请求）
	bindContext := s.ssn.CreateBindContext(task)
	if err := s.ssn.cache.AddBindTask(bindContext); err != nil {
		return err
	}

	// 第二步：绑定请求已发送，将 Task 状态更新为 Binding（等待 apiserver 确认绑定完成）
	if job, found := s.ssn.Jobs[task.Job]; found {
		job.UpdateTaskStatus(task, api.Binding)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, s.ssn.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	// 第三步：更新任务调度延迟指标（记录从 Pod 创建到进入 Assumed 阶段的时间）
	metrics.UpdateTaskScheduleDuration(metrics.TaskStageAssumed, metrics.Duration(task.Pod.CreationTimestamp.Time))
	return nil
}

// unallocate 是 Allocate 的回滚方法（小写开头，仅在内部使用）。
// 将 Task 状态恢复为 Pending，从节点移除 Task，并触发 DeallocateFunc 回调。
// 典型调用场景：Commit 时绑定失败，或 Discard 时回滚分配操作。
func (s *Statement) unallocate(task *api.TaskInfo) error {
	// 第一步：将 Task 状态恢复为 Pending（撤销之前的 Allocated 状态）
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Pending)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unallocating.",
			task.Job, s.ssn.UID)
	}

	// 第二步：从目标节点移除该 Task，释放节点上被占用的资源
	if node, found := s.ssn.Nodes[task.NodeName]; found {
		klog.V(3).Infof("Remove Task <%v> on node <%v>", task.Name, task.NodeName)
		node.RemoveTask(task)
	}

	// 第三步：触发 DeallocateFunc 回调，通知插件该 Task 已释放资源
	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: task,
			})
		}
	}

	// 第四步：清空 Task 的节点关联信息
	task.NodeName = ""
	task.JobAllocatedHyperNode = ""

	return nil
}

// Discard 逆序回滚 Statement 中记录的所有操作。
// 回滚顺序与执行顺序相反（后执行的先回滚），确保状态一致性。
//
// 各操作的回滚逻辑：
//   - Evict    → unevict：将 Task 状态恢复为 Running
//   - Pipeline → unPipeline：将 Task 状态恢复为 Pending，释放节点资源
//   - Allocate → unallocate：将 Task 状态恢复为 Pending，释放节点资源
//
// 执行步骤：
//  1. 逆序遍历 operations 列表
//  2. 为每个 Task 生成 LastTxContext（上次事务上下文）
//  3. 根据操作类型调用对应的回滚方法
//  4. 清空 operations 列表
func (s *Statement) Discard() {
	klog.V(3).Info("Discarding operations ...")

	// 第一步：逆序遍历 operations 列表，后执行的操作先回滚
	for i := len(s.operations) - 1; i >= 0; i-- {
		op := s.operations[i]

		// 第二步：为每个 Task 生成上次事务上下文（用于后续调度决策参考）
		op.task.GenerateLastTxContext()

		// 第三步：根据操作类型调用对应的回滚方法
		switch op.name {
		case Evict:
			// Evict 回滚 → unevict：恢复 Task 为 Running 状态
			err := s.unevict(op.task)
			if err != nil {
				klog.Errorf("Failed to unevict task: %s", err.Error())
			}
		case Pipeline:
			// Pipeline 回滚 → unPipeline：恢复 Task 为 Pending 状态，释放节点资源
			err := s.unPipeline(op.task)
			if err != nil {
				klog.Errorf("Failed to unpipeline task: %s", err.Error())
			}
		case Allocate:
			// Allocate 回滚 → unallocate：恢复 Task 为 Pending 状态，释放节点资源
			err := s.unallocate(op.task)
			if err != nil {
				klog.Errorf("Failed to unallocate task: %s", err.Error())
			}
		}
	}

	// 第四步：清空 operations 列表
	s.operations = nil
}

// Commit 按顺序执行 Statement 中记录的所有操作，将 Session 内存状态落地到 apiserver。
//
// 各操作的提交逻辑：
//   - Evict    → evict：向 apiserver 发送驱逐 Pod 请求
//   - Pipeline → pipeline：当前为空操作（Session 状态已在 Pipeline() 中完成）
//   - Allocate → allocate：向 apiserver 发送绑定 Pod 到节点的请求
//
// 执行步骤：
//  1. 顺序遍历 operations 列表
//  2. 清除每个 Task 的 LastTxContext（表示本次调度已成功提交）
//  3. 根据操作类型调用对应的提交方法
//  4. Allocate 提交失败时立即调用 unallocate 回滚
//  5. 清空 operations 列表
func (s *Statement) Commit() {
	klog.V(3).Info("Committing operations ...")

	// 第一步：顺序遍历 operations 列表
	for _, op := range s.operations {
		// 第二步：清除上次事务上下文（表示本次调度已成功提交）
		op.task.ClearLastTxContext()

		// 第三步：根据操作类型调用对应的提交方法
		switch op.name {
		case Evict:
			// Evict 提交 → evict：向 apiserver 发送驱逐 Pod 请求
			err := s.evict(op.task, op.reason)
			if err != nil {
				klog.Errorf("Failed to evict task: %s", err.Error())
			}
		case Pipeline:
			// Pipeline 提交 → pipeline：当前为空操作
			s.pipeline(op.task)
		case Allocate:
			// Allocate 提交 → allocate：向 apiserver 发送绑定请求
			err := s.allocate(op.task)
			if err != nil {
				// 第四步（失败分支）：绑定失败，回滚该 Task 的分配状态
				if e := s.unallocate(op.task); e != nil {
					klog.Errorf("Failed to unallocate task <%v/%v>: %v.", op.task.Namespace, op.task.Name, e)
				}
				klog.Errorf("Failed to allocate task <%v/%v>: %v.", op.task.Namespace, op.task.Name, err)
			}
		}
	}

	// 第五步：清空 operations 列表
	s.operations = nil
}

// Merge 将多个 Statement 的操作记录合并到当前 Statement 中。
// 合并后，源 Statement 的 operations 被清空，防止重复提交或丢弃。
//
// 使用场景：当一个调度周期内产生多个 Statement 时（如多队列并行调度），
// 可以将它们合并为一个 Statement 统一提交或丢弃。
//
// 注意：源 Statement 的 Session 内存状态变更已经生效，
// Merge 只是转移操作记录的所有权，不影响已生效的内存状态。
//
// 执行步骤：
//  1. 遍历所有源 Statement
//  2. 将源 Statement 的操作记录追加到当前 Statement
//  3. 清空源 Statement 的操作记录，防止重复提交或丢弃
func (s *Statement) Merge(stmts ...*Statement) {
	// 第一步：遍历所有源 Statement
	for _, stmt := range stmts {
		// 第二步：将源 Statement 的操作记录追加到当前 Statement
		s.operations = append(s.operations, stmt.operations...)
		// 第三步：清空源 Statement 的操作记录，防止重复提交或丢弃
		stmt.operations = nil
	}
}

// SaveOperations 将多个 Statement 的操作记录深拷贝到一个新的 Statement 中。
// 与 Merge 不同，SaveOperations 不会清空源 Statement 的操作记录，
// 且对 Task 进行了 Clone，确保后续修改不会影响已保存的操作快照。
//
// 使用场景：在调度决策前保存操作快照，以便在需要时通过 RecoverOperations 恢复。
// 典型应用：模拟调度（simulate）时先保存当前状态，模拟完成后恢复。
//
// 执行步骤：
//  1. 创建临时 Statement
//  2. 遍历所有源 Statement 的操作记录
//  3. 对每个操作中的 Task 进行深拷贝（Clone）
//  4. 将拷贝后的操作追加到临时 Statement
//  5. 返回临时 Statement 作为操作快照
func SaveOperations(stmts ...*Statement) *Statement {
	// 第一步：创建临时 Statement 作为操作快照容器
	stmtTmp := &Statement{}

	// 第二步：遍历所有源 Statement
	for _, stmt := range stmts {
		stmt.outputOperations("Save operations: ", 4)

		// 第三步：遍历每个 Statement 中的操作记录
		for _, op := range stmt.operations {
			// 第四步：深拷贝 Task 信息，避免后续修改影响已保存的操作
			task := op.task.Clone()
			task.EvictionOccurred = op.task.EvictionOccurred

			// 第五步：将拷贝后的操作追加到临时 Statement
			stmtTmp.operations = append(stmtTmp.operations, operation{
				name:   op.name,
				task:   task,
				reason: op.reason,
			})
		}
	}

	return stmtTmp
}

// RecoverOperations 将之前通过 SaveOperations 保存的操作快照重新应用到当前 Statement。
// 它会依次重新执行每个操作（调用对应的 Evict/Pipeline/Allocate 方法），
// 从而恢复 Session 内存状态到保存时的状态。
//
// 使用场景：模拟调度完成后，通过恢复操作快照来撤销模拟过程中的所有变更。
//
// 执行步骤：
//  1. 校验快照是否为 nil
//  2. 输出日志记录恢复操作
//  3. 遍历快照中的操作列表
//  4. 根据操作类型重新执行对应方法（Evict/Pipeline/Allocate）
//  5. 任何操作失败时立即返回错误
//
// 参数：
//   - stmt: 通过 SaveOperations 创建的操作快照
//
// 返回：如果任何操作恢复失败，立即返回错误
func (s *Statement) RecoverOperations(stmt *Statement) error {
	// 第一步：校验快照是否为 nil
	if stmt == nil {
		return errors.New("statement is nil")
	}

	// 第二步：输出日志记录恢复操作
	s.outputOperations("Recover operations: ", 4)

	// 第三步：遍历快照中的操作列表
	for _, op := range stmt.operations {
		// 第四步：根据操作类型重新执行对应方法
		switch op.name {
		case Evict:
			// 重新执行驱逐操作
			s.Evict(op.task, op.reason)
		case Pipeline:
			// 重新执行流水线分配操作
			err := s.Pipeline(op.task, op.task.NodeName, op.task.EvictionOccurred)
			if err != nil {
				// 第五步（失败分支）：流水线恢复失败，立即返回错误
				klog.Errorf("Failed to pipeline task: %s", err.Error())
				return err
			}
		case Allocate:
			// 重新执行分配操作
			node := s.ssn.Nodes[op.task.NodeName]
			err := s.Allocate(op.task, node)
			if err != nil {
				// 第五步（失败分支）：分配恢复失败，立即返回错误
				klog.Errorf("Failed to allocate task <%v/%v>: %v", op.task.Namespace, op.task.Name, err)
				return err
			}
		}
	}
	return nil
}

// outputOperations 以日志形式输出当前 Statement 中的所有操作。
// 仅在指定日志级别开启时才会构建和输出日志内容，避免不必要的性能开销。
//
// 执行步骤：
//  1. 检查日志级别是否开启，未开启则直接返回
//  2. 遍历 operations 列表，按操作类型拼接日志字符串
//  3. 输出拼接后的日志内容
//
// 参数：
//   - msg: 日志前缀消息
//   - level: 日志级别（如 klog.Level(4) 表示 V(4) 级别）
func (s *Statement) outputOperations(msg string, level klog.Level) {
	// 第一步：检查日志级别是否开启，未开启则直接返回，避免无谓的字符串拼接开销
	if !klog.V(level).Enabled() {
		return
	}

	// 第二步：遍历 operations 列表，按操作类型拼接日志字符串
	var buffer string
	for _, op := range s.operations {
		switch op.name {
		case Evict:
			buffer += fmt.Sprintf("task %s evict from node %s ", op.task.Name, op.task.NodeName)
		case Pipeline:
			buffer += fmt.Sprintf("task %s pipeline from node %s ", op.task.Name, op.task.NodeName)
		case Allocate:
			buffer += fmt.Sprintf("task %s allocate from node %s ", op.task.Name, op.task.NodeName)
		}
	}

	// 第三步：输出拼接后的日志内容
	klog.V(level).Info(msg, buffer)
}
