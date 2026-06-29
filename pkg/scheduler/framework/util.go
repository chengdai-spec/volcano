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

// Package framework 提供 Volcano 调度框架的基础工具与数据结构。
// 本文件主要包含 PodLister（Pod 列表器）及其相关实现，用于在 predicates 和 nodeorder 插件中
// 为 Kubernetes 原生调度插件提供符合其接口的 Pod/Node 查询能力。
package framework

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// PodFilter 是用于过滤 Pod 的函数类型。
// 如果 Pod 满足条件返回 true，否则返回 false。
type PodFilter func(*v1.Pod) bool

// PodsLister 接口表示调度器中可以列出 Pod 的对象。
type PodsLister interface {
	// List 返回所有 Pod 列表。
	List(labels.Selector) ([]*v1.Pod, error)
	// FilteredList 与 List 类似，但返回的切片中不包含未通过 podFilter 过滤的 Pod。
	FilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error)
}

// PodLister 在 predicates 和 nodeorder 插件中使用，
// 用于维护 Volcano session 中任务与 Pod 的映射关系。
type PodLister struct {
	Session *Session

	// CachedPods 缓存每个任务的 Pod 副本（主要修正了 NodeName）。
	CachedPods map[api.TaskID]*v1.Pod
	// Tasks 缓存所有已分配状态的任务信息。
	Tasks map[api.TaskID]*api.TaskInfo
	// TaskWithAffinity 缓存所有包含亲和性设置的任务，用于加速亲和性查询。
	TaskWithAffinity map[api.TaskID]*api.TaskInfo
}

// PodAffinityLister 用于列出具有亲和性设置的 Pod。
// 它底层封装了 PodLister，但只返回包含亲和性的 Pod。
type PodAffinityLister struct {
	pl *PodLister
}

// HaveAffinity 检查 Pod 是否包含亲和性/反亲和性设置。
// 包括：NodeAffinity、PodAffinity、PodAntiAffinity 任意一种。
func HaveAffinity(pod *v1.Pod) bool {
	affinity := pod.Spec.Affinity
	return affinity != nil &&
		(affinity.NodeAffinity != nil ||
			affinity.PodAffinity != nil ||
			affinity.PodAntiAffinity != nil)
}

// NewPodLister 基于 Volcano Session 创建 PodLister。
// 它会遍历所有 Job 中处于已分配状态（AllocatedStatus）的任务，
// 将任务及其 Pod 副本缓存到 PodLister 中，并单独记录包含亲和性的任务。
func NewPodLister(ssn *Session) *PodLister {
	pl := &PodLister{
		Session: ssn,

		CachedPods:       make(map[api.TaskID]*v1.Pod),
		Tasks:            make(map[api.TaskID]*api.TaskInfo),
		TaskWithAffinity: make(map[api.TaskID]*api.TaskInfo),
	}

	// 遍历所有 Job，按任务状态索引查找已分配的任务。
	for _, job := range pl.Session.Jobs {
		for status, tasks := range job.TaskStatusIndex {
			// 只处理处于已分配状态的任务。
			if !api.AllocatedStatus(status) {
				continue
			}

			for _, task := range tasks {
				pl.Tasks[task.UID] = task

				// 复制 Pod 并设置当前所在节点名。
				pod := pl.copyTaskPod(task)
				pl.CachedPods[task.UID] = pod

				// 如果 Pod 包含亲和性，记录到亲和性任务集合。
				if HaveAffinity(task.Pod) {
					pl.TaskWithAffinity[task.UID] = task
				}
			}
		}
	}

	return pl
}

// NewPodListerFromNode 基于 Volcano Session 的节点信息创建 PodLister。
// 与 NewPodLister 不同，它直接从 node.Tasks 中收集任务，
// 适用于需要反映节点实际运行任务的场景。
func NewPodListerFromNode(ssn *Session) *PodLister {
	pl := &PodLister{
		Session:          ssn,
		CachedPods:       make(map[api.TaskID]*v1.Pod),
		Tasks:            make(map[api.TaskID]*api.TaskInfo),
		TaskWithAffinity: make(map[api.TaskID]*api.TaskInfo),
	}

	// 遍历所有节点及其上运行的任务。
	for _, node := range pl.Session.Nodes {
		for _, task := range node.Tasks {
			// 只处理已分配或正在释放状态的任务。
			if !api.AllocatedStatus(task.Status) && task.Status != api.Releasing {
				continue
			}

			pl.Tasks[task.UID] = task
			pod := pl.copyTaskPod(task)
			pl.CachedPods[task.UID] = pod
			if HaveAffinity(task.Pod) {
				pl.TaskWithAffinity[task.UID] = task
			}
		}
	}

	return pl
}

// copyTaskPod 深度复制任务的 Pod，并将 Spec.NodeName 设置为任务当前所在节点。
// 这样原生调度插件看到的 Pod 带有正确的节点名信息。
func (pl *PodLister) copyTaskPod(task *api.TaskInfo) *v1.Pod {
	pod := task.Pod.DeepCopy()
	pod.Spec.NodeName = task.NodeName
	return pod
}

// GetPod 返回带有正确 NodeName 的 Pod。
// 如果任务当前节点名与原始 Pod 的节点名一致，直接返回原始 Pod；
// 否则从缓存中获取副本，若缓存未命中则临时复制（并打印警告）。
//
// 注意：该函数保持只读，避免并发操作 map 导致 panic。
func (pl *PodLister) GetPod(task *api.TaskInfo) *v1.Pod {
	if task.NodeName == task.Pod.Spec.NodeName {
		return task.Pod
	}

	pod, found := pl.CachedPods[task.UID]
	if !found {
		// 缓存只读，不将临时复制的 Pod 写回缓存。
		pod = pl.copyTaskPod(task)
		klog.Warningf("DeepCopy for pod %s/%s at PodLister.GetPod is unexpected", pod.Namespace, pod.Name)
	}
	return pod
}

// UpdateTask 使用 nodeName 更新缓存中 Pod 的 NodeName。
// 注意：该函数不是线程安全的，请确保同一时刻只有一个调用者在操作 PodLister。
func (pl *PodLister) UpdateTask(task *api.TaskInfo, nodeName string) *v1.Pod {
	pod, found := pl.CachedPods[task.UID]
	if !found {
		pod = pl.copyTaskPod(task)
		pl.CachedPods[task.UID] = pod
	}
	pod.Spec.NodeName = nodeName

	// 根据任务状态维护 Tasks 和 TaskWithAffinity 索引。
	if !api.AllocatedStatus(task.Status) {
		// 任务不再处于已分配状态，从索引中移除。
		delete(pl.Tasks, task.UID)
		if HaveAffinity(task.Pod) {
			delete(pl.TaskWithAffinity, task.UID)
		}
	} else {
		// 任务处于已分配状态，加入索引。
		pl.Tasks[task.UID] = task
		if HaveAffinity(task.Pod) {
			pl.TaskWithAffinity[task.UID] = task
		}
	}

	return pod
}

// List 返回所有满足标签选择器的 Pod 列表。
func (pl *PodLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for _, task := range pl.Tasks {
		pod := pl.GetPod(task)
		if selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

// filteredListWithTaskSet 从指定的任务集合中返回满足 podFilter 和标签选择器的 Pod 列表。
func (pl *PodLister) filteredListWithTaskSet(taskSet map[api.TaskID]*api.TaskInfo, podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for _, task := range taskSet {
		pod := pl.GetPod(task)
		if podFilter(pod) && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}

	return pods, nil
}

// FilteredList 从所有任务中返回满足过滤条件的 Pod 列表。
func (pl *PodLister) FilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	return pl.filteredListWithTaskSet(pl.Tasks, podFilter, selector)
}

// AffinityFilteredList 从包含亲和性设置的任务中返回满足过滤条件的 Pod 列表。
func (pl *PodLister) AffinityFilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	return pl.filteredListWithTaskSet(pl.TaskWithAffinity, podFilter, selector)
}

// AffinityLister 基于当前 PodLister 生成一个 PodAffinityLister。
func (pl *PodLister) AffinityLister() *PodAffinityLister {
	pal := &PodAffinityLister{
		pl: pl,
	}
	return pal
}

// List 返回所有满足标签选择器的 Pod 列表。
func (pal *PodAffinityLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	return pal.pl.List(selector)
}

// FilteredList 返回所有具有亲和性且满足过滤条件的 Pod 列表。
func (pal *PodAffinityLister) FilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	return pal.pl.AffinityFilteredList(podFilter, selector)
}

// GenerateNodeMapAndSlice 根据 Volcano 的 NodeInfo 生成 Kubernetes 框架所需的 nodeMap。
// nodeMap 的 key 是节点名称，value 是包含节点上所有 Pod 的 k8sframework.NodeInfo。
func GenerateNodeMapAndSlice(nodes map[string]*api.NodeInfo) map[string]fwk.NodeInfo {
	nodeMap := make(map[string]fwk.NodeInfo)
	for _, node := range nodes {
		nodeInfo := k8sframework.NewNodeInfo(node.Pods()...)
		nodeInfo.SetNode(node.Node)
		// 复制节点上的镜像状态，供镜像本地性等调度策略使用。
		nodeInfo.ImageStates = node.CloneImageSummary()
		nodeMap[node.Name] = nodeInfo
	}
	return nodeMap
}

// CachedNodeInfo 在 nodeorder 和 predicate 插件中使用，
// 用于从 Volcano Session 中按名称查询节点信息。
type CachedNodeInfo struct {
	Session *Session
}

// GetNodeInfo 根据节点名称返回对应的 Kubernetes Node 对象。
// 如果节点不存在，返回 NotFound 错误。
func (c *CachedNodeInfo) GetNodeInfo(name string) (*v1.Node, error) {
	node, found := c.Session.Nodes[name]
	if !found {
		return nil, errors.NewNotFound(v1.Resource("node"), name)
	}

	return node.Node, nil
}

// NodeLister 在 nodeorder 插件中使用，用于列出所有节点。
type NodeLister struct {
	Session *Session
}

// List 返回所有节点的 Kubernetes Node 对象列表。
func (nl *NodeLister) List() ([]*v1.Node, error) {
	var nodes []*v1.Node
	for _, node := range nl.Session.Nodes {
		nodes = append(nodes, node.Node)
	}
	return nodes, nil
}
