/*
Copyright 2023 The Volcano Authors.

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

package pdb

import (
	pdbPolicy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	policylisters "k8s.io/client-go/listers/policy/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

/*
示例 PDB：

apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: myapp-pdb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: myapp
*/

// PluginName 表示 Volcano 调度器插件名
const PluginName = "pdb"

// pdbPlugin 是 Volcano 的 PDB 插件结构体
type pdbPlugin struct {
	// 插件参数（由 Volcano 框架传入）
	pluginArguments framework.Arguments

	// PDB 的 lister，用于从 informer cache 中读取 PDB 对象
	lister policylisters.PodDisruptionBudgetLister
}

// New 创建并返回一个 pdbPlugin 实例
func New(arguments framework.Arguments) framework.Plugin {
	return &pdbPlugin{
		pluginArguments: arguments,
		lister:          nil,
	}
}

// Name 返回插件名称
func (pp *pdbPlugin) Name() string {
	return PluginName
}

// OnSessionOpen 在调度 session 开启时调用
func (pp *pdbPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(4).Infof("Enter pdb plugin ...")
	defer klog.V(4).Infof("Leaving pdb plugin.")

	// 0. 初始化 PDB lister
	// 这里从 session 的 informer factory 中拿到 PDB lister
	if pp.lister == nil {
		pp.lister = getPDBLister(ssn.InformerFactory())
	}

	// 1. 定义 victim 过滤函数：
	//    用来过滤掉那些会违反 PDB 约束的 tasks
	pdbFilterFn := func(tasks []*api.TaskInfo) []*api.TaskInfo {
		// victims 表示最终“可以作为 victim 被驱逐/抢占/回收”的 task 列表
		var victims []*api.TaskInfo

		// -------------------------------------------------------
		// 第一步：从 informer/lister 中获取所有 PDB
		// -------------------------------------------------------
		pdbs, err := getPodDisruptionBudgets(pp.lister)
		if err != nil {
			// 如果连 PDB 都拿不到，那就直接返回空列表
			// 这里相当于“保守策略”：无法判断是否违反 PDB，就先不选 victim
			klog.Errorf("Failed to list pdbs condition: %v", err)
			return victims
		}

		// -------------------------------------------------------
		// 第二步：初始化每个 PDB 的“剩余额度”
		//
		// pdbsAllowed[i] 对应 pdbs[i] 当前允许再驱逐多少个 Pod
		//
		// 例如：
		//   pdb.Status.DisruptionsAllowed = 1
		//   那么说明这个 PDB 还允许再驱逐 1 个 Pod
		// -------------------------------------------------------
		pdbsAllowed := make([]int32, len(pdbs))
		for i, pdb := range pdbs {
			pdbsAllowed[i] = pdb.Status.DisruptionsAllowed
		}

		// -------------------------------------------------------
		// 第三步：逐个检查候选 task
		//
		// tasks 可以理解为“候选 victim 列表”
		// Volcano 要判断这些 Pod 中哪些可以被驱逐
		// -------------------------------------------------------
		for _, task := range tasks {
			pod := task.Pod

			// 标记：当前 Pod 是否违反了某个 PDB
			pdbForPodIsViolated := false

			// ---------------------------------------------------
			// 如果 Pod 没有 label，它不可能匹配任何 PDB
			// 因为 PDB 是靠 label selector 来匹配 Pod 的
			// ---------------------------------------------------
			if len(pod.Labels) == 0 {
				continue
			}

			// ---------------------------------------------------
			// 遍历所有 PDB，检查当前 Pod 是否与某个 PDB 匹配
			// ---------------------------------------------------
			for i, pdb := range pdbs {
				// PDB 只匹配同一个 namespace 下的 Pod
				if pdb.Namespace != pod.Namespace {
					continue
				}

				// 把 PDB 中的 LabelSelector 转成 selector 对象
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					// selector 非法，直接忽略这个 PDB
					continue
				}

				// 空 selector 不匹配任何 Pod
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}

				// ---------------------------------------------------
				// 如果这个 Pod 已经出现在 DisruptedPods 中，
				// 说明它已经被 apiserver 接受过驱逐请求，
				// 不应该再次扣减预算
				//
				// 例如：
				//   pod-a 已经在 DisruptedPods 中
				//   表示它的预算已被占用，不必重复算
				// ---------------------------------------------------
				if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
					continue
				}

				// ---------------------------------------------------
				// 这个 Pod 确实匹配该 PDB，并且不在 DisruptedPods 中
				// 那么它如果被驱逐，就会消耗 1 个 budget
				//
				// 举例：
				//   原来剩余 1 个可驱逐额度
				//   现在扣 1 后变成 0
				// ---------------------------------------------------
				pdbsAllowed[i]--

				// ---------------------------------------------------
				// 如果扣减后 < 0，说明超出预算了
				//
				// 举例：
				//   原本 pdbsAllowed[i] = 0
				//   再来一个匹配 Pod，扣减后变成 -1
				//   说明这个 Pod 不能再被选为 victim
				// ---------------------------------------------------
				if pdbsAllowed[i] < 0 {
					pdbForPodIsViolated = true
				}
			}

			// ---------------------------------------------------
			// 如果当前 Pod 没有违反任何 PDB：
			//   就加入 victims
			//
			// 如果违反了 PDB：
			//   就过滤掉，不让它成为 victim
			// ---------------------------------------------------
			if !pdbForPodIsViolated {
				victims = append(victims, task)
			} else {
				klog.V(4).Infof(
					"The pod <%s> of task <%s> violates the pdb constraint, so filter it from the victim list",
					task.Name, task.Pod.Name,
				)
			}
		}

		// 返回最终可以作为 victim 的 task 列表
		return victims
	}
	// 2. 将 pdbFilterFn 包装成 ReclaimableFn / PreemptableFn 需要的签名
	//    返回值中的 util.Permit 表示允许继续进行 reclaim / preempt 逻辑
	wrappedPdbFilterFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		return pdbFilterFn(preemptees), util.Permit
	}

	// 3. 将过滤函数注册到 session
	//    VictimTasksFn: 决定哪些 task 可以作为 victim
	//    ReclaimableFn: 回收场景下的可回收对象过滤
	//    PreemptableFn: 抢占场景下的可抢占对象过滤
	victimsFns := []api.VictimTasksFn{pdbFilterFn}
	ssn.AddVictimTasksFns(pp.Name(), victimsFns)
	ssn.AddReclaimableFn(pp.Name(), wrappedPdbFilterFn)
	ssn.AddPreemptableFn(pp.Name(), wrappedPdbFilterFn)
}

// OnSessionClose 在 session 关闭时调用
func (pp *pdbPlugin) OnSessionClose(ssn *framework.Session) {}

// getPDBLister 返回 PDB lister
func getPDBLister(informerFactory informers.SharedInformerFactory) policylisters.PodDisruptionBudgetLister {
	return informerFactory.Policy().V1().PodDisruptionBudgets().Lister()
}

// getPodDisruptionBudgets 列出所有 PDB
func getPodDisruptionBudgets(pdbLister policylisters.PodDisruptionBudgetLister) ([]*pdbPolicy.PodDisruptionBudget, error) {
	if pdbLister != nil {
		return pdbLister.List(labels.Everything())
	}
	return nil, nil
}
