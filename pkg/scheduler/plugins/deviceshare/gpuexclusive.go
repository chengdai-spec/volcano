/*
Copyright 2026 The Volcano Authors.

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

package deviceshare

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

// GPUExclusiveRulesKey 是 GPU 独占规则的配置键
// 其值一般来自 Volcano 调度器配置参数，格式通常是一个规则列表。
// 每条规则本质上是一个 label 选择器：
// 只有同时满足该规则里所有 label key/value 的 Pod，才会被视为"独占组"。
const (
	GPUExclusiveRulesKey = "deviceshare.GPUExclusiveRules"
)

// podKey 将 Pod 转成唯一 key
// 使用 namespace/name 作为唯一标识，避免不同命名空间下同名 Pod 冲突。
func podKey(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

// exclusiveRule 表示一条独占规则
//
// 规则的结构是：
//   - labels: 一组必须同时满足的标签键值对
//
// 例如：
//
//	{"team":"a", "gpu-excl":"true"}
//
// 表示只有同时带有 team=a 和 gpu-excl=true 的 Pod 才匹配此规则。
type exclusiveRule struct {
	labels map[string]string
}

func (r exclusiveRule) String() string {
	return fmt.Sprintf("%v", r.labels)
}

// gpuExclusiveConfig 保存所有独占规则
type gpuExclusiveConfig struct {
	rules []exclusiveRule
}

// loadGPUExclusiveConfig 从插件参数中读取独占规则配置
func loadGPUExclusiveConfig(args framework.Arguments) gpuExclusiveConfig {
	cfg := gpuExclusiveConfig{}
	if rawRules, ok := args[GPUExclusiveRulesKey]; ok {
		cfg.rules = parseExclusiveRules(rawRules)
	}
	return cfg
}

// parseExclusiveRules 将原始配置解析为 exclusiveRule 列表
//
// 期望输入通常是 YAML 解析后的 []interface{}，每一项是一个 map：
//   - map[string]interface{} 或 map[interface{}]interface{}
//
// 解析后会过滤掉无效项，只保留合法的 label key/value。
func parseExclusiveRules(raw interface{}) []exclusiveRule {
	slice, ok := raw.([]interface{})
	if !ok {
		return nil
	}

	var rules []exclusiveRule

	for _, item := range slice {
		labels := make(map[string]string)

		switch m := item.(type) {
		case map[string]interface{}:
			for k, v := range m {
				if sv, ok := v.(string); ok {
					labels[k] = sv
				}
			}

		case map[interface{}]interface{}:
			for k, v := range m {
				if sk, ok := k.(string); ok {
					if sv, ok := v.(string); ok {
						labels[sk] = sv
					}
				}
			}
		}

		// 只保留非空规则
		if len(labels) > 0 {
			rules = append(rules, exclusiveRule{labels: labels})
		}
	}

	return rules
}

// matchingRules 返回 Pod 命中的规则索引列表
//
// 一个 Pod 只有在"同时满足某条规则里的所有 label 条件"时，
// 才算命中该规则。
func matchingRules(pod *v1.Pod, rules []exclusiveRule) []int {
	if pod.Labels == nil {
		return nil
	}

	var matched []int
	for i, rule := range rules {
		if podMatchesRule(pod, rule) {
			matched = append(matched, i)
		}
	}
	return matched
}

// podMatchesRule 判断 Pod 是否满足某条独占规则
//
// 判断逻辑：
// 规则中的每个 label key/value，Pod 都必须存在且完全相等，
// 才返回 true。
func podMatchesRule(pod *v1.Pod, rule exclusiveRule) bool {
	if pod.Labels == nil {
		return false
	}

	for k, v := range rule.labels {
		if podVal, ok := pod.Labels[k]; !ok || podVal != v {
			return false
		}
	}
	return true
}

// exclusiveGPUDevices 是对 vgpu.GPUDevices 的包装器
//
// 它的目标是：
//   - 对匹配独占规则的 Pod，强制其使用"独占 GPU"
//   - 不让同一规则组里的 Pod 与其他同规则 Pod 共享同一块 GPU
//   - 对不匹配规则的 Pod，完全沿用原始 GPU 设备逻辑
//
// 实现方式不是改写底层 allocator，而是在 FilterNode / Allocate 时
// 临时"屏蔽"掉某些 GPU，使底层 vGPU allocator 看不到这些 GPU，
// 从而达到独占效果。
type exclusiveGPUDevices struct {
	inner    *vgpu.GPUDevices // 真正执行 GPU 分配/过滤的底层设备对象
	cfg      gpuExclusiveConfig
	nodeName string
	plugin   *deviceSharePlugin

	// ruleGPUs[ruleIndex] = 该规则组已经占用/绑定的 GPU index 集合
	// 这是"规则维度"的 GPU 归属关系
	ruleGPUs map[int]map[int]struct{}

	// podRules[podKey] = 这个 Pod 命中了哪些规则
	// 即 namespace/name -> 规则索引集合
	podRules map[string]map[int]struct{}

	// podUIDs 用于把 namespace/name 映射回 Pod UID
	// 因为底层 PodMap 常以 UID 为 key，而上层使用 namespace/name
	podUIDs map[string]string
}

// 编译期接口检查：exclusiveGPUDevices 必须实现 api.Devices
var _ api.Devices = (*exclusiveGPUDevices)(nil)

// reservedGPUsForPod 返回这个 Pod 需要"保留/独占"的 GPU 集合
//
// 规则：
//   - 先找出这个 Pod 命中了哪些独占规则
//   - 再把这些规则已经占用的 GPU 合并起来
//
// 注意：这里只保证"同规则 Pod 之间"共享隔离。
// 不同规则之间是否共享，由当前实现决定（这里允许不同规则之间仍可能共享）。
func (a *exclusiveGPUDevices) reservedGPUsForPod(pod *v1.Pod) map[int]struct{} {
	matched := matchingRules(pod, a.cfg.rules)
	if len(matched) == 0 {
		return nil
	}

	result := make(map[int]struct{})
	for _, ruleIdx := range matched {
		if gpuSet, ok := a.ruleGPUs[ruleIdx]; ok {
			for gpuIdx := range gpuSet {
				result[gpuIdx] = struct{}{}
			}
		}
	}
	return result
}

// capGPUs 临时"屏蔽"指定 GPU
//
// 通过把 GPU 的 Number 设置为 UsedNum，
// 让底层 allocator 认为这块 GPU 已经没有可分配空间，从而跳过它。
//
// 返回值 saved 用于之后恢复原始 Number。
func (a *exclusiveGPUDevices) capGPUs(gpuIndices map[int]struct{}) map[int]uint {
	saved := make(map[int]uint, len(gpuIndices))
	for idx := range gpuIndices {
		if dev, ok := a.inner.Device[idx]; ok && dev != nil {
			saved[idx] = dev.Number
			dev.Number = dev.UsedNum
		}
	}
	return saved
}

// restoreGPUs 恢复之前被 cap 的 GPU 数量
func (a *exclusiveGPUDevices) restoreGPUs(saved map[int]uint) {
	for idx, num := range saved {
		if dev, ok := a.inner.Device[idx]; ok && dev != nil {
			dev.Number = num
		}
	}
}

// trackPodFromPodMap 根据底层 PodMap 记录，追踪某个 Pod 与 GPU/规则的关系
//
// 这个函数主要在 AddResource 阶段使用。
// 它不会重建整个 ruleGPUs，而是只增量地加入新 Pod 的信息。
func (a *exclusiveGPUDevices) trackPodFromPodMap(pod *v1.Pod) {
	matched := matchingRules(pod, a.cfg.rules)
	if len(matched) == 0 {
		return
	}

	// 先记录该 Pod 命中了哪些规则
	ruleSet := make(map[int]struct{}, len(matched))
	for _, idx := range matched {
		ruleSet[idx] = struct{}{}
	}
	a.podRules[podKey(pod)] = ruleSet

	// 只要这个 Pod 在某块 GPU 的 PodMap 中出现，
	// 就认为它已经与这块 GPU 建立了占用关系。
	podUID := string(pod.UID)
	for gpuIdx, dev := range a.inner.Device {
		if dev == nil {
			continue
		}
		if _, ok := dev.PodMap[podUID]; ok {
			for ruleIdx := range ruleSet {
				if a.ruleGPUs[ruleIdx] == nil {
					a.ruleGPUs[ruleIdx] = make(map[int]struct{})
				}
				a.ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
			}
		}
	}
}

// untrackPod 移除 Pod 的规则关联和 GPU 占用关系
//
// 这个函数在 Pod 释放资源时调用。
// 它会清理 podRules，并尝试从 ruleGPUs 中移除已经不再被占用的 GPU。
func (a *exclusiveGPUDevices) untrackPod(pod *v1.Pod) {
	pk := podKey(pod)
	ruleSet, ok := a.podRules[pk]
	if !ok {
		return
	}
	delete(a.podRules, pk)

	for ruleIdx := range ruleSet {
		gpuSet := a.ruleGPUs[ruleIdx]
		if gpuSet == nil {
			continue
		}

		for gpuIdx := range gpuSet {
			dev, ok := a.inner.Device[gpuIdx]
			if !ok || dev == nil {
				continue
			}

			podUID := string(pod.UID)
			stillUsed := false

			// 检查这块 GPU 是否仍被其他 Pod 使用
			for uid := range dev.PodMap {
				if uid != podUID {
					// 通过 podUIDs 反查 namespace/name，判断该 UID 是否仍对应活跃 Pod
					for otherKey := range a.podRules {
						if a.podUIDs[otherKey] == uid {
							stillUsed = true
							break
						}
					}
					if stillUsed {
						break
					}
				}
			}

			// 如果没有其他 Pod 仍在使用，则从规则集合中删除
			if !stillUsed {
				delete(gpuSet, gpuIdx)
			}
		}

		// 如果某个规则已经没有任何 GPU 绑定了，则清理该规则
		if len(gpuSet) == 0 {
			delete(a.ruleGPUs, ruleIdx)
		}
	}
}

// ---------------------------------------------------------
// api.Devices 接口实现
// ---------------------------------------------------------

// AddResource 在 Pod 资源加入时调用
//
// 这里先调用底层 GPUDevices 的 AddResource，
// 再把当前 Pod 的规则和 GPU 关系记录到 wrapper 中。
func (a *exclusiveGPUDevices) AddResource(pod *v1.Pod) {
	a.inner.AddResource(pod)
	a.podUIDs[podKey(pod)] = string(pod.UID)
	a.trackPodFromPodMap(pod)
}

// SubResource 在 Pod 资源释放时调用
func (a *exclusiveGPUDevices) SubResource(pod *v1.Pod) {
	a.inner.SubResource(pod)
	a.untrackPod(pod)
	delete(a.podUIDs, podKey(pod))
}

// AddQueueResource 直接透传到底层实现
func (a *exclusiveGPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	return a.inner.AddQueueResource(pod)
}

// HasDeviceRequest 判断 Pod 是否请求了该类设备
func (a *exclusiveGPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	return a.inner.HasDeviceRequest(pod)
}

// FilterNode 做节点过滤
//
// 如果 Pod 命中了独占规则：
//   - 先把已被该规则组占用的 GPU 暂时 cap 掉
//   - 再调用底层 GPU 过滤逻辑
//   - 最后恢复 GPU 数量
//
// 这样可以保证同规则 Pod 之间不会抢到同一块 GPU。
func (a *exclusiveGPUDevices) FilterNode(pod *v1.Pod, policy string) (int, string, error) {
	reserved := a.reservedGPUsForPod(pod)
	if len(reserved) > 0 {
		saved := a.capGPUs(reserved)
		code, msg, err := a.inner.FilterNode(pod, policy)
		a.restoreGPUs(saved)
		return code, msg, err
	}
	return a.inner.FilterNode(pod, policy)
}

// ScoreNode 直接透传给底层设备
func (a *exclusiveGPUDevices) ScoreNode(pod *v1.Pod, policy string) float64 {
	return a.inner.ScoreNode(pod, policy)
}

// Allocate 执行资源分配
//
// 这里是独占逻辑最关键的地方：
//   - 如果 Pod 命中了规则，就先屏蔽掉相关 GPU 再分配
//   - 分配完成后，再根据 PodMap 记录更新 ruleGPUs
//   - 同时把分配结果持久化到 plugin 的跨 session 缓存中
func (a *exclusiveGPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	matched := matchingRules(pod, a.cfg.rules)
	if len(matched) == 0 {
		// 不匹配规则：完全沿用底层逻辑
		return a.inner.Allocate(kubeClient, pod)
	}

	// 找出当前 Pod 需要独占/避开的 GPU
	reserved := a.reservedGPUsForPod(pod)
	klog.V(4).Infof("gpuexclusive: Allocate pod=%s, matched=%v, reserved=%v, ruleGPUs=%v", pod.Name, matched, reserved, a.ruleGPUs)

	// 如果有需要屏蔽的 GPU，就临时 cap 后再分配
	if len(reserved) > 0 {
		saved := a.capGPUs(reserved)
		err := a.inner.Allocate(kubeClient, pod)
		a.restoreGPUs(saved)
		if err != nil {
			return err
		}
	} else {
		if err := a.inner.Allocate(kubeClient, pod); err != nil {
			return err
		}
	}

	// 保存该 Pod 命中的规则
	ruleSet := make(map[int]struct{}, len(matched))
	for _, idx := range matched {
		ruleSet[idx] = struct{}{}
	}
	pk := podKey(pod)
	a.podRules[pk] = ruleSet

	// 通过 PodMap 识别本次新分配到的 GPU
	// 注意：这里不能依赖 UsedNum，因为分配阶段 UsedNum 可能尚未刷新
	newGPUs := make(map[int]struct{})
	podUID := string(pod.UID)
	for idx, dev := range a.inner.Device {
		if dev == nil {
			continue
		}
		if _, ok := dev.PodMap[podUID]; ok {
			newGPUs[idx] = struct{}{}
			for ruleIdx := range ruleSet {
				if a.ruleGPUs[ruleIdx] == nil {
					a.ruleGPUs[ruleIdx] = make(map[int]struct{})
				}
				a.ruleGPUs[ruleIdx][idx] = struct{}{}
			}
		}
	}

	// 把结果持久化到 plugin 级别缓存，跨 session 保留
	if a.plugin != nil && a.nodeName != "" {
		a.plugin.lock.Lock()
		if a.plugin.persistedGPUs[a.nodeName] == nil {
			a.plugin.persistedGPUs[a.nodeName] = make(map[string]map[int]struct{})
		}
		a.plugin.persistedGPUs[a.nodeName][pk] = newGPUs

		if a.plugin.persistedPodRules[a.nodeName] == nil {
			a.plugin.persistedPodRules[a.nodeName] = make(map[string]map[int]struct{})
		}
		a.plugin.persistedPodRules[a.nodeName][pk] = ruleSet
		a.plugin.lock.Unlock()
	}

	klog.V(4).Infof("gpuexclusive: allocated pod %s, newGPUs=%v, ruleGPUs=%v",
		pk, newGPUs, a.ruleGPUs)
	return nil
}

// Release 释放资源
func (a *exclusiveGPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	err := a.inner.Release(kubeClient, pod)
	if err != nil {
		return err
	}
	a.untrackPod(pod)
	return nil
}

// GetIgnoredDevices 直接透传
func (a *exclusiveGPUDevices) GetIgnoredDevices() []string {
	return a.inner.GetIgnoredDevices()
}

// GetStatus 直接透传
func (a *exclusiveGPUDevices) GetStatus() string {
	return a.inner.GetStatus()
}

// DeepCopy 深拷贝一个 exclusiveGPUDevices
//
// 这个函数常用于 dry-run / 模拟调度，
// 要保证副本和原对象互不干扰，所以要把可变字段都复制出来。
func (a *exclusiveGPUDevices) DeepCopy() interface{} {
	if a == nil {
		return nil
	}

	cp := &exclusiveGPUDevices{
		cfg:      a.cfg,
		nodeName: a.nodeName,
		plugin:   a.plugin,
		ruleGPUs: make(map[int]map[int]struct{}, len(a.ruleGPUs)),
		podRules: make(map[string]map[int]struct{}, len(a.podRules)),
		podUIDs:  make(map[string]string, len(a.podUIDs)),
	}

	// 深拷贝 ruleGPUs
	for ruleIdx, gpuSet := range a.ruleGPUs {
		newSet := make(map[int]struct{}, len(gpuSet))
		for g := range gpuSet {
			newSet[g] = struct{}{}
		}
		cp.ruleGPUs[ruleIdx] = newSet
	}

	// 深拷贝 podRules
	for podKey, ruleSet := range a.podRules {
		newSet := make(map[int]struct{}, len(ruleSet))
		for r := range ruleSet {
			newSet[r] = struct{}{}
		}
		cp.podRules[podKey] = newSet
	}

	// 深拷贝 podUIDs
	for k, v := range a.podUIDs {
		cp.podUIDs[k] = v
	}

	// 深拷贝底层 GPUDevices
	if a.inner != nil {
		cp.inner = a.inner.DeepCopy().(*vgpu.GPUDevices)
	}

	return cp
}

// wrapGPUDevicesForExclusivity
//
// 这个函数在 deviceshare 的 OnSessionOpen 中调用，
// 用于把节点上的 vgpu.GPUDevices 包装成 exclusiveGPUDevices。
//
// 包装后的对象会接管：
//   - FilterNode
//   - Allocate
//   - Release
//   - DeepCopy
//
// 从而在不修改底层 vGPU allocator 的前提下，实现"按规则独占 GPU"。
func (dp *deviceSharePlugin) wrapGPUDevicesForExclusivity(ssn *framework.Session) {
	dp.lock.Lock()
	defer dp.lock.Unlock()

	// 读取独占规则配置
	cfg := loadGPUExclusiveConfig(dp.pluginArguments)
	klog.V(4).Infof("gpuexclusive config: rules=%v", cfg.rules)

	// 没配置规则则直接退出，不做任何包装
	if len(cfg.rules) == 0 {
		klog.V(2).Info("gpuexclusive: no rules configured, skipping GPU exclusivity wrapping")
		return
	}

	// 遍历所有节点，找到其上的 vgpu 设备进行包装
	for _, node := range ssn.Nodes {
		if node.Others == nil {
			continue
		}

		devObj, ok := node.Others[vgpu.DeviceName]
		if !ok || devObj == nil {
			continue
		}

		inner, ok := devObj.(*vgpu.GPUDevices)
		if !ok || inner == nil {
			continue
		}

		// 仅支持 hami-core 模式的 GPU 独占
		// 如果是硬件级 MIG 等模式，则直接跳过
		if inner.Mode != "" && inner.Mode != "hami-core" {
			klog.V(4).Infof("gpuexclusive: skipping node %s with GPU mode %q (only hami-core supported)", node.Name, inner.Mode)
			continue
		}

		// =========================================================
		// 构建当前节点上的 Pod-规则关系
		// =========================================================
		podRules := make(map[string]map[int]struct{})
		podUIDs := make(map[string]string)
		uidToKey := make(map[string]string)

		// 从 node.Tasks 中收集已有 Pod
		for _, task := range node.Tasks {
			if task.Pod == nil {
				continue
			}
			pk := podKey(task.Pod)
			uid := string(task.Pod.UID)
			podUIDs[pk] = uid
			uidToKey[uid] = pk

			matched := matchingRules(task.Pod, cfg.rules)
			if len(matched) == 0 {
				continue
			}

			ruleSet := make(map[int]struct{}, len(matched))
			for _, idx := range matched {
				ruleSet[idx] = struct{}{}
			}
			podRules[pk] = ruleSet
		}

		// 建立 UUID -> GPU index 的反向映射
		// 用于后续从 Pod annotation 中解析 GPU 分配情况
		uuidToIdx := make(map[string]int, len(inner.Device))
		for idx, dev := range inner.Device {
			if dev != nil {
				uuidToIdx[dev.UUID] = idx
			}
		}

		// =========================================================
		// 构建 ruleGPUs：规则组已经占用的 GPU 集合
		// =========================================================
		ruleGPUs := make(map[int]map[int]struct{})

		// 来源 1：从 PodMap 读取
		for gpuIdx, dev := range inner.Device {
			if dev == nil {
				continue
			}
			for podUID := range dev.PodMap {
				pk := uidToKey[podUID]
				if ruleSet, ok := podRules[pk]; ok {
					for ruleIdx := range ruleSet {
						if ruleGPUs[ruleIdx] == nil {
							ruleGPUs[ruleIdx] = make(map[int]struct{})
						}
						ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
					}
				}
			}
		}

		// 来源 2：从 Pod annotation 读取
		for pk, ruleSet := range podRules {
			podUID := podUIDs[pk]

			// 如果 PodMap 已经能找到，就不用再从 annotation 推导
			alreadyTracked := false
			for _, dev := range inner.Device {
				if dev == nil {
					continue
				}
				if _, ok := dev.PodMap[podUID]; ok {
					alreadyTracked = true
					break
				}
			}
			if alreadyTracked {
				continue
			}

			// 遍历任务，找到这个 Pod 对应的 annotation
			for _, task := range node.Tasks {
				if task.Pod == nil || podKey(task.Pod) != pk {
					continue
				}

				ann, ok := task.Pod.Annotations[vgpu.AssignedIDsAnnotations]
				if !ok || ann == "" {
					break
				}

				// 将 annotation 中的 GPU UUID 解析出来
				for _, contDevs := range vgpu.DecodePodDevices(ann) {
					for _, cd := range contDevs {
						if gpuIdx, ok := uuidToIdx[cd.UUID]; ok {
							for ruleIdx := range ruleSet {
								if ruleGPUs[ruleIdx] == nil {
									ruleGPUs[ruleIdx] = make(map[int]struct{})
								}
								ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
							}
						}
					}
				}
				break
			}
		}

		// 来源 3：从跨 session 持久化数据恢复
		if persisted, ok := dp.persistedGPUs[node.Name]; ok {
			persistedRules := dp.persistedPodRules[node.Name]

			for pk, gpuSet := range persisted {
				// 只恢复当前仍然存在的 Pod
				if _, inPodRules := podRules[pk]; !inPodRules {
					continue
				}

				podUID := podUIDs[pk]

				// 如果 PodMap 已经能追踪到了，就不需要再恢复
				alreadyTracked := false
				for _, dev := range inner.Device {
					if dev == nil {
						continue
					}
					if _, ok := dev.PodMap[podUID]; ok {
						alreadyTracked = true
						break
					}
				}
				if alreadyTracked {
					continue
				}

				ruleSet := persistedRules[pk]
				if ruleSet == nil {
					continue
				}

				for gpuIdx := range gpuSet {
					for ruleIdx := range ruleSet {
						if ruleGPUs[ruleIdx] == nil {
							ruleGPUs[ruleIdx] = make(map[int]struct{})
						}
						ruleGPUs[ruleIdx][gpuIdx] = struct{}{}
					}
				}
			}
		}

		// 清理已经不在该节点上的持久化 Pod
		activePods := make(map[string]bool, len(node.Tasks))
		for _, task := range node.Tasks {
			if task.Pod != nil {
				activePods[podKey(task.Pod)] = true
			}
		}
		if persisted, ok := dp.persistedGPUs[node.Name]; ok {
			for pk := range persisted {
				if !activePods[pk] {
					delete(persisted, pk)
					if pr, ok := dp.persistedPodRules[node.Name]; ok {
						delete(pr, pk)
					}
				}
			}
		}

		// 用 wrapper 替换原始 vgpu.GPUDevices
		wrapper := &exclusiveGPUDevices{
			inner:    inner,
			cfg:      cfg,
			nodeName: node.Name,
			plugin:   dp,
			ruleGPUs: ruleGPUs,
			podRules: podRules,
			podUIDs:  podUIDs,
		}
		node.Others[vgpu.DeviceName] = wrapper

		klog.V(4).Infof("gpuexclusive: OnSessionOpen node=%s, podRules=%v, ruleGPUs=%v, tasks=%d",
			node.Name, podRules, ruleGPUs, len(node.Tasks))
	}
}
