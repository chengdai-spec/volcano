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

// wrapGPUDevicesForExclusivity 在每个调度周期(Session)开始时，为启用了 GPU 独占规则的节点
// 把原始的 vgpu.GPUDevices 包装成 exclusiveGPUDevices。
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║  一句话理解：                                                               ║
// ║  "同一个团队的 Pod，不能共用 GPU"                                             ║
// ║                                                                           ║
// ║  实现手段：在调度时临时把"已被同团队占用"的 GPU 标记为已满，                        ║
// ║  让底层分配器看不到这些 GPU，从而自动避开。                                      ║
// ╚═══════════════════════════════════════════════════════════════════════════╝
//
// ┌───────────────────────────────────────────────────────────────────────────┐
// │ 实战案例                                                                    │
// │                                                                           │
// │ 配置：                                                                     │
// │   规则 0: {team: "ai"}      ← 带 team=ai 标签的 Pod 互相独占 GPU              │
// │   规则 1: {team: "render"}  ← 带 team=render 标签的 Pod 互相独占 GPU          │
// │                                                                           │
// │ 节点 "gpu-node-01" 上有 4 块 GPU(编号 0~3)，当前已运行 4 个 Pod：               │
// │                                                                           │
// │   ┌─────────────────────┬──────────────┬───────────────────────┐          │
// │   │ Pod                 │ 标签         │ 占用的 GPU              │          │
// │   ├─────────────────────┼──────────────┼───────────────────────┤          │
// │   │ pod-alice (team=ai) │ team=ai      │ GPU 0（PodMap 已记录）  │          │
// │   │ pod-bob   (team=ai) │ team=ai      │ GPU 1（刚调度，PodMap   │          │
// │   │                     │              │       还没更新）        │          │
// │   │ pod-carol (render)  │ team=render  │ GPU 2（PodMap 已记录）  │          │

// │   └─────────────────────┴──────────────┴───────────────────────┘          │
// │                                                                           │
// │ 调度器重启后，持久化缓存中还保留了上次的数据：                                     │
// │   persistedGPUs["gpu-node-01"]["default/pod-bob"]  = {GPU 1}              │
// │   persistedGPUs["gpu-node-01"]["default/pod-old"]  = {GPU 3} ← Pod 已删除  │
// │                                                                           │
// │ ── 本函数执行后期望得到的结果 ──                                               │
// │                                                                           │
// │   ruleGPUs = {                                                            │
// │     规则0(ai):     {GPU 0, GPU 1},   ← ai 团队已占用 0 和 1                  │
// │     规则1(render): {GPU 2},           ← render 团队已占用 2                  │
// │   }                                                                       │
// │                                                                           │
// │ 效果：                                                                     │
// │   新来 pod-eve (team=ai) 调度时：                                           │
// │     → 查出规则 0 已占 GPU 0,1 → 把 GPU 0,1 临时标记为"已满"                    │
// │     → 底层分配器只能从 GPU 2,3 中选 → Eve 分到 GPU 2 或 3                      │
// │     → 这样就保证了 ai 团队的每个 Pod 都独占自己的 GPU                           │
// └───────────────────────────────────────────────────────────────────────────┘
//
// 整体执行流程（6 步）：
//  1. 读取独占规则配置 → 没有配置就跳过
//  2. 遍历每个节点 → 取出 vgpu 设备（仅支持 hami-core 模式）
//  3. 扫描节点上所有 Pod → 建立「Pod ↔ 规则」的对应关系 (podRules)
//  4. 从 3 个数据源收集「规则 ↔ 已占用 GPU」(ruleGPUs)
//     来源 A：底层 PodMap（最可靠，实时数据）
//     来源 B：Pod annotation（PodMap 未更新时的回退）
//     来源 C：持久化缓存（调度器重启后的兜底）
//  5. 清理持久化缓存中已失效的 Pod 数据
//  6. 用 exclusiveGPUDevices 包装器替换原始设备对象
func (dp *deviceSharePlugin) wrapGPUDevicesForExclusivity(ssn *framework.Session) {
	dp.lock.Lock()
	defer dp.lock.Unlock()

	// ══════════════════════════════════════════════════════════════════
	// 第 1 步：从调度器配置中读取独占规则
	// ══════════════════════════════════════════════════════════════════
	//
	// 配置示例（YAML）：
	//   tiers:
	//   - plugins:
	//     - name: deviceshare
	//       arguments:
	//         deviceshare.GPUExclusiveRules:
	//           - team: "ai"           ← 规则 0：带 team=ai 的 Pod 构成独占组
	//             gpu-excl: "true"     ← Pod 必须同时匹配这两个标签才算命中规则 0
	//           - team: "render"       ← 规则 1：带 team=render 的 Pod 构成独占组
	//
	// 每条规则是一组 label 键值对，Pod 必须同时满足所有键值对才算命中该规则。
	cfg := loadGPUExclusiveConfig(dp.pluginArguments)
	klog.V(4).Infof("gpuexclusive config: rules=%v", cfg.rules)

	// 没有配置任何规则 → 不需要独占逻辑，直接返回
	if len(cfg.rules) == 0 {
		klog.V(2).Info("gpuexclusive: no rules configured, skipping GPU exclusivity wrapping")
		return
	}

	// ══════════════════════════════════════════════════════════════════
	// 第 2 步：遍历所有节点，取出该节点上的 vgpu 设备对象
	// ══════════════════════════════════════════════════════════════════
	for _, node := range ssn.Nodes {
		if node.Others == nil {
			continue
		}

		// 从节点的 Others 字典中取出 vgpu 设备对象
		// Others 是 Volcano 为每种设备类型维护的 map，key 是设备名称
		devObj, ok := node.Others[vgpu.DeviceName]
		if !ok || devObj == nil {
			continue // 该节点没有 vgpu 设备，跳过
		}

		// 类型断言为底层 *vgpu.GPUDevices
		// 只有成功断言才能拿到 GPU 列表、PodMap 等信息
		inner, ok := devObj.(*vgpu.GPUDevices)
		if !ok || inner == nil {
			continue
		}

		// 仅支持 hami-core 模式（软件层 vGPU 切分）。
		// 因为独占的实现手段是“修改 GPU 的 Number 字段让分配器认为已满”，
		// 只有 hami-core 模式下 Number 才有这个语义。
		// 其他模式如 MIG（硬件级切分）不能用这种方式。
		if inner.Mode != "" && inner.Mode != "hami-core" {
			klog.V(4).Infof("gpuexclusive: skipping node %s with GPU mode %q (only hami-core supported)", node.Name, inner.Mode)
			continue
		}

		// ══════════════════════════════════════════════════════════════
		// 第 3 步：扫描节点上所有 Pod，建立三个辅助 map
		// ══════════════════════════════════════════════════════════════
		//
		// 这一步产出三个 map，供后续步骤使用：
		//
		// ① podRules：哪个 Pod 命中了哪些规则
		//    key:   "namespace/name"（Pod 唯一标识）
		//    value: 规则索引集合（如 {0} 表示命中规则 0）
		//    案例结果：
		//      podRules = {
		//        "default/pod-alice": {0},   ← team=ai → 命中规则 0
		//        "default/pod-bob":   {0},   ← team=ai → 命中规则 0
		//        "default/pod-carol": {1},   ← team=render → 命中规则 1
		//      }
		//      pod-dave 无标签，不命中任何规则，不进入 podRules
		//
		// ② podUIDs：namespace/name → Pod UID 的映射
		//    用途：后续需要通过 UID 去 PodMap 或 annotation 中查找
		//
		// ③ uidToKey：Pod UID → namespace/name 的反向映射
		//    用途：底层 GPU 的 PodMap 以 UID 为 key，
		//    需要反查到 namespace/name 才能和 podRules 对应
		podRules := make(map[string]map[int]struct{})
		podUIDs := make(map[string]string)
		uidToKey := make(map[string]string)

		// 遍历节点上所有已调度的 Pod
		for _, task := range node.Tasks {
			if task.Pod == nil {
				continue
			}
			pk := podKey(task.Pod) // "namespace/name"
			uid := string(task.Pod.UID)
			podUIDs[pk] = uid
			uidToKey[uid] = pk

			// 检查这个 Pod 的标签命中了哪些独占规则
			matched := matchingRules(task.Pod, cfg.rules)
			if len(matched) == 0 {
				continue // 不命中任何规则（如 pod-dave），跳过
			}

			// 把命中的规则索引存入 ruleSet
			ruleSet := make(map[int]struct{}, len(matched))
			for _, idx := range matched {
				ruleSet[idx] = struct{}{}
			}
			podRules[pk] = ruleSet
		}

		// 额外建立 GPU UUID → GPU 编号 的反向映射
		// 原因：底层 GPU 设备用 UUID 标识，但 ruleGPUs 用编号（0,1,2,3）标识
		// 案例中：
		//   uuidToIdx = {"gpu-uuid-0": 0, "gpu-uuid-1": 1, "gpu-uuid-2": 2, "gpu-uuid-3": 3}
		uuidToIdx := make(map[string]int, len(inner.Device))
		for idx, dev := range inner.Device {
			if dev != nil {
				uuidToIdx[dev.UUID] = idx
			}
		}

		// ══════════════════════════════════════════════════════════════
		// 第 4 步：从 3 个数据源收集「规则 ↔ 已占用 GPU」(ruleGPUs)
		// ══════════════════════════════════════════════════════════════
		//
		// ruleGPUs 是本函数的核心产出：
		//   key:   规则索引（如 0 代表 {team: "ai"}）
		//   value: 该规则已占用的 GPU 编号集合
		//
		// 为什么要 3 个数据源？
		//   因为 GPU 占用信息的“可靠性”和“时效性”不同：
		//
		//   来源 A（PodMap）：实时数据，最可靠，但可能有延迟
		//     → Pod 刚调度完时，PodMap 可能还没更新
		//
		//   来源 B（Annotation）：Pod 分配 GPU 后写入的注解
		//     → PodMap 没更新时，可以从这里补上
		//
		//   来源 C（持久化缓存）：调度器重启前保存的快照
		//     → 前两个都找不到时的兆底方案
		//
		// 三个来源按优先级从高到低依次处理，后面的只补充前面遗漏的。
		ruleGPUs := make(map[int]map[int]struct{})

		// ─────────────────────────────────────────────────────────────
		// 来源 A（最可靠）：从底层 GPU 设备的 PodMap 读取
		// ─────────────────────────────────────────────────────────────
		//
		// 每块 GPU 设备都有一个 PodMap，记录哪些 Pod（以 UID 为 key）正在使用它。
		// 遍历所有 GPU 的 PodMap：如果某个 Pod 命中了独占规则，
		// 就把这块 GPU 标记为该规则的"已占用"。
		//
		// 案例执行过程：
		//   GPU 0 的 PodMap: {uid-alice, uid-dave}
		//     → uid-alice → pod-alice → 命中规则 0 → ruleGPUs[0] += GPU 0 ✔
		//     → uid-dave  → pod-dave  → 无规则      → 跳过
		//   GPU 1 的 PodMap: {uid-dave}
		//     → uid-dave → pod-dave → 无规则 → 跳过
		//     ⚠️ pod-bob 在 GPU 1 上，但 PodMap 还没更新！
		//   GPU 2 的 PodMap: {uid-carol}
		//     → uid-carol → pod-carol → 命中规则 1 → ruleGPUs[1] += GPU 2 ✔
		//   GPU 3 的 PodMap: {} (空)
		//
		// 来源 A 结束后：
		//   ruleGPUs = { 0: {0}, 1: {2} }
		//   ⚠️ 缺少 pod-bob 在 GPU 1 上的信息，需要后续来源补充
		for gpuIdx, dev := range inner.Device {
			if dev == nil {
				continue
			}
			for podUID := range dev.PodMap {
				// 通过 UID 反查 namespace/name
				pk := uidToKey[podUID]
				// 检查这个 Pod 是否命中了独占规则
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

		// ─────────────────────────────────────────────────────────────
		// 来源 B（回退方案）：从 Pod annotation 读取
		// ─────────────────────────────────────────────────────────────
		//
		// 什么时候需要这个来源？
		//   Pod 刚被调度、还在 Allocate 阶段时，PodMap 可能还没更新。
		//   但 GPU 分配器已经把结果写入了 Pod 的 annotation 中
		//   （key = vgpu.AssignedIDsAnnotations）。
		//
		// 处理策略：
		//   只对"PodMap 中找不到"的 Pod 查 annotation，避免重复。
		//
		// 案例执行过程：
		//   pod-alice: GPU 0 的 PodMap 中有 uid-alice → 已跟踪 → 跳过
		//   pod-bob: 所有 GPU 的 PodMap 都没有 uid-bob → 查 annotation
		//     → 读 pod-bob 的 annotation，解码得到 GPU UUID "gpu-uuid-1"
		//     → uuidToIdx["gpu-uuid-1"] = 1
		//     → ruleGPUs[0] += GPU 1 ✔
		//   pod-carol: GPU 2 的 PodMap 中有 uid-carol → 已跟踪 → 跳过
		//
		// 来源 B 结束后：
		//   ruleGPUs = { 0: {0, 1}, 1: {2} }   ← 补上了 pod-bob 的信息！
		for pk, ruleSet := range podRules {
			podUID := podUIDs[pk]

			// 先检查 PodMap 中是否已经能找到这个 Pod
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
				continue // PodMap 已经有了，不需要从 annotation 推导
			}

			// 在 node.Tasks 中找到这个 Pod，读取它的 annotation
			for _, task := range node.Tasks {
				if task.Pod == nil || podKey(task.Pod) != pk {
					continue
				}

				// 读取 vGPU 分配 annotation
				ann, ok := task.Pod.Annotations[vgpu.AssignedIDsAnnotations]
				if !ok || ann == "" {
					break // 没有 annotation，无法推导
				}

				// 解码 annotation，得到每个容器分配的 GPU 设备信息
				// DecodePodDevices 返回 [][]ContainerDevice，每个 ContainerDevice 包含 UUID
				for _, contDevs := range vgpu.DecodePodDevices(ann) {
					for _, cd := range contDevs {
						// 把 GPU UUID 转换为 GPU index
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
				break // 找到对应 Pod 后就可以退出了
			}
		}

		// ─────────────────────────────────────────────────────────────
		// 来源 C（兆底方案）：从跨 Session 持久化缓存恢复
		// ─────────────────────────────────────────────────────────────
		//
		// 什么时候需要这个来源？
		//   调度器重启后，PodMap 和 annotation 可能都丢失了，
		//   但插件级别的持久化缓存（persistedGPUs）跨 session 保留。
		//
		// 数据结构：
		//   persistedGPUs[nodeName][podKey]     = GPU 编号集合
		//   persistedPodRules[nodeName][podKey] = 规则索引集合
		//
		// 恢复条件（两个必须同时满足）：
		//   a. Pod 仍然存在于当前节点（在 podRules 中）
		//   b. PodMap 中找不到该 Pod（否则来源 A 已处理）
		//
		// 案例执行过程：
		//   persistedGPUs["gpu-node-01"]["default/pod-bob"] = {1}
		//     → pod-bob 在 podRules 中 ✔
		//     → PodMap 中找不到 uid-bob → 需要恢复
		//     → ruleGPUs[0] += GPU 1（重复添加也无妨，set 自动去重）
		//
		//   persistedGPUs["gpu-node-01"]["default/pod-old"] = {3}
		//     → pod-old 不在 podRules 中 → Pod 已不存在 → 跳过
		//
		// 来源 C 结束后：
		//   ruleGPUs = { 0: {0, 1}, 1: {2} }  （无变化，本例中来源 B 已处理）
		if persisted, ok := dp.persistedGPUs[node.Name]; ok {

			persistedRules := dp.persistedPodRules[node.Name]

			for pk, gpuSet := range persisted {
				// 条件 a：Pod 必须仍然存在于当前节点
				if _, inPodRules := podRules[pk]; !inPodRules {
					continue // Pod 已不在节点上，跳过
				}

				podUID := podUIDs[pk]

				// 条件 b：PodMap 中找不到才需要恢复
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
					continue // PodMap 已经有了，不需要从持久化恢复
				}

				// 从持久化缓存中恢复 GPU 归属关系
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

		// ══════════════════════════════════════════════════════════════
		// 第 5 步：清理持久化缓存中已失效的 Pod 数据
		// ══════════════════════════════════════════════════════════════
		//
		// 持久化缓存中可能有过期的条目（Pod 已被删除或迁移到其他节点），
		// 如果不清理，会导致：
		//   - 内存泄漏（积累越来越多无效数据）
		//   - 错误占用（已删除 Pod 的 GPU 仍然被标记为“被占用”）
		//
		// 案例执行过程：
		//   activePods = {pod-alice, pod-bob, pod-carol, pod-dave}
		//   persisted 中有 "default/pod-old" → 不在 activePods 中 → 删除
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

		// ══════════════════════════════════════════════════════════════
		// 第 6 步：用 exclusiveGPUDevices 包装器替换原始设备对象
		// ══════════════════════════════════════════════════════════════
		//
		// 替换后，整个调度周期内对该节点的 vgpu 设备的所有操作：
		//   - FilterNode(判断 Pod 能否调度到该节点)
		//   - Allocate(分配 GPU 资源)
		//   - Release(释放 GPU 资源)
		//   - DeepCopy(调度模拟时的深拷贝)
		// 都会经过 exclusiveGPUDevices 的包装逻辑,从而实现 GPU 独占
		//
		// 案例最终状态：
		//   wrapper.ruleGPUs = { 0: {0, 1}, 1: {2} }
		//
		//   含义：
		//     - 规则 0（team=ai）已占用 GPU 0 和 1
		//     - 规则 1（team=render）已占用 GPU 2
		//     - GPU 3 空闲
		//
		// 后续调度效果：
		//   新来 pod-eve (team=ai) 调度时：
		//     ① reservedGPUsForPod(pod-eve) 返回 {0, 1}
		//     ② capGPUs({0,1}) 把 GPU 0,1 的 Number 设为 UsedNum
		//     ③ 底层分配器认为 GPU 0,1 已满，只能看到 GPU 2,3
		//     ④ pod-eve 被分配到 GPU 2 或 3
		//     ⑤ restoreGPUs 恢复 GPU 0,1 的原始 Number
		//     → 实现了 ai 团队内每个 Pod 独占 GPU 的目标！
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
