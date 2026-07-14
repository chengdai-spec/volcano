/*
Copyright 2025 The Volcano Authors.

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

package vgpu

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
)

// MIGFactory 实现 MIG（Multi-Instance GPU）模式的 SharingFactory。
//
// MIG 是 NVIDIA A100/H100 等显卡支持的硬件级虚拟化技术，
// 可以将一块物理 GPU 在硬件层面切分为多个互相隔离的 GPU 实例。
// 每个实例拥有独立的显存、计算核心和缓存，彼此完全隔离。
//
// 与 hami-core 不同，MIG 模式下 Volcano 调度器需要理解几何切分模板，
// 并跟踪每个 MIG 实例的占用情况。
type MIGFactory struct{}

func init() {
	RegisterFactory(vGPUControllerMIG, MIGFactory{})
}

// TryAddPod 在 predicate 阶段尝试为该 Pod 分配一个 MIG 实例。
//
// 流程：
//  1. 根据配置的 GPUMemoryFactor 对请求显存进行缩放。
//  2. 调用 findMatch 在 MigTemplate 和 MigUsage 中查找满足显存需求的 MIG 实例。
//  3. 若找到，更新设备的 UsedNum、UsedMem、UsedCore，返回 MIG 实例 ID。
func (f MIGFactory) TryAddPod(gd *GPUDevice, mem uint, core uint) (bool, string) {
	requestMemory := mem
	memoryFactor := getConfig().GPUMemoryFactor
	if memoryFactor > 1 {
		requestMemory = requestMemory * memoryFactor
		klog.V(5).Infof("rawRequestMemory: %d, realRequestMemory: %d, memoryFactor: %d", mem, requestMemory, memoryFactor)
	}
	found, dev, usedMem := findMatch(gd.UUID, requestMemory, gd.MigUsage, gd.MigTemplate)
	if !found {
		return false, ""
	}
	realMem := usedMem
	if memoryFactor > 1 {
		realMem = usedMem / memoryFactor
		klog.V(5).Infof("rawUsedMemory: %d, realUsedMemory: %d, memoryFactor: %d", usedMem, realMem, memoryFactor)
	}

	gd.UsedNum++
	gd.UsedMem += realMem
	gd.UsedCore += core
	return true, dev
}

// AddPod 真正将 Pod 加入指定的 MIG 实例。
//
// 流程：
//  1. 从 devID 中解析出 group 名称和 position（MIG 实例位置）。
//  2. 调用 addMigUsed 更新 MigUsage，标记该 MIG 实例已被占用。
//  3. 更新设备的 UsedNum、UsedMem、UsedCore 以及 PodMap。
func (f MIGFactory) AddPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	group, index, err := decodeMIGID(devID)
	if err != nil {
		klog.ErrorS(err, "Failed to add pod")
		return err
	}

	_, ok := gd.PodMap[podUID]
	if !ok {
		gd.PodMap[podUID] = &GPUUsage{
			UsedMem:  0,
			UsedCore: 0,
		}
	} else {
		return nil
	}
	usedMem := addMigUsed(gd, group, index)
	realMem := usedMem
	memoryFactor := getConfig().GPUMemoryFactor
	if memoryFactor > 1 {
		realMem = usedMem / memoryFactor
		klog.V(5).Infof("rawUsedMemory: %d, realUsedMemory: %d, memoryFactor: %d", usedMem, realMem, memoryFactor)
	}
	gd.UsedNum++
	gd.UsedMem += realMem
	gd.UsedCore += core

	gd.PodMap[podUID].UsedMem += realMem
	gd.PodMap[podUID].UsedCore += core

	klog.V(4).Infoln("add Pod: ", podUID, realMem, gd.PodMap[podUID].UsedMem, gd.PodMap[podUID].UsedCore)
	return nil
}

// SubPod 将 Pod 从指定的 MIG 实例中释放。
//
// 流程：
//  1. 从 devID 中解析出 group 名称和 position。
//  2. 调用 subMigUsed 更新 MigUsage，回收 MIG 实例。
//  3. 更新设备的 UsedNum、UsedMem、UsedCore，并从 PodMap 中删除 Pod。
func (f MIGFactory) SubPod(gd *GPUDevice, mem uint, core uint, podUID string, devID string) error {
	groupName, index, err := decodeMIGID(devID)
	if err != nil {
		return fmt.Errorf("Failed to sub pod: %v", err)
	}

	_, ok := gd.PodMap[podUID]
	if !ok {
		return fmt.Errorf("pod not exist in GPU pod map")
	}

	usedMem := subMigUsed(gd, groupName, index)
	realMem := usedMem
	memoryFactor := getConfig().GPUMemoryFactor
	if memoryFactor > 1 {
		realMem = usedMem / memoryFactor
		klog.V(5).Infof("rawUsedMemory: %d, realUsedMemory: %d, memoryFactor: %d", usedMem, realMem, memoryFactor)
	}
	gd.UsedNum--
	gd.UsedMem -= realMem
	gd.UsedCore -= core

	klog.V(4).Infoln("sub Pod: ", podUID, realMem)
	delete(gd.PodMap, podUID)
	return nil
}

// findMatch 根据请求的显存在 MIG 几何模板中查找一个可用实例。
//
// 参数：
//   - uuid: 物理 GPU 的 UUID
//   - requestMem: 请求显存（已考虑 memoryFactor 缩放）
//   - usage: 当前 MIG 使用状态
//   - allowedGeometries: 该 GPU 支持的几何切分模板列表
//
// 返回值：
//   - bool: 是否找到匹配实例
//   - string: 匹配到的 MIG 实例 ID（格式见 encodeMIGID）
//   - uint: 该实例实际占用的显存
//
// 逻辑：
//   - 如果当前已经选定了一个 group（usage.Index >= 0），则只在该 group 内查找。
//   - 否则遍历所有 group，按顺序尝试匹配。
//
// 实际案例：
// 某 A100 支持 group2：3×2g.20gb + 1×1g.10gb。
// 当前未使用 MIG，Pod 请求 15GiB，则 findMatch 会返回 2g.20gb 实例（20GiB >= 15GiB）。
func findMatch(
	uuid string,
	requestMem uint,
	usage config.MigInUse,
	allowedGeometries []config.Geometry,
) (bool, string, uint) {
	// If a group is already in use
	if usage.Index >= 0 {
		group := allowedGeometries[usage.Index]
		fitted, position, realMem := pickFromGroup(group, usage.UsageList, requestMem)
		if fitted {
			MIGID := encodeMIGID(uuid, group.Group, position)
			return true, MIGID, realMem
		} else {
			return false, "", 0
		}
	}

	// No group in use yet, try groups in order
	for _, group := range allowedGeometries {
		fitted, position, realMem := pickFromGroup(group, nil, requestMem)
		if fitted {
			MIGID := encodeMIGID(uuid, group.Group, position)
			return true, MIGID, realMem
		} else {
			continue
		}
	}
	return false, "", 0
}

// pickFromGroup 在一个 group 内查找满足显存需求且仍有空闲槽位的 MIG 实例。
//
// 查找策略：
//   - 按实例显存从小到大排序。
//   - 优先选择能满足 requestMemory 的最小实例，减少显存浪费。
//   - 若该类型实例有空闲槽位（Count - len(UsedIndex) > 0），则分配。
//
// 返回值：
//   - bool: 是否找到
//   - int: MIG 实例在 group 中的 position
//   - uint: 实例显存容量
func pickFromGroup(group config.Geometry, usage config.MIGS, requestMemory uint) (bool, int, uint) {
	type MigTemplateWithIndex struct {
		Index    int
		Instance config.MigTemplate
	}
	// Sort instances by memory ascending
	var instances []MigTemplateWithIndex
	for i, inst := range group.Instances {
		instances = append(instances, MigTemplateWithIndex{
			Index:    i,
			Instance: inst,
		})
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].Instance.Memory < instances[j].Instance.Memory
	})

	for _, inst := range instances {
		if len(usage) == 0 && inst.Instance.Memory >= requestMemory {
			position := getPosition(group, inst.Instance.Count, []int{}, inst.Index)
			klog.V(4).Infoln("pick mig group with no used group: ", inst.Index, inst.Instance.Memory, group.Group)
			return true, position, inst.Instance.Memory
		}
		for _, usedInst := range usage {
			if usedInst.Name == inst.Instance.Name {
				available := inst.Instance.Count - len(usedInst.UsedIndex)
				if available > 0 && inst.Instance.Memory >= requestMemory {
					position := getPosition(group, inst.Instance.Count, usedInst.UsedIndex, inst.Index)
					klog.V(4).Infoln("pick mig group with used group: ", usedInst.Name, inst.Instance.Name, position)
					return true, position, inst.Instance.Memory
				}
			}
		}
	}
	klog.V(2).Infoln("pick mig group but no suitalbe")
	return false, -1, 0
}

// getPosition 计算某个 MIG 实例在 group 中的全局 position。
//
// position 的计算方式：
//   - 先累加该实例之前所有类型实例的数量。
//   - 再在该类型实例中找一个未被使用的索引 i，累加 i。
//
// 例如 group 包含 3 个 2g.20gb 和 1 个 1g.10gb，
// 则 1g.10gb 的类型索引为 1，前面类型数量为 3，若其第一个实例未被使用，则 position = 3 + 0 = 3。
func getPosition(group config.Geometry, count int, usedIndex []int, index int) int {
	position := 0
	for i := 0; i < index; i++ {
		position += group.Instances[i].Count
	}

	for i := 0; i < count; i++ {
		found := false
		for _, v := range usedIndex {
			if v == i {
				found = true
				break
			}
		}
		if !found {
			position += i
			break
		}
	}

	return position
}

// findPosition 根据全局 position 反推其在 group 中的类型索引和资源索引。
func findPosition(group config.Geometry, position int) (instanceIndex, resourceIndex int) {
	sum := 0
	for i, instance := range group.Instances {
		if position < sum+instance.Count {
			return i, position - sum
		}
		sum += instance.Count
	}
	return -1, -1
}

// encodeMIGID 将物理 GPU UUID、group 名称和 position 编码为 MIG 实例 ID。
//
// 格式：UUID[group-position]
// 例如：GPU-0fc3eda5-e98b-a25b-5b0d-cf5c855d1448[group2-3]
func encodeMIGID(uuid, group string, position int) string {
	return fmt.Sprintf("%s[%s-%d]", uuid, group, position)
}

// decodeMIGID 从 MIG 实例 ID 中解析出 group 名称和 position。
func decodeMIGID(id string) (group string, position int, err error) {
	// Find the opening bracket
	start := strings.Index(id, "[")
	end := strings.Index(id, "]")

	if start == -1 || end == -1 || start > end {
		return "", 0, fmt.Errorf("invalid format")
	}

	content := id[start+1 : end]
	parts := strings.Split(content, "-")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid format inside brackets")
	}

	group = parts[0]
	position, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid index: %v", err)
	}

	return group, position, nil
}

// addMigUsed 标记某个 MIG 实例已被占用，并返回其实际显存。
//
// 流程：
//  1. 根据 groupName 找到对应的 group 模板。
//  2. 根据 position 找到具体实例类型和资源索引。
//  3. 更新 MigUsage.UsageList，将该资源索引加入 UsedIndex。
//  4. 返回该实例类型的显存容量。
func addMigUsed(gd *GPUDevice, groupName string, position int) uint {
	for groupIndex, group := range gd.MigTemplate {
		if group.Group == groupName {
			klog.V(4).Infoln("add mig used: ", group.Group, groupIndex)
			gd.MigUsage.Index = groupIndex
			instanceIndex, resourceIndex := findPosition(group, position)
			migName := group.Instances[instanceIndex].Name
			klog.V(4).Infoln("add mig: ", migName, gd.MigUsage.UsageList)
			for i := range gd.MigUsage.UsageList {
				if gd.MigUsage.UsageList[i].Name == migName {
					gd.MigUsage.UsageList[i].UsedIndex = insert(gd.MigUsage.UsageList[i].UsedIndex, resourceIndex)
					return gd.MigUsage.UsageList[i].Memory
				}
			}
			newUsage := config.MigTemplateUsage{
				Name:      group.Instances[instanceIndex].Name,
				Memory:    group.Instances[instanceIndex].Memory,
				InUse:     true,
				UsedIndex: []int{resourceIndex},
			}
			gd.MigUsage.UsageList = append(gd.MigUsage.UsageList, newUsage)
			return group.Instances[instanceIndex].Memory
		}
	}
	return 0
}

// subMigUsed 回收某个 MIG 实例，并返回其实际显存。
//
// 流程：
//  1. 根据 groupName 找到对应 group 模板。
//  2. 根据 position 找到具体实例类型和资源索引。
//  3. 从 MigUsage.UsageList 对应类型的 UsedIndex 中移除该资源索引。
//  4. 若某类型实例全部释放，则从 UsageList 中删除该类型。
func subMigUsed(gd *GPUDevice, groupName string, position int) uint {
	for groupIndex, group := range gd.MigTemplate {
		if group.Group == groupName {
			gd.MigUsage.Index = groupIndex
			instanceIndex, resourceIndex := findPosition(group, position)
			migName := group.Instances[instanceIndex].Name
			for i := range gd.MigUsage.UsageList {
				if gd.MigUsage.UsageList[i].Name != migName {
					continue
				}
				gd.MigUsage.UsageList[i].UsedIndex = remove(gd.MigUsage.UsageList[i].UsedIndex, resourceIndex)
				klog.V(4).Infoln("sub mig used after remove:", gd.MigUsage.UsageList[i].UsedIndex)
				mem := gd.MigUsage.UsageList[i].Memory
				if len(gd.MigUsage.UsageList[i].UsedIndex) == 0 {
					gd.MigUsage.UsageList = append(gd.MigUsage.UsageList[:i], gd.MigUsage.UsageList[i+1:]...)
				}
				return mem
			}
			return 0
		}
	}
	return 0
}

// insert 将一个资源索引按升序插入 UsedIndex 列表。
//
// 若该索引已存在，说明出现重复分配，打印错误并返回原列表。
func insert(list []int, value int) []int {
	klog.V(4).Infoln("insert mig used list before: ", list, value)
	// Find the correct index to insert
	i := 0
	for ; i < len(list); i++ {
		if value < list[i] {
			break
		} else if value == list[i] {
			klog.Error("insert mig used list but invalid gpu location: ", list, value)
			return list
		}
	}
	// Insert at index i
	list = append(list, 0)
	copy(list[i+1:], list[i:])
	list[i] = value
	klog.V(4).Infoln("insert mig used list after: ", list)
	return list
}

// remove 从 UsedIndex 列表中移除第一个出现的 value。
func remove(list []int, value int) []int {
	klog.V(4).Info("remove mig used list before: ", list, value)
	for i, v := range list {
		if v == value {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list // value not found
}
