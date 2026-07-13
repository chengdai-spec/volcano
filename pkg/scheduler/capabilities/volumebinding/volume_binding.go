/*
Copyright 2019 The Kubernetes Authors.

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

// Package volumebinding 实现了 Volcano 调度器中的卷绑定插件。
// 该插件负责在调度过程中处理 Pod 的 PVC（PersistentVolumeClaim）绑定逻辑，
// 包括检查 PVC 状态、查找匹配的 PV、节点亲和性校验、动态供给以及最终绑定等。
package volumebinding

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"

	"volcano.sh/volcano/cmd/scheduler/app/options"
)

const (
	// stateKey 用于在 CycleState 中存储/读取 volumeBinding 插件的状态数据。
	stateKey fwk.StateKey = Name

	// maxUtilization 表示存储利用率的最大值（100%），用于评分归一化。
	maxUtilization = 100
)

// stateData 保存了 volumeBinding 插件在整个调度周期（PreFilter -> Filter -> Reserve -> PreBind）中需要的状态。
// 该状态在 PreFilter 阶段初始化，后续阶段通过同一个指针访问，因此无需显式 Write 更新。
type stateData struct {
	// allBound 标记该 Pod 的所有 PVC 是否都已绑定(AssumePodVolumes 的结果)
	allBound bool
	// podVolumesByNode 记录 Filter 阶段为每个候选节点找到的卷绑定方案(PodVolumes)
	// 在 PreFilter 阶段初始化为空 map，在 Filter 阶段按节点填充
	podVolumesByNode map[string]*PodVolumes
	// podVolumeClaims 保存 Pod 中所有 PVC 的分类信息(已绑定、延迟绑定未绑定、立即绑定未绑定)
	podVolumeClaims *PodVolumeClaims
	// hasStaticBindings 声明 Pod 是否包含一个或多个静态绑定(StaticBinding)
	// 如果没有静态绑定，volumeBinding 将跳过 Score 扩展点
	hasStaticBindings bool
	// 互斥锁，保护 podVolumesByNode 和 hasStaticBindings 的并发访问
	// 因为多个节点会并发执行 Filter，必须通过锁保证状态安全
	sync.Mutex
}

// Clone 返回自身指针。
// 由于 CycleState 可能在多个节点间共享时会被复制，但 stateData 内部使用锁保护共享字段，
// 因此直接返回指针以保留同一份状态。
func (d *stateData) Clone() fwk.StateData {
	return d
}

// VolumeBinding 是卷绑定插件的主体结构体。
// 它在 Filter 阶段为 Pod 创建绑定缓存，并在 Reserve 和 PreBind 阶段使用这些缓存完成卷的假定绑定和真实绑定
type VolumeBinding struct {
	// Binder 是卷绑定器，负责查找/假定/回滚/绑定 Pod 卷
	Binder SchedulerVolumeBinder
	// PVCLister 用于列出 PersistentVolumeClaim
	PVCLister corelisters.PersistentVolumeClaimLister
	// classLister 用于列出 StorageClass
	classLister storagelisters.StorageClassLister
	// scorer 是存储容量评分函数，基于 StorageClass 的容量利用率打分
	scorer volumeCapacityScorer
	// fts 保存调度框架特性开关，例如 EnableStorageCapacityScoring、EnableSchedulingQueueHint
	fts feature.Features
}

// 以下接口断言确保 VolumeBinding 实现了对应的调度框架扩展点。
var _ fwk.PreFilterPlugin = &VolumeBinding{}
var _ fwk.FilterPlugin = &VolumeBinding{}
var _ fwk.ReservePlugin = &VolumeBinding{}
var _ fwk.PreBindPlugin = &VolumeBinding{}
var _ fwk.PreScorePlugin = &VolumeBinding{}
var _ fwk.ScorePlugin = &VolumeBinding{}
var _ fwk.EnqueueExtensions = &VolumeBinding{}
var _ fwk.SignPlugin = &VolumeBinding{}

// Name 是插件在注册表和配置中的名称。
const Name = names.VolumeBinding

// Name 返回插件名称，用于日志等场景。
func (pl *VolumeBinding) Name() string {
	return Name
}

// SignPod 基于非合成卷源为 Pod 生成签名片段。
// 返回值包含一个 VolumesSignerName 片段，其值为根据 Pod 卷计算出的签名，
// 用于调度器识别具有相同卷需求的 Pod 组。
func (pl *VolumeBinding) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.VolumesSignerName, Value: fwk.VolumesSigner(pod)},
	}, nil
}

// EventsToRegister 返回可能使被本插件判定为不可调度的 Pod 重新变为可调度的集群事件。
// 调度队列通过监听这些事件，在相关资源发生变化时重新尝试调度之前失败的 Pod。
func (pl *VolumeBinding) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	// 当节点标签与 StorageClass 的 allowedTopologies 或 PV 的节点亲和性不匹配时，
	// Pod 可能无法找到可用 PV。新增或更新节点可能使 Pod 变为可调度。
	//
	// 关于 UpdateNodeTaint 事件的说明：
	// 理论上只需要 Add | UpdateNodeLabel，因为 UpdateNodeTaint 不会影响本插件的结果。
	// 但由于 preCheck 可能漏掉 Node/Add 事件，因此对所有注册 Node/Add 的插件同时注册 UpdateNodeTaint | UpdateNodeLabel。
	// 参见：https://github.com/kubernetes/kubernetes/issues/109437
	nodeActionType := fwk.Add | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint
	if pl.fts.EnableSchedulingQueueHint {
		// 启用调度队列提示后，不再使用有问题的 preCheck，因此无需注册 UpdateNodeTaint。
		nodeActionType = fwk.Add | fwk.UpdateNodeLabel
	}
	events := []fwk.ClusterEventWithHint{
		// StorageClass 缺失或配置错误(如 allowedTopologies、volumeBindingMode)会导致 Pod 不可调度。
		// StorageClass 新增或更新时，可能使 Pod 重新变为可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.StorageClass, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterStorageClassChange},

		// PVC 与 PV 的绑定关系会直接影响 Pod 可调度性，因此监听 PVC/PV 的新增和更新。
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimChange},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Add | fwk.Update}},

		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: nodeActionType}},

		// 依赖 CSINode 将 in-tree PV 翻译为 CSI。
		// TODO: 当所有卷插件完成 CSI 迁移后，kube-scheduler 将取消注册 CSINode 事件。
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSINodeChange},

		// 启用 CSI 存储容量跟踪时，CSI 驱动和存储容量的变化可能使 Pod 变为可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.CSIDriver, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSIDriverChange},
		{Event: fwk.ClusterEvent{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSIStorageCapacityChange},
	}

	return events, nil
}

// isSchedulableAfterCSINodeChange 判断 CSINode 变更是否可能使 Pod 变为可调度。
// 主要关注 migrated plugins annotation 是否发生变化，因为该注解变化可能影响 in-tree 到 CSI 的迁移结果。
func (pl *VolumeBinding) isSchedulableAfterCSINodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	if oldObj == nil {
		logger.V(5).Info("CSINode creation could make the pod schedulable")
		return fwk.Queue, nil
	}
	oldCSINode, modifiedCSINode, err := util.As[*storagev1.CSINode](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"CSINode", klog.KObj(modifiedCSINode),
	)

	if oldCSINode.ObjectMeta.Annotations[v1.MigratedPluginsAnnotationKey] != modifiedCSINode.ObjectMeta.Annotations[v1.MigratedPluginsAnnotationKey] {
		logger.V(5).Info("CSINode's migrated plugins annotation is updated and that may make the pod schedulable")
		return fwk.Queue, nil
	}

	logger.V(5).Info("CISNode was created or updated but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterPersistentVolumeClaimChange 判断 PVC 变更是否可能使 Pod 变为可调度。
// 只有 Pod 实际引用的、且与 Pod 同命名空间的 PVC 新增或更新时，才认为可能重新调度。
func (pl *VolumeBinding) isSchedulableAfterPersistentVolumeClaimChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, newPVC, err := util.As[*v1.PersistentVolumeClaim](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"PersistentVolumeClaim", klog.KObj(newPVC),
	)

	if pod.Namespace != newPVC.Namespace {
		logger.V(5).Info("PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable because the PVC belongs to a different namespace")
		return fwk.QueueSkip, nil
	}

	for _, vol := range pod.Spec.Volumes {
		var pvcName string
		switch {
		case vol.PersistentVolumeClaim != nil:
			pvcName = vol.PersistentVolumeClaim.ClaimName
		case vol.Ephemeral != nil:
			pvcName = ephemeral.VolumeClaimName(pod, &vol)
		default:
			continue
		}

		if pvcName == newPVC.Name {
			// 返回 Queue，因为这种情况下 PVC 的新增和大多数更新都可能使目标 Pod 变为可调度。
			logger.V(5).Info("PersistentVolumeClaim the pod requires was created or updated, potentially making the target Pod schedulable")
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterStorageClassChange 判断 StorageClass 变更是否可能使 Pod 变为可调度。
// StorageClass 的新增以及 allowedTopologies 字段的更新都可能改变 Pod 的可调度性。
// 注意：volumeBindingMode 不允许更新，因此无需考虑该字段变化。
func (pl *VolumeBinding) isSchedulableAfterStorageClassChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldSC, newSC, err := util.As[*storagev1.StorageClass](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"StorageClass", klog.KObj(newSC),
	)

	if oldSC == nil {
		// 对于新增事件，无法进一步过滤，总是返回 Queue。
		logger.V(5).Info("A new StorageClass was created, which could make a Pod schedulable")
		return fwk.Queue, nil
	}

	if !apiequality.Semantic.DeepEqual(newSC.AllowedTopologies, oldSC.AllowedTopologies) {
		logger.V(5).Info("StorageClass got an update in AllowedTopologies", "AllowedTopologies", newSC.AllowedTopologies)
		return fwk.Queue, nil
	}

	logger.V(5).Info("StorageClass was updated, but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterCSIStorageCapacityChange 判断 CSIStorageCapacity 变更是否可能使 Pod 变为可调度。
// CSIStorageCapacity 的新增以及 volume limit(基于 capacity 和 maximumVolumeSize 计算)的提升都可能使 Pod 可调度。
// 注意：nodeTopology 和 storageClassName 不允许更新，因此无需考虑。
func (pl *VolumeBinding) isSchedulableAfterCSIStorageCapacityChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldCap, newCap, err := util.As[*storagev1beta1.CSIStorageCapacity](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if oldCap == nil {
		logger.V(5).Info(
			"A new CSIStorageCapacity was created, which could make a Pod schedulable",
			"Pod", klog.KObj(pod),
			"CSIStorageCapacity", klog.KObj(newCap),
		)
		return fwk.Queue, nil
	}
	/*
		apiVersion: storage.k8s.io/v1
		kind: CSIStorageCapacity
		metadata:
		  name: example-capacity
		  namespace: kube-system
		storageClassName: fast
		capacity: 100Gi
		maximumVolumeSize: 20Gi
		nodeTopology:
		  matchLabelExpressions:
		    - key: topology.kubernetes.io/zone
		      values:
		        - us-east-1a
	*/

	oldLimit := volumeLimit(oldCap)
	newLimit := volumeLimit(newCap)

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"CSIStorageCapacity", klog.KObj(newCap),
		"volumeLimit(new)", newLimit,
		"volumeLimit(old)", oldLimit,
	)

	if newLimit != nil && (oldLimit == nil || newLimit.Value() > oldLimit.Value()) {
		logger.V(5).Info("VolumeLimit was increased, which could make a Pod schedulable")
		return fwk.Queue, nil
	}

	logger.V(5).Info("CSIStorageCapacity was updated, but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterCSIDriverChange 判断 CSIDriver 变更是否可能使 Pod 变为可调度。
// 当 Pod 使用了某个 CSI 驱动，且该驱动的 StorageCapacity 从启用变为禁用时，
// 意味着不再受容量限制约束，可能使之前因容量不足而不可调度的 Pod 变为可调度。
func (pl *VolumeBinding) isSchedulableAfterCSIDriverChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalCSIDriver, modifiedCSIDriver, err := util.As[*storagev1.CSIDriver](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"CSIDriver", klog.KObj(modifiedCSIDriver),
	)

	for _, vol := range pod.Spec.Volumes {
		if vol.CSI == nil || vol.CSI.Driver != modifiedCSIDriver.Name {
			continue
		}
		if (originalCSIDriver.Spec.StorageCapacity != nil && *originalCSIDriver.Spec.StorageCapacity) &&
			(modifiedCSIDriver.Spec.StorageCapacity == nil || !*modifiedCSIDriver.Spec.StorageCapacity) {
			logger.V(5).Info("CSIDriver was updated and storage capacity got disabled, which may make the pod schedulable")
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("CSIDriver was created or updated but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// podHasPVCs 返回两个值：
// 1. 给定 Pod 是否定义了任何 PVC；
// 2. 如果请求的 PVC 不合法，返回相应错误。
func (pl *VolumeBinding) podHasPVCs(pod *v1.Pod) (bool, error) {
	hasPVC := false
	for _, vol := range pod.Spec.Volumes {
		var pvcName string
		isEphemeral := false
		switch {
		case vol.PersistentVolumeClaim != nil:
			pvcName = vol.PersistentVolumeClaim.ClaimName
		case vol.Ephemeral != nil:
			pvcName = ephemeral.VolumeClaimName(pod, &vol)
			isEphemeral = true
		default:
			// 该 Volume 未使用 PVC，忽略。
			continue
		}
		hasPVC = true
		pvc, err := pl.PVCLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
		if err != nil {
			// 错误信息通常已经足够（如 persistentvolumeclaim "myclaim" not found），
			// 但对于通用临时卷（generic ephemeral inline volumes），创建 Pod 后立即出现未找到是正常现象，
			// 因此给出更友好的提示。
			if isEphemeral && apierrors.IsNotFound(err) {
				err = fmt.Errorf("waiting for ephemeral volume controller to create the persistentvolumeclaim %q", pvcName)
			}
			return hasPVC, err
		}

		if pvc.Status.Phase == v1.ClaimLost {
			return hasPVC, fmt.Errorf("persistentvolumeclaim %q bound to non-existent persistentvolume %q", pvc.Name, pvc.Spec.VolumeName)
		}

		if pvc.DeletionTimestamp != nil {
			return hasPVC, fmt.Errorf("persistentvolumeclaim %q is being deleted", pvc.Name)
		}

		if isEphemeral {
			if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
				return hasPVC, err
			}
		}
	}
	return hasPVC, nil
}

// PreFilter 在 prefilter 扩展点被调用，检查 Pod 的所有 immediate PVC 是否都已绑定。
// 如果存在未绑定的 immediate PVC，则返回 UnschedulableAndUnresolvable，
// Pod 会被放入 active/backoff 队列，等待 PV controller 完成绑定后再重试。
func (pl *VolumeBinding) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	logger := klog.FromContext(ctx)
	// 如果 Pod 没有引用任何 PVC，则无需后续处理。
	if hasPVC, err := pl.podHasPVCs(pod); err != nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
	} else if !hasPVC {
		state.Write(stateKey, &stateData{})
		return nil, fwk.NewStatus(fwk.Skip)
	}
	podVolumeClaims, err := pl.Binder.GetPodVolumeClaims(logger, pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if len(podVolumeClaims.unboundClaimsImmediate) > 0 {
		// 如果 immediate 类型的 PVC 未绑定，返回 UnschedulableAndUnresolvable
		// Pod 将在这些 PVC 被 PV controller 绑定后，重新进入调度队列
		status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable)
		status.AppendReason("pod has unbound immediate PersistentVolumeClaims")
		return nil, status
	}
	state.Write(stateKey, &stateData{
		podVolumesByNode: make(map[string]*PodVolumes),
		podVolumeClaims: &PodVolumeClaims{
			boundClaims:                podVolumeClaims.boundClaims,
			unboundClaimsDelayBinding:  podVolumeClaims.unboundClaimsDelayBinding,
			unboundVolumesDelayBinding: podVolumeClaims.unboundVolumesDelayBinding,
		},
	})
	return nil, nil
}

// PreFilterExtensions 返回 prefilter 扩展接口（Pod 添加/移除时的处理）。
// volumeBinding 不需要该扩展，因此返回 nil。
func (pl *VolumeBinding) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// getStateData 从 CycleState 中读取并转换为 stateData。
func getStateData(cs fwk.CycleState) (*stateData, error) {
	state, err := cs.Read(stateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*stateData)
	if !ok {
		return nil, errors.New("unable to convert state into stateData")
	}
	return s, nil
}

// Filter 在 filter 扩展点被调用，评估 Pod 是否能因所请求的卷而适配到当前节点。
//
// 对于已绑定的 PVC，检查对应 PV 的节点亲和性是否被给定节点满足。
//
// 对于未绑定的 PVC，尝试找到可用的 PV，使其满足 PVC 需求且 PV 节点亲和性被节点满足。
//
// 如果启用了存储容量跟踪，还需为节点和仍需创建的卷预留足够空间。
//
// 当所有已绑定 PVC 的 PV 与节点兼容，且所有未绑定 PVC 都能匹配到可用且节点兼容的 PV 时，返回成功。
func (pl *VolumeBinding) Filter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	state, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}

	podVolumes, reasons, err := pl.Binder.FindPodVolumes(logger, pod, state.podVolumeClaims, node)
	if err != nil {
		return fwk.AsStatus(err)
	}

	if len(reasons) > 0 {
		status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable)
		for _, reason := range reasons {
			status.AppendReason(string(reason))
		}
		return status
	}

	// 多个 goroutine 会并发在不同节点上调用 Filter，且 CycleState 可能被复制，
	// 因此必须加本地锁保护共享状态。
	state.Lock()
	state.podVolumesByNode[node.Name] = podVolumes
	state.hasStaticBindings = state.hasStaticBindings || (podVolumes != nil && len(podVolumes.StaticBindings) > 0)
	state.Unlock()
	return nil
}

// PreScore 在 prescore 扩展点被调用，判断 volumeBinding 是否可以跳过 Score。
// 如果未配置 scorer，或者没有静态绑定且未启用存储容量评分，则跳过 Score。
func (pl *VolumeBinding) PreScore(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	if pl.scorer == nil {
		return fwk.NewStatus(fwk.Skip)
	}
	state, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if state.hasStaticBindings || pl.fts.EnableStorageCapacityScoring {
		return nil
	}
	return fwk.NewStatus(fwk.Skip)
}

// Score 在 score 扩展点被调用，对节点进行存储容量评分。
// 评分逻辑根据静态绑定或动态供给分别汇总每个 StorageClass 的请求容量和可用容量，
// 然后通过 scorer 函数计算最终分数。
//
// 这个函数的目标不是判断“能不能调度”，
// 而是判断“在所有可调度节点里，哪个节点更适合这个 Pod 的卷需求”。
//
// 举例：
// - Pod 需要 50Gi 的 fast 存储；
// - node-1 的 fast 存储剩余 100Gi，利用率 50%；
// - node-2 的 fast 存储剩余 60Gi，利用率 83.3%。
// 如果 scorer 的策略是“利用率越低分越高”，则 node-1 得分更高。
func (pl *VolumeBinding) Score(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// 如果没有配置 scorer，说明本插件不参与打分
	if pl.scorer == nil {
		return 0, nil
	}

	// 从 CycleState 中取出 PreFilter 阶段保存的状态
	state, err := getStateData(cs)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}

	// 当前正在评分的节点名
	nodeName := nodeInfo.Node().Name

	// 取出 Filter 阶段为该节点计算好的卷绑定方案
	// 这一步说明：不同节点的卷匹配结果是不同的
	podVolumes, ok := state.podVolumesByNode[nodeName]
	if !ok {
		// 如果当前节点没有对应的卷方案，说明这个节点在 Filter 阶段没通过
		// 理论上不应该走到这里，但这里做兜底保护
		return 0, nil
	}

	/*
		classResources 的含义：
		按 StorageClass 聚合该 Pod 在当前节点上的存储请求和可用容量。

		例如：
		Pod 有两个 PVC：
		  - pvc-a: storageClass=fast, request=20Gi
		  - pvc-b: storageClass=fast, request=30Gi

		则聚合结果可能是：
		  classResources["fast"] = {Requested: 50Gi, Capacity: 100Gi}
	*/
	classResources := make(classResourceMap)

	// 这里分两种情况：
	// 1. 存在静态绑定
	// 2. 没启用 StorageCapacity scoring
	//
	// 这两种情况下，都按静态绑定 PV 的容量进行统计
	if len(podVolumes.StaticBindings) != 0 || !pl.fts.EnableStorageCapacityScoring {
		// 遍历所有静态绑定关系，把同一 StorageClass 的资源聚合起来
		for _, staticBinding := range podVolumes.StaticBindings {
			// 该绑定对应的 StorageClass 名称
			class := staticBinding.StorageClassName()

			// 获取该 PVC 请求容量与 PV 总容量
			storageResource := staticBinding.StorageResource()

			// 如果这个 StorageClass 还没初始化，就新建一个桶
			if _, ok := classResources[class]; !ok {
				classResources[class] = &StorageResource{
					Requested: 0,
					Capacity:  0,
				}
			}

			// 累加请求容量
			classResources[class].Requested += storageResource.Requested

			// 累加容量
			classResources[class].Capacity += storageResource.Capacity
		}
	} else {
		// 如果没有静态绑定，并且启用了 StorageCapacity scoring，
		// 那么就按动态供给场景统计 CSIStorageCapacity
		for _, provision := range podVolumes.DynamicProvisions {
			// NodeCapacity 可能为空，说明没有拿到容量信息，直接跳过
			if provision.NodeCapacity == nil {
				continue
			}

			// 动态供给时，取 PVC 的 StorageClass
			class := *provision.PVC.Spec.StorageClassName

			// 初始化聚合桶
			if _, ok := classResources[class]; !ok {
				classResources[class] = &StorageResource{
					Requested: 0,
					Capacity:  0,
				}
			}

			/*
				注意：这里不能用 +=

				原因：
				如果一个 Pod 有两个同类卷：
				  - 卷1请求 50Gi
				  - 卷2请求 50Gi
				而该节点该 StorageClass 的 CSIStorageCapacity 是 100Gi。

				如果你写成：
				  Capacity += 100Gi
				会变成 200Gi，这是错误的，因为节点总容量不是两次叠加的。
				所以这里直接赋值即可。
			*/
			classResources[class].Capacity = provision.NodeCapacity.Capacity.Value()

			// 累加请求容量
			requestedQty := provision.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
			classResources[class].Requested += requestedQty.Value()
		}
	}

	// 最后调用 scorer 计算分数
	// scorer 会根据每个 StorageClass 的 Requested / Capacity 比例，
	// 算出节点整体存储适配得分
	return pl.scorer(classResources), nil
}

// ScoreExtensions 返回 Score 插件的扩展接口。
func (pl *VolumeBinding) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// Reserve 在 reserve 扩展点被调用，假定绑定 Pod 的卷并将绑定状态保存到 cycle state。
// 对于选定的节点，调用 Binder.AssumePodVolumes 在内部缓存中假定 PV/PVC 绑定，
// 返回 allBound 表示是否所有卷都已经绑定。
func (pl *VolumeBinding) Reserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	state, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}
	// 给定 Pod 只会 reserve 一个节点，因此无需加锁
	podVolumes, ok := state.podVolumesByNode[nodeName]
	if ok {
		allBound, err := pl.Binder.AssumePodVolumes(klog.FromContext(ctx), pod, nodeName, podVolumes)
		if err != nil {
			return fwk.AsStatus(err)
		}
		state.allBound = allBound
	} else {
		// 如果 Pod 没有引用任何 PVC，map 中可能不存在该节点
		state.allBound = true
	}
	return nil
}

// PreBind 执行真正的 API 更新以完成假定绑定，并等待 PV controller 完成绑定操作
// 如果绑定出错、超时或被撤销，则返回错误以重试调度。
func (pl *VolumeBinding) PreBind(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	s, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if s.allBound {
		// 所有卷已绑定，无需再绑定。
		return nil
	}
	// 给定 Pod 只会 pre-bind 一个节点，因此无需加锁。
	podVolumes, ok := s.podVolumesByNode[nodeName]
	if !ok {
		return fwk.AsStatus(fmt.Errorf("no pod volumes found for node %q", nodeName))
	}
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Trying to bind volumes for pod", "pod", klog.KObj(pod))
	err = pl.Binder.BindPodVolumes(ctx, pod, podVolumes)
	if err != nil {
		logger.V(5).Info("Failed to bind volumes for pod", "pod", klog.KObj(pod), "err", err)
		return fwk.AsStatus(err)
	}
	logger.V(5).Info("Success binding volumes for pod", "pod", klog.KObj(pod))
	return nil
}

// Unreserve 清除假定绑定的 PV 和 PVC 缓存。
// 它是幂等的，如果没有找到对应缓存则什么都不做。
func (pl *VolumeBinding) Unreserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) {
	s, err := getStateData(cs)
	if err != nil {
		return
	}
	// 给定 Pod 只会 unreserve 一个节点，因此无需加锁。
	podVolumes, ok := s.podVolumesByNode[nodeName]
	if !ok {
		return
	}
	pl.Binder.RevertAssumedPodVolumes(podVolumes)
}

// New 初始化并返回一个新的 volumeBinding 插件实例。
// 该函数创建 informer、VolumeBinder、以及可选的容量评分函数。
func New(ctx context.Context, plArgs runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	args, ok := plArgs.(*config.VolumeBindingArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type VolumeBindingArgs, got %T", plArgs)
	}
	if err := validation.ValidateVolumeBindingArgsWithOptions(nil, args, validation.VolumeBindingArgsValidationOptions{
		AllowStorageCapacityScoring: fts.EnableStorageCapacityScoring,
	}); err != nil {
		return nil, err
	}
	podInformer := fh.SharedInformerFactory().Core().V1().Pods()
	nodeInformer := fh.SharedInformerFactory().Core().V1().Nodes()
	pvcInformer := fh.SharedInformerFactory().Core().V1().PersistentVolumeClaims()
	pvInformer := fh.SharedInformerFactory().Core().V1().PersistentVolumes()
	storageClassInformer := fh.SharedInformerFactory().Storage().V1().StorageClasses()
	csiNodeInformer := fh.SharedInformerFactory().Storage().V1().CSINodes()
	var capacityCheck *CapacityCheck
	if options.ServerOpts.EnableCSIStorage {
		capacityCheck = &CapacityCheck{
			CSIDriverInformer: fh.SharedInformerFactory().Storage().V1().CSIDrivers(),
			// k8s 1.27 之前的 CSIStorageCapacity API 版本为 v1beta1，
			// 因此 Volcano 在这里使用 v1beta1 客户端以保持对旧版本的兼容。
			CSIStorageCapacityInformer: fh.SharedInformerFactory().Storage().V1beta1().CSIStorageCapacities(),
		}
	}
	binder, err := NewVolumeBinder(
		klog.FromContext(ctx),
		fh.ClientSet(),
		fts,
		podInformer,
		nodeInformer,
		csiNodeInformer,
		pvcInformer,
		pvInformer, storageClassInformer, capacityCheck, time.Duration(args.BindTimeoutSeconds)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to build volume binder: %w", err)
	}

	// 构建评分函数。
	var scorer volumeCapacityScorer
	if fts.EnableStorageCapacityScoring {
		shape := make(helper.FunctionShape, 0, len(args.Shape))
		for _, point := range args.Shape {
			shape = append(shape, helper.FunctionShapePoint{
				Utilization: int64(point.Utilization),
				Score:       int64(point.Score) * (fwk.MaxNodeScore / config.MaxCustomPriorityScore),
			})
		}
		scorer = buildScorerFunction(shape)
	}
	return &VolumeBinding{
		Binder:      binder,
		PVCLister:   pvcInformer.Lister(),
		classLister: storageClassInformer.Lister(),
		scorer:      scorer,
		fts:         fts,
	}, nil
}

// PreBindPreFlight 在 PreBind 之前被调用，检查 Pod 是否有需要绑定的卷。
// 如果所有卷都已绑定，则返回 Skip；如果找不到对应节点的卷方案，则返回错误。
func (pl *VolumeBinding) PreBindPreFlight(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	s, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}

	if s.allBound {
		return fwk.NewStatus(fwk.Skip)
	}

	if _, ok := s.podVolumesByNode[nodeName]; !ok {
		return fwk.AsStatus(fmt.Errorf("no pod volumes found for node %q", nodeName))
	}

	return fwk.NewStatus(fwk.Success)
}
