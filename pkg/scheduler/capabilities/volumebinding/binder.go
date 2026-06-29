/*
Copyright 2017 The Kubernetes Authors.

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

package volumebinding

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1"
	storageinformersv1beta1 "k8s.io/client-go/informers/storage/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	storagelistersv1beta1 "k8s.io/client-go/listers/storage/v1beta1"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/component-helpers/storage/volume"
	csitrans "k8s.io/csi-translation-lib"
	csiplugins "k8s.io/csi-translation-lib/plugins"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding/metrics"
)

// ConflictReason 表示导致某个节点无法满足卷绑定的原因字符串
type ConflictReason string

// ConflictReasons 是多个 ConflictReason 的集合
type ConflictReasons []ConflictReason

func (reasons ConflictReasons) Len() int           { return len(reasons) }
func (reasons ConflictReasons) Less(i, j int) bool { return reasons[i] < reasons[j] }
func (reasons ConflictReasons) Swap(i, j int)      { reasons[i], reasons[j] = reasons[j], reasons[i] }

const (
	// ErrReasonBindConflict：没有找到可用于绑定的 PV
	ErrReasonBindConflict ConflictReason = "node(s) didn't find available persistent volumes to bind"
	// ErrReasonNodeConflict：PV 的 node affinity 不匹配
	ErrReasonNodeConflict ConflictReason = "node(s) didn't match PersistentVolume's node affinity"
	// ErrReasonNotEnoughSpace：节点存储容量不足
	ErrReasonNotEnoughSpace = "node(s) did not have enough free storage"
	// ErrReasonPVNotExist：PVC 绑定到了不存在的 PV
	ErrReasonPVNotExist = "node(s) unavailable due to one or more pvc(s) bound to non-existent pv(s)"
)

// BindingInfo 保存一个 PV 与 PVC 的绑定关系
type BindingInfo struct {
	// 需要绑定的 PVC
	pvc *v1.PersistentVolumeClaim

	// 准备绑定到该 PVC 的 PV
	pv *v1.PersistentVolume
}

// StorageClassName 返回 PV 的 StorageClass 名称
func (b *BindingInfo) StorageClassName() string {
	return b.pv.Spec.StorageClassName
}

// StorageResource 表示存储资源信息
type StorageResource struct {
	Requested int64
	Capacity  int64
}

// StorageResource 返回该绑定关系对应的请求容量和 PV 容量
func (b *BindingInfo) StorageResource() *StorageResource {
	// 这里假设两个字段都存在
	requestedQty := b.pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	capacityQty := b.pv.Spec.Capacity[v1.ResourceName(v1.ResourceStorage)]
	return &StorageResource{
		Requested: requestedQty.Value(),
		Capacity:  capacityQty.Value(),
	}
}

// DynamicProvision 表示需要动态创建的卷
type DynamicProvision struct {
	PVC          *v1.PersistentVolumeClaim
	NodeCapacity *storagev1beta1.CSIStorageCapacity
}

// PodVolumes 保存 Pod 的卷调度信息
type PodVolumes struct {
	// StaticBindings 表示可通过现有 PV 静态绑定的 PVC
	StaticBindings []*BindingInfo
	// DynamicProvisions 表示需要动态供给的 PVC
	DynamicProvisions []*DynamicProvision
}

// InTreeToCSITranslator 负责判断 PV 是否可迁移以及完成转换
type InTreeToCSITranslator interface {
	IsPVMigratable(pv *v1.PersistentVolume) bool
	GetInTreePluginNameFromSpec(pv *v1.PersistentVolume, vol *v1.Volume) (string, error)
	TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error)
}

// SchedulerVolumeBinder 是 scheduler 的卷绑定接口
// 它负责 PVC/PV 绑定，以及动态供给
type SchedulerVolumeBinder interface {
	// GetPodVolumeClaims 返回 Pod 的 PVC 分类：
	// 1.已绑定
	// 2.未绑定且延迟绑定（包括动态供给）
	// 3.未绑定且立即绑定（包括预绑定）
	// 4.属于延迟绑定 PVC 所属 StorageClass 的可用 PV
	GetPodVolumeClaims(logger klog.Logger, pod *v1.Pod) (podVolumeClaims *PodVolumeClaims, err error)

	// FindPodVolumes 检查某个节点是否能满足 Pod 的卷要求
	FindPodVolumes(logger klog.Logger, pod *v1.Pod, podVolumeClaims *PodVolumeClaims, node *v1.Node) (podVolumes *PodVolumes, reasons ConflictReasons, err error)

	// AssumePodVolumes 在调度器内部缓存中“假设”卷绑定成功
	AssumePodVolumes(logger klog.Logger, assumedPod *v1.Pod, nodeName string, podVolumes *PodVolumes) (allFullyBound bool, err error)

	// RevertAssumedPodVolumes 回滚缓存中的假设绑定
	RevertAssumedPodVolumes(podVolumes *PodVolumes)

	// BindPodVolumes 进行真正的 API 更新，并等待绑定完成
	BindPodVolumes(ctx context.Context, assumedPod *v1.Pod, podVolumes *PodVolumes) error
}

// PodVolumeClaims 保存 Pod 中 PVC 的分类结果
type PodVolumeClaims struct {
	// boundClaims：已经绑定的 PVC
	boundClaims []*v1.PersistentVolumeClaim
	// unboundClaimsDelayBinding：延迟绑定的未绑定 PVC
	unboundClaimsDelayBinding []*v1.PersistentVolumeClaim
	// unboundClaimsImmediate：立即绑定但当前还未绑定的 PVC
	unboundClaimsImmediate []*v1.PersistentVolumeClaim
	// unboundVolumesDelayBinding：属于延迟绑定 PVC 的可用 PV
	unboundVolumesDelayBinding map[string][]*v1.PersistentVolume
}

// volumeBinder 是 SchedulerVolumeBinder 的实现
type volumeBinder struct {
	kubeClient                  clientset.Interface
	enableVolumeAttributesClass bool
	enableCSIMigrationPortworx  bool

	classLister   storagelisters.StorageClassLister
	podLister     corelisters.PodLister
	nodeLister    corelisters.NodeLister
	csiNodeLister storagelisters.CSINodeLister

	pvcCache PVCAssumeCache
	pvCache  PVAssumeCache

	// 绑定等待超时时间
	bindTimeout time.Duration

	translator InTreeToCSITranslator

	// 是否启用了容量检查
	capacityCheckEnabled     bool
	csiDriverLister          storagelisters.CSIDriverLister
	csiStorageCapacityLister storagelistersv1beta1.CSIStorageCapacityLister
}

var _ SchedulerVolumeBinder = &volumeBinder{}

// CapacityCheck 包含容量检查需要的 informer
type CapacityCheck struct {
	CSIDriverInformer          storageinformers.CSIDriverInformer
	CSIStorageCapacityInformer storageinformersv1beta1.CSIStorageCapacityInformer
}

// NewVolumeBinder 创建 volumeBinder 并初始化缓存
func NewVolumeBinder(
	logger klog.Logger,
	kubeClient clientset.Interface,
	fts feature.Features,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	csiNodeInformer storageinformers.CSINodeInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	pvInformer coreinformers.PersistentVolumeInformer,
	storageClassInformer storageinformers.StorageClassInformer,
	capacityCheck *CapacityCheck,
	bindTimeout time.Duration) (SchedulerVolumeBinder, error) {

	// 初始化 PVC / PV 假设缓存
	pvcCache, err1 := NewPVCAssumeCache(logger, pvcInformer.Informer())
	pvCache, err2 := NewPVAssumeCache(logger, pvInformer.Informer())
	if err := errors.Join(err1, err2); err != nil {
		return nil, err
	}

	b := &volumeBinder{
		kubeClient:                  kubeClient,
		enableVolumeAttributesClass: fts.EnableVolumeAttributesClass,
		enableCSIMigrationPortworx:  fts.EnableCSIMigrationPortworx,
		podLister:                   podInformer.Lister(),
		classLister:                 storageClassInformer.Lister(),
		nodeLister:                  nodeInformer.Lister(),
		csiNodeLister:               csiNodeInformer.Lister(),
		pvcCache:                    pvcCache,
		pvCache:                     pvCache,
		bindTimeout:                 bindTimeout,
		translator:                  csitrans.New(),
	}

	// 如果启用了容量检查，则初始化相关 lister
	if capacityCheck != nil {
		b.capacityCheckEnabled = true
		b.csiDriverLister = capacityCheck.CSIDriverInformer.Lister()
		b.csiStorageCapacityLister = capacityCheck.CSIStorageCapacityInformer.Lister()
	}
	return b, nil
}

// FindPodVolumes 查找某个节点是否能满足 Pod 卷绑定需求
func (b *volumeBinder) FindPodVolumes(logger klog.Logger, pod *v1.Pod, podVolumeClaims *PodVolumeClaims, node *v1.Node) (podVolumes *PodVolumes, reasons ConflictReasons, err error) {
	podVolumes = &PodVolumes{}

	// 这里日志级别较高，因为可能会被频繁打印
	logger.V(5).Info("FindPodVolumes", "pod", klog.KObj(pod), "node", klog.KObj(node))

	// 下面几个标志位用于最终构造冲突原因
	unboundVolumesSatisfied := true
	boundVolumesSatisfied := true
	sufficientStorage := true
	boundPVsFound := true

	// 根据检查结果生成冲突原因
	defer func() {
		if err != nil {
			return
		}
		if !boundVolumesSatisfied {
			reasons = append(reasons, ErrReasonNodeConflict)
		}
		if !unboundVolumesSatisfied {
			reasons = append(reasons, ErrReasonBindConflict)
		}
		if !sufficientStorage {
			reasons = append(reasons, ErrReasonNotEnoughSpace)
		}
		if !boundPVsFound {
			reasons = append(reasons, ErrReasonPVNotExist)
		}
	}()

	// 统计调度阶段失败指标
	defer func() {
		if err != nil {
			metrics.VolumeSchedulingStageFailed.WithLabelValues("predicate").Inc()
		}
	}()

	var (
		staticBindings    []*BindingInfo
		dynamicProvisions []*DynamicProvision
	)

	// 统一把空切片归一成 nil，便于测试和后续判断
	defer func() {
		if len(staticBindings) == 0 {
			staticBindings = nil
		}
		if len(dynamicProvisions) == 0 {
			dynamicProvisions = nil
		}
		podVolumes.StaticBindings = staticBindings
		podVolumes.DynamicProvisions = dynamicProvisions
	}()

	// 先检查已经绑定的 PVC 对应的 PV 的 node affinity
	if len(podVolumeClaims.boundClaims) > 0 {
		boundVolumesSatisfied, boundPVsFound, err = b.checkBoundClaims(logger, podVolumeClaims.boundClaims, node, pod)
		if err != nil {
			return
		}
	}

	// 处理未绑定但支持延迟绑定的 PVC
	if len(podVolumeClaims.unboundClaimsDelayBinding) > 0 {
		var (
			claimsToFindMatching []*v1.PersistentVolumeClaim
			claimsToProvision    []*v1.PersistentVolumeClaim
		)

		// 如果 PVC 上已经指定了 selectedNode，说明它只能在该节点上动态供给
		for _, claim := range podVolumeClaims.unboundClaimsDelayBinding {
			if selectedNode, ok := claim.Annotations[volume.AnnSelectedNode]; ok {
				if selectedNode != node.Name {
					// 如果节点不匹配，直接判失败
					unboundVolumesSatisfied = false
					return
				}
				claimsToProvision = append(claimsToProvision, claim)
			} else {
				claimsToFindMatching = append(claimsToFindMatching, claim)
			}
		}

		// 为未绑定 PVC 寻找已存在的、可静态绑定的 PV
		if len(claimsToFindMatching) > 0 {
			var unboundClaims []*v1.PersistentVolumeClaim
			/*
				按 PVC 请求容量从小到大排序，优先满足小卷；
				对每个 PVC，从其 StorageClass 对应的候选 PV 列表中，调用 volume.FindMatchingVolume 寻找匹配 PV；
				使用 chosenPVs 防止同一个 PV 被多个 PVC 选中；
				找到则生成 BindingInfo，未找到则加入 unboundClaim

			*/
			unboundVolumesSatisfied, staticBindings, unboundClaims, err = b.findMatchingVolumes(logger, pod, claimsToFindMatching, podVolumeClaims.unboundVolumesDelayBinding, node)
			if err != nil {
				return
			}
			// 没找到静态 PV 的 PVC，后面尝试动态供给
			claimsToProvision = append(claimsToProvision, unboundClaims...)
		}

		// 为找不到静态 PV 的 PVC 检查能否动态供给新卷
		if len(claimsToProvision) > 0 {
			/*
				对每个 PVC，获取其 StorageClass；
				检查 provisioner 是否支持动态供给；
				检查节点是否满足 StorageClass 的 AllowedTopologies 拓扑约束；
				如果启用了容量检查，再检查 CSIStorageCapacity 是否足够；
				通过则生成 DynamicProvision。

			*/
			unboundVolumesSatisfied, sufficientStorage, dynamicProvisions, err = b.checkVolumeProvisions(logger, pod, claimsToProvision, node)
			if err != nil {
				return
			}
		}
	}

	return
}

// convertDynamicProvisionsToPVCs 将 DynamicProvision 切片转换成 PVC 切片
func convertDynamicProvisionsToPVCs(dynamicProvisions []*DynamicProvision) []*v1.PersistentVolumeClaim {
	pvcs := make([]*v1.PersistentVolumeClaim, 0, len(dynamicProvisions))
	for _, dynamicProvision := range dynamicProvisions {
		pvcs = append(pvcs, dynamicProvision.PVC)
	}
	return pvcs
}

// AssumePodVolumes 在调度器缓存中假设卷绑定成功
func (b *volumeBinder) AssumePodVolumes(logger klog.Logger, assumedPod *v1.Pod, nodeName string, podVolumes *PodVolumes) (allFullyBound bool, err error) {
	logger.V(4).Info("AssumePodVolumes", "pod", klog.KObj(assumedPod), "node", klog.KRef("", nodeName))
	defer func() {
		if err != nil {
			metrics.VolumeSchedulingStageFailed.WithLabelValues("assume").Inc()
		}
	}()

	// 如果 Pod 的卷已经全部绑定，就不用处理
	if allBound := b.arePodVolumesBound(logger, assumedPod); allBound {
		logger.V(4).Info("AssumePodVolumes: all PVCs bound and nothing to do", "pod", klog.KObj(assumedPod), "node", klog.KRef("", nodeName))
		return true, nil
	}

	// 1) 先假设静态 PV 绑定成功
	newBindings := []*BindingInfo{}
	for _, binding := range podVolumes.StaticBindings {
		newPV, dirty, err := volume.GetBindVolumeToClaim(binding.pv, binding.pvc)
		logger.V(5).Info("AssumePodVolumes: GetBindVolumeToClaim",
			"pod", klog.KObj(assumedPod),
			"PV", klog.KObj(binding.pv),
			"PVC", klog.KObj(binding.pvc),
			"newPV", klog.KObj(newPV),
			"dirty", dirty,
		)
		if err != nil {
			logger.Error(err, "AssumePodVolumes: fail to GetBindVolumeToClaim")
			b.revertAssumedPVs(newBindings)
			return false, err
		}
		// dirty 表示 PV 发生了需要缓存更新的变化
		if dirty {
			err = b.pvCache.Assume(newPV)
			if err != nil {
				b.revertAssumedPVs(newBindings)
				return false, err
			}
		}
		newBindings = append(newBindings, &BindingInfo{pv: newPV, pvc: binding.pvc})
	}

	// 2) 再假设动态供给所需的 PVC 已经写入 selectedNode
	newProvisionedPVCs := []*DynamicProvision{}
	for _, dynamicProvision := range podVolumes.DynamicProvisions {
		// 这里必须 deep copy，因为原始对象可能来自 informer cache
		claimClone := dynamicProvision.PVC.DeepCopy()
		metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, volume.AnnSelectedNode, nodeName)
		err = b.pvcCache.Assume(claimClone)
		if err != nil {
			pvcs := convertDynamicProvisionsToPVCs(newProvisionedPVCs)
			b.revertAssumedPVs(newBindings)
			b.revertAssumedPVCs(pvcs)
			return
		}

		newProvisionedPVCs = append(newProvisionedPVCs, &DynamicProvision{PVC: claimClone})
	}

	podVolumes.StaticBindings = newBindings
	podVolumes.DynamicProvisions = newProvisionedPVCs
	return
}

// RevertAssumedPodVolumes 回滚假设绑定
func (b *volumeBinder) RevertAssumedPodVolumes(podVolumes *PodVolumes) {
	pvcs := convertDynamicProvisionsToPVCs(podVolumes.DynamicProvisions)
	b.revertAssumedPVs(podVolumes.StaticBindings)
	b.revertAssumedPVCs(pvcs)
}

// BindPodVolumes 执行真正的 API 更新，并等待绑定结果
func (b *volumeBinder) BindPodVolumes(ctx context.Context, assumedPod *v1.Pod, podVolumes *PodVolumes) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("BindPodVolumes", "pod", klog.KObj(assumedPod), "node", klog.KRef("", assumedPod.Spec.NodeName))

	defer func() {
		if err != nil {
			metrics.VolumeSchedulingStageFailed.WithLabelValues("bind").Inc()
		}
	}()

	if podVolumes == nil {
		klog.Infof("BindPodVolumes for pod(%s): pod volumes is nil", assumedPod.Name)
		return nil
	}

	bindings := podVolumes.StaticBindings
	claimsToProvision := convertDynamicProvisionsToPVCs(podVolumes.DynamicProvisions)

	// 开始执行 API 更新
	err = b.bindAPIUpdate(ctx, assumedPod, bindings, claimsToProvision)
	if err != nil {
		return err
	}

	// 轮询等待绑定完成
	err = wait.PollUntilContextTimeout(ctx, time.Second, b.bindTimeout, false, func(ctx context.Context) (bool, error) {
		b, err := b.checkBindings(logger, assumedPod, bindings, claimsToProvision)
		return b, err
	})
	if err != nil {
		return fmt.Errorf("binding volumes: %w", err)
	}
	return nil
}

func getPodName(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

func getPVCName(pvc *v1.PersistentVolumeClaim) string {
	return pvc.Namespace + "/" + pvc.Name
}

// bindAPIUpdate 负责真正发起 PV/PVC 的 API 更新
func (b *volumeBinder) bindAPIUpdate(ctx context.Context, pod *v1.Pod, bindings []*BindingInfo, claimsToProvision []*v1.PersistentVolumeClaim) error {
	logger := klog.FromContext(ctx)
	podName := getPodName(pod)
	if bindings == nil {
		return fmt.Errorf("failed to get cached bindings for pod %q", podName)
	}
	if claimsToProvision == nil {
		return fmt.Errorf("failed to get cached claims to provision for pod %q", podName)
	}

	lastProcessedBinding := 0
	lastProcessedProvisioning := 0

	// 如果中途失败，仅回滚尚未完成的假设缓存
	defer func() {
		if lastProcessedBinding < len(bindings) {
			b.revertAssumedPVs(bindings[lastProcessedBinding:])
		}
		if lastProcessedProvisioning < len(claimsToProvision) {
			b.revertAssumedPVCs(claimsToProvision[lastProcessedProvisioning:])
		}
	}()

	var (
		binding *BindingInfo
		i       int
		claim   *v1.PersistentVolumeClaim
	)

	// 先更新 PV，让它预绑定到 PVC
	for _, binding = range bindings {
		logger.V(5).Info("Updating PersistentVolume: binding to claim", "pod", klog.KObj(pod), "PV", klog.KObj(binding.pv), "PVC", klog.KObj(binding.pvc))
		newPV, err := b.kubeClient.CoreV1().PersistentVolumes().Update(ctx, binding.pv, metav1.UpdateOptions{})
		if err != nil {
			logger.V(4).Info("Updating PersistentVolume: binding to claim failed", "pod", klog.KObj(pod), "PV", klog.KObj(binding.pv), "PVC", klog.KObj(binding.pvc), "err", err)
			return err
		}

		// 保存 API Server 返回的最新对象
		binding.pv = newPV
		lastProcessedBinding++
	}

	// 再更新 PVC，触发动态供给
	for i, claim = range claimsToProvision {
		logger.V(5).Info("Updating claims objects to trigger volume provisioning", "pod", klog.KObj(pod), "PVC", klog.KObj(claim))
		newClaim, err := b.kubeClient.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			logger.V(4).Info("Updating PersistentVolumeClaim: binding to volume failed", "PVC", klog.KObj(claim), "err", err)
			return err
		}

		claimsToProvision[i] = newClaim
		lastProcessedProvisioning++
	}

	return nil
}

var versioner = storage.APIObjectVersioner{}

// checkBindings 检查绑定结果是否真正完成
// 这里检查的是 API Server 中的对象，而不是本地 cache
// 原因是 cache 可能存在延迟，必须确认最新状态
func (b *volumeBinder) checkBindings(logger klog.Logger, pod *v1.Pod, bindings []*BindingInfo, claimsToProvision []*v1.PersistentVolumeClaim) (bool, error) {
	podName := getPodName(pod)
	if bindings == nil {
		return false, fmt.Errorf("failed to get cached bindings for pod %q", podName)
	}
	if claimsToProvision == nil {
		return false, fmt.Errorf("failed to get cached claims to provision for pod %q", podName)
	}

	node, err := b.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return false, fmt.Errorf("failed to get node %q: %w", pod.Spec.NodeName, err)
	}

	csiNode, err := b.csiNodeLister.Get(node.Name)
	if err != nil {
		// CSINode 可能还没创建，暂时容忍
		logger.V(4).Info("Could not get a CSINode object for the node", "node", klog.KObj(node), "err", err)
	}

	// 如果 Pod 已被删除，则中止后续绑定检查
	_, err = b.podLister.Pods(pod.Namespace).Get(pod.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("pod does not exist any more: %w", err)
		}
		logger.Error(err, "Failed to get pod from the lister", "pod", klog.KObj(pod))
	}

	// 检查静态绑定的 PV/PVC
	for _, binding := range bindings {
		pv, err := b.pvCache.GetAPIObj(binding.pv.Name)
		if err != nil {
			return false, fmt.Errorf("failed to check binding: %w", err)
		}

		pvc, err := b.pvcCache.GetAPIObj(getPVCName(binding.pvc))
		if err != nil {
			return false, fmt.Errorf("failed to check binding: %w", err)
		}

		// 如果缓存对象比已更新对象旧，则等待 cache 同步
		if versioner.CompareResourceVersion(binding.pv, pv) > 0 {
			return false, nil
		}

		pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
		if err != nil {
			return false, fmt.Errorf("failed to translate pv to csi: %w", err)
		}

		// 检查 PV 的 node affinity 是否满足当前 node
		if err := volume.CheckNodeAffinity(pv, node.Labels); err != nil {
			return false, fmt.Errorf("pv %q node affinity doesn't match node %q: %w", pv.Name, node.Name, err)
		}

		// ClaimRef 不能被清空
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID == "" {
			return false, fmt.Errorf("ClaimRef got reset for pv %q", pv.Name)
		}

		// PVC 必须已经完全绑定
		if !b.isPVCFullyBound(pvc) {
			return false, nil
		}
	}

	// 检查动态供给的 PVC
	for _, claim := range claimsToProvision {
		pvc, err := b.pvcCache.GetAPIObj(getPVCName(claim))
		if err != nil {
			return false, fmt.Errorf("failed to check provisioning pvc: %w", err)
		}

		// 如果 cache 还没同步到最新版本，先等待
		if versioner.CompareResourceVersion(claim, pvc) > 0 {
			return false, nil
		}

		// selectedNode 注解必须还在
		if pvc.Annotations == nil {
			return false, fmt.Errorf("selectedNode annotation reset for PVC %q", pvc.Name)
		}
		selectedNode := pvc.Annotations[volume.AnnSelectedNode]
		if selectedNode != pod.Spec.NodeName {
			// 如果 provision 失败，通常会移除 selectedNode
			return false, fmt.Errorf("provisioning failed for PVC %q", pvc.Name)
		}

		// 如果 PVC 已经绑定 PV，那么检查该 PV 的 node affinity
		if pvc.Spec.VolumeName != "" {
			pv, err := b.pvCache.GetAPIObj(pvc.Spec.VolumeName)
			if err != nil {
				if apierrors.IsNotFound(err) {
					// 可能只是 API 延迟，稍后再试
					return false, nil
				}
				return false, fmt.Errorf("failed to get pv %q from cache: %w", pvc.Spec.VolumeName, err)
			}

			pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
			if err != nil {
				return false, err
			}

			if err := volume.CheckNodeAffinity(pv, node.Labels); err != nil {
				return false, fmt.Errorf("pv %q node affinity doesn't match node %q: %w", pv.Name, node.Name, err)
			}
		}

		// PVC 必须完全绑定
		if !b.isPVCFullyBound(pvc) {
			return false, nil
		}
	}

	logger.V(2).Info("All PVCs for pod are bound", "pod", klog.KObj(pod))
	return true, nil
}

// isVolumeBound 判断某个 volume 是否已经绑定
func (b *volumeBinder) isVolumeBound(logger klog.Logger, pod *v1.Pod, vol *v1.Volume) (bound bool, pvc *v1.PersistentVolumeClaim, err error) {
	pvcName := ""
	isEphemeral := false

	switch {
	case vol.PersistentVolumeClaim != nil:
		/*
			volumes:
			- name: data
			  persistentVolumeClaim:
				claimName: my-pvc
		*/
		pvcName = vol.PersistentVolumeClaim.ClaimName
	case vol.Ephemeral != nil:
		// inline ephemeral volume 也会对应一个 PVC，只是名字是计算出来的
		/*
			volumes:
			- name: cache
			  ephemeral:
			    volumeClaimTemplate:
			      spec:
			        storageClassName: standard
		*/
		pvcName = ephemeral.VolumeClaimName(pod, vol)
		isEphemeral = true
	default:
		return true, nil, nil
	}

	bound, pvc, err = b.isPVCBound(logger, pod.Namespace, pvcName)

	// ephemeral PVC 必须属于当前 Pod
	if isEphemeral && err == nil && pvc != nil {
		if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
			return false, nil, err
		}
	}
	return
}

// isPVCBound 判断 PVC 是否绑定完成
func (b *volumeBinder) isPVCBound(logger klog.Logger, namespace, pvcName string) (bool, *v1.PersistentVolumeClaim, error) {
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	pvcKey := getPVCName(claim)
	pvc, err := b.pvcCache.Get(pvcKey)
	if err != nil || pvc == nil {
		return false, nil, fmt.Errorf("error getting PVC %q: %v", pvcKey, err)
	}

	fullyBound := b.isPVCFullyBound(pvc)
	if fullyBound {
		logger.V(5).Info("PVC is fully bound to PV", "PVC", klog.KObj(pvc), "PV", klog.KRef("", pvc.Spec.VolumeName))
	} else {
		if pvc.Spec.VolumeName != "" {
			logger.V(5).Info("PVC is not fully bound to PV", "PVC", klog.KObj(pvc), "PV", klog.KRef("", pvc.Spec.VolumeName))
		} else {
			logger.V(5).Info("PVC is not bound", "PVC", klog.KObj(pvc))
		}
	}
	return fullyBound, pvc, nil
}

// isPVCFullyBound 判断 PVC 是否“完全绑定”
// 这里要求：
/*
1. PVC.Spec.VolumeName != ""
2. PVC 上有 AnnBindCompleted 注解

spec:
  volumeName: pv-demo
metadata:
  annotations:
    volume.kubernetes.io/bind-completed: "yes"
*/
func (b *volumeBinder) isPVCFullyBound(pvc *v1.PersistentVolumeClaim) bool {
	return pvc.Spec.VolumeName != "" && metav1.HasAnnotation(pvc.ObjectMeta, volume.AnnBindCompleted)
}

// arePodVolumesBound 判断 Pod 的所有卷是否都已经绑定
func (b *volumeBinder) arePodVolumesBound(logger klog.Logger, pod *v1.Pod) bool {
	for _, vol := range pod.Spec.Volumes {
		if isBound, _, _ := b.isVolumeBound(logger, pod, &vol); !isBound {
			// 只要有一个卷未绑定，就返回 false
			return false
		}
	}
	return true
}

// GetPodVolumeClaims 收集 Pod 中所有 PVC，并进行分类
func (b *volumeBinder) GetPodVolumeClaims(logger klog.Logger, pod *v1.Pod) (podVolumeClaims *PodVolumeClaims, err error) {
	podVolumeClaims = &PodVolumeClaims{
		boundClaims:               []*v1.PersistentVolumeClaim{},
		unboundClaimsImmediate:    []*v1.PersistentVolumeClaim{},
		unboundClaimsDelayBinding: []*v1.PersistentVolumeClaim{},
	}

	for _, vol := range pod.Spec.Volumes {
		volumeBound, pvc, err := b.isVolumeBound(logger, pod, &vol)
		if err != nil {
			return podVolumeClaims, err
		}
		if pvc == nil {
			continue
		}
		if volumeBound {
			podVolumeClaims.boundClaims = append(podVolumeClaims.boundClaims, pvc)
		} else {
			// 判断 PVC 是否是延迟绑定模式
			delayBindingMode, err := volume.IsDelayBindingMode(pvc, b.classLister)
			if err != nil {
				return podVolumeClaims, err
			}
			// 预绑定 PVC 也归入 immediate
			if delayBindingMode && pvc.Spec.VolumeName == "" {
				podVolumeClaims.unboundClaimsDelayBinding = append(podVolumeClaims.unboundClaimsDelayBinding, pvc)
			} else {
				// 非延迟绑定或已指定 VolumeName 的 PVC
				podVolumeClaims.unboundClaimsImmediate = append(podVolumeClaims.unboundClaimsImmediate, pvc)
			}
		}
	}

	// 收集所有延迟绑定 PVC 对应 StorageClass 下的可用 PV
	podVolumeClaims.unboundVolumesDelayBinding = map[string][]*v1.PersistentVolume{}
	for _, pvc := range podVolumeClaims.unboundClaimsDelayBinding {
		storageClassName := volume.GetPersistentVolumeClaimClass(pvc)
		pvs, err := b.pvCache.ListPVs(storageClassName)
		if err != nil {
			return nil, err
		}
		podVolumeClaims.unboundVolumesDelayBinding[storageClassName] = pvs
	}
	return podVolumeClaims, nil
}

// checkBoundClaims 检查已经绑定的 PVC 所对应的 PV 是否能在当前节点上使用。
//
// 返回值说明（与上层 FindPodVolumes 中的 boundVolumesSatisfied、boundPVsFound 对应）：
//   - 第一个 bool：boundVolumesSatisfied，表示 PV 的节点亲和性是否都满足；
//   - 第二个 bool：boundPVsFound，表示所有 PV 是否都成功从缓存中找到；
//   - error：处理过程中发生的错误。
//
// 处理流程：
//  1. 获取当前节点对应的 CSINode 对象，用于判断 CSI 迁移状态；
//  2. 遍历每个已绑定 PVC，从 PV 缓存中取出对应 PV；
//  3. 调用 tryTranslatePVToCSI 将 in-tree PV 翻译成 CSI PV（如果该 PV/节点已迁移）；
//  4. 使用 volume.CheckNodeAffinity 校验 PV 的节点亲和性与当前节点标签是否匹配。
func (b *volumeBinder) checkBoundClaims(logger klog.Logger, claims []*v1.PersistentVolumeClaim, node *v1.Node, pod *v1.Pod) (bool, bool, error) {
	csiNode, err := b.csiNodeLister.Get(node.Name)
	if err != nil {
		// CSINode 对象可能暂时不存在，这通常发生在节点刚注册时。
		// 此时 CSI 迁移无法进行，但不影响 PV 节点亲和性检查，因此仅记录日志后继续。
		logger.V(4).Info("Could not get a CSINode object for the node", "node", klog.KObj(node), "err", err)
	}

	for _, pvc := range claims {
		pvName := pvc.Spec.VolumeName
		pv, err := b.pvCache.Get(pvName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// PV 缓存中未找到对应 PV，返回 boundVolumesSatisfied=true（尚未发现冲突）、
				// boundPVsFound=false，通知上层记录 ErrReasonPVNotExist。
				err = nil
			}
			return true, false, err
		}

		// 尝试将 in-tree PV 翻译成 CSI PV。
		// 如果该 PV 对应的 in-tree 插件在当前节点尚未迁移到 CSI，则返回原 PV 不变。
		pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
		if err != nil {
			return false, true, err
		}

		// 校验 PV 的节点亲和性（nodeAffinity）是否被当前节点满足。
		err = volume.CheckNodeAffinity(pv, node.Labels)
		if err != nil {
			logger.V(4).Info("PersistentVolume and node mismatch for pod", "PV", klog.KRef("", pvName), "node", klog.KObj(node), "pod", klog.KObj(pod), "err", err)
			return false, true, nil
		}
		logger.V(5).Info("PersistentVolume and node matches for pod", "PV", klog.KRef("", pvName), "node", klog.KObj(node), "pod", klog.KObj(pod))
	}

	logger.V(4).Info("All bound volumes for pod match with node", "pod", klog.KObj(pod), "node", klog.KObj(node))
	return true, true, nil
}

// findMatchingVolumes 为 PVC 找可绑定的 PV
func (b *volumeBinder) findMatchingVolumes(logger klog.Logger, pod *v1.Pod, claimsToBind []*v1.PersistentVolumeClaim, unboundVolumesDelayBinding map[string][]*v1.PersistentVolume, node *v1.Node) (foundMatches bool, bindings []*BindingInfo, unboundClaims []*v1.PersistentVolumeClaim, err error) {
	// 按请求容量从小到大排序，优先满足小卷
	sort.Sort(byPVCSize(claimsToBind))

	chosenPVs := map[string]*v1.PersistentVolume{}
	foundMatches = true

	for _, pvc := range claimsToBind {
		storageClassName := volume.GetPersistentVolumeClaimClass(pvc)
		pvs := unboundVolumesDelayBinding[storageClassName]

		// 在候选 PV 中找一个匹配当前 PVC 和 node 的 PV
		pv, err := volume.FindMatchingVolume(pvc, pvs, node, chosenPVs, true, b.enableVolumeAttributesClass)
		if err != nil {
			return false, nil, nil, err
		}
		if pv == nil {
			logger.V(4).Info("No matching volumes for pod", "pod", klog.KObj(pod), "PVC", klog.KObj(pvc), "node", klog.KObj(node))
			unboundClaims = append(unboundClaims, pvc)
			foundMatches = false
			continue
		}

		// 防止同一个 PV 被多个 PVC 选中
		chosenPVs[pv.Name] = pv
		bindings = append(bindings, &BindingInfo{pv: pv, pvc: pvc})
		logger.V(5).Info("Found matching PV for PVC for pod", "PV", klog.KObj(pv), "PVC", klog.KObj(pvc), "node", klog.KObj(node), "pod", klog.KObj(pod))
	}

	if foundMatches {
		logger.V(4).Info("Found matching volumes for pod", "pod", klog.KObj(pod), "node", klog.KObj(node))
	}

	return
}

// checkVolumeProvisions 检查这些未绑定 PVC 是否可以动态供给
/*
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: alicloud-disk
provisioner: diskplugin.csi.alibabacloud.com
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
allowedTopologies:
  - matchLabelExpressions:
      - key: topology.diskplugin.csi.alibabacloud.com/zone
        values:
          - cn-hangzhou-a
          - cn-hangzhou-b
*/
func (b *volumeBinder) checkVolumeProvisions(logger klog.Logger, pod *v1.Pod, claimsToProvision []*v1.PersistentVolumeClaim, node *v1.Node) (provisionSatisfied, sufficientStorage bool, dynamicProvisions []*DynamicProvision, err error) {
	dynamicProvisions = []*DynamicProvision{}

	for _, claim := range claimsToProvision {
		pvcName := getPVCName(claim)
		className := volume.GetPersistentVolumeClaimClass(claim)
		if className == "" {
			return false, false, nil, fmt.Errorf("no class for claim %q", pvcName)
		}

		class, err := b.classLister.Get(className)
		if err != nil {
			return false, false, nil, fmt.Errorf("failed to find storage class %q", className)
		}
		provisioner := class.Provisioner
		if provisioner == "" || provisioner == volume.NotSupportedProvisioner {
			logger.V(4).Info("Storage class of claim does not support dynamic provisioning", "storageClassName", className, "PVC", klog.KObj(claim))
			return false, true, nil, nil
		}

		// 检查节点是否满足拓扑约束
		if !v1helper.MatchTopologySelectorTerms(class.AllowedTopologies, labels.Set(node.Labels)) {
			logger.V(4).Info("Node cannot satisfy provisioning topology requirements of claim", "node", klog.KObj(node), "PVC", klog.KObj(claim))
			return false, true, nil, nil
		}

		// 检查容量是否足够
		sufficient, capacity, err := b.hasEnoughCapacity(logger, provisioner, claim, class, node)
		if err != nil {
			return false, false, nil, err
		}
		if !sufficient {
			return true, false, nil, nil
		}

		dynamicProvisions = append(dynamicProvisions, &DynamicProvision{
			PVC:          claim,
			NodeCapacity: capacity,
		})
	}
	logger.V(4).Info("Provisioning for claims of pod that has no matching volumes...", "claimCount", len(claimsToProvision), "pod", klog.KObj(pod), "node", klog.KObj(node))

	return true, true, dynamicProvisions, nil
}

// revertAssumedPVs 回滚假设绑定的 PV
func (b *volumeBinder) revertAssumedPVs(bindings []*BindingInfo) {
	for _, BindingInfo := range bindings {
		b.pvCache.Restore(BindingInfo.pv)
	}
}

// revertAssumedPVCs 回滚假设绑定的 PVC
func (b *volumeBinder) revertAssumedPVCs(claims []*v1.PersistentVolumeClaim) {
	for _, claim := range claims {
		b.pvcCache.Restore(claim)
	}
}

// hasEnoughCapacity 检查某个节点是否有足够的 CSI 存储容量
func (b *volumeBinder) hasEnoughCapacity(logger klog.Logger, provisioner string, claim *v1.PersistentVolumeClaim, storageClass *storagev1.StorageClass, node *v1.Node) (bool, *storagev1beta1.CSIStorageCapacity, error) {
	// 未启用容量检查时，默认认为容量足够
	if !b.capacityCheckEnabled {
		return true, nil, nil
	}

	quantity, ok := claim.Spec.Resources.Requests[v1.ResourceStorage]
	if !ok {
		// PVC 没有请求 storage，则无需检查
		return true, nil, nil
	}

	// 只对声明支持 CSI 容量检查的 driver 做检查
	driver, err := b.csiDriverLister.Get(provisioner)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// 不是 CSI driver，或者 driver 不支持容量调度
			return true, nil, nil
		}
		return false, nil, err
	}
	if driver.Spec.StorageCapacity == nil || !*driver.Spec.StorageCapacity {
		return true, nil, nil
	}

	// 查找 CSIStorageCapacity 对象
	/*
		apiVersion: storage.k8s.io/v1
		kind: CSIStorageCapacity
		metadata:
		  name: example
		storageClassName: alicloud-disk
		capacity: 100Gi
		nodeTopology:
		  matchLabelExpressions:
		    - key: topology.diskplugin.csi.alibabacloud.com/zone
		      values:
		        - cn-hangzhou-a
	*/
	capacities, err := b.csiStorageCapacityLister.List(labels.Everything())
	if err != nil {
		return false, nil, err
	}

	sizeInBytes := quantity.Value()
	for _, capacity := range capacities {
		/*
			apiVersion: storage.k8s.io/v1beta1
			kind: CSIStorageCapacity
			metadata:
			  name: fast-ssd-zone-a
			  namespace: default
			storageClassName: fast-ssd
			capacity: 500Gi
			nodeTopology:
			  matchLabels:
			    topology.kubernetes.io/zone: zone-a
		*/
		if capacity.StorageClassName == storageClass.Name &&
			capacitySufficient(capacity, sizeInBytes) &&
			b.nodeHasAccess(logger, node, capacity) {
			return true, capacity, nil
		}
	}

	logger.V(4).Info("Node has no accessible CSIStorageCapacity with enough capacity for PVC",
		"node", klog.KObj(node), "PVC", klog.KObj(claim), "size", sizeInBytes, "storageClass", klog.KObj(storageClass))
	return false, nil, nil
}

// capacitySufficient 为兼容旧版本，判断容量是否足够
func capacitySufficient(capacity *storagev1beta1.CSIStorageCapacity, sizeInBytes int64) bool {
	limit := volumeLimit(capacity)
	return limit != nil && limit.Value() >= sizeInBytes
}

// volumeLimit 获取容量对象中真正可用的容量上限
func volumeLimit(capacity *storagev1beta1.CSIStorageCapacity) *resource.Quantity {
	if capacity.MaximumVolumeSize != nil {
		// 优先使用 MaximumVolumeSize，语义更准确
		return capacity.MaximumVolumeSize
	}
	return capacity.Capacity
}

// nodeHasAccess 判断节点是否匹配该 CSIStorageCapacity 的拓扑要求
func (b *volumeBinder) nodeHasAccess(logger klog.Logger, node *v1.Node, capacity *storagev1beta1.CSIStorageCapacity) bool {
	if capacity.NodeTopology == nil {
		// 没有拓扑信息，认为不可用
		return false
	}
	// 仅支持 label selector 方式匹配
	selector, err := metav1.LabelSelectorAsSelector(capacity.NodeTopology)
	if err != nil {
		logger.Error(err, "Unexpected error converting to a label selector", "nodeTopology", capacity.NodeTopology)
		return false
	}
	return selector.Matches(labels.Set(node.Labels))
}

// byPVCSize 用于按照 PVC 请求存储大小排序
type byPVCSize []*v1.PersistentVolumeClaim

func (a byPVCSize) Len() int {
	return len(a)
}

func (a byPVCSize) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byPVCSize) Less(i, j int) bool {
	iSize := a[i].Spec.Resources.Requests[v1.ResourceStorage]
	jSize := a[j].Spec.Resources.Requests[v1.ResourceStorage]
	return iSize.Cmp(jSize) == -1
}

// isCSIMigrationOnForPlugin 判断某个 in-tree 存储插件是否在集群层面开启了 CSI 迁移（CSI Migration）。
//
// CSI 迁移背景：Kubernetes 逐渐将内置的 in-tree 存储插件（如 AWS EBS、GCE PD、Azure Disk 等）
// 迁移到 out-of-tree 的 CSI 驱动。开启迁移后，in-tree 的 PV/PVC 在调度时会被视为 CSI PV/PVC，
// 以便使用 CSI 驱动的拓扑、容量等能力。
//
// 参数 enableCSIMigrationPortworx 用于控制 Portworx 的迁移开关，因为该插件的迁移需要显式启用。
// 其他主流插件（AWS EBS、GCE PD、Azure Disk、Cinder）默认视为已开启迁移。
func isCSIMigrationOnForPlugin(pluginName string, enableCSIMigrationPortworx bool) bool {
	switch pluginName {
	case csiplugins.AWSEBSInTreePluginName:
		return true
	case csiplugins.GCEPDInTreePluginName:
		return true
	case csiplugins.AzureDiskInTreePluginName:
		return true
	case csiplugins.CinderInTreePluginName:
		return true
	case csiplugins.PortworxVolumePluginName:
		return enableCSIMigrationPortworx
	}
	return false
}

// isPluginMigratedToCSIOnNode 判断某个 in-tree 存储插件是否已经在指定节点上迁移到了 CSI。
//
// Kubernetes 通过 CSINode 对象上的 `csi.migrated-plugins` 注解记录该节点上已完成迁移的 in-tree 插件列表。
// 该注解值为逗号分隔的插件名称。只有当插件同时满足以下条件时，才认为该 PV 在该节点上需要按 CSI 处理：
//  1. 集群层面开启了该插件的 CSI 迁移（isCSIMigrationOnForPlugin 返回 true）；
//  2. 当前节点已经完成了该插件的迁移（本函数返回 true）。
func isPluginMigratedToCSIOnNode(pluginName string, csiNode *storagev1.CSINode) bool {
	if csiNode == nil {
		// 没有 CSINode 信息，无法确认迁移状态，保守认为未迁移。
		return false
	}

	csiNodeAnn := csiNode.GetAnnotations()
	if csiNodeAnn == nil {
		return false
	}

	var mpaSet sets.Set[string]
	mpa := csiNodeAnn[v1.MigratedPluginsAnnotationKey]
	if len(mpa) == 0 {
		mpaSet = sets.New[string]()
	} else {
		// 注解值为逗号分隔的已迁移插件名称列表。
		tok := strings.Split(mpa, ",")
		mpaSet = sets.New(tok...)
	}

	return mpaSet.Has(pluginName)
}

// tryTranslatePVToCSI 尝试在满足 CSI 迁移条件时，将 in-tree PV 转换为 CSI PV。
//
// 转换条件（必须同时满足，否则返回原 PV 不变）：
//  1. PV 本身是可迁移的（translator.IsPVMigratable）。例如 PV 使用 AWS EBS、GCE PD 等 in-tree 插件；
//  2. 能从 PV spec 中解析出对应的 in-tree 插件名称（GetInTreePluginNameFromSpec）；
//  3. 集群层面开启了该插件的 CSI 迁移（isCSIMigrationOnForPlugin）；
//  4. 当前目标节点已经完成了该插件的 CSI 迁移（isPluginMigratedToCSIOnNode）。
//
// 为什么需要转换：
//
//	当节点已经迁移到 CSI 后，kubelet 不再使用 in-tree 插件挂载卷，而是通过 CSI 驱动。
//	此时 PV 的节点亲和性（nodeAffinity）也应该按 CSI 驱动对应的拓扑来校验，
//	而不是按旧的 in-tree 插件拓扑。因此调度器需要在 Filter 阶段将 in-tree PV 翻译成 CSI PV，
//	确保后续 CheckNodeAffinity 使用的是 CSI 拓扑。
//
// 调用位置：
//   - checkBoundClaims：校验已绑定 PVC 的 PV 与节点亲和性前；
//   - checkBindings（AssumePodVolumes 后的绑定检查）：校验静态绑定 PV 与节点亲和性前；
//   - checkBindings 动态供给分支：校验已 provision 出的 PV 与节点亲和性前。
func (b *volumeBinder) tryTranslatePVToCSI(logger klog.Logger, pv *v1.PersistentVolume, csiNode *storagev1.CSINode) (*v1.PersistentVolume, error) {
	// 第一步：判断该 PV 是否属于可迁移的 in-tree PV。
	// 如果 PV 本身就是 CSI PV，或者使用的是未启用迁移的 in-tree 插件，则直接返回原 PV。
	if !b.translator.IsPVMigratable(pv) {
		return pv, nil
	}

	// 第二步：从 PV spec 中识别出对应的 in-tree 插件名称。
	// 第二个参数传 nil 表示不传入 PVC，仅根据 PV 推断插件名。
	pluginName, err := b.translator.GetInTreePluginNameFromSpec(pv, nil)
	if err != nil {
		return nil, fmt.Errorf("could not get plugin name from pv: %v", err)
	}

	// 第三步：检查集群层面是否开启了该插件的 CSI 迁移。
	if !isCSIMigrationOnForPlugin(pluginName, b.enableCSIMigrationPortworx) {
		return pv, nil
	}

	// 第四步：检查目标节点是否已将该插件迁移到 CSI。
	// 节点未迁移时，kubelet 仍使用 in-tree 插件，无需转换 PV。
	if !isPluginMigratedToCSIOnNode(pluginName, csiNode) {
		return pv, nil
	}

	// 第五步：执行 in-tree PV 到 CSI PV 的转换。
	// 转换后的 transPV 会带有 CSI 驱动名和 CSI 卷句柄，其 nodeAffinity 也对应 CSI 拓扑。
	transPV, err := b.translator.TranslateInTreePVToCSI(logger, pv)
	if err != nil {
		return nil, fmt.Errorf("could not translate pv: %v", err)
	}

	return transPV, nil
}
