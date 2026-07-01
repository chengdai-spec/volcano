/*
Copyright 2021 The Volcano Authors.

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

package mutate

import (
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	whv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/api"
	wkconfig "volcano.sh/volcano/pkg/webhooks/config"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/schema"
	"volcano.sh/volcano/pkg/webhooks/util"
)

// patchOperation define the patch operation structure
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// init register mutate pod
func init() {
	router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path:   "/pods/mutate",
	Func:   Pods,
	Config: config,
	MutatingConfig: &whv1.MutatingWebhookConfiguration{
		Webhooks: []whv1.MutatingWebhook{{
			Name: "mutatepod.volcano.sh",
			Rules: []whv1.RuleWithOperations{
				{
					Operations: []whv1.OperationType{whv1.Create},
					Rule: whv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"pods"},
					},
				},
			},
		}},
	},
}

var config = &router.AdmissionServiceConfig{}

// Pods mutate pods.
func Pods(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("mutating pods -- %s", ar.Request.Operation)
	pod, err := schema.DecodePod(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return util.ToAdmissionResponse(err)
	}

	if pod.Namespace == "" {
		pod.Namespace = ar.Request.Namespace
	}

	var patchBytes []byte
	switch ar.Request.Operation {
	case admissionv1.Create:
		patchBytes, _ = createPatch(pod)
	default:
		err = fmt.Errorf("expect operation to be 'CREATE' ")
		return util.ToAdmissionResponse(err)
	}

	reviewResponse := admissionv1.AdmissionResponse{
		Allowed: true,
		Patch:   patchBytes,
	}
	if len(patchBytes) > 0 {
		pt := admissionv1.PatchTypeJSONPatch
		reviewResponse.PatchType = &pt
	}

	return &reviewResponse
}

// createPatch patch pod
func createPatch(pod *v1.Pod) ([]byte, error) {
	if config.ConfigData == nil {
		klog.V(5).Infof("admission configuration is empty.")
		return nil, nil
	}

	var patch []patchOperation
	config.ConfigData.Lock()
	defer config.ConfigData.Unlock()

	// Add scheduling gates if opted-in
	patchGates := patchSchedulingGates(pod)
	if patchGates != nil {
		patch = append(patch, *patchGates)
	}

	for _, resourceGroup := range config.ConfigData.ResGroupsConfig {
		klog.V(3).Infof("resourceGroup %s", resourceGroup.ResourceGroup)
		group := GetResGroup(resourceGroup)
		if !group.IsBelongResGroup(pod, resourceGroup) {
			continue
		}

		patchLabel := patchLabels(pod, resourceGroup)
		if patchLabel != nil {
			patch = append(patch, *patchLabel)
		}

		patchAffinity := patchAffinity(pod, resourceGroup)
		if patchAffinity != nil {
			patch = append(patch, *patchAffinity)
		}

		patchToleration := patchTaintToleration(pod, resourceGroup)
		if patchToleration != nil {
			patch = append(patch, *patchToleration)
		}
		patchScheduler := patchSchedulerName(resourceGroup)
		if patchScheduler != nil {
			patch = append(patch, *patchScheduler)
		}

		klog.V(5).Infof("pod patch %v", patch)
		return json.Marshal(patch)
	}

	return json.Marshal(patch)
}

// patchSchedulingGates 为 Volcano 管理的 Pod 添加一个 scheduling gate（调度门闩）。
// 这个 gate 会阻止集群自动扩缩容器（cluster autoscaler）看到这个 Pod，
// 直到 Volcano 判断它已经准备好（队列准入 + gang 调度条件都满足）。
func patchSchedulingGates(pod *v1.Pod) *patchOperation {
	// 如果没有启用 SchedulingGatesQueueAdmission 特性开关，则跳过
	if !utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) {
		return nil
	}

	// 检查是否存在 opt-in 注解
	if !api.HasQueueAllocationGateAnnotation(pod) {
		klog.V(4).Infof("Pod %s/%s does not have opt-in annotation, skipping gate",
			pod.Namespace, pod.Name)
		return nil
	}

	gate := v1.PodSchedulingGate{Name: schedulingv1beta1.QueueAllocationGateKey}

	// 幂等性处理：不要重复添加同一个 Volcano gate。
	// 这样可以防止在 mutation 重试时重复追加同样的 gate。
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == gate.Name {
			return nil
		}
	}

	// 父字段不存在：说明 schedulingGates 切片还没有初始化。
	// 这时必须在基础路径上使用 add，并传入一个包含该 gate 的数组。
	if pod.Spec.SchedulingGates == nil {
		return &patchOperation{
			Op:    "add",
			Path:  "/spec/schedulingGates",
			Value: []v1.PodSchedulingGate{gate},
		}
	}

	// 父字段已存在：可以安全地向现有数组追加。
	// 这里使用 JSON Patch 的 "-" 路径操作符表示追加到数组末尾，
	// 从而避免覆盖其他 webhook 并发添加的 gate。
	return &patchOperation{
		Op:    "add",
		Path:  "/spec/schedulingGates/-",
		Value: gate,
	}
}

// patchLabels patch label
func patchLabels(pod *v1.Pod, resGroupConfig wkconfig.ResGroupConfig) *patchOperation {
	if len(resGroupConfig.Labels) == 0 {
		return nil
	}

	nodeSelector := make(map[string]string)
	for key, label := range pod.Spec.NodeSelector {
		nodeSelector[key] = label
	}

	for key, label := range resGroupConfig.Labels {
		nodeSelector[key] = label
	}

	return &patchOperation{Op: "add", Path: "/spec/nodeSelector", Value: nodeSelector}
}

// patchAffinity patch affinity
func patchAffinity(pod *v1.Pod, resGroupConfig wkconfig.ResGroupConfig) *patchOperation {
	if resGroupConfig.Affinity == "" {
		return nil
	}

	if pod.Spec.Affinity != nil {
		klog.V(5).Infof("pod affinity exist: %s", pod.Name)
		return nil
	}

	var affinity v1.Affinity
	err := json.Unmarshal([]byte(resGroupConfig.Affinity), &affinity)
	if err != nil {
		fmt.Println("Failed to unmarshal JSON:", err)
		klog.V(3).Infof("Failed to unmarshal JSON: %s", err)
		return nil
	}

	return &patchOperation{Op: "add", Path: "/spec/affinity", Value: affinity}
}

// patchTaintToleration patch taint toleration
func patchTaintToleration(pod *v1.Pod, resGroupConfig wkconfig.ResGroupConfig) *patchOperation {
	if len(resGroupConfig.Tolerations) == 0 {
		return nil
	}

	var dst []v1.Toleration
	dst = append(dst, pod.Spec.Tolerations...)
	dst = append(dst, resGroupConfig.Tolerations...)

	return &patchOperation{Op: "add", Path: "/spec/tolerations", Value: dst}
}

// patchSchedulerName patch scheduler
func patchSchedulerName(resGroupConfig wkconfig.ResGroupConfig) *patchOperation {
	if resGroupConfig.SchedulerName == "" {
		return nil
	}

	return &patchOperation{Op: "add", Path: "/spec/schedulerName", Value: resGroupConfig.SchedulerName}
}
