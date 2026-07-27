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

package jobflow

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

// getJobName 根据 JobFlow 名称和 JobTemplate 名称生成子 Job 的名称
// 格式："<jobFlowName>-<jobTemplateName>"，例如 "my-flow-step1"
func getJobName(jobFlowName string, jobTemplateName string) string {
	return jobFlowName + "-" + jobTemplateName
}

// GenerateObjectString 生成对象标识字符串，格式为 "<namespace>.<name>"
// 用于 Labels 和 Annotations 的值，例如 "default.my-template"
func GenerateObjectString(namespace, name string) string {
	return namespace + "." + name
}

// isControlledBy 判断对象是否被指定的 GVK（GroupVersionKind）控制
// 通过检查 OwnerReference 中的 Controller 引用判断
func isControlledBy(obj metav1.Object, gvk schema.GroupVersionKind) bool {
	controllerRef := metav1.GetControllerOf(obj)
	if controllerRef == nil {
		return false
	}
	// 检查 Controller 引用的 APIVersion 和 Kind 是否匹配指定的 GVK
	if controllerRef.APIVersion == gvk.GroupVersion().String() && controllerRef.Kind == gvk.Kind {
		return true
	}
	return false
}

// getJobFlowNameByJob 从 Job 的 OwnerReferences 中提取父 JobFlow 的名称
// 遍历所有 OwnerReference，找到 Kind 为 "JobFlow" 且 APIVersion 包含 "volcano" 的引用
// 如果未找到则返回空字符串
func getJobFlowNameByJob(job *batch.Job) string {
	for _, owner := range job.OwnerReferences {
		if owner.Kind == JobFlow && strings.Contains(owner.APIVersion, Volcano) {
			return owner.Name
		}
	}
	return ""
}
