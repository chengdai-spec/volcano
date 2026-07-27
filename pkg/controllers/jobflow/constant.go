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

// 常量定义：用于标识 JobFlow 控制器中的资源类型和标签/注解键
const (
	// Volcano Volcano API Group 的标识字符串，用于判断 OwnerReference 的 APIVersion 是否属于 Volcano
	Volcano = "volcano"
	// JobFlow JobFlow 资源的 Kind 名称，用于判断 OwnerReference 的 Kind 是否为 JobFlow
	JobFlow = "JobFlow"
	// CreatedByJobTemplate 标记 VCJob 是由哪个 JobTemplate 创建的标签/注解键
	// 值格式："<namespace>.<jobTemplateName>"，例如 "default.my-template"
	CreatedByJobTemplate = "volcano.sh/createdByJobTemplate"
	// CreatedByJobFlow 标记 VCJob 是由哪个 JobFlow 创建的标签/注解键
	// 值格式："<namespace>.<jobFlowName>"，例如 "default.my-jobflow"
	// 同时也用于 Label Selector 查询某个 JobFlow 创建的所有子 Job
	CreatedByJobFlow = "volcano.sh/createdByJobFlow"
)
