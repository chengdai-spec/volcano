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

package gpushare

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestGetGPUMemoryOfPod 验证 getGPUMemoryOfPod 对普通容器与 Init 容器的显存计算规则。
//
// 测试覆盖：
//   - 仅普通容器请求 gpu-memory：结果为各容器请求之和。
//   - 同时存在 Init 容器与普通容器：Init 容器取最大值，再与普通容器总和取较大值。
func TestGetGPUMemoryOfPod(t *testing.T) {
	testCases := []struct {
		name string
		pod  *v1.Pod
		want uint
	}{
		{
			name: "GPUs required only in Containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUResource: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUResource: resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			want: 4,
		},
		{
			name: "GPUs required both in initContainers and Containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUResource: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUResource: resource.MustParse("3"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUResource: resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			want: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getGPUMemoryOfPod(tc.pod)
			if tc.want != got {
				t.Errorf("unexpected result, want: %v, got: %v", tc.want, got)
			}
		})
	}
}

// TestGetGPUNumberOfPod 验证 getGPUNumberOfPod 对普通容器与 Init 容器的整卡数计算规则。
//
// 测试覆盖：
//   - 仅普通容器请求 gpu-number：结果为各容器请求之和。
//   - 同时存在 Init 容器与普通容器：Init 容器取最大值，再与普通容器总和取较大值。
func TestGetGPUNumberOfPod(t *testing.T) {
	testCases := []struct {
		name string
		pod  *v1.Pod
		want int
	}{
		{
			name: "GPUs required only in Containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUNumber: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUNumber: resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			want: 4,
		},
		{
			name: "GPUs required both in initContainers and Containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUNumber: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUNumber: resource.MustParse("3"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									VolcanoGPUNumber: resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			want: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getGPUNumberOfPod(tc.pod)
			if tc.want != got {
				t.Errorf("unexpected result, want: %v, got: %v", tc.want, got)
			}
		})
	}
}
