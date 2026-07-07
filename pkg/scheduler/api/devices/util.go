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

/*
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

package devices

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// These are predefined codes used in a Status.
// 这些是预定义的状态码，用于表示调度操作的结果
const (
	// Success 表示插件运行成功，Pod 可以调度
	// 注意：nil 状态也被视为 "Success"
	Success int = iota

	// Error 表示插件内部错误、意外输入等问题
	Error

	// Unschedulable 表示 Pod 当前不可调度，但调度器可能会尝试抢占其他 Pod 来让它调度成功
	// 伴随的状态消息应该解释为什么 Pod 不可调度
	Unschedulable

	// UnschedulableAndUnresolvable 表示 Pod 不可调度且抢占也无法解决问题
	// 与 Unschedulable 的区别是：调度器会跳过抢占尝试
	// 例如：没有节点满足设备的硬件要求
	UnschedulableAndUnresolvable

	// Wait 用于 Permit 插件，表示 Pod 调度需要等待（例如等待某些条件满足）
	Wait

	// Skip 用于 Bind 插件，表示跳过绑定步骤
	Skip
)

// kubeClient 是全局的 Kubernetes 客户端实例
// 使用包级变量实现单例模式，避免重复创建连接
var kubeClient *kubernetes.Clientset

// GetClient 获取 Kubernetes 客户端实例
//
// 这是一个懒加载的单例模式实现：
// - 首次调用时创建客户端
// - 后续调用直接返回已创建的实例
// - 如果创建失败，会记录错误日志但不 panic
func GetClient() kubernetes.Interface {
	var err error
	if kubeClient == nil {
		kubeClient, err = NewClient()
		if err != nil {
			klog.ErrorS(err, "deviceshare initClient failed")
		}
	}
	return kubeClient
}

// NewClient 创建连接到 API Server 的 Kubernetes 客户端
//
// 这个函数使用 InClusterConfig，意味着它只能在 Kubernetes 集群内运行
// （例如作为 Deployment 运行在 Pod 中）。
//
// InClusterConfig 会自动从以下位置读取配置：
// - /var/run/secrets/kubernetes.io/serviceaccount/token (服务账户令牌)
// - /var/run/secrets/kubernetes.io/serviceaccount/ca.crt (CA 证书)
// - KUBERNETES_SERVICE_HOST 和 KUBERNETES_SERVICE_PORT 环境变量
func NewClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(config)
	return client, err
}

// GetNode 根据节点名称获取节点对象
//
// 参数：
//   - nodename: 要查询的节点名称
//
// 返回值：
//   - *v1.Node: 节点对象指针
//   - error: 错误信息（包括节点不存在、未授权等）
//
// 这个函数做了完善的错误处理：
// - 检查节点名称是否为空
// - 区分 NotFound、Unauthorized 等不同错误类型
// - 提供详细的日志记录
func GetNode(nodename string) (*v1.Node, error) {
	if nodename == "" {
		klog.ErrorS(nil, "Node name is empty")
		return nil, fmt.Errorf("nodename is empty")
	}

	klog.V(5).InfoS("Fetching node", "nodeName", nodename)
	n, err := GetClient().CoreV1().Nodes().Get(context.Background(), nodename, metav1.GetOptions{})
	if err != nil {
		switch {
		case apierrors.IsNotFound(err):
			// 节点不存在：可能是节点已被删除或名称拼写错误
			klog.ErrorS(err, "Node not found", "nodeName", nodename)
			return nil, fmt.Errorf("node %s not found", nodename)
		case apierrors.IsUnauthorized(err):
			// 未授权：RBAC 权限不足或服务账户配置错误
			klog.ErrorS(err, "Unauthorized to access node", "nodeName", nodename)
			return nil, fmt.Errorf("unauthorized to access node %s", nodename)
		default:
			// 其他错误：网络问题、API Server 故障等
			klog.ErrorS(err, "Failed to get node", "nodeName", nodename)
			return nil, fmt.Errorf("failed to get node %s: %v", nodename, err)
		}
	}

	klog.V(5).InfoS("Successfully fetched node", "nodeName", nodename)
	return n, nil
}

// PatchPodAnnotations 为 Pod 添加或更新注解
//
// 参数：
//   - kubeClient: Kubernetes 客户端
//   - pod: 要修改的 Pod 对象
//   - annotations: 要添加/更新的注解键值对
//
// 返回值：
//   - error: 错误信息
//
// 实现原理：
// 使用 Strategic Merge Patch 策略性合并补丁，只更新 metadata.annotations 字段，
// 不会影响 Pod 的其他部分。这种方式比 Update 更高效，因为：
// 1. 不需要先 Get 再 Update，减少 API 调用次数
// 2. 避免了并发冲突（不需要处理 ResourceVersion）
// 3. 只传输变更部分，减少网络开销
//
// 典型用途：
// - 记录 GPU 分配结果（如分配的 GPU UUID、显存大小等）
// - 标记 Pod 的设备调度状态
func PatchPodAnnotations(kubeClient kubernetes.Interface, pod *v1.Pod, annotations map[string]string) error {
	// patchMetadata 定义要补丁的元数据结构
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}

	// patchPod 定义整个补丁结构
	// 这里只包含 Metadata，因为只需要修改注解
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	// 将补丁对象序列化为 JSON
	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// 执行 Patch 操作
	// StrategicMergePatchType 会使用 Kubernetes 的策略性合并策略
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", pod.Name, err)
	}

	return err
}

// PatchNodeAnnotations 为 Node 添加或更新注解
//
// 参数：
//   - node: 要修改的 Node 对象
//   - annotations: 要添加/更新的注解键值对
//
// 返回值：
//   - error: 错误信息
//
// 与 PatchPodAnnotations 类似，但操作对象是节点。
//
// 典型用途：
// - 记录节点上的设备状态（如 GPU 健康状态、分配情况）
// - 标记节点的设备能力（如支持的 vGPU 几何形状）
// - 保存调试信息用于故障排查
func PatchNodeAnnotations(node *v1.Node, annotations map[string]string) error {
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		//Spec     patchSpec     `json:"spec,omitempty"`
	}

	p := patchPod{}
	p.Metadata.Annotations = annotations

	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = GetClient().CoreV1().Nodes().
		Patch(context.Background(), node.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		// 这里使用了 Info 级别日志，并打印了注解内容，方便调试
		klog.Infoln("annotations=", annotations)
		klog.Infof("patch node %v failed, %v", node.Name, err)
	}
	return err
}

// ExtractResourceRequest 从 Pod 中提取设备资源请求
//
// 参数说明：
//   - pod: 要分析的 Pod 对象
//   - resourceType: 设备类型标识（如 "NVIDIA"、"Ascend"）
//   - countName: 设备数量的资源名称（如 "nvidia.com/gpu"）
//   - memoryName: 设备内存的资源名称（如 "nvidia.com/gpumem"）
//   - percentageName: 设备内存百分比的资源名称（如 "nvidia.com/gpumem-percentage"），可为空
//   - coreName: 设备核心数的资源名称（如 "nvidia.com/gpucores"），可为空
//
// 返回值：
//   - []ContainerDeviceRequest: 每个容器的设备请求列表
//
// 提取逻辑：
// 1. 遍历 Pod 中的所有容器
// 2. 对于每个容器，依次尝试从 Limits 和 Requests 中读取：
//   - 设备数量（优先从 countName 读取）
//   - 设备内存（从 memoryName 读取）
//   - 设备内存百分比（从 percentageName 读取，如果提供）
//   - 设备核心数（从 coreName 读取，如果提供）
//
// 3. 组装成 ContainerDeviceRequest 对象
//
// 特殊处理：
// - 如果没有指定 countName，但指定了 memoryName，则认为请求了单个设备（singledevice=true）
// - 如果同时没有内存和内存百分比，默认设置内存百分比为 100%
// - 优先从 Limits 读取，如果没有则从 Requests 读取
//
// 典型用途：
// - 在调度器 Filter/Score 阶段了解 Pod 的设备需求
// - 为设备分配算法提供输入数据
func ExtractResourceRequest(pod *v1.Pod, resourceType, countName, memoryName, percentageName, coreName string) []ContainerDeviceRequest {
	resourceName := v1.ResourceName(countName)
	resourceMem := v1.ResourceName(memoryName)
	counts := []ContainerDeviceRequest{}

	// Count Nvidia GPU
	// 遍历 Pod 中的所有容器，提取每个容器的设备请求
	for i := 0; i < len(pod.Spec.Containers); i++ {
		singledevice := false

		// 首先尝试从 Limits 中读取设备数量
		v, ok := pod.Spec.Containers[i].Resources.Limits[resourceName]
		if !ok {
			// 如果没有找到数量限制，尝试读取内存限制
			// 这种情况通常表示请求单个设备，但指定了内存需求
			v, ok = pod.Spec.Containers[i].Resources.Limits[resourceMem]
			singledevice = true
		}

		if ok {
			// 确定设备数量
			n := int64(1)
			if !singledevice {
				n, _ = v.AsInt64()
			}

			// 提取设备内存需求
			memnum := int32(0)
			mem, ok := pod.Spec.Containers[i].Resources.Limits[resourceMem]
			if !ok {
				// 如果 Limits 中没有，尝试从 Requests 读取
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMem]
			}
			if ok {
				memnums, ok := mem.AsInt64()
				if ok {
					memnum = int32(memnums)
				}
			}

			// 提取设备内存百分比需求
			// mempnum 初始值为 101，表示"未设置"
			mempnum := int32(101)
			if percentageName != "" {
				resourceMemPercentage := v1.ResourceName(percentageName)
				mem, ok = pod.Spec.Containers[i].Resources.Limits[resourceMemPercentage]
				if !ok {
					mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMemPercentage]
				}
				if ok {
					mempnums, ok := mem.AsInt64()
					if ok {
						mempnum = int32(mempnums)
					}
				}
			}

			// 如果内存百分比未设置（101）且内存也为 0，则默认设置为 100%
			// 这表示请求设备的完整内存
			if mempnum == 101 && memnum == 0 {
				mempnum = 100
			}

			// 提取设备核心数需求
			corenum := int32(0)
			if coreName != "" {
				resourceCores := v1.ResourceName(coreName)
				core, ok := pod.Spec.Containers[i].Resources.Limits[resourceCores]
				if !ok {
					core, ok = pod.Spec.Containers[i].Resources.Requests[resourceCores]
				}
				if ok {
					corenums, ok := core.AsInt64()
					if ok {
						corenum = int32(corenums)
					}
				}
			}

			// 组装设备请求对象并添加到列表
			counts = append(counts, ContainerDeviceRequest{
				Nums:             int32(n),       // 设备数量
				Type:             resourceType,   // 设备类型
				Memreq:           memnum,         // 内存需求(MB 或其他单位)
				MemPercentagereq: int32(mempnum), // 内存百分比(0-100)
				Coresreq:         corenum,        // 核心数需求百分比(0-100)
			})
		}
	}

	klog.V(3).Infoln("counts=", counts)
	return counts
}
