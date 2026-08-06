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

// Package nodelock 提供基于 Kubernetes Node Annotations 的轻量级分布式锁机制。
//
// 【设计背景】
// Volcano 调度器在进行设备分配（GPU共享、vGPU、Ascend NPU 等）时，可能存在多个调度实例
// 或 goroutine 同时对同一节点进行设备分配操作，导致资源超卖或分配冲突。
// 本包利用 Node 的 Annotation 字段作为分布式锁，确保同一时刻只有一个调度器
// 能对特定节点的特定设备类型执行分配操作。
//
// 【锁实现原理】
//   - 锁载体：Node 对象的 Annotations 字段
//   - 锁键名（lockName）：设备类型标识，如 "gpu"、"nvidia.com/gpu"、"huawei.com/Ascend910" 等
//   - 锁值：加锁时间的 RFC3339 格式时间戳字符串
//   - 锁超时：5分钟自动过期（防止持有者崩溃后死锁）
//   - 并发安全：依赖 Kubernetes API Server 的乐观锁（ResourceVersion）+ 重试机制
//
// 【使用场景】
//   - gpushare 设备分配前加锁：nodelock.LockNode(nodeName, "gpu")
//   - vgpu 设备分配前加锁：nodelock.LockNode(nodeName, "hami-vgpu")
//   - Ascend NPU 设备分配前加锁：nodelock.LockNode(nodeName, ads.Type)
//
// 【典型调用流程】
//   1. nodelock.UseClient(kubeClient)  -- 注入已有的 K8s 客户端
//   2. nodelock.LockNode(nodeName, lockName)  -- 尝试加锁（失败则跳过该节点）
//   3. 执行设备分配逻辑（选择设备、Patch Pod Annotations 等）
//   4. nodelock.ReleaseNodeLock(nodeName, lockName)  -- 释放锁
package nodelock

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// MaxLockRetry 定义更新 Node Annotations 时的最大重试次数。
// 当 Update 操作因冲突（如 ResourceVersion 不一致）失败时，最多重试 5 次。
const MaxLockRetry = 5

// kubeClient 是包级别的全局 Kubernetes 客户端实例。
// 所有锁操作（加锁/解锁）都通过此客户端与 API Server 交互。
// 可通过 NewClient() 创建或 UseClient() 注入。
var kubeClient kubernetes.Interface

// GetClient 返回当前全局 Kubernetes 客户端实例。
// 注意：如果未调用 NewClient() 或 UseClient() 初始化，返回值可能为 nil。
func GetClient() kubernetes.Interface {
	return kubeClient
}

// NewClient 创建并初始化一个 Kubernetes API 客户端连接。
//
// 【连接策略（优先级从高到低）】
//  1. 优先使用 InClusterConfig（Pod 内运行时自动读取 ServiceAccount 凭证）
//  2. 若集群内配置不可用，则回退到 kubeconfig 文件：
//     - 优先读取环境变量 KUBECONFIG 指定的路径
//     - 否则使用默认路径 ~/.kube/config
//
// 【副作用】创建成功后会将客户端赋值给全局变量 kubeClient。
func NewClient() (kubernetes.Interface, error) {
	// 获取 kubeconfig 文件路径：优先使用 KUBECONFIG 环境变量
	kubeConfig := os.Getenv("KUBECONFIG")
	if kubeConfig == "" {
		// 未设置环境变量时，使用默认的 ~/.kube/config 路径
		kubeConfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	// 优先尝试集群内配置（适用于调度器以 Pod 形式部署在集群内的场景）
	config, err := rest.InClusterConfig()
	if err != nil {
		// 集群内配置失败（如本地开发调试），回退到 kubeconfig 文件
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			return nil, err
		}
	}
	// 根据配置创建 Kubernetes 客户端
	client, err := kubernetes.NewForConfig(config)
	// 将新创建的客户端设置为全局客户端，供后续锁操作使用
	kubeClient = client
	return client, err
}

// UseClient 注入一个已有的 Kubernetes 客户端到全局变量中。
//
// 【使用场景】调度器主流程已经持有 kubeClient，设备分配时直接复用，
// 避免重复创建连接。例如在 gpushare/vgpu/ascend 的 Allocate 方法中：
//
//	nodelock.UseClient(kubeClient)
//	err := nodelock.LockNode(nodeName, "gpu")
func UseClient(client kubernetes.Interface) error {
	kubeClient = client
	return nil
}

// updateNodeAnnotations 是节点 Annotation 更新的核心辅助函数，封装了带重试的更新逻辑。
//
// 【参数说明】
//   - ctx: 上下文，用于超时控制和取消传播
//   - node: 目标节点对象（会进行 DeepCopy，不修改原始对象）
//   - updateFunc: 具体的 Annotation 修改逻辑（闭包），由调用方定义如何修改 annotations map
//
// 【重试机制】
// 当 Update 失败时（通常因为 ResourceVersion 冲突，即其他组件同时修改了该 Node），
// 会重新 Get 最新的 Node 对象，重新应用 updateFunc，再次尝试 Update。
// 最多重试 MaxLockRetry(5) 次，每次重试间隔 100ms。
//
// 【乐观锁原理】
// Kubernetes 的 Update 操作基于 ResourceVersion 实现乐观并发控制：
// 如果提交的对象 ResourceVersion 与服务端不一致，API Server 会返回 409 Conflict，
// 此时需要重新获取最新状态再重试。
func updateNodeAnnotations(ctx context.Context, node *v1.Node, updateFunc func(annotations map[string]string)) error {
	// 深拷贝节点对象，避免修改调用方持有的原始数据
	newNode := node.DeepCopy()
	// 对副本的 Annotations 执行调用方指定的修改逻辑
	updateFunc(newNode.ObjectMeta.Annotations)
	nodeName := newNode.Name
	// 第一次尝试更新 Node 对象到 API Server
	_, err := kubeClient.CoreV1().Nodes().Update(ctx, newNode, metav1.UpdateOptions{})
	// 如果更新失败，进入重试循环（最多 MaxLockRetry 次）
	for i := 0; i < MaxLockRetry && err != nil; i++ {
		klog.ErrorS(err, "Failed to update node", "node", nodeName, "retry", i)
		// 等待 100ms 后重试，给其他并发操作留出完成时间
		time.Sleep(100 * time.Millisecond)
		// 重新从 API Server 获取最新的 Node 对象（获取最新 ResourceVersion）
		node, err = kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Failed to get node when retry to update", "node", nodeName)
			continue
		}
		// 基于最新状态重新深拷贝并应用修改
		newNode = node.DeepCopy()
		updateFunc(newNode.ObjectMeta.Annotations)
		// 再次尝试提交更新
		_, err = kubeClient.CoreV1().Nodes().Update(ctx, newNode, metav1.UpdateOptions{})
	}
	// 所有重试都失败，返回错误
	if err != nil {
		klog.ErrorS(err, "Failed to update node", "node", nodeName)
		return fmt.Errorf("failed to update node %s, exceeded retry count %d", nodeName, MaxLockRetry)
	}
	return nil
}

// setNodeLock 在指定节点上设置分布式锁（内部函数，由 LockNode 调用）。
//
// 【加锁原理】
// 向 Node 的 Annotations 中写入一个键值对：
//   - Key = lockName（设备类型标识，如 "gpu"）
//   - Value = 当前时间的 RFC3339 格式字符串（作为锁的时间戳）
//
// 【前置检查】
// 如果该 Annotation Key 已存在，说明锁已被其他调度器持有，直接返回错误。
//
// 【示例】加锁后 Node Annotations 中会出现：
//
//	annotations:
//	  "gpu": "2024-01-15T10:30:00Z"
func setNodeLock(nodeName string, lockName string) error {
	ctx := context.Background()
	// 从 API Server 获取目标节点的最新状态
	node, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	// 检查锁是否已被持有：如果 Annotation 中已存在该 lockName，说明其他实例已加锁
	if _, ok := node.ObjectMeta.Annotations[lockName]; ok {
		klog.V(3).Infof("node %s is locked", nodeName)
		return fmt.Errorf("node %s is locked", nodeName)
	}
	// 定义 Annotation 更新逻辑：写入当前时间作为锁的创建时间
	updateFunc := func(annotations map[string]string) {
		annotations[lockName] = time.Now().Format(time.RFC3339)
	}
	// 执行带重试的 Annotation 更新
	err = updateNodeAnnotations(ctx, node, updateFunc)
	if err != nil {
		return fmt.Errorf("setNodeLock exceeds retry count %d", MaxLockRetry)
	}
	klog.InfoS("Node lock set", "node", nodeName)
	return nil
}

// ReleaseNodeLock 释放指定节点上的分布式锁（删除对应的 Annotation）。
//
// 【释放逻辑】
// 从 Node 的 Annotations 中删除 lockName 对应的键值对。
// 如果锁本身不存在（可能已被其他操作释放或超时清理），视为释放成功（幂等性）。
//
// 【调用时机】
// 设备分配操作完成后必须调用此函数释放锁，否则其他调度器在 5 分钟内
// 无法对该节点的同类设备进行分配操作。
//
// 【示例调用】（在 Ascend NPU Allocate 完成后）
//
//	nodelock.ReleaseNodeLock(ads.NodeName, ads.Type)
func ReleaseNodeLock(nodeName string, lockName string) error {
	ctx := context.Background()
	// 获取目标节点最新状态
	node, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	// 如果锁不存在，说明已经被释放或从未加锁，直接返回成功（幂等设计）
	if _, ok := node.ObjectMeta.Annotations[lockName]; !ok {
		klog.V(3).InfoS("Node lock not set", "node", nodeName)
		return nil
	}
	// 定义 Annotation 更新逻辑：删除锁对应的键
	updateFunc := func(annotations map[string]string) {
		delete(annotations, lockName)
	}
	// 执行带重试的 Annotation 更新（删除锁键）
	err = updateNodeAnnotations(ctx, node, updateFunc)
	if err != nil {
		return fmt.Errorf("releaseNodeLock exceeds retry count %d", MaxLockRetry)
	}
	klog.InfoS("Node lock released", "node", nodeName)
	return nil
}

// LockNode 尝试对指定节点的指定设备类型加锁（对外暴露的主入口函数）。
//
// 【参数说明】
//   - nodeName: 目标节点名称（如 "node-01"）
//   - lockName: 锁名称/设备类型标识（如 "gpu"、"hami-vgpu"、"huawei.com/Ascend910"）
//
// 【完整加锁流程】
//  1. 获取节点当前状态
//  2. 如果锁不存在 → 直接加锁（调用 setNodeLock）
//  3. 如果锁已存在 → 解析锁的时间戳
//     a. 如果锁已超过 5 分钟 → 视为过期锁，先释放再重新加锁
//     b. 如果锁未过期 → 返回错误，表示该节点正在被其他调度器操作
//
// 【超时机制（防死锁）】
// 锁的 TTL 为 5 分钟。如果持锁的调度器崩溃或异常退出未能释放锁，
// 5 分钟后其他调度器可以自动接管（释放过期锁并重新加锁）。
//
// 【返回值】
//   - nil: 加锁成功，调用方可以继续执行设备分配
//   - error: 加锁失败（锁被他人持有且未过期），调用方应跳过该节点
//
// 【典型使用模式】
//
//	if NodeLockEnable {
//	    nodelock.UseClient(kubeClient)
//	    err := nodelock.LockNode(nodeName, "gpu")
//	    if err != nil {
//	        return err  // 加锁失败，跳过该节点
//	    }
//	    defer nodelock.ReleaseNodeLock(nodeName, "gpu")
//	}
func LockNode(nodeName string, lockName string) error {
	ctx := context.Background()
	// 第一步：获取目标节点的最新状态
	node, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	// 第二步：检查锁是否已存在
	if _, ok := node.ObjectMeta.Annotations[lockName]; !ok {
		// 锁不存在，说明当前无人持有，直接加锁
		return setNodeLock(nodeName, lockName)
	}
	// 第三步：锁已存在，解析锁的创建时间（RFC3339 格式）
	lockTime, err := time.Parse(time.RFC3339, node.ObjectMeta.Annotations[lockName])
	if err != nil {
		// 时间格式异常，返回错误（可能是人为篡改或数据损坏）
		return err
	}
	// 第四步：判断锁是否已过期（超过 5 分钟）
	if time.Since(lockTime) > time.Minute*5 {
		// 锁已过期：说明持锁者可能已崩溃，执行「释放过期锁 + 重新加锁」
		klog.V(3).InfoS("Node lock expired", "node", nodeName, "lockTime", lockTime)
		err = ReleaseNodeLock(nodeName, lockName)
		if err != nil {
			klog.ErrorS(err, "Failed to release node lock", "node", nodeName)
			return err
		}
		// 释放成功后重新加锁，抢占该节点的设备分配权
		return setNodeLock(nodeName, lockName)
	}
	// 第五步：锁未过期，说明其他调度器正在操作该节点，返回错误让调用方跳过
	return fmt.Errorf("node %s has been locked within 5 minutes", nodeName)
}
