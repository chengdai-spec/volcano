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
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	versionedscheme "volcano.sh/apis/pkg/client/clientset/versioned/scheme"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	batchinformer "volcano.sh/apis/pkg/client/informers/externalversions/batch/v1alpha1"
	flowinformer "volcano.sh/apis/pkg/client/informers/externalversions/flow/v1alpha1"
	batchlister "volcano.sh/apis/pkg/client/listers/batch/v1alpha1"
	flowlister "volcano.sh/apis/pkg/client/listers/flow/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/apis"
	"volcano.sh/volcano/pkg/controllers/framework"
	jobflowstate "volcano.sh/volcano/pkg/controllers/jobflow/state"
)

// init 在包加载时自动将 jobflowcontroller 注册到控制器框架
func init() {
	framework.RegisterController(&jobflowcontroller{})
}

// jobflowcontroller JobFlow 控制器的主结构体
// 采用经典的 Kubernetes Controller 模式：Informer 监听事件 → 工作队列缓冲 → Worker 消费处理
// 通过状态机（state 包）驱动 JobFlow 的状态流转，按依赖顺序部署和监控 VCJob
type jobflowcontroller struct {
	// kubeClient: Kubernetes 原生客户端，用于发送事件等
	kubeClient kubernetes.Interface
	// vcClient: Volcano 自定义资源客户端，用于操作 Job/JobFlow/JobTemplate
	vcClient vcclientset.Interface

	// ---- Informer 层：监听 CR 变更事件 ----
	// jobFlowInformer: JobFlow 资源的 Informer，监听 JobFlow 的创建和更新
	jobFlowInformer flowinformer.JobFlowInformer
	// jobTemplateInformer: JobTemplate 资源的 Informer，仅用于本地缓存查询，不监听事件
	jobTemplateInformer flowinformer.JobTemplateInformer
	// jobInformer: VCJob 资源的 Informer，监听子 Job 的状态变化以触发父 JobFlow 的同步
	jobInformer batchinformer.JobInformer

	// vcInformerFactory: Volcano 共享 Informer 工厂，统一管理所有 Informer 的生命周期
	vcInformerFactory vcinformer.SharedInformerFactory

	// ---- Lister 层：提供本地缓存的只读查询 ----
	// jobFlowLister: JobFlow 列表器，从本地缓存获取 JobFlow
	jobFlowLister flowlister.JobFlowLister
	// jobFlowSynced: JobFlow Informer 同步完成检查函数
	jobFlowSynced cache.InformerSynced

	// jobTemplateLister: JobTemplate 列表器，从本地缓存获取 JobTemplate
	jobTemplateLister flowlister.JobTemplateLister
	// jobTemplateSynced: JobTemplate Informer 同步完成检查函数
	jobTemplateSynced cache.InformerSynced

	// jobLister: VCJob 列表器，从本地缓存获取 VCJob
	jobLister batchlister.JobLister
	// jobSynced: VCJob Informer 同步完成检查函数
	jobSynced cache.InformerSynced

	// recorder: Kubernetes 事件记录器，用于在 JobFlow 资源上记录事件
	recorder record.EventRecorder

	// queue: 带限速的工作队列，存放待处理的 FlowRequest
	queue workqueue.TypedRateLimitingInterface[apis.FlowRequest]
	// enqueueJobFlow: 入队函数，将 FlowRequest 放入工作队列
	enqueueJobFlow func(req apis.FlowRequest)

	// syncHandler: 同步处理函数，实际执行 JobFlow 状态同步的核心逻辑
	syncHandler func(req *apis.FlowRequest) error

	// maxRequeueNum: 最大重试次数，-1 表示无限重试
	maxRequeueNum int
}

// Name 返回控制器名称，用于框架识别和注册
func (jf *jobflowcontroller) Name() string {
	return "jobflow-controller"
}

// Initialize 初始化控制器
// 设置 Informer、注册事件处理器、创建工作队列、绑定状态机同步函数
func (jf *jobflowcontroller) Initialize(opt *framework.ControllerOption) error {
	jf.kubeClient = opt.KubeClient
	jf.vcClient = opt.VolcanoClient

	factory := opt.VCSharedInformerFactory
	jf.vcInformerFactory = factory

	// 初始化 JobFlow Informer 并注册事件处理器（监听 JobFlow 的创建和更新）
	jf.jobFlowInformer = factory.Flow().V1alpha1().JobFlows()
	jf.jobFlowSynced = jf.jobFlowInformer.Informer().HasSynced
	jf.jobFlowLister = jf.jobFlowInformer.Lister()
	jf.jobFlowInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jf.addJobFlow,    // JobFlow 创建时触发同步
		UpdateFunc: jf.updateJobFlow, // JobFlow 更新时触发同步（仅在成功+Delete策略时）
	})

	// 初始化 JobTemplate Informer（仅用于本地缓存查询，不注册事件处理器）
	jf.jobTemplateInformer = factory.Flow().V1alpha1().JobTemplates()
	jf.jobTemplateSynced = jf.jobTemplateInformer.Informer().HasSynced
	jf.jobTemplateLister = jf.jobTemplateInformer.Lister()

	// 初始化 VCJob Informer 并注册事件处理器（监听子 Job 的状态变化）
	jf.jobInformer = factory.Batch().V1alpha1().Jobs()
	jf.jobSynced = jf.jobInformer.Informer().HasSynced
	jf.jobLister = jf.jobInformer.Lister()
	jf.jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: jf.updateJob, // 子 Job 状态变化时触发父 JobFlow 的同步
	})

	// 设置最大重试次数，<0 表示无限重试
	jf.maxRequeueNum = opt.MaxRequeueNum
	if jf.maxRequeueNum < 0 {
		jf.maxRequeueNum = -1
	}

	// 初始化 Kubernetes 事件记录器
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: jf.kubeClient.CoreV1().Events("")})

	jf.recorder = eventBroadcaster.NewRecorder(versionedscheme.Scheme, v1.EventSource{Component: "vc-controller-manager"})
	jf.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[apis.FlowRequest]())

	// 绑定核心处理函数
	jf.enqueueJobFlow = jf.enqueue
	jf.syncHandler = jf.handleJobFlow

	// 将同步函数注入到 state 包（状态机的 Execute 方法需要通过此函数调用控制器层的同步逻辑）
	jobflowstate.SyncJobFlow = jf.syncJobFlow
	return nil
}

// Run 启动控制器的主循环
// 流程：启动所有 Informer → 等待缓存同步完成 → 启动 Worker 协程消费工作队列 → 阻塞等待停止信号
func (jf *jobflowcontroller) Run(stopCh <-chan struct{}) {
	defer jf.queue.ShutDown()

	// 启动所有 Informer
	jf.vcInformerFactory.Start(stopCh)
	// 等待所有 Informer 的本地缓存同步完成
	for informerType, ok := range jf.vcInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	// 启动单个 Worker 协程，每秒检查一次队列中是否有待处理的任务
	go wait.Until(jf.worker, time.Second, stopCh)

	klog.Infof("JobFlowController is running ...... ")

	// 阻塞等待停止信号
	<-stopCh
}

// worker Worker 协程，持续从队列中取任务并处理，直到队列关闭
func (jf *jobflowcontroller) worker() {
	for jf.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的下一个工作项
// 从队列取出 FlowRequest，调用 syncHandler 执行同步，然后处理错误（重试或丢弃）
func (jf *jobflowcontroller) processNextWorkItem() bool {
	req, shutdown := jf.queue.Get()
	if shutdown {
		// 队列已关闭，停止工作
		return false
	}

	// Done 告知队列当前项已处理完毕
	// 如果发生瞬时错误，通过 AddRateLimited 将请求重新入队，而不是调用 Forget
	defer jf.queue.Done(req)

	err := jf.syncHandler(&req)
	jf.handleJobFlowErr(err, req)

	return true
}

// handleJobFlow 处理单个 JobFlow 同步请求的核心逻辑
// 流程：从缓存获取 JobFlow → 创建状态机 → 执行对应状态的动作
func (jf *jobflowcontroller) handleJobFlow(req *apis.FlowRequest) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing jobflow %s (%v).", req.JobFlowName, time.Since(startTime))
	}()

	// 从本地缓存获取 JobFlow 资源
	jobflow, err := jf.jobFlowLister.JobFlows(req.Namespace).Get(req.JobFlowName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// JobFlow 已被删除，无需处理
			klog.V(4).Infof("JobFlow %s has been deleted.", req.JobFlowName)
			return nil
		}

		return fmt.Errorf("get jobflow %s failed for %v", req.JobFlowName, err)
	}

	// 根据 JobFlow 当前 Phase 创建对应的状态机实例(Pending/Running/Succeed/Failed/Terminating)
	jobFlowState := jobflowstate.NewState(jobflow)
	if jobFlowState == nil {
		return fmt.Errorf("jobflow %s state %s is invalid", jobflow.Name, jobflow.Status.State)
	}

	klog.V(4).Infof("Begin execute %s action for jobflow %s", req.Action, req.JobFlowName)
	// 执行状态机中对应状态的动作（实际会调用 syncJobFlow 进行同步）
	if err := jobFlowState.Execute(req.Action); err != nil {
		return fmt.Errorf("sync jobflow %s failed for %v, event is %v, action is %s",
			req.JobFlowName, err, req.Event, req.Action)
	}

	return nil
}

// handleJobFlowErr 处理同步过程中的错误
// 成功：调用 Forget 从队列移除；
// 失败且未超限：限速重试；
// 失败且超限：记录警告事件并丢弃请求
func (jf *jobflowcontroller) handleJobFlowErr(err error, req apis.FlowRequest) {
	if err == nil {
		jf.queue.Forget(req)
		return
	}

	// 未超过最大重试次数，将请求限速重新入队
	if jf.maxRequeueNum == -1 || jf.queue.NumRequeues(req) < jf.maxRequeueNum {
		klog.V(4).Infof("Error syncing jobFlow request %v for %v.", req, err)
		jf.queue.AddRateLimited(req)
		return
	}

	// 超过最大重试次数，记录警告事件并丢弃
	jf.recordEventsForJobFlow(req.Namespace, req.JobFlowName, v1.EventTypeWarning, string(req.Action),
		fmt.Sprintf("%v JobFlow failed for %v", req.Action, err))
	klog.V(4).Infof("Dropping JobFlow request %v out of the queue for %v.", req, err)
	jf.queue.Forget(req)
}

// recordEventsForJobFlow 在指定的 JobFlow 资源上记录 Kubernetes 事件
// 用于向用户报告控制器的操作结果（如重试超限的警告）
func (jf *jobflowcontroller) recordEventsForJobFlow(namespace, name, eventType, reason, message string) {
	jobFlow, err := jf.jobFlowLister.JobFlows(namespace).Get(name)
	if err != nil {
		klog.Errorf("Get JobFlow %s failed for %v.", name, err)
		return
	}

	jf.recorder.Event(jobFlow, eventType, reason, message)
}
