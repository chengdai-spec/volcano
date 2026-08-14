/*
Copyright 2017 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

本文件是 Volcano Job Controller 的控制器入口和调度队列处理逻辑，主要职责包括：

1. 注册 job-controller；
2. 初始化 Kubernetes client、Volcano client、Informer、Lister、工作队列、事件记录器；
3. 监听 Job、Pod、PodGroup、Command、Queue、PVC、Service、PriorityClass 等资源变化；
4. 将资源事件转换为 controller 内部 Request；
5. 使用多个 worker 并发处理 Job 请求；
6. 基于 Job key hash，将同一个 Job 的事件固定分配到同一个 worker，避免同一个 Job 并发处理导致状态错乱；
7. 执行 Job 状态机动作；
8. 支持延迟动作 delayAction，例如 Pod Pending、Failed、Evicted 后延迟处理；
9. 在 Pod 状态变化时清理过期的延迟动作；
10. 处理执行失败后的重试和达到最大重试次数后的终止逻辑。
*/

package job

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kubeschedulinginformers "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	kubeschedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	vcscheme "volcano.sh/apis/pkg/client/clientset/versioned/scheme"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	batchinformer "volcano.sh/apis/pkg/client/informers/externalversions/batch/v1alpha1"
	businformer "volcano.sh/apis/pkg/client/informers/externalversions/bus/v1alpha1"
	schedulinginformers "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
	batchlister "volcano.sh/apis/pkg/client/listers/batch/v1alpha1"
	buslister "volcano.sh/apis/pkg/client/listers/bus/v1alpha1"
	schedulinglisters "volcano.sh/apis/pkg/client/listers/scheduling/v1beta1"

	"volcano.sh/volcano/pkg/controllers/apis"
	jobcache "volcano.sh/volcano/pkg/controllers/cache"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/controllers/job/state"
	"volcano.sh/volcano/pkg/features"
)

// init 在包初始化时将 jobcontroller 注册到 Volcano controller framework 中。
// controller-manager 启动时会通过 framework 加载已注册的 controller。
func init() {
	framework.RegisterController(&jobcontroller{})
}

// delayAction 表示一个"延迟执行动作"
//
// Volcano Job Controller 中有些事件不会立即触发动作，而是延迟一段时间后再执行
// 典型场景：
//   - Pod Pending 后等待一段时间，如果仍未恢复，再执行某个动作
//   - Pod Failed 后等待一段时间，再重启任务或终止 Job
//   - Pod Evicted 后等待一段时间，再执行恢复逻辑
//
// 延迟动作需要支持取消。
// 因为 Pod 状态可能在 delay 时间到达前发生变化，比如 Pending -> Running，
// 此时之前 Pending 对应的延迟动作就已经过期，应该被取消。
type delayAction struct {
	// jobKey 是 Job 的命名空间级别 key，通常格式为 namespace/name。
	// 用于定位这个延迟动作属于哪个 Job。
	jobKey string

	// taskName 表示延迟动作关联的 Task 名称。
	// 如果是 Task 级别动作，会使用该字段判断是否属于同一个 Task。
	taskName string

	// podName 表示延迟动作关联的 Pod 名称。
	// delayActionMap 的内层 map 就是以 podName 为 key。
	podName string

	// podUID 表示延迟动作关联的 Pod UID。
	// 注意：Pod 名称可能复用，但 UID 不会复用。
	// 因此在清理 Pending 延迟动作时，需要用 UID 判断是否还是同一个 Pod。
	podUID types.UID

	// partition 表示延迟动作关联的 Partition 分组。
	// 用于 Partition 级别动作的匹配与清理。
	partition string

	// event 表示触发该延迟动作的事件。
	// 例如 PodPendingEvent、PodFailedEvent、PodEvictedEvent 等。
	event busv1alpha1.Event

	// action 表示延迟到期后真正要执行的动作。
	// 例如 SyncJobAction、RestartJobAction、TerminateJobAction 等。
	action busv1alpha1.Action

	// delay 表示延迟执行时间。
	// 如果 delay == 0，表示不需要延迟，立即执行。
	delay time.Duration

	// cancel 是取消函数。
	// 通过 context.WithTimeout 创建延迟任务时，会得到 cancel 函数。
	// 如果 Pod 状态提前变化，就调用 cancel 取消该延迟动作。
	cancel context.CancelFunc
}

// jobcontroller 是 Volcano Job Controller 的主体结构。
//
// 它持有：
//   - Kubernetes client；
//   - Volcano client；
//   - 各种 informer/lister；
//   - 工作队列；
//   - cache；
//   - event recorder；
//   - 延迟动作缓存；
//   - worker 数量和重试参数。
type jobcontroller struct {
	// kubeClient 用于访问 Kubernetes 原生资源，例如 Pod、PVC、Service、Event 等。
	kubeClient kubernetes.Interface

	// vcClient 用于访问 Volcano 自定义资源，例如 Job、PodGroup、Command、Queue 等。
	vcClient vcclientset.Interface

	// Job informer，监听 Volcano Job 资源变化。
	jobInformer batchinformer.JobInformer

	// Pod informer，监听 Kubernetes Pod 资源变化。
	podInformer coreinformers.PodInformer

	// PVC informer，监听 PersistentVolumeClaim 资源。
	pvcInformer coreinformers.PersistentVolumeClaimInformer

	// PodGroup informer，监听 Volcano PodGroup 资源。
	pgInformer schedulinginformers.PodGroupInformer

	// Service informer，监听 Service 资源。
	svcInformer coreinformers.ServiceInformer

	// Command informer，监听 Volcano Command 资源。
	cmdInformer businformer.CommandInformer

	// PriorityClass informer，监听 Kubernetes PriorityClass。
	pcInformer kubeschedulinginformers.PriorityClassInformer

	// Queue informer，监听 Volcano Queue 资源。
	queueInformer schedulinginformers.QueueInformer

	// Kubernetes 原生资源 informer factory。
	informerFactory informers.SharedInformerFactory

	// Volcano 自定义资源 informer factory。
	vcInformerFactory vcinformer.SharedInformerFactory

	// jobLister 用于从 informer 本地缓存中读取 Job。
	jobLister batchlister.JobLister

	// jobSynced 用于判断 Job informer 缓存是否已经同步完成。
	jobSynced func() bool

	// podLister 用于从 informer 本地缓存中读取 Pod。
	podLister corelisters.PodLister

	// podSynced 用于判断 Pod informer 缓存是否已经同步完成。
	podSynced func() bool

	// pvcLister 用于从 informer 本地缓存中读取 PVC。
	pvcLister corelisters.PersistentVolumeClaimLister

	// pvcSynced 用于判断 PVC informer 缓存是否已经同步完成。
	pvcSynced func() bool

	// pgLister 用于从 informer 本地缓存中读取 PodGroup。
	pgLister schedulinglisters.PodGroupLister

	// pgSynced 用于判断 PodGroup informer 缓存是否已经同步完成。
	pgSynced func() bool

	// svcLister 用于从 informer 本地缓存中读取 Service。
	svcLister corelisters.ServiceLister

	// svcSynced 用于判断 Service informer 缓存是否已经同步完成。
	svcSynced func() bool

	// cmdLister 用于从 informer 本地缓存中读取 Command。
	cmdLister buslister.CommandLister

	// cmdSynced 用于判断 Command informer 缓存是否已经同步完成。
	cmdSynced func() bool

	// pcLister 用于从 informer 本地缓存中读取 PriorityClass。
	pcLister kubeschedulinglisters.PriorityClassLister

	// pcSynced 用于判断 PriorityClass informer 缓存是否已经同步完成。
	pcSynced func() bool

	// queueLister 用于从 informer 本地缓存中读取 Volcano Queue。
	queueLister schedulinglisters.QueueLister

	// queueSynced 用于判断 Queue informer 缓存是否已经同步完成。
	queueSynced func() bool

	// queueList 是 Job 请求处理队列列表。
	//
	// Volcano Job Controller 使用多个 worker，每个 worker 对应一个队列。
	// 同一个 Job 的事件会根据 jobKey hash 到固定队列中，
	// 这样可以保证同一个 Job 的事件串行处理，避免并发修改同一个 Job 状态。
	queueList []workqueue.TypedRateLimitingInterface[any]

	// commandQueue 是 Command 资源事件队列。
	// Command 是 Volcano bus 中用于向 Job 发送操作指令的资源。
	commandQueue workqueue.TypedRateLimitingInterface[any]

	// cache 是 Job Controller 内部缓存。
	// 它会聚合 Job、Pod、Task、PodGroup 等信息，生成 apis.JobInfo。
	cache jobcache.Cache

	// recorder 用于向 Kubernetes Event 系统记录事件。
	recorder record.EventRecorder

	// errTasks 是错误任务重同步队列。
	// 当某些 Pod 删除失败、处理失败时，可能会加入该队列后续重试。
	errTasks workqueue.TypedRateLimitingInterface[any]

	// workers 表示 worker 数量。
	workers uint32

	// maxRequeueNum 表示一个请求最大重试次数。
	// 如果为 -1，表示无限重试。
	maxRequeueNum int

	// delayActionMapLock 用于保护 delayActionMap。
	// delayActionMap 会被 worker goroutine 和延迟动作 goroutine 并发访问，所以需要加锁。
	delayActionMapLock sync.RWMutex

	// delayActionMap 存储 Job 的延迟动作。
	//
	// 结构：
	//   outer key: jobKey，格式 namespace/name；
	//   inner key: podName；
	//   value: 具体的 delayAction。
	//
	// 也就是说，一个 Job 下可以保存多个 Pod 相关的延迟动作。
	delayActionMap map[string]map[string]*delayAction
}

// Name 返回 controller 名称。
// framework 会使用该名称识别 controller。
func (cc *jobcontroller) Name() string {
	return "job-controller"
}

// Initialize 初始化 Job Controller。
//
// 主要步骤：
//  1. 初始化 Kubernetes client 和 Volcano client
//  2. 初始化事件广播器和 recorder
//  3. 初始化多个 worker queue
//  4. 初始化 commandQueue errTasks cache
//  5. 根据 feature gate 注册对应 informer
//  6. 给 Job Pod PodGroup Command 注册事件处理函数
//  7. 初始化各种 lister 和 synced 函数
//  8. 初始化 delayActionMap
//  9. 将状态机需要的核心动作绑定到当前 controller 方法
func (cc *jobcontroller) Initialize(opt *framework.ControllerOption) error {
	// 初始化 Kubernetes client 和 Volcano client。
	cc.kubeClient = opt.KubeClient
	cc.vcClient = opt.VolcanoClient

	sharedInformers := opt.SharedInformerFactory
	workers := opt.WorkerNum

	// 初始化事件广播器
	// recorder.Event / Eventf 最终会把事件写入 Kubernetes Event
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: cc.kubeClient.CoreV1().Events("")})

	// 创建事件记录器
	// Component 表示事件来源组件名称
	recorder := eventBroadcaster.NewRecorder(vcscheme.Scheme, v1.EventSource{Component: "vc-controller-manager"})

	cc.informerFactory = sharedInformers

	// 根据 worker 数量创建多个队列
	// 每个 worker 独占一个队列
	cc.queueList = make([]workqueue.TypedRateLimitingInterface[any], workers)

	// commandQueue 用于处理 Command 资源
	cc.commandQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[any]())

	// 初始化内部 Job cache
	cc.cache = jobcache.New()

	// 初始化错误任务重试队列
	cc.errTasks = newRateLimitingQueue()

	cc.recorder = recorder
	cc.workers = workers
	cc.maxRequeueNum = opt.MaxRequeueNum

	// maxRequeueNum < 0 时统一设置为 -1, 表示无限重试
	if cc.maxRequeueNum < 0 {
		cc.maxRequeueNum = -1
	}

	// 为每个 worker 创建一个 rate limiting queue。
	var i uint32
	for i = 0; i < workers; i++ {
		cc.queueList[i] = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[any]())
	}

	// Volcano CRD informer factory
	factory := opt.VCSharedInformerFactory
	cc.vcInformerFactory = factory

	// 如果开启 VolcanoJobSupport，则监听 Volcano Job
	if utilfeature.DefaultFeatureGate.Enabled(features.VolcanoJobSupport) {
		cc.jobInformer = factory.Batch().V1alpha1().Jobs()

		// 注册 Job 事件处理函数
		// 这些函数通常会把 Job 对应的 Request 加入工作队列
		cc.jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    cc.addJob,
			UpdateFunc: cc.updateJob,
			DeleteFunc: cc.deleteJob,
		})

		cc.jobLister = cc.jobInformer.Lister()
		cc.jobSynced = cc.jobInformer.Informer().HasSynced
	}

	// 如果开启 QueueCommandSync，则监听 Command。
	// Command 是 Volcano bus 中用于控制 Job 的命令资源。
	if utilfeature.DefaultFeatureGate.Enabled(features.QueueCommandSync) {
		cc.cmdInformer = factory.Bus().V1alpha1().Commands()

		// 使用 FilteringResourceEventHandler 过滤 Command。
		// 这里只处理 TargetObject 是 Volcano Job 的 Command。
		cc.cmdInformer.Informer().AddEventHandler(
			cache.FilteringResourceEventHandler{
				FilterFunc: func(obj interface{}) bool {
					switch v := obj.(type) {
					case *busv1alpha1.Command:
						if v.TargetObject != nil &&
							v.TargetObject.APIVersion == batchv1alpha1.SchemeGroupVersion.String() &&
							v.TargetObject.Kind == "Job" {
							return true
						}

						return false
					default:
						return false
					}
				},
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc: cc.addCommand,
				},
			},
		)

		cc.cmdLister = cc.cmdInformer.Lister()
		cc.cmdSynced = cc.cmdInformer.Informer().HasSynced
	}

	// 监听 Pod 事件
	// Pod 是 Job 运行状态变化的重要来源
	cc.podInformer = sharedInformers.Core().V1().Pods()
	cc.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    cc.addPod,
		UpdateFunc: cc.updatePod,
		DeleteFunc: cc.deletePod,
	})

	cc.podLister = cc.podInformer.Lister()
	cc.podSynced = cc.podInformer.Informer().HasSynced

	// PVC informer
	// Job 可能声明 Volume，需要创建或检查 PVC
	cc.pvcInformer = sharedInformers.Core().V1().PersistentVolumeClaims()
	cc.pvcLister = cc.pvcInformer.Lister()
	cc.pvcSynced = cc.pvcInformer.Informer().HasSynced

	// Service informer
	// 一些插件或任务可能需要 Service
	cc.svcInformer = sharedInformers.Core().V1().Services()
	cc.svcLister = cc.svcInformer.Lister()
	cc.svcSynced = cc.svcInformer.Informer().HasSynced

	// PodGroup informer
	// PodGroup 是 Volcano gang scheduling 的关键资源
	// 当 PodGroup 状态变化时，Job Controller 需要重新同步 Job
	cc.pgInformer = factory.Scheduling().V1beta1().PodGroups()
	cc.pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: cc.updatePodGroup,
	})
	cc.pgLister = cc.pgInformer.Lister()
	cc.pgSynced = cc.pgInformer.Informer().HasSynced

	// PriorityClass informer。
	// 如果开启 PriorityClass feature，则用于计算 PodGroup 最小资源时考虑任务优先级。
	if utilfeature.DefaultFeatureGate.Enabled(features.PriorityClass) {
		cc.pcInformer = sharedInformers.Scheduling().V1().PriorityClasses()
		cc.pcLister = cc.pcInformer.Lister()
		cc.pcSynced = cc.pcInformer.Informer().HasSynced
	}

	// Queue informer。
	// Job 属于某个 Queue，Queue 信息会影响调度和跨集群转发等逻辑。
	cc.queueInformer = factory.Scheduling().V1beta1().Queues()
	cc.queueLister = cc.queueInformer.Lister()
	cc.queueSynced = cc.queueInformer.Informer().HasSynced

	// 初始化延迟动作缓存。
	cc.delayActionMap = make(map[string]map[string]*delayAction)

	// 注册状态机动作。
	// state 包中状态机执行具体动作时，会调用这里绑定的方法。
	state.SyncJob = cc.syncJob
	state.KillJob = cc.killJob
	state.KillTarget = cc.killTarget

	return nil
}

// Run 启动 Job Controller。
//
// 启动流程：
//  1. 启动 Kubernetes informer factory；
//  2. 启动 Volcano informer factory；
//  3. 等待所有 informer cache 同步完成；
//  4. 启动 Command 处理循环；
//  5. 启动多个 Job worker；
//  6. 启动内部 cache；
//  7. 启动错误任务重同步循环。
func (cc *jobcontroller) Run(stopCh <-chan struct{}) {
	// 启动 Kubernetes informer。
	cc.informerFactory.Start(stopCh)

	// 启动 Volcano informer。
	cc.vcInformerFactory.Start(stopCh)

	// 等待 Kubernetes informer cache 同步完成。
	for informerType, ok := range cc.informerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	// 等待 Volcano informer cache 同步完成。
	for informerType, ok := range cc.vcInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	// 启动 Command 处理循环。
	// wait.Until 的 period 为 0，表示函数退出后立即再次执行，直到 stopCh 关闭。
	go wait.Until(cc.handleCommands, 0, stopCh)

	// 启动多个 worker，每个 worker 处理一个独立队列。
	var i uint32
	for i = 0; i < cc.workers; i++ {
		go func(num uint32) {
			wait.Until(
				func() {
					cc.worker(num)
				},
				time.Second,
				stopCh)
		}(i)
	}

	// 启动内部 cache
	go cc.cache.Run(stopCh)

	// 启动错误任务重同步循环
	go wait.Until(cc.processResyncTask, 0, stopCh)

	klog.Infof("JobController is running ...... ")
}

// worker 是单个工作协程的主循环。
// 每个 worker 会不断从自己对应的 queue 中取 Request 并处理。
func (cc *jobcontroller) worker(i uint32) {
	klog.Infof("worker %d start ...... ", i)

	// processNextReq 返回 false 时说明队列关闭，worker 退出。
	for cc.processNextReq(i) {
	}
}

// belongsToThisRoutine 判断某个 jobKey 是否应该由当前 worker 处理。
//
// 设计目的：
//   - 同一个 Job 的所有事件都应该落到同一个 worker；
//   - 避免同一个 Job 被多个 worker 并发处理；
//   - 通过 hash(jobKey) % workers 实现固定分配。
func (cc *jobcontroller) belongsToThisRoutine(key string, count uint32) bool {
	val := cc.genHash(key)
	return val%cc.workers == count
}

// getWorkerQueue 根据 jobKey 获取该 Job 应该进入的 worker queue。
func (cc *jobcontroller) getWorkerQueue(key string) workqueue.TypedRateLimitingInterface[any] {
	val := cc.genHash(key)
	queue := cc.queueList[val%cc.workers]
	return queue
}

// genHash 使用 FNV 算法计算字符串 hash。
//
// FNV 是一种简单快速的非加密哈希算法，适合这里用于队列分片。
func (cc *jobcontroller) genHash(key string) uint32 {
	hashVal := fnv.New32()
	hashVal.Write([]byte(key))
	return hashVal.Sum32()
}

/*
processNextReq 从指定 worker 队列中取出一个 Request 并处理

核心流程:
1.从队列中取 Request
2.校验该 Request 是否属于当前 worker
3.清理可能过期的 Pod 延迟动作
4.从 cache 中获取 JobInfo
5.根据 Job 当前状态创建状态机
6.根据策略 applyPolicies 计算需要执行的动作
7.如果动作需要延迟，则注册延迟动作并返回
8.否则立即执行状态机动作
9.执行失败则重试或终止
10.执行成功则 Forget 请求
11.对非内部动作,清理同类型延迟动作
*/
func (cc *jobcontroller) processNextReq(count uint32) bool {
	queue := cc.queueList[count]

	// 从队列中取一个对象
	obj, shutdown := queue.Get()
	if shutdown {
		klog.Errorf("Fail to pop item from queue")
		return false
	}

	// 队列中存放的是 apis.Request
	req := obj.(apis.Request)
	defer queue.Done(req)

	// 根据 Request 获取 Job key，一般为 namespace/name
	key := jobcache.JobKeyByReq(&req)

	// 校验该请求是否属于当前 worker
	// 如果不属于，说明入队时发生了异常，重新放入正确的 worker queue
	if !cc.belongsToThisRoutine(key, count) {
		klog.Errorf("should not occur The job does not belongs to this routine key:%s, worker:%d...... ", key, count)
		queueLocal := cc.getWorkerQueue(key)
		queueLocal.Add(req)
		return true
	}

	klog.V(3).Infof("Try to handle request <%v>", req)

	// 在真正处理当前 Pod 事件之前，先清理可能已经过期的延迟动作
	// 例如 Pod 从 Pending 变成 Running，则之前 Pending 对应的延迟动作应该取消
	cc.CleanPodDelayActionsIfNeed(req)

	// 从 controller cache 中获取完整 JobInfo。
	jobInfo, err := cc.cache.Get(key)
	if err != nil {
		// TODO(k82cn): ignore not-ready error.
		klog.Errorf("Failed to get job by <%v> from cache: %v", req, err)
		return true
	}

	// 根据当前 JobInfo 创建状态机。
	st := state.NewState(jobInfo)
	if st == nil {
		klog.Errorf("Invalid state <%s> of Job <%v/%v>",
			jobInfo.Job.Status.State, jobInfo.Job.Namespace, jobInfo.Job.Name)
		return true
	}

	// 根据 Job 当前配置、事件和策略计算需要执行的动作
	// 返回的 delayAct 中包含 action、delay、event、podName 等信息
	delayAct := applyPolicies(jobInfo.Job, &req)

	// 如果 delay 不为 0，说明动作不是立即执行，而是延迟执行。
	if delayAct.delay != 0 {
		klog.V(3).Infof("Execute <%v> on Job <%s/%s> after %s",
			delayAct.action, req.Namespace, req.JobName, delayAct.delay.String())

		cc.recordJobEvent(jobInfo.Job.Namespace, jobInfo.Job.Name, batchv1alpha1.ExecuteAction, fmt.Sprintf(
			"Execute action %s after %s", delayAct.action, delayAct.delay.String()))

		// 注册延迟动作。
		cc.AddDelayActionForJob(req, delayAct)
		return true
	}

	// delay == 0，立即执行动作。
	klog.V(3).Infof("Execute <%v> on Job <%s/%s> in <%s> by <%T>.",
		delayAct.action, req.Namespace, req.JobName, jobInfo.Job.Status.State.Phase, st)

	// 非 SyncJobAction 的动作记录事件，方便用户排查。
	if delayAct.action != busv1alpha1.SyncJobAction {
		cc.recordJobEvent(jobInfo.Job.Namespace, jobInfo.Job.Name, batchv1alpha1.ExecuteAction, fmt.Sprintf(
			"Start to execute action %s ", delayAct.action))
	}

	// 将 delayAction 转换为状态机 Action。
	action := GetStateAction(delayAct)

	// 执行状态机动作。
	if err := st.Execute(action); err != nil {
		cc.handleJobError(queue, req, st, err, delayAct.action)
		return true
	}

	// 执行成功后，清除该请求的限速重试记录。
	queue.Forget(req)

	// 如果不是内部动作，则清理同类型的延迟动作。
	//
	// 内部动作一般是 controller 自身同步行为，不能随意清理其他延迟动作。
	// 用户动作或状态转换动作执行后，其他同类型延迟动作可能已经过期，需要取消。
	if !isInternalAction(delayAct.action) {
		cc.cleanupDelayActions(delayAct)
	}

	return true
}

// CleanPodDelayActionsIfNeed 用于在 Pod 状态变化时清理过期的延迟动作。
//
// 背景：
//
//	Volcano Job Controller 支持“延迟动作”。
//	例如 Pod Pending、Failed、Evicted 后，可能不会立即重启或终止，
//	而是等待一段时间后再执行动作。
//
// 但 Pod 状态可能在等待期间发生变化：
//   - Pending -> Running；
//   - Failed/Evicted -> Running；
//   - 旧 Pod 删除，新 Pod 使用同名重新创建。
//
// 如果不清理旧的延迟动作，就可能导致：
//   - Pod 已经 Running，但 Pending 延迟动作仍然执行；
//   - Pod 已经恢复，但 Failed/Evicted 延迟动作仍然执行；
//   - 新 Pod 被旧 Pod 的延迟动作误伤。
//
// 清理规则：
//  1. 只处理 Pod 相关事件；
//  2. 如果当前事件不是 PodPendingEvent，则可以尝试清理之前的 PodPending 延迟动作；
//  3. 清理 PodPending 延迟动作时必须校验 UID，防止同名新 Pod 被旧动作影响；
//  4. 如果当前事件是 PodRunningEvent，则清理之前的 PodFailedEvent 或 PodEvictedEvent 延迟动作。
//
// 为什么要过滤 req.Event != PodPendingEvent：
//
//	Pending 事件通常是延迟动作的起点，而不是取消条件。
//	如果 Pod 仍然处于 Pending，就不应该把 Pending 对应的延迟动作取消。
//	只有当 Pod 变成 Running / Failed / Evicted 等非 Pending 状态时，
//	才说明之前 Pending 的延迟动作可能已经失效。
func (cc *jobcontroller) CleanPodDelayActionsIfNeed(req apis.Request) {
	// 非 Pod 生命周期事件不需要清理 Pod 延迟动作。
	if !cc.isPodEvent(req) {
		return
	}

	// 当前事件是 PodPendingEvent 时不清理。
	// 因为 Pending 事件通常会产生一个延迟动作，
	// 如果这里立即清理，就会出现“刚添加就被取消”的问题。
	if req.Event != busv1alpha1.PodPendingEvent {
		key := jobcache.JobKeyByReq(&req)

		// delayActionMap 可能被 worker 和延迟 goroutine 并发访问，必须加锁。
		cc.delayActionMapLock.Lock()
		defer cc.delayActionMapLock.Unlock()

		if taskMap, exists := cc.delayActionMap[key]; exists {
			if delayAct, exists := taskMap[req.PodName]; exists {
				shouldCancel := false

				// 如果之前保存的是 PodPending 延迟动作，
				// 当前事件已经不是 Pending，说明 Pod 状态发生变化。
				if delayAct.event == busv1alpha1.PodPendingEvent {
					// 必须校验 UID。
					//
					// 原因：
					// Pod 名称可能复用。
					// 例如旧 Pod 被删除后，新 Pod 使用相同名称创建。
					// 如果只根据 PodName 取消动作，可能会把新 Pod 的动作和旧 Pod 的动作混淆。
					//
					// 只有当前事件中的 PodUID 与延迟动作中的 podUID 一致，
					// 才说明它们确实是同一个 Pod。
					if req.PodUID == delayAct.podUID {
						shouldCancel = true
					}
				}

				// 如果之前是 Failed 或 Evicted 延迟动作，
				// 但当前 Pod 已经 Running，说明 Pod 已经恢复，
				// 那么失败/驱逐对应的延迟动作就不应该继续执行。
				if (delayAct.event == busv1alpha1.PodFailedEvent || delayAct.event == busv1alpha1.PodEvictedEvent) &&
					req.Event == busv1alpha1.PodRunningEvent {
					shouldCancel = true
				}

				// 满足取消条件，则调用 cancel 并从 map 中删除。
				if shouldCancel {
					klog.V(3).Infof("Cancel delayed action <%v> for pod <%s> because of event <%s> of Job <%s>", delayAct.action, req.PodName, req.Event, delayAct.jobKey)
					delayAct.cancel()
					delete(taskMap, req.PodName)
				}
			}
		}
	}
}

// isPodEvent 判断 Request 是否是 Pod 生命周期相关事件。
//
// 当前只关注：
//   - PodPendingEvent；
//   - PodRunningEvent；
//   - PodFailedEvent；
//   - PodEvictedEvent。
func (cc *jobcontroller) isPodEvent(req apis.Request) bool {
	return req.Event == busv1alpha1.PodPendingEvent ||
		req.Event == busv1alpha1.PodRunningEvent ||
		req.Event == busv1alpha1.PodFailedEvent ||
		req.Event == busv1alpha1.PodEvictedEvent
}

// AddDelayActionForJob 给某个 Job 添加延迟动作。
//
// 核心流程：
//  1. 将 delayAction 保存到 delayActionMap；
//  2. 使用 context.WithTimeout 创建一个定时上下文；
//  3. 启动 goroutine 等待 timeout；
//  4. 如果被 cancel，则直接退出；
//  5. 如果 timeout 到期，则执行延迟动作；
//  6. 执行后清理同类型延迟动作。
func (cc *jobcontroller) AddDelayActionForJob(req apis.Request, delayAct *delayAction) {
	cc.delayActionMapLock.Lock()
	defer cc.delayActionMapLock.Unlock()

	// 获取当前 Job 对应的延迟动作 map。
	m, ok := cc.delayActionMap[delayAct.jobKey]
	if !ok {
		m = make(map[string]*delayAction)
		cc.delayActionMap[delayAct.jobKey] = m
	}

	// 如果同一个 Pod 已经存在相同 action 的延迟动作，则不重复添加。
	if oldDelayAct, exists := m[req.PodName]; exists && oldDelayAct.action == delayAct.action {
		return
	}

	// 保存当前 Pod 的延迟动作。
	m[req.PodName] = delayAct

	// 创建带 timeout 的 context。
	// timeout 到期后，ctx.Done() 会被触发。
	// 如果中途调用 cancel，则 ctx.Err() == context.Canceled。
	ctx, cancel := context.WithTimeout(context.Background(), delayAct.delay)
	delayAct.cancel = cancel

	// 启动 goroutine 等待延迟动作到期。
	go func() {
		<-ctx.Done()

		// 如果是被主动取消，则不执行动作。
		if ctx.Err() == context.Canceled {
			klog.V(4).Infof("Job<%s/%s>'s delayed action %s is canceled", req.Namespace, req.JobName, delayAct.action)
			return
		}

		// 如果不是 canceled，通常就是 timeout 到期，需要执行延迟动作。
		klog.V(4).Infof("Job<%s/%s>'s delayed action %s is expired, execute it", req.Namespace, req.JobName, delayAct.action)

		// 从 cache 获取 JobInfo。
		jobInfo, err := cc.cache.Get(delayAct.jobKey)
		if err != nil {
			klog.Errorf("Failed to get job by <%v> from cache: %v", req, err)
			return
		}

		// 创建状态机。
		st := state.NewState(jobInfo)
		if st == nil {
			klog.Errorf("Invalid state <%s> of Job <%v/%v>",
				jobInfo.Job.Status.State, jobInfo.Job.Namespace, jobInfo.Job.Name)
			return
		}

		// 获取该 Job 对应的 worker queue。
		queue := cc.getWorkerQueue(delayAct.jobKey)

		// 执行延迟动作。
		if err := st.Execute(GetStateAction(delayAct)); err != nil {
			cc.handleJobError(queue, req, st, err, delayAct.action)
		}

		// 清除该请求的重试记录。
		queue.Forget(req)

		// 延迟动作执行完成后，清理同类型延迟动作，避免重复执行。
		cc.cleanupDelayActions(delayAct)
	}()
}

// handleJobError 处理 Job 动作执行失败。
//
// 处理逻辑：
//  1. 如果未达到最大重试次数，则将请求重新加入限速队列；
//  2. 如果达到最大重试次数，则记录事件，并执行 TerminateJobAction 终止 Job；
//  3. 终止后丢弃该请求。
func (cc *jobcontroller) handleJobError(queue workqueue.TypedRateLimitingInterface[any], req apis.Request, st state.State, err error, action busv1alpha1.Action) {
	// maxRequeueNum == -1 表示无限重试。
	if cc.maxRequeueNum == -1 || queue.NumRequeues(req) < cc.maxRequeueNum {
		klog.V(2).Infof("Failed to handle Job <%s/%s>: %v",
			req.Namespace, req.JobName, err)

		// AddRateLimited 会根据 rate limiter 延迟重新入队。
		queue.AddRateLimited(req)
		return
	}

	// 达到最大重试次数，记录事件。
	cc.recordJobEvent(req.Namespace, req.JobName, batchv1alpha1.ExecuteAction,
		fmt.Sprintf("Job failed on action %s for retry limit reached", action))

	klog.Warningf("Terminating Job <%s/%s> and releasing resources", req.Namespace, req.JobName)

	// 执行终止 Job 动作。
	if err = st.Execute(state.Action{Action: busv1alpha1.TerminateJobAction}); err != nil {
		klog.Errorf("Failed to terminate Job<%s/%s>: %v", req.Namespace, req.JobName, err)
	}

	klog.Warningf("Dropping job<%s/%s> out of the queue: %v because max retries has reached",
		req.Namespace, req.JobName, err)
}

// cleanupDelayActions 清理延迟动作。
//
// 调用场景：
//   - 一个延迟动作已经执行完成；
//   - 一个非内部动作已经立即执行完成；
//   - 此时同一个 Job 下同类型延迟动作可能已经没有必要继续存在。
//
// 清理原则：
//  1. 获取当前动作的动作级别：Job / Task / Pod / Partition；
//  2. 遍历当前 Job 下所有 delayAction；
//  3. 只清理同类型动作；
//  4. Task 级别只清理同一个 taskName；
//  5. Pod 级别只清理同一个 podName；
//  6. Partition 级别只清理同一个 partition；
//  7. 调用 cancel 并从 delayActionMap 中删除。
//
// 这样可以避免重复执行同一类延迟动作。
func (cc *jobcontroller) cleanupDelayActions(currentDelayAction *delayAction) {
	cc.delayActionMapLock.Lock()
	defer cc.delayActionMapLock.Unlock()

	// 获取当前动作类型。
	// 例如 JobAction、TaskAction、PodAction、PartitionAction。
	actionType := GetActionType(currentDelayAction.action)

	if m, exists := cc.delayActionMap[currentDelayAction.jobKey]; exists {
		for _, delayAct := range m {
			// 只清理同类型动作。
			if GetActionType(delayAct.action) == actionType {
				// Task 级别动作，只清理同一个 Task 的延迟动作。
				if actionType == TaskAction && delayAct.taskName != currentDelayAction.taskName {
					continue
				}

				// Pod 级别动作，只清理同一个 Pod 的延迟动作。
				if actionType == PodAction && delayAct.podName != currentDelayAction.podName {
					continue
				}

				// Partition 级别动作，只清理同一个 Partition 的延迟动作。
				if actionType == PartitionAction && delayAct.partition != currentDelayAction.partition {
					continue
				}

				// 如果存在 cancel 函数，先取消 goroutine 中的延迟等待。
				if delayAct.cancel != nil {
					klog.V(3).Infof("Cancel delayed action <%v> for pod <%s> because of event <%s> and action <%s> of Job <%s>", delayAct.action, delayAct.podName, currentDelayAction.event, currentDelayAction.action, delayAct.jobKey)
					delayAct.cancel()
				}

				// 从 map 中删除该延迟动作。
				delete(m, delayAct.podName)
			}
		}
	}
}
