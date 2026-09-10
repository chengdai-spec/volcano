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

package tdm

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	tutil "volcano.sh/volcano/pkg/scheduler/plugins/util"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// ==================== 常量定义 ====================

const (
	// PluginName 插件名称，用于在调度器配置中标识 TDM 插件
	PluginName = "tdm"
	// revocableZoneLayout 可撤销区域时间窗口的解析格式，采用 "时:分" 格式
	revocableZoneLayout = "15:04"
	// revocableZoneLabelPrefix 可撤销区域配置键的前缀
	// 配置格式示例: tdm.revocable-zone.rz1: "10:00-21:00"
	revocableZoneLabelPrefix = "tdm.revocable-zone."
	// evictPeriodLabel 驱逐检查周期的配置键
	// 配置格式示例: tdm.evict.period: "1m"
	evictPeriodLabel = "tdm.evict.period"
	// defaultPodEvictNum 每次驱逐周期的默认最大驱逐 Pod 数量
	defaultPodEvictNum = 1
)

// lastEvictAt 记录上一次驱逐操作的时间戳（包级别全局变量）
// 用于控制驱逐频率，确保两次驱逐之间至少间隔 evictPeriod 时间
var lastEvictAt time.Time

// ==================== 插件配置示例（YAML 格式）====================
//
// actions: "enqueue, reclaim, allocate, preempt"
// tiers:
// - plugins:
//   - name: tdm
//     arguments:
//       tdm.revocable-zone.rz1: 10:00-21:00   # 定义名为 rz1 的可撤销区域，时间窗口为 10:00~21:00
//       tdm.revocable-zone.rz2: 12:00-14:00   # 定义名为 rz2 的可撤销区域，时间窗口为 12:00~14:00
//       tdm.evict.period: 1m                   # 驱逐检查周期为 1 分钟
//
// ==================== 核心设计思想 ====================
//
// TDM（Time-Division Multiplexing，时分复用）插件实现了基于时间窗口的资源复用调度策略。
//
// 核心用途：只允许特定任务（如 GPU Pod）在配置的时间窗口内调度到可撤销节点，
// 时间窗口过期后自动驱逐这些任务，将节点资源归还给原始所有者。
//
// 典型场景：集群中存在一批 GPU 节点，在非高峰时段（如 10:00-21:00）允许 GPU Pod
// 使用这些节点进行训练；但到了高峰时段，需要驱逐 GPU Pod，将节点资源归还给在线服务。
// TDM 插件通过时间窗口机制自动完成这一调度管控，无需人工干预。
//
// 工作流程：
// 1. 节点通过标签 "volcano.sh/revocable-zone=<zoneName>" 标记为可撤销节点
// 2. 调度器配置中定义各可撤销区域的时间窗口（如 rz1: 10:00-21:00）
// 3. GPU Pod 通过注解 "volcano.sh/revocable-zone=*" 声明可使用可撤销节点
// 4. predicateFn 确保：只有在时间窗口激活期间，GPU Pod 才能调度到可撤销节点
// 5. victimsFn 确保：时间窗口过期后，自动驱逐可撤销节点上的 GPU Pod
// 6. 非 GPU Pod（未设置 revocable-zone 注解）永远不会被调度到可撤销节点
//
// 两级任务体系：
// - 弹性任务（GPU Pod 等）：Preemptable=true, RevocableZone="*"
//   → 可被驱逐，不能抢占别人，只能在时间窗口内使用可撤销节点
// - 刚性任务（在线服务等）：Preemptable=false, RevocableZone=""
//   → 不可被驱逐，可以抢占弹性任务，永远不会被调度到可撤销节点

// tdmPlugin TDM 插件结构体
type tdmPlugin struct {
	// revocableZone 可撤销区域配置映射
	// key: 区域名称（如 "rz1"），value: 时间窗口字符串（如 "10:00-21:00"）
	revocableZone map[string]string
	// evictPeriod 驱逐检查周期，控制两次驱逐操作之间的最小间隔
	// 默认值为 1 分钟
	evictPeriod time.Duration
}

// New 创建并返回一个 TDM 插件实例
// 参数:
//   - args: 插件配置参数，从调度器配置的 arguments 字段传入
//
// 返回:
//   - framework.Plugin: TDM 插件实例
//
// 步骤:
//  1. 初始化默认的可撤销区域映射和驱逐周期（默认 1 分钟）
//  2. 遍历配置参数，提取以 "tdm.revocable-zone." 为前缀的键值对，构建可撤销区域映射
//  3. 解析 "tdm.evict.period" 配置项，若合法则覆盖默认驱逐周期
//  4. 返回构造完成的 tdmPlugin 实例
func New(args framework.Arguments) framework.Plugin {
	// 步骤 1: 初始化默认值
	revocableZone := make(map[string]string)
	evictPeriod := time.Minute

	// 步骤 2: 解析可撤销区域配置
	// 遍历所有参数，筛选出以 "tdm.revocable-zone." 为前缀的配置项
	// 例如: "tdm.revocable-zone.rz1" -> key="rz1", value="10:00-21:00"
	for k, v := range args {
		if strings.Contains(k, revocableZoneLabelPrefix) {
			revocableZone[strings.Replace(k, revocableZoneLabelPrefix, "", 1)] = v.(string)
		}
	}

	// 步骤 3: 解析驱逐周期配置
	if period, ok := args[evictPeriodLabel]; ok {
		if d, err := time.ParseDuration(period.(string)); err == nil {
			evictPeriod = d
		}
	}

	// 步骤 4: 返回插件实例
	return &tdmPlugin{revocableZone, evictPeriod}
}

// Name 返回插件名称，用于在 Session 中注册和标识插件
func (tp *tdmPlugin) Name() string {
	return PluginName
}

// parseRevocableZone 解析可撤销区域的时间窗口字符串，返回当天的起止时间
// 参数:
//   - rzRaw: 时间窗口字符串，格式为 "HH:MM-HH:MM"（如 "10:00-21:00"）
//
// 返回:
//   - start: 时间窗口的开始时间（精确到当天）
//   - end: 时间窗口的结束时间（若跨夜则自动加一天）
//   - err: 解析错误信息
//
// 步骤:
//  1. 以 "-" 分割时间窗口字符串，校验必须恰好包含两个值
//  2. 分别解析起始和结束时间为 time.Time 对象
//  3. 将解析出的时分组合为当天的具体时间
//  4. 处理跨夜场景：若开始时间 >= 结束时间（如 "22:00-06:00"），结束时间自动加一天
func parseRevocableZone(rzRaw string) (start, end time.Time, err error) {
	// 步骤 1: 分割时间窗口字符串
	rzValues := strings.Split(strings.TrimSpace(rzRaw), "-")

	if len(rzValues) != 2 {
		err = fmt.Errorf("revocable zone %v format error", rzRaw)
		return
	}

	// 步骤 2: 解析起始时间
	t1, err := time.Parse(revocableZoneLayout, rzValues[0])
	if err != nil {
		return
	}

	// 步骤 3: 解析结束时间
	t2, err := time.Parse(revocableZoneLayout, rzValues[1])
	if err != nil {
		return
	}

	// 步骤 4: 组合为当天的具体时间，并处理跨夜场景
	now := time.Now()

	// 将起始时间设置为今天的 t1 时刻
	start = time.Date(now.Year(), now.Month(), now.Day(), t1.Hour(), t1.Minute(), 0, 0, now.Location())
	// 跨夜判断：若开始时间在结束时间之后或相等（如 "22:00-06:00" 或 "10:00-10:00"），结束时间设为明天
	if t1.After(t2) || t1.Equal(t2) {
		end = time.Date(now.Year(), now.Month(), now.Day()+1, t2.Hour(), t2.Minute(), 0, 0, now.Location())
	} else {
		end = time.Date(now.Year(), now.Month(), now.Day(), t2.Hour(), t2.Minute(), 0, 0, now.Location())
	}

	return
}

// availableRevocableZone 判断指定的可撤销区域当前是否处于激活状态
// 参数:
//   - rz: 可撤销区域名称（如 "rz1"）
//
// 返回:
//   - error: 若区域不存在或当前时间不在时间窗口内，返回错误；否则返回 nil
//
// 步骤:
//  1. 在插件的可撤销区域映射中查找指定区域，不存在则返回错误
//  2. 解析该区域的时间窗口，获取起止时间
//  3. 判断当前时间是否在 [start, end] 时间窗口内
func (tp *tdmPlugin) availableRevocableZone(rz string) error {
	// 步骤 1: 查找可撤销区域配置
	// rzRaw 格式示例: "10:00-21:00"
	rzRaw, ok := tp.revocableZone[rz]
	if !ok {
		return fmt.Errorf("revocable zone %v not support", rz)
	}

	// 步骤 2: 解析时间窗口
	now := time.Now()

	start, end, err := parseRevocableZone(rzRaw)
	if err != nil {
		return err
	}

	// 步骤 3: 判断当前时间是否在激活窗口内
	if now.Unix() < start.Unix() || now.Unix() > end.Unix() {
		return fmt.Errorf("current time beyond revocable zone %v:%v", rz, rzRaw)
	}

	return nil
}

// OnSessionOpen 在调度会话开启时调用，注册 TDM 插件的各个扩展点函数
// 参数:
//   - ssn: 当前调度会话对象，包含所有节点、任务、队列等信息
//
// 步骤:
//  1. 注册 predicateFn（谓词过滤）：判断任务是否可以调度到指定节点
//     - 非可撤销节点：直接通过
//     - 可撤销节点：检查区域是否激活且任务允许使用该区域
//  2. 注册 nodeOrderFn（节点打分）：为可撤销节点上的合法任务提供最高分
//  3. 注册 preemptableFn（抢占候选者筛选）：筛选出可以被抢占的任务
//     - 抢占方若是可抢占任务或可使用可撤销区域，则不允许抢占
//     - 仅选择非可撤销节点上的可抢占运行中任务作为候选者
//  4. 注册 victimsFn（驱逐任务选择）：从过期的可撤销区域中驱逐任务
//  5. 注册 jobOrderFn（作业排序）：优先调度不可抢占的作业
//  6. 注册 jobPipelinedFn（流水线状态检查）：判断作业是否处于流水线状态
//  7. 注册 jobStarvingFn（饥饿检测）：检测是否有待调度的不可抢占任务
func (tp *tdmPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter tdm plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving tdm plugin.")
	}()

	// ==================== 1. PredicateFn - 谓词过滤函数 ====================
	// 功能：判断任务是否可以调度到指定节点
	// 核心逻辑：只允许 GPU Pod（设置了 revocable-zone 注解）在时间窗口内调度到可撤销节点
	// 返回 nil 表示可以通过，返回 error 表示不能调度
	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		tdmStatus := &api.Status{
			Plugin: PluginName,
		}
		predicateStatus := []*api.Status{tdmStatus}
		// 情况 1: 节点不是可撤销节点，直接放行（普通节点不受 TDM 管控）
		if node.RevocableZone == "" {
			return nil
		}

		// 情况 2: 节点是可撤销节点，但当前不在时间窗口内，拒绝调度
		// 例如：GPU 节点在高峰时段（如 21:00 后）不再接受新的 GPU Pod
		if err := tp.availableRevocableZone(node.RevocableZone); err != nil {
			tdmStatus.Code = api.UnschedulableAndUnresolvable
			tdmStatus.Reason = err.Error()
			return api.NewFitErrWithStatus(task, node, predicateStatus...)
		}

		klog.V(4).Infof("TDM node %v revocable zone %v:%v is active", node.Name, node.RevocableZone, tp.revocableZone[node.RevocableZone])

		// 情况 3: 节点是可撤销节点且在时间窗口内，但任务未声明可使用可撤销区域，拒绝调度
		// 只有设置了 "volcano.sh/revocable-zone=*" 的 GPU Pod 才能调度到可撤销节点
		// 普通 Pod（未设置该注解）即使节点有空闲资源也不能调度上去
		if len(task.RevocableZone) == 0 {
			tdmStatus.Code = api.UnschedulableAndUnresolvable
			tdmStatus.Reason = "not allow to dispatch to revocable node"
			return api.NewFitErrWithStatus(task, node, predicateStatus...)
		}

		klog.V(4).Infof("TDM filter for Task %s/%s on node %s pass.", task.Namespace, task.Name, node.Name)
		return nil
	}

	// ==================== 2. NodeOrderFn - 节点打分函数 ====================
	// 功能：为节点计算分数，影响任务调度优先级
	// 核心逻辑：GPU Pod 在时间窗口内调度到可撤销节点时给予最高分，优先选择这些节点
	// 返回值范围：0 ~ MaxNodeScore（通常为 100）
	nodeOrderFn := func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		score := 0.0

		// 情况 1: 节点不是可撤销节点，返回 0 分（不特别偏好普通节点）
		if node.RevocableZone == "" {
			return score, nil
		}

		// 情况 2: 节点是可撤销节点但不在时间窗口内，返回 0 分并记录错误
		if err := tp.availableRevocableZone(node.RevocableZone); err != nil {
			klog.V(4).Infof("TDM not available %s", err)
			return score, err
		}

		// 情况 3: 节点是可撤销节点且在时间窗口内，但任务不是 GPU Pod，返回 0 分
		if len(task.RevocableZone) == 0 {
			klog.V(4).Infof("TDM task %s/%s is not allow to dispatch to revocable node %s", task.Namespace, task.Name, node.Name)
			return score, nil
		}

		// 情况 4: GPU Pod + 可撤销节点 + 时间窗口激活 → 返回最高分，优先调度
		score = float64(fwk.MaxNodeScore)

		klog.V(4).Infof("TDM score for Task %s/%s on node %s is: %v", task.Namespace, task.Name, node.Name, score)
		return score, nil
	}

	// ==================== 3. PreemptableFn - 抢占候选者筛选函数 ====================
	// 功能：当刚性任务（如在线服务）需要抢占资源时，从候选任务列表中筛选出可以被驱逐的 GPU Pod
	// 核心逻辑：
	//   - GPU Pod（弹性任务）不能抢占别人，只能被别人抢占
	//   - 只能抢占非可撤销节点上的弹性任务（可撤销节点上的由 victimsFn 处理）
	// 返回：
	//   - []*api.TaskInfo: 被选中的受害者任务列表
	//   - int: tutil.Permit（允许抢占）或 tutil.Reject（拒绝抢占）
	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		// 规则：若抢占方本身是 GPU Pod（弹性任务）或可使用可撤销区域，则不允许它抢占其他任务
		// GPU Pod 应该使用可撤销节点的资源，而不是抢占在线服务的资源
		if preemptor.Preemptable || len(preemptor.RevocableZone) > 0 {
			klog.V(4).Infof("TDM task %s/%s is preemptable, do nothing skip", preemptor.Namespace, preemptor.Name)
			return nil, tutil.Reject
		}

		var victims []*api.TaskInfo
		tasksMap := make(map[api.JobID][]*api.TaskInfo)

		// 步骤 1: 遍历候选任务列表，筛选出符合条件的可抢占 GPU Pod
		// 条件：
		//   a. 任务标记为可抢占（Preemptable=true，即 GPU Pod）
		//   b. 任务处于 Running 状态
		//   c. 任务所在节点不是可撤销节点（可撤销节点上的 GPU Pod 由 victimsFn 处理）
		for _, task := range preemptees {
			if !task.Preemptable || task.Status != api.Running {
				continue
			}

			node, ok := ssn.Nodes[task.NodeName]
			if !ok {
				continue
			}

			// 跳过位于可撤销节点上的 GPU Pod，这些任务应该由 victimsFn 按时间窗口处理
			if node.RevocableZone != "" {
				continue
			}

			tasksMap[task.Job] = append(tasksMap[task.Job], task)
		}

		// 步骤 2: 对每个作业的候选任务应用最大驱逐数限制
		for jobID, preemptableTasks := range tasksMap {
			if job, ok := ssn.Jobs[jobID]; ok {
				victims = append(victims, tp.maxVictims(job, preemptableTasks)...)
			}
		}

		klog.V(4).Infof("TDM victims are %+v", victims)

		return victims, tutil.Permit
	}

	// ==================== 4. VictimsFn - 驱逐任务选择函数 ====================
	// 功能：定期扫描已过期的可撤销区域，从中选出需要驱逐的 GPU Pod
	// 核心逻辑：当可撤销区域不在时间窗口内（如高峰时段到来），自动驱逐该区域节点上的 GPU Pod
	// 触发时机：在每个调度周期中被调用（如 preempt action）
	victimsFn := func([]*api.TaskInfo) []*api.TaskInfo {
		// 步骤 1: 检查距离上次驱逐的时间是否超过配置的周期
		// 目的：避免过于频繁的驱逐操作，保护 GPU 训练任务的稳定性
		if lastEvictAt.Add(tp.evictPeriod).After(time.Now()) {
			klog.V(4).Infof("TDM next evict time at %v", lastEvictAt)
			return nil
		}

		klog.V(4).Infof("TDM start to find victims")

		// 步骤 2: 遍历所有配置的可撤销区域，找出已过期的区域
		// 例如：rz1 配置为 10:00-21:00，当前时间 21:30，则 rz1 已过期，需要驱逐其上的 GPU Pod
		victims := make([]*api.TaskInfo, 0)
		for rz := range tp.revocableZone {
			// 若区域不在时间窗口内（已过期），则需要驱逐该区域中的 GPU Pod
			if err := tp.availableRevocableZone(rz); err != nil {
				klog.V(4).Infof("TDM revocable zone %v disactive, %v", rz, err)
				// 从该区域的节点中收集所有可抢占的运行中 GPU Pod
				for jobID, preemtableTasks := range tp.revocableNodePreemptableTask(rz, ssn) {
					if job, ok := ssn.Jobs[jobID]; ok {
						victims = append(victims, tp.maxVictims(job, preemtableTasks)...)
					}
				}
			}
		}

		// 步骤 3: 更新上次驱逐时间戳
		// 注意：这里没有并发控制，但在单线程调度器中是安全的
		lastEvictAt = time.Now()

		klog.V(4).Infof("TDM got %v victims", len(victims))

		return victims
	}

	// ==================== 5. JobOrderFn - 作业排序函数 ====================
	// 功能：在调度队列中对作业进行排序，决定调度优先级
	// 核心逻辑：刚性任务（在线服务等）优先于弹性任务（GPU Pod）调度
	// 返回值：
	//   - -1: l 排在 r 前面（l 优先级更高）
	//   - 0: l 和 r 优先级相同
	//   - 1: l 排在 r 后面（r 优先级更高）
	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		// 若两个作业的可抢占性相同，则保持原有顺序
		if lv.Preemptable == rv.Preemptable {
			return 0
		}

		// 不可抢占的刚性作业（在线服务等）优先级更高，排在前面
		// 确保在线服务优先获得资源，GPU Pod 等弹性任务排队等待
		if !lv.Preemptable {
			return -1
		}

		return 1
	}

	// ==================== 6. JobPipelinedFn - 流水线状态检查函数 ====================
	// 功能：判断作业是否可以进入流水线状态（pipelined）
	// 流水线状态：表示作业的部分任务已分配资源，但尚未全部满足 MinAvailable
	jobPipelinedFn := func(obj interface{}) int {
		jobInfo := obj.(*api.JobInfo)
		if jobInfo.IsPipelined() {
			return tutil.Permit
		}
		return tutil.Reject
	}

	// ==================== 7. JobStarvingFn - 饥饿检测函数 ====================
	// 功能：判断作业是否处于"饥饿"状态（有待调度的任务且无法立即满足）
	// 核心逻辑：GPU Pod（弹性任务）不触发抢占，只有刚性任务（在线服务等）才允许抢占
	// 返回值 true 表示作业饥饿，可能触发抢占逻辑
	jobStarvingFn := func(obj interface{}) bool {
		jobInfo := obj.(*api.JobInfo)
		// GPU Pod（Preemptable=true）不判定为饥饿，不允许它去抢占别人
		// 这是 TDM 的核心设计：弹性任务可以被驱逐，但不能反过来抢占刚性任务
		if jobInfo.Preemptable {
			return false
		}
		// 刚性任务（在线服务等）有 Pending 任务则判定为饥饿，允许它去抢占 GPU Pod 的资源
		return len(jobInfo.TaskStatusIndex[api.Pending]) > 0
	}

	// ==================== 注册所有扩展点函数到 Session ====================
	victimsFns := make([]api.VictimTasksFn, 0)
	victimsFns = append(victimsFns, victimsFn)
	ssn.AddPredicateFn(tp.Name(), predicateFn)
	ssn.AddNodeOrderFn(tp.Name(), nodeOrderFn)
	ssn.AddPreemptableFn(tp.Name(), preemptableFn)
	ssn.AddVictimTasksFns(tp.Name(), victimsFns)
	ssn.AddJobOrderFn(tp.Name(), jobOrderFn)
	ssn.AddJobPipelinedFn(tp.Name(), jobPipelinedFn)
	ssn.AddJobStarvingFns(tp.Name(), jobStarvingFn)
}

// maxVictims 根据作业的中断预算限制，计算最多可以驱逐多少个任务
// 参数:
//   - job: 作业信息对象，包含中断预算配置
//   - victims: 潜在的受害者任务列表
//
// 返回:
//   - []*api.TaskInfo: 实际可以驱逐的任务列表（可能被截断）
//
// 步骤:
//  1. 调用 getMaxPodEvictNum 获取该作业允许的最大驱逐数量
//  2. 取最大驱逐数和潜在受害者数量的最小值，确保不超过预算限制
//  3. 返回截断后的受害者列表
func (tp *tdmPlugin) maxVictims(job *api.JobInfo, victims []*api.TaskInfo) []*api.TaskInfo {
	maxPodEvictNum := tp.getMaxPodEvictNum(job)
	targetNum := util.GetMinInt(maxPodEvictNum, len(victims))
	klog.V(3).Infof("Job <%s/%s> max evict:%v, potential victims number:%v, max victims number:%v",
		job.Namespace, job.Name, maxPodEvictNum, len(victims), targetNum)

	return victims[:targetNum]
}

// getMaxPodEvictNum 根据作业的中断预算配置，计算最多可以驱逐多少个 Pod
// 支持两种预算配置方式：
//  1. MaxUnavailable（最大不可用数）：保证至少有 (总任务数 - MaxUnavailable) 个任务可用
//  2. MinAvailable（最小可用数）：保证至少有 MinAvailable 个任务可用
//
// 步骤:
//  1. 统计当前处于 Running 状态的任务数量
//  2. 若配置了 MaxUnavailable，计算当前已不可用的任务数，剩余配额即为可驱逐数
//  3. 若配置了 MinAvailable，计算当前可用任务超出最小需求的数量，超出的部分可驱逐
//  4. 若均未配置，返回默认值 defaultPodEvictNum（通常为 1）
func (tp *tdmPlugin) getMaxPodEvictNum(job *api.JobInfo) int {
	// 步骤 1: 统计正在运行的任务数量
	jobRunningTaskNum := len(job.TaskStatusIndex[api.Running])

	// 步骤 2: 优先使用 MaxUnavailable 配置
	if job.Budget.MaxUnavailable != "" {
		// 解析 MaxUnavailable 值（支持整数或百分比）
		maxUnavailable := tp.parseIntStr(job.Budget.MaxUnavailable, len(job.Tasks))
		// 计算已完成（成功或失败）的任务数量
		finalTaskNum := len(job.TaskStatusIndex[api.Succeeded]) + len(job.TaskStatusIndex[api.Failed])
		// 计算当前实际不可用的任务数 = 总任务数 - 已完成任务数 - 正在运行任务数
		realUnavailable := len(job.Tasks) - finalTaskNum - jobRunningTaskNum
		// 若当前不可用数已达到或超过上限，不允许再驱逐
		if realUnavailable >= maxUnavailable {
			return 0
		}
		// 返回剩余的不可用配额
		return maxUnavailable - realUnavailable
	}

	// 步骤 3: 使用 MinAvailable 配置
	if job.Budget.MinAvailable != "" {
		// 解析 MinAvailable 值（支持整数或百分比）
		minAvailable := tp.parseIntStr(job.Budget.MinAvailable, len(job.Tasks))
		// 若当前运行任务数超过最小需求，超出的部分可以驱逐
		if jobRunningTaskNum >= minAvailable {
			return jobRunningTaskNum - minAvailable
		}
	}

	// 步骤 4: 返回默认驱逐数
	return defaultPodEvictNum
}

// parseIntStr 解析整数字符串或百分比字符串为具体的整数值
// 参数:
//   - input: 输入字符串，可以是整数（如 "5"）或百分比（如 "20%"）
//   - taskNum: 任务总数，用于将百分比转换为具体数值
//
// 步骤:
//  1. 使用 k8s.io/apimachinery/pkg/util/intstr 解析输入字符串
//  2. 若为整数类型，直接返回整数值
//  3. 若为百分比类型，基于 taskNum 计算实际数值
func (tp *tdmPlugin) parseIntStr(input string, taskNum int) int {
	resultValue := 0
	tmp := intstr.Parse(input)
	switch tmp.Type {
	case intstr.Int:
		// 情况 1: 整数类型，直接取值
		resultValue = tmp.IntValue()
	case intstr.String:
		// 情况 2: 百分比类型，计算实际值
		if v, err := intstr.GetValueFromIntOrPercent(&tmp, taskNum, true); err == nil {
			resultValue = v
		} else {
			klog.Warningf("TDM get percent value err: %v", err)
		}
	}

	return resultValue
}

// revocableNodePreemptableTask 收集指定可撤销区域中所有可抢占的运行中任务
// 参数:
//   - rz: 可撤销区域名称
//   - ssn: 调度会话对象
//
// 返回:
//   - map[api.JobID][]*api.TaskInfo: 按作业 ID 分组的可抢占任务映射
//
// 步骤:
//  1. 遍历 Session 中的所有可撤销节点，筛选出属于指定区域的节点
//  2. 在这些节点上查找所有可抢占且处于 Running 状态的任务
//  3. 按作业 ID 分组后返回
func (tp *tdmPlugin) revocableNodePreemptableTask(rz string, ssn *framework.Session) map[api.JobID][]*api.TaskInfo {
	tasksMap := make(map[api.JobID][]*api.TaskInfo)
	// 步骤 1: 遍历可撤销节点，筛选指定区域的节点
	for _, node := range ssn.RevocableNodes {
		if node.RevocableZone != rz {
			continue
		}

		// 步骤 2-3: 收集该节点上的可抢占运行中任务，按作业分组
		for _, task := range node.Tasks {
			if task.Preemptable {
				if task.Status == api.Running {
					tasksMap[task.Job] = append(tasksMap[task.Job], task)
				}
			}
		}
	}

	return tasksMap
}

// OnSessionClose 在调度会话关闭时调用，用于清理资源或执行收尾操作
// 当前 TDM 插件不需要在会话关闭时执行任何操作，因此为空实现
func (tp *tdmPlugin) OnSessionClose(ssn *framework.Session) {}
