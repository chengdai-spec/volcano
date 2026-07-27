# 调度器插件扩展点

## 背景

Volcano 调度器采用 **Action + Plugin** 架构。**Action** 定义调度操作的骨架流程（如 `enqueue`、`allocate`、`preempt`），而 **Plugin** 通过向 `Session` 注册回调函数（Fn）来注入具体的调度策略。每个调度周期会打开一个新的 Session；插件通过 `OnSessionOpen` 注册回调函数，Action 在相应阶段调用这些函数。

本文档全面列出了 `pkg/scheduler/api/types.go` 中定义的所有扩展点函数类型，包括它们的签名、用途以及被哪些 Action 调用。

## 目标

- 提供一个统一的参考文档，帮助理解插件回调函数与调度 Action 之间的关系
- 帮助插件开发者了解实现不同调度能力需要实现哪些函数
- 帮助 Action 开发者了解每个阶段可以使用哪些钩子

## 非目标

- 每个插件或 Action 的详细实现说明
- 插件开发的逐步教程（参见 `custom-plugin.md`）

---

## 架构概览

Volcano 调度器的调度周期按顺序执行多个 Action，每个 Action 通过 Session 调用插件注册的回调函数：

- **enqueue** → **allocate** → **preempt** → **reclaim** → **gangpreempt** → **gangreclaim** → **backfill** → **shuffle**

插件通过 `ssn.AddXxxFn(name, fn)` 在 `OnSessionOpen` 阶段注册回调函数，Session 内部维护各类型函数的映射表。Action 执行时按 Tier → Plugin 顺序遍历，调用已注册的函数。

---
E:\goWorks\github.com\volcano-sh\volcano\docs\design\schedule-plugin-action.md
// ... existing code ...
## Action 说明

### 1. enqueue（入队动作）

将作业从 `Pending` 状态转为 `Inqueue` 状态。这是作业进入调度管道的入口。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序，决定哪个队列先处理 |
| `CompareFn` | `JobOrderFn` | 作业排序，决定哪个作业先入队 |
| `VoteFn` | `JobEnqueueable` | 判断作业是否满足入队条件（如资源配额是否够） |
| `JobEnqueuedFn` | `JobEnqueued` | 作业入队后的通知回调（如 DRF 插件更新份额） |

### 2. allocate（分配动作）

核心资源分配动作。通过多级优先队列层次结构将任务分配到节点：队列 → 作业 → 子作业 → 任务 → 节点过滤 → 节点打分 → 绑定。支持通过 HyperNode 梯度搜索和干跑优化进行拓扑感知调度。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序 |
| `CompareFn` | `JobOrderFn` | 作业排序 |
| `CompareFn` | `TaskOrderFn` | 任务排序 |
| `CompareFn` | `SubJobOrderFn` | 子作业排序 |
| `ValidateExFn` | `JobValid` | 作业合法性校验 |
| `ValidateFn` | `Overused` | 判断队列是否超用资源 |
| `ValidateFn` | `SubJobReady` | 判断子作业是否已就绪 |
| `ValidateFn` | `JobReady` | 判断作业是否已就绪（minAvailable 满足） |
| `VoteFn` | `JobPipelined` | 判断作业是否流水线化（资源足够运行） |
| `PrePredicateFn` | `PrePredicateFn` | 预过滤（如 topology-aware 预处理） |
| `PredicateFn` | `Predicate` / `PredicateForAllocateAction` | 节点过滤，判断 task 能否跑在 node 上 |
| `NodeOrderFn` | `NodeOrderFn` | 节点打分（单个节点） |
| `BatchNodeOrderFn` | `BatchNodeOrderFn` | 批量节点打分 |
| `NodeMapFn` | `NodeOrderMapFn` | MapReduce 模式打分的 Map 阶段 |
| `NodeReduceFn` | `NodeOrderReduceFn` | MapReduce 模式打分的 Reduce 阶段 |
| `HyperNodeOrderFn` | `HyperNodeOrderFn` | HyperNode 打分 |
| `HyperNodeOrderMapFn` | `HyperNodeOrderMapFn` | HyperNode MapReduce 打分 |
| `BestNodeFn` | `BestNodeFn` | 从打分结果中选择最优节点 |
| `AllocatableFn` | `Allocatable` | 判断队列是否还有可分配配额 |
| `HyperNodeGradientForJobFn` | `HyperNodeGradientForJobFn` | 作业级 HyperNode 梯度搜索 |
| `HyperNodeGradientForSubJobFn` | `HyperNodeGradientForSubJobFn` | 子作业级 HyperNode 梯度搜索 |

### 3. preempt（抢占动作）

在同一队列内抢占低优先级任务，为高优先级饥饿任务腾出空间。支持普通抢占和拓扑感知抢占（使用模拟函数）。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序 |
| `CompareFn` | `JobOrderFn` | 作业排序（选出抢占者） |
| `CompareFn` | `TaskOrderFn` | 任务排序 |
| `ValidateExFn` | `JobValid` | 作业合法性校验 |
| `ValidateFn` | `JobStarving` | 判断作业是否"饥饿"（还需要资源） |
| `VoteFn` | `JobPipelined` | 抢占成功后判断是否流水线化以提交 |
| `PrePredicateFn` | `PrePredicateFn` | 预过滤 |
| `PredicateFn` | `PredicateFn` / `PredicateForPreemptAction` | 节点过滤 |
| `EvictableFn` | `Preemptable` | 判断哪些任务可以被抢占（选出受害者） |
| `NodeOrderFn` | `NodeOrderFn` | 节点打分（选择最优抢占节点） |
| `BatchNodeOrderFn` | `BatchNodeOrderFn` | 批量节点打分 |
| `NodeMapFn` | `NodeOrderMapFn` | 打分 Map 阶段 |
| `NodeReduceFn` | `NodeOrderReduceFn` | 打分 Reduce 阶段 |
| `AllocatableFn` | `Allocatable` | 判断队列是否可分配 |
| `VictimCompareFn` | `VictimQueueOrderFn` | 受害者排序（跨队列时） |
| `SimulateRemoveTaskFn` | `SimulateRemoveTaskFn` | 拓扑感知抢占中模拟移除任务 |
| `SimulateAddTaskFn` | `SimulateAddTaskFn` | 拓扑感知抢占中模拟添加任务 |
| `SimulatePredicateFn` | `SimulatePredicateFn` | 拓扑感知抢占中模拟谓词检查 |
| `SimulateAllocatableFn` | `SimulateAllocatableFn` | 拓扑感知抢占中模拟配额检查 |

### 4. reclaim（回收动作）

从超配队列中回收资源，为欠配队列中的饥饿作业提供资源。跨队列资源再平衡。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序 |
| `CompareFn` | `JobOrderFn` | 作业排序 |
| `CompareFn` | `TaskOrderFn` | 任务排序 |
| `ValidateExFn` | `JobValid` | 作业合法性校验 |
| `ValidateFn` | `Overused` | 判断队列是否超用 |
| `ValidateFn` | `JobStarving` | 判断作业是否饥饿 |
| `VoteFn` | `JobPipelined` | 回收成功后判断是否流水线化 |
| `ValidateWithCandidateFn` | `Preemptive` | 判断队列是否有权回收（reclaim 资格检查） |
| `PrePredicateFn` | `PrePredicateFn` | 预过滤 |
| `PredicateFn` | `PredicateForPreemptAction` | 节点过滤 |
| `EvictableFn` | `Reclaimable` | 判断哪些任务可以被回收 |
| `VictimCompareFn` | `VictimQueueOrderFn` | 受害者排序 |

### 5. gangpreempt（组抢占动作）

作业级别的 Gang 感知抢占。驱除整个作业组（或选定的任务束）为更高优先级的饥饿作业腾出空间，同时考虑拓扑约束。为子作业设置 `NominatedHyperNode` 以实现快速路径分配。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序 |
| `CompareFn` | `JobOrderFn` | 作业排序 |
| `ValidateExFn` | `JobValid` | 作业合法性校验 |
| `ValidateFn` | `JobStarving` | 判断作业是否饥饿 |
| `VoteFn` | `JobPipelined` | 抢占成功后判断是否提交 |
| `UnifiedEvictableFn` | `UnifiedEvictable` | 组级别的受害者过滤（gang-aware） |
| `PredicateFn` | `PredicateForAllocateAction` | 提名计划中的节点验证 |
| `HyperNodeGradientForJobFn` | 间接通过 `GetCandidateDomains` | 获取候选域 |

### 6. gangreclaim（组回收动作）

跨队列的 Gang 感知回收。与 gangpreempt 类似，但针对其他队列中超过应有份额的作业。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序 |
| `CompareFn` | `JobOrderFn` | 作业排序 |
| `ValidateExFn` | `JobValid` | 作业合法性校验 |
| `ValidateFn` | `Overused` | 判断队列是否超用 |
| `ValidateFn` | `JobStarving` | 判断作业是否饥饿 |
| `VoteFn` | `JobPipelined` | 回收成功后判断是否提交 |
| `ValidateWithCandidateFn` | `Preemptive` | 回收资格检查 |
| `UnifiedEvictableFn` | `UnifiedEvictable` | 组级别的受害者过滤 |
| `VictimCompareFn` | `VictimQueueOrderFn` | 受害者队列排序 |

### 7. backfill（回填动作）

用 BestEffort 任务填充空闲资源。使用简化的调度路径，不进行队列配额检查。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `CompareFn` | `QueueOrderFn` | 队列排序 |
| `CompareFn` | `JobOrderFn` | 作业排序 |
| `CompareFn` | `TaskOrderFn` | 任务排序 |
| `ValidateExFn` | `JobValid` | 作业合法性校验 |
| `PrePredicateFn` | `PrePredicateFn` | 预过滤 |
| `PredicateFn` | `PredicateForAllocateAction` | 节点过滤 |
| `BatchNodeOrderFn` | `BatchNodeOrderFn` | 批量节点打分 |
| `NodeOrderMapFn` | `NodeOrderMapFn` | 打分 Map |
| `NodeOrderReduceFn` | `NodeOrderReduceFn` | 打分 Reduce |
| `BestNodeFn` | `BestNodeFn` | 选择最优节点 |

### 8. shuffle（打乱动作）

根据插件定义的策略选择并驱除运行中的任务（如为负载均衡进行重调度）。

| 调用的 Fn | Session 方法 | 用途 |
|---|---|---|
| `VictimTasksFn` | `VictimTasks` | 选择要驱逐的任务（如重调度场景） |

---
## 扩展点函数类型

### 一、排序函数

排序函数决定所有 Action 中队列、作业、任务和子作业的优先级顺序。

#### `LessFn`

    type LessFn func(interface{}, interface{}) bool

通用的比较函数，用于排序或优先队列。内部由 `util.PriorityQueue` 使用。

#### `CompareFn`

    type CompareFn func(interface{}, interface{}) int

通用三值比较函数，返回负数、零或正数。插件通过以下方式注册：

| Session 注册方法 | 调用方法 | 用途 |
|---|---|---|
| `AddJobOrderFn` | `JobOrderFn(l, r)` | 比较两个作业的优先级 |
| `AddQueueOrderFn` | `QueueOrderFn(l, r)` | 比较两个队列的优先级 |
| `AddTaskOrderFn` | `TaskOrderFn(l, r)` | 比较两个任务的优先级 |
| `AddSubJobOrderFn` | `SubJobOrderFn(l, r)` | 比较两个子作业的优先级 |
| `AddClusterOrderFn` | `ClusterOrderFn(l, r)` | 比较两个集群的优先级 |

#### `VictimCompareFn`

    type VictimCompareFn func(interface{}, interface{}, interface{}) int

三参数比较函数，用于受害者优先级排序，第三个参数是抢占者上下文。通过 `AddVictimQueueOrderFn` 注册，由 `VictimQueueOrderFn(l, r, preemptor)` 调用。

当受害者分布在多个队列中时使用——受害者队列的排序可能取决于抢占者所在队列的策略。

---

### 二、验证函数

验证函数检查对象是否满足特定条件。

#### `ValidateFn`

    type ValidateFn func(interface{}) bool

如果对象通过验证则返回 `true`。通过以下方式注册：

| Session 注册方法 | 调用方法 | 用途 |
|---|---|---|
| `AddJobReadyFn` | `JobReady(obj)` | 检查作业是否有足够的就绪任务（minAvailable 是否满足） |
| `AddOverusedFn` | `Overused(queue)` | 检查队列是否已超出资源分配 |
| `AddJobStarvingFns` | `JobStarving(obj)` | 检查作业是否仍需要更多资源（"饥饿"状态） |
| `AddSubJobReadyFn` | `SubJobReady(job, subJob)` | 检查子作业是否有足够的就绪任务 |

#### `ValidateWithCandidateFn`

    type ValidateWithCandidateFn func(interface{}, []*TaskInfo) bool

与 `ValidateFn` 类似，但将候选任务考虑在内。通过 `AddPreemptiveFn` 注册，由 `Preemptive(queue, candidates)` 调用。

判断队列是否有权从其他队列回收资源，同时考虑将要调度的候选任务。

#### `ValidateResult` 与 `ValidateExFn`

    type ValidateResult struct {
        Pass    bool
        Reason  string
        Message string
    }

    type ValidateExFn func(interface{}) *ValidateResult

扩展验证函数，返回带有原因的结构化结果。通过 `AddJobValidFn` 注册，由 `JobValid(obj)` 调用。

用于在调度前验证作业合法性（如检查队列配额、作业状态一致性等）。

---

### 三、投票函数

投票函数实现"投票"机制，插件可以投票同意/弃权/反对。

#### `VoteFn`

    type VoteFn func(interface{}) int

返回负数（反对）、零（弃权）或正数（同意）。通过以下方式注册：

| Session 注册方法 | 调用方法 | 用途 |
|---|---|---|
| `AddJobPipelinedFn` | `JobPipelined(obj)` | 投票决定作业是否有足够资源进行流水线化 |
| `AddJobEnqueueableFn` | `JobEnqueueable(obj)` | 投票决定作业是否可以入队 |
| `AddSubJobPipelinedFn` | `SubJobPipelined(job, subJob)` | 投票决定子作业是否可以流水线化 |

投票语义：在同一层级中，如果任何插件投同意票（>0）且没有插件投反对票（<0），则结果为同意。如果任何插件投反对票，则结果为反对。如果全部弃权，则默认为同意。

---

### 四、通知函数

#### `JobEnqueuedFn`

    type JobEnqueuedFn func(interface{})

作业成功入队后的通知回调。通过 `AddJobEnqueuedFn` 注册，由 `JobEnqueued(obj)` 调用。

DRF 等插件使用此回调在作业进入队列时更新资源份额计算。

---

### 五、谓词函数

谓词函数过滤节点，决定任务可以或不可以调度到哪些节点。

#### `PredicateFn`

    type PredicateFn func(*TaskInfo, *NodeInfo) error

如果任务可以调度到该节点则返回 `nil`，否则返回错误说明原因。通过 `AddPredicateFn` 注册。

这是核心的节点过滤函数，在 `allocate`、`preempt`、`reclaim` 和 `backfill` 动作中调用。

#### `PrePredicateFn`

    type PrePredicateFn func(*TaskInfo) error

谓词评估前的预处理步骤。通过 `AddPrePredicateFn` 注册。

用于与具体节点无关的每任务设置（如计算拓扑约束）。在遍历节点之前，每个任务只调用一次。

---

### 六、节点打分函数

节点打分函数对候选节点进行排名，选择最优放置位置。

#### `NodeOrderFn`

    type NodeOrderFn func(*TaskInfo, *NodeInfo) (float64, error)

返回单个节点的优先级分数。通过 `AddNodeOrderFn` 注册。所有插件的分数累加。

#### `BatchNodeOrderFn`

    type BatchNodeOrderFn func(*TaskInfo, []*NodeInfo) (map[string]float64, error)

一次性返回所有节点的分数。通过 `AddBatchNodeOrderFn` 注册。当插件可以批量计算分数时，比 `NodeOrderFn` 更高效。

#### `NodeMapFn` 与 `NodeReduceFn`

    type NodeMapFn func(*TaskInfo, *NodeInfo) (float64, error)
    type NodeReduceFn func(*TaskInfo, fwk.NodeScoreList) error

MapReduce 风格的打分模型。`NodeMapFn` 生成每个插件的分数（通过 `AddNodeMapFn` 注册），`NodeReduceFn` 跨节点聚合分数（通过 `AddNodeReduceFn` 注册）。

#### `BestNodeFn`

    type BestNodeFn func(*TaskInfo, map[float64][]*NodeInfo) *NodeInfo

从打分结果中选择最优节点。通过 `AddBestNodeFn` 注册。只有第一个返回非 nil 结果的插件生效。

---

### 七、HyperNode（拓扑）打分函数

HyperNode 打分函数对拓扑域（如机架、交换机组）进行排名，用于拓扑感知调度。

#### `HyperNodeOrderFn`

    type HyperNodeOrderFn func(*SubJobInfo, map[string][]*NodeInfo) (map[string]float64, error)

返回 HyperNode 的分数。通过 `AddHyperNodeOrderFn` 注册。

#### `HyperNodeGradientForJobFn` 与 `HyperNodeGradientForSubJobFn`

    type HyperNodeGradientForJobFn func(job *JobInfo, hyperNode *HyperNodeInfo, purpose SearchPurpose) [][]*HyperNodeInfo
    type HyperNodeGradientForSubJobFn func(subJob *SubJobInfo, hyperNode *HyperNodeInfo, purpose SearchPurpose) [][]*HyperNodeInfo

将 HyperNode 分组为梯度（从细粒度到粗粒度的拓扑层级），并过滤掉不满足拓扑要求的 HyperNode。结果是一个梯度级别列表，每个级别包含候选 HyperNode。

分别通过 `AddHyperNodeGradientForJobFn` 和 `AddHyperNodeGradientForSubJobFn` 注册。

---

### 八、驱除函数

驱除函数决定哪些任务可以被驱除（抢占或回收），以便为更高优先级的工作负载腾出空间。

#### `EvictableFn`

    type EvictableFn func(*TaskInfo, []*TaskInfo) ([]*TaskInfo, int)

返回可以被驱除的候选子集，以及弃权计数（0 表示弃权）。通过以下方式注册：

| Session 注册方法 | 调用方法 | 用途 |
|---|---|---|
| `AddPreemptableFn` | `Preemptable(preemptor, preemptees)` | 决定哪些任务可以被抢占 |
| `AddReclaimableFn` | `Reclaimable(reclaimer, reclaimees)` | 决定哪些任务可以被回收 |

同一层级内多个插件的结果取交集——任务必须被该层级内所有插件允许才能被驱除。

#### `UnifiedEvictableFn`

    type UnifiedEvictableFn func(ctx *EvictionContext, candidates []*TaskInfo) ([]*TaskInfo, int)

Gang 级别的受害者过滤函数，携带完整的驱除上下文。通过 `AddUnifiedEvictableFn` 注册，由 `UnifiedEvictable(ctx, candidates)` 调用。

`EvictionContext` 携带以下信息：

    type EvictionContext struct {
        Kind      EvictionKind
        Job       *JobInfo
        Task      *TaskInfo
        HyperNode string
    }

`EvictionKind` 取值：

| Kind | 描述 |
|---|---|
| `EvictionKindGangPreempt` | Gang 级别抢占（gangpreempt 动作） |
| `EvictionKindGangReclaim` | Gang 级别回收（gangreclaim 动作） |
| `EvictionKindTaskPreempt` | 传统任务级抢占 |
| `EvictionKindTaskReclaim` | 传统任务级回收 |

详见 `evictablefn-evolution-for-gang-eviction.md` 了解设计背景。

#### `VictimTasksFn`

    type VictimTasksFn func([]*TaskInfo) []*TaskInfo

从运行中的任务池中选择要驱除的任务。通过 `AddVictimTasksFns` 注册，由 `VictimTasks(tasks)` 调用。

由 `shuffle` 动作用于重调度场景（如负载均衡）。

---

### 九、资源管理函数

#### `AllocatableFn`

    type AllocatableFn func(*QueueInfo, *TaskInfo) bool

检查任务是否可以从队列的剩余配额中分配。通过 `AddAllocatableFn` 注册。

#### `TargetJobFn`

    type TargetJobFn func([]*JobInfo) *JobInfo

根据插件定义的标准从候选作业列表中选择目标作业。通过 `AddTargetJobFn` 注册。

#### `ReservedNodesFn`

    type ReservedNodesFn func()

插件用于执行保留节点维护的回调。通过 `AddReservedNodesFn` 注册，由 `ReservedNodes()` 调用。

---

### 十、模拟函数（拓扑感知抢占）

模拟函数允许插件参与拓扑感知抢占期间的"干跑"调度，调度器需要在不提交变更的情况下预测添加/移除任务的结果。

#### `SimulateRemoveTaskFn`

    type SimulateRemoveTaskFn func(ctx context.Context, state fwk.CycleState, taskToSchedule *TaskInfo, taskInfoToRemove *TaskInfo, nodeInfo *NodeInfo) error

模拟从节点移除任务的效果。通过 `AddSimulateRemoveTaskFn` 注册。

#### `SimulateAddTaskFn`

    type SimulateAddTaskFn func(ctx context.Context, state fwk.CycleState, taskToSchedule *TaskInfo, taskInfoToAdd *TaskInfo, nodeInfo *NodeInfo) error

模拟向节点添加任务的效果。通过 `AddSimulateAddTaskFn` 注册。

#### `SimulatePredicateFn`

    type SimulatePredicateFn func(ctx context.Context, state fwk.CycleState, task *TaskInfo, nodeInfo *NodeInfo) error

拓扑感知抢占中模拟谓词检查。通过 `AddSimulatePredicateFn` 注册。

#### `SimulateAllocatableFn`

    type SimulateAllocatableFn func(ctx context.Context, state fwk.CycleState, queue *QueueInfo, task *TaskInfo) bool

拓扑感知抢占中模拟可分配检查。通过 `AddSimulateAllocatableFn` 注册。

---

## 各 Action 调用的扩展点矩阵

下表展示了每个 Action 调用了哪些扩展点：

| 扩展点 | enqueue | allocate | preempt | reclaim | gangpreempt | gangreclaim | backfill | shuffle |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| QueueOrderFn | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | |
| JobOrderFn | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | |
| TaskOrderFn | | ✅ | ✅ | ✅ | | | ✅ | |
| SubJobOrderFn | | ✅ | | | | | | |
| VictimQueueOrderFn | | | ✅ | ✅ | | ✅ | | |
| JobValid | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | |
| Overused | | ✅ | | ✅ | | ✅ | | |
| JobReady | | ✅ | | | | | | |
| SubJobReady | | ✅ | | | | | | |
| JobStarving | | | ✅ | ✅ | ✅ | ✅ | | |
| JobPipelined | | ✅ | ✅ | ✅ | ✅ | ✅ | | |
| JobEnqueueable | ✅ | | | | | | | |
| JobEnqueued | ✅ | | | | | | | |
| PrePredicateFn | | ✅ | ✅ | ✅ | | | ✅ | |
| PredicateFn | | ✅ | ✅ | ✅ | ✅¹ | | ✅ | |
| Preemptable | | | ✅ | | | | | |
| Reclaimable | | | | ✅ | | | | |
| UnifiedEvictable | | | | | ✅ | ✅ | | |
| Preemptive | | | | ✅ | | ✅ | | |
| Allocatable | | ✅ | ✅ | | | | | |
| NodeOrderFn | | ✅ | ✅ | | | | ✅ | |
| BatchNodeOrderFn | | ✅ | ✅ | | | | ✅ | |
| NodeMapFn | | ✅ | ✅ | | | | ✅ | |
| NodeReduceFn | | ✅ | ✅ | | | | ✅ | |
| HyperNodeOrderFn | | ✅ | | | | | | |
| BestNodeFn | | ✅ | | | | | ✅ | |
| HyperNodeGradient (Job) | | ✅ | | | ✅² | ✅² | | |
| HyperNodeGradient (SubJob) | | ✅ | | | ✅² | ✅² | | |
| VictimTasksFn | | | | | | | | ✅ |
| TargetJobFn | | ✅³ | | | | | | |
| ReservedNodesFn | | ✅⁴ | | | | | | |
| SimulateRemoveTaskFn | | | ✅ | | | | | |
| SimulateAddTaskFn | | | ✅ | | | | | |
| SimulatePredicateFn | | | ✅ | | | | | |
| SimulateAllocatableFn | | | ✅ | | | | | |

备注：
1. gangpreempt 中间接通过提名计划验证调用
2. 间接通过 `GetCandidateDomains` 工具函数调用
3. allocate 中在某些插件场景下用于选择目标作业
4. allocate 完成后调用以更新保留节点状态

---

## Action 说明

### enqueue（入队）

将作业从 `Pending` 状态转为 `Inqueue` 状态。这是作业进入调度管道的入口。

**关键扩展点：** `QueueOrderFn`、`JobOrderFn`、`JobEnqueueable`、`JobEnqueued`

### allocate（分配）

核心资源分配动作。通过多级优先队列层次结构将任务分配到节点：队列 → 作业 → 子作业 → 任务 → 节点过滤 → 节点打分 → 绑定。

支持通过 HyperNode 梯度搜索和干跑优化进行拓扑感知调度。

**关键扩展点：** 几乎所有——这是使用 Fn 最多的动作。

### preempt（抢占）

在同一队列内抢占低优先级任务，为高优先级饥饿任务腾出空间。支持普通抢占和拓扑感知抢占（使用模拟函数）。

**关键扩展点：** `Preemptable`、`PredicateFn`、`PrePredicateFn`、`Simulate*Fn`

### reclaim（回收）

从超配队列中回收资源，为欠配队列中的饥饿作业提供资源。跨队列资源再平衡。

**关键扩展点：** `Reclaimable`、`Preemptive`、`PredicateFn`、`PrePredicateFn`

### gangpreempt（组抢占）

作业级别的 Gang 感知抢占。驱除整个作业组（或选定的任务束）为更高优先级的饥饿作业腾出空间，同时考虑拓扑约束。为子作业设置 `NominatedHyperNode` 以实现快速路径分配。

**关键扩展点：** `UnifiedEvictable`、`HyperNodeGradient`

### gangreclaim（组回收)

跨队列的 Gang 感知回收。与 gangpreempt 类似，但针对其他队列中超过应有份额的作业。

**关键扩展点：** `UnifiedEvictable`、`Preemptive`、`VictimQueueOrderFn`

### backfill（回填）

用 BestEffort 任务填充空闲资源。使用简化的调度路径，不进行队列配额检查。

**关键扩展点：** `PredicateFn`、`PrePredicateFn`、`BatchNodeOrderFn`、`BestNodeFn`

### shuffle（打乱）

根据插件定义的策略选择并驱除运行中的任务（如为负载均衡进行重调度）。

**关键扩展点：** `VictimTasksFn`

---

## 插件开发指南

开发新插件时，根据期望的能力选择要实现的扩展点：

| 期望能力 | 需要实现的扩展点 |
|---|---|
| 影响作业调度优先级 | `JobOrderFn` |
| 影响任务调度优先级 | `TaskOrderFn` |
| 影响队列调度优先级 | `QueueOrderFn` |
| 控制哪些作业可以入队 | `JobEnqueueableFn` |
| 响应作业入队事件 | `JobEnqueuedFn` |
| 添加节点过滤规则 | `PredicateFn`、`PrePredicateFn` |
| 添加节点打分规则 | `NodeOrderFn` 或 `BatchNodeOrderFn` + `NodeMapFn`/`NodeReduceFn` |
| 控制抢占受害者 | `PreemptableFn` 和/或 `UnifiedEvictableFn` |
| 控制回收受害者 | `ReclaimableFn` 和/或 `UnifiedEvictableFn` |
| 控制队列资源限制 | `OverusedFn`、`AllocatableFn`、`PreemptiveFn` |
| 定义作业就绪条件 | `JobReadyFn`、`JobPipelinedFn` |
| 参与拓扑感知调度 | `HyperNodeOrderFn`、`HyperNodeGradientForJobFn`、`HyperNodeGradientForSubJobFn` |
| 参与拓扑感知抢占 | `SimulatePredicateFn`、`SimulateAllocatableFn`、`SimulateAddTaskFn`、`SimulateRemoveTaskFn` |
| 选择需要重调度的任务 | `VictimTasksFn` |
| 从候选中选择最优节点 | `BestNodeFn` |
