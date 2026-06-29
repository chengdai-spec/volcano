/*
Copyright 2025 The Kubernetes Authors.

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

package volumebinding

import (
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// informer 是 newAssumeCache 依赖的 cache.SharedInformer 的最小接口子集。
// 通过该接口可以添加事件处理器并访问底层索引器。
type informer interface {
	AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error)
	GetIndexer() cache.Indexer
}

// passiveAssumeCache 是建立在 informer 之上的假设缓存层，允许在 informer 事件之外
// 对对象进行内存级更新，同时也支持恢复到 informer cache 中的版本。
//
// 核心设计约束：
//   - informer 的更新始终优先于 assumed 对象；
//   - 不参与事件分发，只维护一份与 informer 保持一致的本地覆盖视图；
//   - 只允许假设“尚未提交到 apiserver”的对象，禁止假设 apiserver 返回的对象。
//
// 与 pkg/scheduler/util/assumecache 的区别：
//   - 不对外派发事件；
//   - 始终与 informer 保持同步；
//   - 仅允许假设还未发送到 apiserver 的对象，而不是已经从 apiserver 返回的对象。
type passiveAssumeCache[T v1.Object] struct {
	// logger 创建缓存时传入的日志器，所有操作共用该 logger。
	logger klog.Logger
	// gr 缓存对象的 GroupResource，用于构造 NotFound 等错误信息。
	gr schema.GroupResource

	// rwMutex 用于保护本结构体中所有字段的并发访问。
	// 虽然 store 自身有锁，但在比较 stored 与 assumed 的 ResourceVersion 时，
	// 必须先持有本锁，以保证两者版本比较的原子性和一致性视图。
	rwMutex sync.RWMutex

	// store 来自 informer 的索引器，保存 apiserver 同步下来的对象。
	store cache.Indexer
	// assumed 保存本地假设（未提交到 apiserver）的对象副本。
	// key 使用对象的 namespace/name，value 为假设对象。
	assumed map[string]T
}

// newAssumeCache 创建指定类型 T 的假设缓存。
// 它会向 informer 注册 Add/Update/Delete 事件处理器，以便在 informer
// 感知到对象变化时，使本地 assumed 对象适时过期。
func newAssumeCache[T v1.Object](logger klog.Logger, informer informer, gr schema.GroupResource) (*passiveAssumeCache[T], error) {
	c := &passiveAssumeCache[T]{
		logger:  logger,
		gr:      gr,
		store:   informer.GetIndexer(),
		assumed: make(map[string]T),
	}

	_, err := informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.add,
			UpdateFunc: c.update,
			DeleteFunc: c.delete,
		},
	)
	return c, err
}

// mayExpire 在收到 informer 事件时被调用，用于判断并清理过期的 assumed 对象。
//
// 过期策略：
//   - 如果 informer 中已不存在该对象，则 assumed 对象过期；
//   - 如果 informer 中存在该对象，但 ResourceVersion 与 assumed 不一致，
//     说明 apiserver 端已有更新，assumed 对象过期；
//   - 如果 ResourceVersion 相同，说明只是 informer resync，保留 assumed 对象。
func (c *passiveAssumeCache[T]) mayExpire(key string) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	assumed, ok := c.assumed[key]
	if !ok {
		// 该对象没有被假设过，无需处理。
		return
	}

	// 从 store 获取当前最新版本，避免误删 Assume 刚写入的新对象。
	obj, exists, err := c.store.GetByKey(key)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "mayExpire get", "key", key)
		return
	}

	expire := true
	if exists {
		newMeta, err := meta.Accessor(obj)
		if err != nil {
			utilruntime.HandleErrorWithLogger(c.logger, err, "mayExpire meta", "key", key)
			return
		}

		// 仅当版本相同（resync）时保留 assumed 对象；
		// 若版本不同，说明 apiserver 已有更新，本地假设失效。
		if assumed.GetResourceVersion() == newMeta.GetResourceVersion() {
			c.logger.V(10).Info("ignoring resync of assumed object", "key", key, "version", assumed.GetResourceVersion())
			expire = false
		} else {
			c.logger.V(4).Info("assumed object expired", "newVersion", newMeta.GetResourceVersion(),
				"key", key, "version", assumed.GetResourceVersion())
		}
	} else {
		c.logger.V(4).Info("assumed object expired", "key", key, "version", assumed.GetResourceVersion())
	}
	if expire {
		delete(c.assumed, key)
	}
}

// add 处理 informer 的 Add 事件。
// 它根据对象 key 触发 mayExpire，以便在对象被重新加入时使旧 assumed 失效。
func (c *passiveAssumeCache[T]) add(obj any) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "Add object get key")
		return
	}
	c.mayExpire(key)
}

// update 处理 informer 的 Update 事件，复用 add 逻辑。
func (c *passiveAssumeCache[T]) update(_, obj any) {
	c.add(obj)
}

// delete 处理 informer 的 Delete 事件，触发 mayExpire 清理 assumed 对象。
func (c *passiveAssumeCache[T]) delete(obj any) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "Delete object get key")
		return
	}
	c.mayExpire(key)
}

// ByIndex 按指定索引名和索引值从 store 中查询对象，并返回本地假设覆盖后的结果。
//
// 注意：索引计算只基于 store 中的对象，assumed 对象不会参与索引。
// 因此 ByIndex 返回的是 store 命中的对象集合，再对其中的 assumed 对象做替换。
func (c *passiveAssumeCache[T]) ByIndex(indexName, indexedValue string) ([]T, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	objs, err := c.store.ByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return c.replaceAssumed(objs), nil
}

// Get 根据 key 获取对象。
//
// 返回值优先级：
//   1. 如果对象未被假设，或 informer 中的对象版本比 assumed 更新，则返回 informer cache 中的对象；
//   2. 如果 assumed 对象版本与 informer cache 一致，则返回 assumed 对象（本地未提交的修改视图）。
func (c *passiveAssumeCache[T]) Get(key string) (T, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	obj, err := c.GetAPIObj(key)
	if err != nil {
		return obj, err
	}

	assumed, ok := c.assumed[key]
	if !ok || assumed.GetResourceVersion() != obj.GetResourceVersion() { // 未假设，或 informer 对象更新
		return obj, nil
	}
	return assumed, nil
}

// GetAPIObj 从 informer cache 中直接获取指定 key 的对象。
// 如果对象不存在，返回 NotFound 错误。
func (c *passiveAssumeCache[T]) GetAPIObj(key string) (T, error) {
	obj, ok, err := c.store.GetByKey(key)
	var zero T
	if err != nil {
		return zero, err
	}
	if !ok {
		return zero, apierrors.NewNotFound(c.gr, key)
	}
	v, ok := obj.(T)
	if !ok {
		return zero, fmt.Errorf("object is not of type %T", zero)
	}
	return v, nil
}

// keyOf 根据对象元数据构造缓存使用的 key（namespace/name 形式）。
func keyOf[T v1.Object](obj T) string {
	return cache.MetaObjectToName(obj).String()
}

// replaceAssumed 将 store 查询结果中的 assumed 对象替换为本地假设版本。
// 只有当 assumed 对象的 ResourceVersion 与 store 对象一致时才会替换，
// 这表示该 assumed 对象还未被 informer 同步（未真正提交到 apiserver）。
func (c *passiveAssumeCache[T]) replaceAssumed(objs []any) []T {
	allObjs := make([]T, 0, len(objs))
	for _, obj := range objs {
		v, ok := obj.(T)
		if !ok {
			utilruntime.HandleErrorWithLogger(c.logger, nil, "listed object has wrong type", "type", fmt.Sprintf("%T", obj))
			continue
		}
		assumed, ok := c.assumed[keyOf(v)]
		if ok && assumed.GetResourceVersion() == v.GetResourceVersion() {
			// assumed 对象尚未进入 informer，使用本地假设版本
			v = assumed
		}
		allObjs = append(allObjs, v)
	}
	return allObjs
}

// Assume 仅在内存中更新对象（不会调用 apiserver）。
//
// 调用前提：
//   - 传入对象的 ResourceVersion 必须与 informer cache 中当前对象的 ResourceVersion 完全一致；
//   - 该对象应是“准备提交给 apiserver 但尚未提交”的本地修改版本。
//
// 安全机制：
//   - 如果调用 Assume 期间 informer 已经收到了该对象的更新事件，则 stored 与 assume 的
//     ResourceVersion 会不一致，此时返回 out of sync 错误，防止用旧版本覆盖新版本；
//   - 假设成功后，若后续 informer 收到同一对象的更新（ResourceVersion 不同），
//     mayExpire 会自动使该 assumed 对象失效。
func (c *passiveAssumeCache[T]) Assume(obj T) error {
	key := keyOf(obj)

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	// 获取 informer cache 中当前存储的对象版本。
	stored, err := c.GetAPIObj(key)
	if err != nil {
		return err
	}

	// out of sync 发生的核心原因：
	// 1. 对象在假设前已被其他组件更新。
	//    调度器从 informer 拿到 PV/PVC 后，在调用 Assume() 之前，如果其他控制器
	//    （如 PV controller、PVC controller）已经更新了该对象，或者另一个调度线程/
	//    并发流程修改了该对象，那么 informer cache 里的 stored 版本号已经变了，
	//    但手里拿的 obj 还是旧版本，就会出现 stored != assume。
	// 2. 缓存已过期/假设对象被清除。
	//    如果 Assume 调用前，informer 已经同步了该对象的新版本并触发 mayExpire 删除了
	//    旧的 assumed 记录，再次用旧版本调用 Assume() 也会失败。
	// 3. 错误地对 apiserver 返回的对象调用 Assume。
	//    passiveAssumeCache 只允许假设“还未发送到 apiserver”的对象。apiserver 返回的
	//    对象必然带有新的 ResourceVersion，与 cache 中旧版本不一致，因此会报错。
	if stored.GetResourceVersion() != obj.GetResourceVersion() {
		return fmt.Errorf("%q is out of sync (stored: %s, assume: %s)", key, stored.GetResourceVersion(), obj.GetResourceVersion())
	}
	c.assumed[key] = obj
	c.logger.V(4).Info("Assumed object", "key", key, "version", obj.GetResourceVersion())
	return nil
}

// Restore 将对象恢复为 informer cache 中的版本，即删除本地 assumed 覆盖。
// 只有当传入对象的 ResourceVersion 与当前 assumed 对象一致时才会执行删除，
// 避免误删更新的 assumed 记录。
func (c *passiveAssumeCache[T]) Restore(obj T) {
	key := keyOf(obj)

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	assumed, ok := c.assumed[key]
	if ok && assumed.GetResourceVersion() == obj.GetResourceVersion() {
		delete(c.assumed, key)
		c.logger.V(4).Info("Restored object", "key", key, "version", obj.GetResourceVersion())
	}
}
