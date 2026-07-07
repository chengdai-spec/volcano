package cache

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
)

type QueueObjectWrapper struct {
	Object          string
	IsInInitialList bool
}

// InitialEventAsyncHandlerTracker track the queue handling status. For initial event put in queue by event handler,
// use tracker to track whether the initial list handling is completed. In add event handler, call Add(obj) to
// add initial event in to tracker. After event in queue is handled, call Done(obj) to mark initial event handled.
/*
	informer 启动时会同步一批已有对象，这些对象叫 initial list
	isInInitialList 用来标记这个 Add 事件是不是来自 initial list
	如果是 initial list，就把它加入 InitialEventAsyncHandlerTracker
	当队列里真正处理完这个对象后，再调用 Done(obj)
	tracker 通过 Add/Done 判断初始同步是否全部完成

	InitialEventAsyncHandlerTracker 用来记录这些初始对象是否都已经处理完，判断初始同步是否完成
*/
type InitialEventAsyncHandlerTracker struct {
	UpstreamHasSynced func() bool
	ObjectSet         sets.Set[string]
	mu                sync.Mutex
}

// NewQueueHandlerTracker create a tracker to track event handling in queue
func NewQueueHandlerTracker(handler cache.ResourceEventHandlerRegistration) *InitialEventAsyncHandlerTracker {
	return &InitialEventAsyncHandlerTracker{UpstreamHasSynced: handler.HasSynced, ObjectSet: sets.Set[string]{}}
}

// Add track the object to be handled from queue
func (tracker *InitialEventAsyncHandlerTracker) Add(obj string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.ObjectSet.Insert(obj)
}

// Done mark object has been handled in tracker
func (tracker *InitialEventAsyncHandlerTracker) Done(obj string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.ObjectSet.Delete(obj)
}

// HasSynced 报告父级是否已同步，以及队列中所有被追踪的对象是否都已处理完成
func (tracker *InitialEventAsyncHandlerTracker) HasSynced() bool {
	if !tracker.UpstreamHasSynced() {
		return false
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.ObjectSet.Len() == 0
}
