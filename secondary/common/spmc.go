package common

import "sync/atomic"
import "unsafe"
import "runtime"

type Node struct {
	next unsafe.Pointer
	v    interface{}
}

type SPMCQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	size int64
}

func NewSPMC() *SPMCQueue {
        dummy := unsafe.Pointer(&Node{})
	return &SPMCQueue{head: dummy, tail: dummy}
}

func (q *SPMCQueue) Push(v interface{}) {
	n := unsafe.Pointer(&Node{v: v})
repeat:
	prev := atomic.LoadPointer(&q.head)
	if !atomic.CompareAndSwapPointer(&q.head, prev, n) {
		goto repeat
	}

	(*Node)(prev).next = n
	atomic.AddInt64(&q.size, 1)

}

func (q *SPMCQueue) Pop() interface{} {
repeat:
	tail := (*Node)(q.tail)
	if tail.next != nil {
		q.tail = tail.next
		atomic.AddInt64(&q.size, -1)
		return (*Node)(q.tail).v
	}

	runtime.Gosched()
	goto repeat

	return nil
}

func (q *SPMCQueue) Size() int64 {
	return atomic.LoadInt64(&q.size)
}
