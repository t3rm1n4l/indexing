package skiplist

import (
	"sync/atomic"
	"unsafe"
)

type BarrierSessionCallback func(interface{})

type BarrierSession struct {
	counter *int32
	payload interface{}
}

func newBarrierSession() *BarrierSession {
	bs := &BarrierSession{
		counter: new(int32),
	}
	*bs.counter = 1

	return bs
}

type AccessBarrier struct {
	active  bool
	session unsafe.Pointer
	callb   BarrierSessionCallback
}

func newAccessBarrier(active bool, callb BarrierSessionCallback) *AccessBarrier {
	return &AccessBarrier{
		active:  active,
		session: unsafe.Pointer(newBarrierSession()),
		callb:   callb,
	}
}

func (ab *AccessBarrier) Acquire() *BarrierSession {
	if ab.active {
	retry:
		bs := (*BarrierSession)(atomic.LoadPointer(&ab.session))
		counter := atomic.AddInt32(bs.counter, 1)
		if counter == 1 {
			goto retry
		}

		return bs
	}

	return nil
}

func (ab *AccessBarrier) Release(bs *BarrierSession) {
	if ab.active {
		counter := atomic.AddInt32(bs.counter, -1)
		if counter == 0 {
			ab.callb(bs.payload)
		}
	}
}

func (ab *AccessBarrier) FlushSession(p interface{}) {
	if ab.active {
	retry:
		bsPtr := atomic.LoadPointer(&ab.session)
		newBsPtr := unsafe.Pointer(newBarrierSession())
		if atomic.CompareAndSwapPointer(&ab.session, bsPtr, newBsPtr) {
			bs := (*BarrierSession)(bsPtr)
			bs.payload = p
			ab.Release(bs)
		} else {
			goto retry
		}
	}
}
