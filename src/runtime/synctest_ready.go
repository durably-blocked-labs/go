// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/runtime/atomic"
	"unsafe"
)

// synctestReady acknowledges registration, not application completion.
// Its leaf lock never nests any other runtime lock.
type synctestReady struct {
	mu      mutex
	used    bool
	done    atomic.Uint32
	waiters gQueue
}

//go:linkname synctestNewReady internal/synctest.newReady
func synctestNewReady() any {
	r := new(synctestReady)
	lockInit(&r.mu, lockRankLeafRank)
	return r
}

//go:linkname synctestReadyDone internal/synctest.readyDone
func synctestReadyDone(token any) bool { return token.(*synctestReady).done.Load() != 0 }

//go:linkname synctestReadyWait internal/synctest.readyWait
func synctestReadyWait(token any) {
	gp := getg()
	if gp.bubble != nil || gp.bubbleHome != nil {
		panic("synctest: Ready.Wait must be outside a bubble")
	}
	r := token.(*synctestReady)
	lock(&r.mu)
	if r.done.Load() != 0 {
		unlock(&r.mu)
		return
	}
	gopark(synctestReadyPark, unsafe.Pointer(r), waitReasonSemacquire, traceBlockSync, 0)
}

func synctestReadyPark(gp *g, p unsafe.Pointer) bool {
	r := (*synctestReady)(p)
	r.waiters.pushBack(gp)
	unlock(&r.mu)
	return true
}

//go:linkname synctestArmReady internal/synctest.armReady
func synctestArmReady(token any) {
	gp := getg()
	if gp.bubble == nil || !gp.bubbleExternal {
		panic("synctest: readiness token requires ExternalWaitReady")
	}
	r := token.(*synctestReady)
	lock(&r.mu)
	if r.used {
		unlock(&r.mu)
		panic("synctest: readiness token reused")
	}
	r.used = true
	unlock(&r.mu)
	gp.bubbleReady = r
}

// synctestReadyRegistered must follow insertion into the owner runqueue or
// insertion into inbound while bubble.mu still excludes its owner. The done
// store therefore precedes any target execution. Wake returned waiters ONLY
// after releasing bubble.mu. No user callbacks or channel operations occur.
func synctestReadyRegistered(gp *g) gQueue {
	r := gp.bubbleReady
	if r == nil || r.done.Load() != 0 {
		return gQueue{}
	}
	if !gp.bubbleReadyFinish && gp.waitreason != waitReasonChanReceive && gp.waitreason != waitReasonSelect {
		// GC assists and other incidental runtime wakes are not the logical
		// channel receive. Nonparking callbacks acknowledge explicitly at exit.
		return gQueue{}
	}

	lock(&r.mu)
	r.done.Store(1)
	q := r.waiters
	r.waiters = gQueue{}
	unlock(&r.mu)
	return q
}

func synctestWakeReady(q gQueue) {
	for !q.empty() {
		goready(q.pop(), 0)
	}
}

//go:linkname synctestFinishReady internal/synctest.finishReady
func synctestFinishReady() {
	gp := getg()
	if gp.bubbleReady != nil && gp.bubbleReady.done.Load() == 0 {
		// The callback did not park. Register the running goroutine cooperatively
		// before returning to ordinary target code, including for buffered receives.
		gp.bubbleReadyFinish = true
		Gosched()
		gp.bubbleReadyFinish = false
	}
	// Clear on the user stack, where write barriers are permitted. ready()
	// itself may be reached from a no-write-barrier GC path.
	gp.bubbleReady = nil
}
