// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/abi"
	"internal/runtime/atomic"
	_ "unsafe"
)

// Layout must match internal/synctest.Observation. No pointers are retained.
type bubbleObservation struct {
	Sequence             uint64
	Kind                 uint8
	BGID                 uint32
	NativeStep           int32
	Tag                  uint32
	PC                   uintptr
	GCCycle              uint32
	Stopped              bool
	SelfScan             bool
	StatusBeforeSuspend  uint32
	GCWaiting            bool
	PreemptStop          bool
	SyncSafePoint        bool
	AsyncSafePoint       bool
	STWReason            uint8
	ControlQueueN        uint8
	ControlQueueBGIDs    [16]uint32
	ControlQueueStarts   [16]uintptr
	ControlRootMask      uint32
	ControlMemberMask    uint32
	ControlRootStatus    uint32
	ControlRootInHook    bool
	ControlDecisionReady bool
	ControlSignal        uint32
	ControlRunning       int32
	ControlActive        int32
	ControlExternal      int32
	StackN               uint8
	StackPCs             [16]uintptr
}

const (
	bubbleObservationCapacity                = 4096
	bubbleObservationSchedule          uint8 = 1
	bubbleObservationUser              uint8 = 2
	bubbleObservationGCScanBegin       uint8 = 3
	bubbleObservationGCResumeRequested uint8 = 4
	bubbleObservationPreempt           uint8 = 5
	// 6 is reserved for requests emitted by earlier diagnostic builds. Remote
	// requests do not own the target G, so this runtime does not inspect it.
	bubbleObservationWorldStopWait      uint8  = 7
	bubbleObservationWorldStopResumed   uint8  = 8
	bubbleObservationWorldStopContinued uint8  = 9
	bubbleObservationControlQueue       uint8  = 10
	bubbleObservationScanPark           uint8  = 11
	bubbleObservationRuntimeContinued   uint8  = 12
	bubbleObservationGCPause            uint8  = 13
	bubbleControlBeforeSchedule         uint32 = 1
	bubbleControlBeforeFrontier         uint32 = 2
	bubbleGCPauseYield                  uint32 = 1
	bubbleGCPauseAssistDebt             uint32 = 2
	bubbleGCPauseWait                   uint32 = 3
)

type bubbleObservationSlot struct {
	published atomic.Uint32
	event     bubbleObservation
}

type bubbleObservations struct {
	nativeStep atomic.Int32
	next       atomic.Uint64
	overflow   atomic.Bool
	slots      [bubbleObservationCapacity]bubbleObservationSlot
}

// observationBuffer checks capacity before a caller walks a stack or takes
// diagnostic locks. Overflow still invalidates the snapshot. Concurrent writers
// can consume the final slot after this check; recordObservation checks again.
func (b *synctestBubble) observationBuffer() *bubbleObservations {
	ob := b.observations.Load()
	if ob == nil {
		return nil
	}
	if ob.next.Load() >= bubbleObservationCapacity {
		ob.overflow.Store(true)
		return nil
	}
	return ob
}

// recordObservation never allocates, invokes callbacks, locks, or yields. Each
// writer owns one slot, and publishes only after completing the payload. The
// shared sequence orders reservations, not completion of concurrent writes.
func (b *synctestBubble) recordObservation(event bubbleObservation) {
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	n := ob.next.Add(1)
	if n > bubbleObservationCapacity {
		ob.overflow.Store(true)
		return
	}
	event.Sequence = n
	event.GCCycle = work.cycles.Load()
	slot := &ob.slots[n-1]
	slot.event = event
	slot.published.Store(1)
}

//go:linkname synctestEnableObservations internal/synctest.EnableObservations
func synctestEnableObservations() {
	b := getg().bubble
	if b == nil {
		panic("synctest.EnableObservations outside bubble")
	}
	if b.observations.Load() != nil {
		panic("synctest observations already enabled")
	}
	ob := new(bubbleObservations)
	ob.nativeStep.Store(-1)
	if !b.observations.CompareAndSwap(nil, ob) {
		panic("synctest observations already enabled")
	}
}

//go:noinline
//go:linkname synctestRecordObservation internal/synctest.RecordObservation
func synctestRecordObservation(tag uint32) {
	gp := getg()
	b := gp.bubble
	if b == nil {
		panic("synctest.RecordObservation outside bubble")
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	event := bubbleObservation{Kind: bubbleObservationUser, BGID: gp.bubbleGid, NativeStep: ob.nativeStep.Load(), Tag: tag}
	// Skip this frame and the public noinline RecordObservation wrapper. The
	// internal declaration is linknamed directly to this function (no wrapper).
	event.StackN = uint8(callers(2, event.StackPCs[:]))
	if event.StackN != 0 {
		event.PC = event.StackPCs[0]
	}
	b.recordObservation(event)
}

//go:linkname synctestObservations internal/synctest.Observations
func synctestObservations() ([]bubbleObservation, bool) {
	b := getg().bubble
	if b == nil {
		panic("synctest.Observations outside bubble")
	}
	ob := b.observations.Load()
	if ob == nil {
		return nil, false
	}
	n := ob.next.Load()
	incomplete := ob.overflow.Load() || n > bubbleObservationCapacity
	if n > bubbleObservationCapacity {
		n = bubbleObservationCapacity
	}
	out := make([]bubbleObservation, 0, int(n))
	for i := uint64(0); i < n; i++ {
		slot := &ob.slots[i]
		if slot.published.Load() == 0 {
			incomplete = true
			break
		}
		out = append(out, slot.event)
	}
	return out, incomplete
}

// bubbleObserveGCScan runs on g0 after suspendG acquired stack ownership and
// before scanstack can shrink it. Only Go frames are collected; no registered
// cgo callbacks are invoked. Do not capture stacks after resumeG.
func bubbleObserveGCScan(gp *g, stopped, selfScan bool, status uint32) *synctestBubble {
	b := gp.bubble
	if b == nil {
		b = gp.bubbleHome
	}
	if b == nil {
		return nil
	}
	ob := b.observationBuffer()
	if ob == nil {
		return nil
	}
	event := bubbleObservation{Kind: bubbleObservationGCScanBegin, BGID: gp.bubbleGid, NativeStep: ob.nativeStep.Load(), Stopped: stopped, SelfScan: selfScan, StatusBeforeSuspend: status}
	event.StackN = uint8(bubbleObservationGoStack(gp, event.StackPCs[:]))
	if event.StackN != 0 {
		event.PC = event.StackPCs[0]
	}
	b.recordObservation(event)
	return b
}

// This records a resume request, not the point at which application execution
// resumes. A false Stopped means resumeG only releases stack-scan ownership.
func bubbleObserveGCResume(gp *g, b *synctestBubble, stopped, selfScan bool, status uint32) {
	if b == nil {
		return
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	b.recordObservation(bubbleObservation{Kind: bubbleObservationGCResumeRequested, BGID: gp.bubbleGid, NativeStep: ob.nativeStep.Load(), Stopped: stopped, SelfScan: selfScan, StatusBeforeSuspend: status})
}

// bubbleObservationGoStack mirrors tracebackPCs' logical Go-frame expansion and
// return-PC normalization, but deliberately never calls u.cgoCallers: collection
// in the stack-scanning path must not invoke a registered user cgo unwinder.
// The caller holds gp's stack ownership and executes on g0. Foreign frames are
// omitted; this is not a complete C/cgo backtrace.
func bubbleObservationGoStack(gp *g, pcs []uintptr) int {
	var u unwinder
	u.init(gp, unwindSilentErrors)
	n := 0
	for ; n < len(pcs) && u.valid(); u.next() {
		for iu, uf := newInlineUnwinder(u.frame.fn, u.symPC()); n < len(pcs) && uf.valid(); uf = iu.next(uf) {
			sf := iu.srcFunc(uf)
			if sf.funcID != abi.FuncIDWrapper || !elideWrapperCalling(u.calleeFuncID) {
				pcs[n] = uf.pc + 1
				n++
			}
			u.calleeFuncID = sf.funcID
		}
	}
	return n
}

// Diagnostic STW reason is published before gcwaiting. These events do not
// change scheduling policy. All target stacks below are captured on g0 while
// it owns the current G. Remote preemption requests are not observed.
var bubbleDiagnosticSTWReason atomic.Uint32

func bubbleObservePreempt(gp *g) {
	b := gp.bubble
	if b == nil {
		b = gp.bubbleHome
	}
	if b == nil {
		return
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	event := bubbleObservation{Kind: bubbleObservationPreempt, BGID: gp.bubbleGid, NativeStep: ob.nativeStep.Load(),
		GCWaiting: sched.gcwaiting.Load(), PreemptStop: gp.preemptStop,
		SyncSafePoint: gp.syncSafePoint, AsyncSafePoint: gp.asyncSafePoint,
		StatusBeforeSuspend: readgstatus(gp)}
	if event.GCWaiting {
		event.STWReason = uint8(bubbleDiagnosticSTWReason.Load())
	}
	event.StackN = uint8(bubbleObservationGoStack(gp, event.StackPCs[:]))
	if event.StackN != 0 {
		event.PC = event.StackPCs[0]
	}
	b.recordObservation(event)
}

func bubbleObserveWorldStop(b *synctestBubble, kind uint8) {
	if b == nil {
		return
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	b.recordObservation(bubbleObservation{Kind: kind, NativeStep: ob.nativeStep.Load(), GCWaiting: sched.gcwaiting.Load(), STWReason: uint8(bubbleDiagnosticSTWReason.Load())})
}

// Owner-P diagnostic queue snapshots; no target callbacks or allocations.
func bubbleObserveControl(pp *p, b *synctestBubble, stage uint32) {
	if b == nil {
		return
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	e := bubbleObservation{Kind: bubbleObservationControlQueue, Tag: stage, NativeStep: ob.nativeStep.Load(),
		ControlRootStatus: readgstatus(b.root), ControlRootInHook: b.rootInHook,
		ControlDecisionReady: b.decisionReady, ControlSignal: b.signal}
	add := func(gp *g) {
		n := e.ControlQueueN
		if n >= 16 {
			return
		}
		e.ControlQueueBGIDs[n] = gp.bubbleGid
		e.ControlQueueStarts[n] = gp.startpc
		if gp == b.root {
			e.ControlRootMask |= 1 << n
		}
		if gp.bubble == b || gp.bubbleHome == b {
			e.ControlMemberMask |= 1 << n
		}
		e.ControlQueueN++
	}
	if pp.runnext != 0 {
		add(pp.runnext.ptr())
	}
	h, t := atomic.LoadAcq(&pp.runqhead), pp.runqtail
	for i := h; i < t && e.ControlQueueN < 16; i++ {
		add(pp.runq[i%uint32(len(pp.runq))].ptr())
	}
	lock(&b.mu)
	e.ControlRunning, e.ControlActive, e.ControlExternal = int32(b.running), int32(b.active), int32(b.external)
	unlock(&b.mu)
	b.recordObservation(e)
}

func bubbleObservePreemptPark(gp *g) {
	b := gp.bubble
	if b == nil {
		b = gp.bubbleHome
	}
	if b == nil {
		return
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	e := bubbleObservation{Kind: bubbleObservationScanPark, BGID: gp.bubbleGid, NativeStep: ob.nativeStep.Load(), GCWaiting: sched.gcwaiting.Load(), PreemptStop: gp.preemptStop, SyncSafePoint: gp.syncSafePoint, AsyncSafePoint: gp.asyncSafePoint, StatusBeforeSuspend: readgstatus(gp)}
	e.StackN = uint8(bubbleObservationGoStack(gp, e.StackPCs[:]))
	if e.StackN != 0 {
		e.PC = e.StackPCs[0]
	}
	b.recordObservation(e)
}

// Actual owned-stack GC scheduler boundary. Tag 1 is an internal yield, tag 2
// a committed debt park (parkunlock_c always succeeds), tag 3 another scoped
// internal wait. A generic waitunlock callback may still reject a tag3 park.
func bubbleObserveGCPause(gp *g, tag uint32) {
	b := gp.bubble
	if b == nil {
		b = gp.bubbleHome
	}
	if b == nil {
		return
	}
	ob := b.observationBuffer()
	if ob == nil {
		return
	}
	event := bubbleObservation{Kind: bubbleObservationGCPause, Tag: tag, BGID: gp.bubbleGid, NativeStep: ob.nativeStep.Load(), StatusBeforeSuspend: readgstatus(gp)}
	event.StackN = uint8(bubbleObservationGoStack(gp, event.StackPCs[:]))
	if event.StackN != 0 {
		event.PC = event.StackPCs[0]
	}
	b.recordObservation(event)
}
