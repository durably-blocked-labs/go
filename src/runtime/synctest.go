// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/runtime/atomic"
	"internal/runtime/sys"
	"unsafe"
)

// bubbleMaxDecisions is the maximum number of scheduling decisions
// that can be recorded per bubble. Allocated via persistentalloc.
const bubbleMaxDecisions = 512

// bubbleMaxRunq is the maximum number of runnable goroutines tracked
// in a single scheduling decision snapshot.
const bubbleMaxRunq = 16

// bubbleDecision records one scheduling decision within a bubble.
// At each yield point (call to schedule()), the scheduler records which
// goroutine was picked and what the runnable set looked like.
type bubbleDecision struct {
	// The decision: which goroutine to pick from the runq.
	index int32 // index into the runnable set (0 = runnext or head)

	// Observability: identity of the chosen goroutine.
	chosenBgid    uint32  // bubble-local goroutine ID (deterministic)
	chosenSpawnPC uintptr // PC of the "go" statement that created it

	// Observability: state of the runnable set at this decision point.
	runqSize  int32              // total runnable goroutines (including chosen)
	runqBgids [bubbleMaxRunq]uint32 // bubble gids of all runnable goroutines

	// Context.
	step       int32 // sequence number within this bubble
	waitReason uint8 // why the previous goroutine yielded (waitReason enum)
}

// bubbleState is the state passed to the onDecision hook at frontier decision points.
// Layout must match internal/synctest.BubbleState exactly (used across go:linkname).
type bubbleState struct {
	step         int32
	runnableN    int32
	runnableBgid [bubbleMaxRunq]uint32
	runnableGlob [bubbleMaxRunq]bool
	blocked      int32
	idle         bool
	now          int64
	timerCount   int32
	nextTimer    int64
	lastBgid     uint32
}

// Signal types for communication between findRunnable (g0) and root goroutine.
const (
	bubbleSignalNone         = 0
	bubbleSignalNeedDecision = 1
)

// A synctestBubble is a set of goroutines started by synctest.Run.
type synctestBubble struct {
	mu      mutex
	timers  timers
	id      uint64 // unique id
	now     int64  // current fake time
	root    *g     // caller of synctest.Run
	waiter  *g     // caller of synctest.Wait
	main    *g     // goroutine started by synctest.Run
	waiting bool   // true if a goroutine is calling synctest.Wait
	done    bool   // true if main has exited

	// The bubble is active (not blocked) so long as running > 0 || active > 0.
	//
	// running is the number of goroutines which are not "durably blocked":
	// Goroutines which are either running, runnable, or non-durably blocked
	// (for example, blocked in a syscall).
	//
	// active is used to keep the bubble from becoming blocked,
	// even if all goroutines in the bubble are blocked.
	// For example, park_m can choose to immediately unpark a goroutine after parking it.
	// It increments the active count to keep the bubble active until it has determined
	// that the park operation has completed.
	total    int // total goroutines
	running  int // non-blocked goroutines
	active   int // other sources of activity
	external int // goroutines currently inside CallExternal/External

	// Deterministic goroutine IDs within this bubble.
	// Assigned sequentially as goroutines are created.
	nextGid uint32

	// Decision tracking (unified model).
	// decisions[0..decisionLen-1] is the recorded trace so far.
	// decisionLen is both the step counter and the next write slot.
	// If decisionFence > 0, pre-loaded decisions exist in decisions[0..decisionFence-1].
	// When decisionLen < decisionFence, findRunnable follows the pre-loaded decision
	// (using its index field) instead of default FIFO. At the frontier
	// (decisionLen >= decisionFence), the root goroutine is woken to decide.
	// Either way, the actual decision is always recorded at decisions[decisionLen].
	// Allocated via persistentalloc (not GC-managed, contains no pointers).
	decisions     *[bubbleMaxDecisions]bubbleDecision
	decisionLen   int32 // decisions recorded so far (= step counter)
	decisionFence int32 // pre-loaded decisions boundary (0 = no pre-load)

	// Signaling between findRunnable (g0) and root goroutine.
	// When findRunnable reaches the frontier and the runq is non-empty,
	// it snapshots the runq into pendingRunq, sets signal = needDecision,
	// and wakes root. Root reads the snapshot, decides, writes decidedIndex,
	// sets decisionReady = true, and goparks. findRunnable picks the chosen goroutine.
	signal        uint32
	decisionReady bool
	decidedIndex  int32
	pendingRunq   [bubbleMaxRunq]uint32 // runq snapshot (bubble gids)
	pendingGlob   [bubbleMaxRunq]bool   // runq snapshot (global flags)
	pendingSize   int32

	// The P this bubble is pinned to. Set once in synctestRunImpl.
	// Used by ready() to route woken bubble goroutines back to the right P.
	pp *p

	// rootInHook is true while the root goroutine is executing the onDecision hook.
	// When set, findRunnable must not try to wake root again (the hook may block,
	// e.g., waiting for an orchestrator response on a channel).
	rootInHook bool

	// lastScheduledBgid is the bubble gid of the goroutine that was most recently
	// scheduled on this bubble's P. When the hook fires, this tells the orchestrator
	// which goroutine just yielded (since whoever ran last must have yielded to
	// get back into findRunnable).
	lastScheduledBgid uint32

	// Orchestrator hooks, set before synctestRun by the orchestrator.
	// Called by the root goroutine from the synctestRun control loop.
	// nil = default behavior (FIFO, index 0).
	onDecision func(bubbleState) int32
}

// changegstatus is called when the non-lock status of a g changes.
// It is never called with a Gscanstatus.
func (bubble *synctestBubble) changegstatus(gp *g, oldval, newval uint32) {
	// Determine whether this change in status affects the idleness of the bubble.
	// If this isn't a goroutine starting, stopping, durably blocking,
	// or waking up after durably blocking, then return immediately without
	// locking bubble.mu.
	//
	// For example, stack growth (newstack) will changegstatus
	// from _Grunning to _Gcopystack. This is uninteresting to synctest,
	// but if stack growth occurs while bubble.mu is held, we must not recursively lock.
	totalDelta := 0
	wasRunning := true
	switch oldval {
	case _Gdead, _Gdeadextra:
		wasRunning = false
		totalDelta++
	case _Gwaiting:
		if gp.waitreason.isIdleInSynctest() {
			wasRunning = false
		}
	}
	isRunning := true
	switch newval {
	case _Gdead, _Gdeadextra:
		isRunning = false
		totalDelta--
		if gp == bubble.main {
			bubble.done = true
		}
	case _Gwaiting:
		if gp.waitreason.isIdleInSynctest() {
			isRunning = false
		}
	}
	// It's possible for wasRunning == isRunning while totalDelta != 0;
	// for example, if a new goroutine is created in a non-running state.
	if wasRunning == isRunning && totalDelta == 0 {
		return
	}

	lock(&bubble.mu)
	bubble.total += totalDelta
	if wasRunning != isRunning {
		if isRunning {
			bubble.running++
		} else {
			bubble.running--
			if raceenabled && newval != _Gdead && newval != _Gdeadextra {
				// Record that this goroutine parking happens before
				// any subsequent Wait.
				racereleasemergeg(gp, bubble.raceaddr())
			}
		}
	}
	if bubble.total < 0 {
		fatal("total < 0")
	}
	if bubble.running < 0 {
		fatal("running < 0")
	}
	wake := bubble.maybeWakeLocked()
	unlock(&bubble.mu)
	if wake != nil {
		goready(wake, 0)
	}
}

// incActive increments the active-count for the bubble.
// A bubble does not become durably blocked while the active-count is non-zero.
func (bubble *synctestBubble) incActive() {
	lock(&bubble.mu)
	bubble.active++
	unlock(&bubble.mu)
}

// decActive decrements the active-count for the bubble.
func (bubble *synctestBubble) decActive() {
	lock(&bubble.mu)
	bubble.active--
	if bubble.active < 0 {
		throw("active < 0")
	}
	wake := bubble.maybeWakeLocked()
	unlock(&bubble.mu)
	if wake != nil {
		goready(wake, 0)
	}
}

// maybeWakeLocked returns a g to wake if the bubble is durably blocked.
func (bubble *synctestBubble) maybeWakeLocked() *g {
	if bubble.running > 0 || bubble.active > 0 {
		return nil
	}
	if bubble.external > 0 {
		// Goroutines are blocked on external events (External/CallExternal).
		// Don't wake root — it would just park again in synctestidle_c,
		// creating a CPU-wasting bounce loop. The goroutine will eventually
		// return from External/CallExternal and resume bubble activity.
		return nil
	}
	// Increment the bubble active count, since we've determined to wake something.
	// The woken goroutine will decrement the count.
	// We can't just call goready and let it increment bubble.running,
	// since we can't call goready with bubble.mu held.
	//
	// Incrementing the active count here is only necessary if something has gone wrong,
	// and a goroutine that we considered durably blocked wakes up unexpectedly.
	// Two wakes happening at the same time leads to very confusing failure modes,
	// so we take steps to avoid it happening.
	bubble.active++
	next := bubble.timers.wakeTime()
	if next > 0 && next <= bubble.now {
		// A timer is scheduled to fire. Wake the root goroutine to handle it.
		return bubble.root
	}
	if gp := bubble.waiter; gp != nil {
		// A goroutine is blocked in Wait. Wake it.
		return gp
	}
	// All goroutines in the bubble are durably blocked, and nothing has called Wait.
	// Wake the root goroutine.
	return bubble.root
}

func (bubble *synctestBubble) raceaddr() unsafe.Pointer {
	// Address used to record happens-before relationships created by the bubble.
	//
	// Wait creates a happens-before relationship between itself and
	// the blocking operations which caused other goroutines in the bubble to park.
	return unsafe.Pointer(bubble)
}

var bubbleGen atomic.Uint64 // bubble ID counter

//go:linkname synctestRun internal/synctest.Run
func synctestRun(f func()) {
	synctestRunImpl(f, nil)
}

//go:linkname synctestRunExplore internal/synctest.RunExplore
func synctestRunExplore(f func(), prefix []bubbleDecision) []bubbleDecision {
	return synctestRunImpl(f, prefix)
}

func synctestRunImpl(f func(), prefix []bubbleDecision) []bubbleDecision {
	if debug.asynctimerchan.Load() != 0 {
		panic("synctest.Run not supported with asynctimerchan!=0")
	}

	gp := getg()
	if gp.bubble != nil {
		panic("synctest.Run called from within a synctest bubble")
	}
	bubble := &synctestBubble{
		id:      bubbleGen.Add(1),
		total:   1,
		running: 1,
		root:    gp,
	}
	const synctestBaseTime = 946684800000000000 // midnight UTC 2000-01-01
	bubble.now = synctestBaseTime
	lockInit(&bubble.mu, lockRankSynctest)
	lockInit(&bubble.timers.mu, lockRankTimers)

	// Allocate the decisions array via persistentalloc (non-GC, no pointers inside).
	bubble.decisions = (*[bubbleMaxDecisions]bubbleDecision)(
		persistentalloc(unsafe.Sizeof([bubbleMaxDecisions]bubbleDecision{}), 0, &memstats.other_sys))

	// Load prefix into the decisions array before any goroutine runs.
	if len(prefix) > 0 {
		n := int32(len(prefix))
		if n > bubbleMaxDecisions {
			n = bubbleMaxDecisions
		}
		for i := int32(0); i < n; i++ {
			bubble.decisions[i] = prefix[i]
		}
		bubble.decisionFence = n
	}

	gp.bubble = bubble
	gp.bubbleGid = 0 // root goroutine is B0
	gp.bubbleSpawnPC = sys.GetCallerPC()

	// Pin this P to the bubble so findRunnable can follow/record decisions.
	// bubble.pp is the reverse pointer used by ready() to route woken goroutines.
	// If the current P already has a bubble (e.g., multiple bubbles spawned from
	// the same goroutine land on the same P), acquire a free P first.
	pp := gp.m.p.ptr()
	if pp.bubble != nil {
		systemstack(func() {
			oldpp := releasep()
			// Don't use pidleput — the old P has goroutines on its runq.
			// handoffp starts a new M for it so those goroutines keep running.
			handoffp(oldpp)
			// Acquire a fresh idle P for our bubble.
			lock(&sched.lock)
			newpp, _ := pidlegetSpinning(0)
			if newpp == nil {
				newpp, _ = pidleget(0)
			}
			unlock(&sched.lock)
			if newpp == nil {
				throw("synctest: no idle P available for new bubble")
			}
			acquirep(newpp)
		})
		pp = gp.m.p.ptr()
	}
	pp.bubble = bubble
	bubble.pp = pp
	defer func() {
		gp.bubble = nil
		pp.bubble = nil
		bubble.pp = nil
	}()

	// Migrate any non-bubble goroutines from this P's local runq to the global
	// runq. These goroutines were queued before the bubble was created and should
	// not stay on the bubble P — they would be stranded by the rootInHook spin
	// loop (which only picks root from runnext) or missed when synctestidle_c
	// returns false (root spins via execute, bypassing findRunnable entirely).
	systemstack(func() {
		migrated := false
		if next := pp.runnext; next != 0 {
			if pp.runnext.cas(next, 0) {
				lock(&sched.lock)
				globrunqput(next.ptr())
				unlock(&sched.lock)
				migrated = true
			}
		}
		for {
			gp, _ := runqget(pp)
			if gp == nil {
				break
			}
			lock(&sched.lock)
			globrunqput(gp)
			unlock(&sched.lock)
			migrated = true
		}
		if migrated {
			wakep()
		}
	})

	// This is newproc, but also records the new g in bubble.main.
	pc := sys.GetCallerPC()
	systemstack(func() {
		fv := *(**funcval)(unsafe.Pointer(&f))
		bubble.main = newproc1(fv, gp, pc, false, waitReasonZero)
		pp := getg().m.p.ptr()
		runqput(pp, bubble.main, true)
		wakep()
	})

	lock(&bubble.mu)
	bubble.active++
	for {
		unlock(&bubble.mu)
		systemstack(func() {
			// Clear gp.m.curg while running timers,
			// so timer goroutines inherit their child race context from g0.
			curg := gp.m.curg
			gp.m.curg = nil
			gp.bubble.timers.check(bubble.now, bubble)
			gp.m.curg = curg
		})
		gopark(synctestidle_c, nil, waitReasonSynctestRun, traceBlockSynctest, 0)

		// Handle frontier decision signal before locking.
		// findRunnable woke us because it needs a scheduling decision.
		if bubble.signal == bubbleSignalNeedDecision {
			bubble.signal = bubbleSignalNone
			idx := int32(0) // default FIFO
			if bubble.onDecision != nil {
				state := bubbleState{
					step:      bubble.decisionLen,
					runnableN: bubble.pendingSize,
					blocked:   int32(bubble.total - bubble.running),
					now:       bubble.now,
					nextTimer: bubble.timers.wakeTime(),
					lastBgid:  bubble.lastScheduledBgid,
				}
				for i := int32(0); i < bubble.pendingSize && i < bubbleMaxRunq; i++ {
					state.runnableBgid[i] = bubble.pendingRunq[i]
					state.runnableGlob[i] = bubble.pendingGlob[i]
				}
				bubble.rootInHook = true
				idx = bubble.onDecision(state)
				bubble.rootInHook = false
			}
			bubble.decidedIndex = idx
			bubble.decisionReady = true
			lock(&bubble.mu)
			continue // back to top → unlock → timer check → gopark → findRunnable sees decisionReady
		}

		lock(&bubble.mu)
		if bubble.active < 0 {
			throw("active < 0")
		}
		next := bubble.timers.wakeTime()
		if next == 0 {
			if bubble.external > 0 {
				// Goroutines are waiting on external events (External/CallExternal).
				// Don't declare deadlock — go back to sleep.
				continue
			}
			if bubble.running > 1 {
				// Other goroutines besides root are still running (running includes
				// root). This can happen when maybeWakeLocked fires from a goroutine
				// parking while another is still active. Go back to sleep.
				continue
			}
			break
		}
		if next < bubble.now {
			throw("time went backwards")
		}
		if bubble.done {
			// Time stops once the bubble's main goroutine has exited.
			break
		}
		bubble.now = next
	}

	total := bubble.total
	unlock(&bubble.mu)
	if raceenabled {
		// Establish a happens-before relationship between bubbled goroutines exiting
		// and Run returning.
		raceacquireg(gp, gp.bubble.raceaddr())
	}
	if total != 1 {
		var reason string
		if bubble.done {
			reason = "deadlock: main bubble goroutine has exited but blocked goroutines remain"
		} else {
			reason = "deadlock: all goroutines in bubble are blocked"
		}
		panic(synctestDeadlockError{reason: reason, bubble: bubble})
	}
	if gp.timer != nil && gp.timer.isFake {
		// Verify that we haven't marked this goroutine's sleep timer as fake.
		// This could happen if something in Run were to call timeSleep.
		throw("synctest root goroutine has a fake timer")
	}

	// Copy trace out of the bubble (persistentalloc memory → GC-managed slice).
	n := bubble.decisionLen
	if n > 0 {
		trace := make([]bubbleDecision, n)
		for i := int32(0); i < n; i++ {
			trace[i] = bubble.decisions[i]
		}
		return trace
	}
	return nil
}

type synctestDeadlockError struct {
	reason string
	bubble *synctestBubble
}

func (e synctestDeadlockError) Error() string {
	return e.reason
}

func synctestidle_c(gp *g, _ unsafe.Pointer) bool {
	lock(&gp.bubble.mu)
	canIdle := true
	if gp.bubble.running == 0 && gp.bubble.active == 1 {
		if gp.bubble.external > 0 {
			// Goroutines are waiting on external events (CallExternal/External).
			// Park the root. Cross-P goready will deposit goroutine on our runq;
			// findRunnable's osyield loop will pick it up.
			gp.bubble.active--
			canIdle = true
		} else {
			// All goroutines in the bubble have blocked or exited.
			canIdle = false
		}
	} else {
		gp.bubble.active--
	}
	unlock(&gp.bubble.mu)
	return canIdle
}

//go:linkname synctestWait internal/synctest.Wait
func synctestWait() {
	gp := getg()
	if gp.bubble == nil {
		panic("goroutine is not in a bubble")
	}
	lock(&gp.bubble.mu)
	// We use a bubble.waiting bool to detect simultaneous calls to Wait rather than
	// checking to see if bubble.waiter is non-nil. This avoids a race between unlocking
	// bubble.mu and setting bubble.waiter while parking.
	if gp.bubble.waiting {
		unlock(&gp.bubble.mu)
		panic("wait already in progress")
	}
	gp.bubble.waiting = true
	unlock(&gp.bubble.mu)
	gopark(synctestwait_c, nil, waitReasonSynctestWait, traceBlockSynctest, 0)

	lock(&gp.bubble.mu)
	gp.bubble.active--
	if gp.bubble.active < 0 {
		throw("active < 0")
	}
	gp.bubble.waiter = nil
	gp.bubble.waiting = false
	unlock(&gp.bubble.mu)

	// Establish a happens-before relationship on the activity of the now-blocked
	// goroutines in the bubble.
	if raceenabled {
		raceacquireg(gp, gp.bubble.raceaddr())
	}
}

func synctestwait_c(gp *g, _ unsafe.Pointer) bool {
	lock(&gp.bubble.mu)
	if gp.bubble.running == 0 && gp.bubble.active == 0 {
		// This shouldn't be possible, since gopark increments active during unlockf.
		throw("running == 0 && active == 0")
	}
	gp.bubble.waiter = gp
	unlock(&gp.bubble.mu)
	return true
}

//go:linkname synctest_isInBubble internal/synctest.IsInBubble
func synctest_isInBubble() bool {
	return getg().bubble != nil
}

//go:linkname synctest_acquire internal/synctest.acquire
func synctest_acquire() any {
	if bubble := getg().bubble; bubble != nil {
		bubble.incActive()
		return bubble
	}
	return nil
}

//go:linkname synctest_release internal/synctest.release
func synctest_release(bubble any) {
	bubble.(*synctestBubble).decActive()
}

//go:linkname synctest_inBubble internal/synctest.inBubble
func synctest_inBubble(bubble any, f func()) {
	gp := getg()
	if gp.bubble != nil {
		panic("goroutine is already bubbled")
	}
	gp.bubble = bubble.(*synctestBubble)
	defer func() {
		gp.bubble = nil
	}()
	f()
}

// synctestMarkGlobal marks the current goroutine as global within its bubble.
// Children of a global goroutine inherit the global flag.
// Must be called from within a bubble.
//
//go:linkname synctestMarkGlobal internal/synctest.MarkGlobal
func synctestMarkGlobal() {
	gp := getg()
	if gp.bubble == nil {
		return
	}
	gp.bubbleGlobal = true
}

// synctestIncExternal increments the external counter for the current bubble.
// Used by both External and CallExternal to signal that a goroutine is
// performing an external operation. When external > 0, synctestidle_c parks
// root and synctestRunImpl doesn't declare deadlock.
//
//go:linkname synctestIncExternal internal/synctest.incExternal
func synctestIncExternal() {
	gp := getg()
	b := gp.bubble
	if b == nil {
		return
	}
	lock(&b.mu)
	b.external++
	unlock(&b.mu)
}

// synctestDecExternal decrements the external counter for the current bubble.
//
//go:linkname synctestDecExternal internal/synctest.decExternal
func synctestDecExternal() {
	gp := getg()
	b := gp.bubble
	if b == nil {
		b = gp.bubbleHome // during External, gp.bubble is nil
	}
	if b == nil {
		return
	}
	lock(&b.mu)
	b.external--
	wake := b.maybeWakeLocked()
	unlock(&b.mu)
	if wake != nil {
		goready(wake, 0)
	}
}

// synctestDetachBubble detaches the current goroutine from its bubble.
// Used by External (NOT CallExternal) to make channel operations during fn()
// invisible to the bubble's boundary checks. Channels created while detached
// are untagged (c.bubble=nil), allowing external servers to send on them.
//
// Also decrements running so the bubble correctly tracks active goroutines.
// The goroutine's state transitions (park/unpark) are invisible to the bubble
// while detached, so running must be adjusted manually.
//
//go:linkname synctestDetachBubble internal/synctest.detachBubble
func synctestDetachBubble() {
	gp := getg()
	b := gp.bubble
	if b == nil {
		return
	}
	lock(&b.mu)
	b.running--
	unlock(&b.mu)
	gp.bubbleHome = b
	gp.bubble = nil
}

// synctestReattachBubble re-attaches the current goroutine to its bubble
// after External returns. Restores gp.bubble and increments running.
//
//go:linkname synctestReattachBubble internal/synctest.reattachBubble
func synctestReattachBubble() {
	gp := getg()
	b := gp.bubbleHome
	if b == nil {
		return
	}
	gp.bubble = b
	gp.bubbleHome = nil
	lock(&b.mu)
	b.running++
	unlock(&b.mu)
}

// synctestGetDecisions returns a copy of the current bubble's decisions.
// Must be called from within a bubble.
//
//go:linkname synctestGetDecisions
func synctestGetDecisions() []bubbleDecision {
	gp := getg()
	if gp.bubble == nil || gp.bubble.decisions == nil {
		return nil
	}
	b := gp.bubble
	n := b.decisionLen
	if b.decisionFence > n {
		n = b.decisionFence // include pre-loaded decisions not yet followed
	}
	if n <= 0 {
		return nil
	}
	result := make([]bubbleDecision, n)
	for i := int32(0); i < n; i++ {
		result[i] = b.decisions[i]
	}
	return result
}

// synctestSetDecisions pre-loads decisions into the current bubble.
// Must be called from within a bubble before any goroutines have started executing.
// Sets decisionIdx to 0 so findRunnable will follow these decisions.
//
//go:linkname synctestSetDecisions
func synctestSetDecisions(decisions []bubbleDecision) {
	gp := getg()
	if gp.bubble == nil || gp.bubble.decisions == nil {
		return
	}
	b := gp.bubble
	n := int32(len(decisions))
	if n > bubbleMaxDecisions {
		n = bubbleMaxDecisions
	}
	for i := int32(0); i < n; i++ {
		b.decisions[i] = decisions[i]
	}
	b.decisionFence = n
	// decisionLen is NOT reset — it tracks the current step.
	// The follow code uses decisions[decisionLen].index when decisionLen < fence.
}

// synctestSetDecisionHook sets the onDecision hook for the current bubble.
// The hook is called by the root goroutine at each frontier decision point.
// Must be called from within a bubble.
//
//go:linkname synctestSetDecisionHook internal/synctest.SetDecisionHook
func synctestSetDecisionHook(fn func(bubbleState) int32) {
	gp := getg()
	if gp.bubble == nil {
		return
	}
	gp.bubble.onDecision = fn
}

// specialBubble is a special used to associate objects with bubbles.
type specialBubble struct {
	_        sys.NotInHeap
	special  special
	bubbleid uint64
}

// Keep these in sync with internal/synctest.
const (
	bubbleAssocUnbubbled     = iota // not associated with any bubble
	bubbleAssocCurrentBubble        // associated with the current bubble
	bubbleAssocOtherBubble          // associated with a different bubble
)

// getOrSetBubbleSpecial checks the special record for p's bubble membership.
//
// If add is true and p is not associated with any bubble,
// it adds a special record for p associating it with bubbleid.
//
// It returns ok==true if p is associated with bubbleid
// (including if a new association was added),
// and ok==false if not.
func getOrSetBubbleSpecial(p unsafe.Pointer, bubbleid uint64, add bool) (assoc int) {
	span := spanOfHeap(uintptr(p))
	if span == nil {
		// This is probably a package var.
		// We can't attach a special to it, so always consider it unbubbled.
		return bubbleAssocUnbubbled
	}

	// Ensure that the span is swept.
	// Sweeping accesses the specials list w/o locks, so we have
	// to synchronize with it. And it's just much safer.
	mp := acquirem()
	span.ensureSwept()

	offset := uintptr(p) - span.base()

	lock(&span.speciallock)

	// Find splice point, check for existing record.
	iter, exists := span.specialFindSplicePoint(offset, _KindSpecialBubble)
	if exists {
		// p is already associated with a bubble.
		// Return true iff it's the same bubble.
		s := (*specialBubble)((unsafe.Pointer)(*iter))
		if s.bubbleid == bubbleid {
			assoc = bubbleAssocCurrentBubble
		} else {
			assoc = bubbleAssocOtherBubble
		}
	} else if add {
		// p is not associated with a bubble,
		// and we've been asked to add an association.
		lock(&mheap_.speciallock)
		s := (*specialBubble)(mheap_.specialBubbleAlloc.alloc())
		unlock(&mheap_.speciallock)
		s.bubbleid = bubbleid
		s.special.kind = _KindSpecialBubble
		s.special.offset = offset
		s.special.next = *iter
		*iter = (*special)(unsafe.Pointer(s))
		spanHasSpecials(span)
		assoc = bubbleAssocCurrentBubble
	} else {
		// p is not associated with a bubble.
		assoc = bubbleAssocUnbubbled
	}

	unlock(&span.speciallock)
	releasem(mp)

	return assoc
}

// synctest_associate associates p with the current bubble.
// It returns false if p is already associated with a different bubble.
//
//go:linkname synctest_associate internal/synctest.associate
func synctest_associate(p unsafe.Pointer) int {
	return getOrSetBubbleSpecial(p, getg().bubble.id, true)
}

// synctest_disassociate disassociates p from its bubble.
//
//go:linkname synctest_disassociate internal/synctest.disassociate
func synctest_disassociate(p unsafe.Pointer) {
	removespecial(p, _KindSpecialBubble)
}

// synctest_isAssociated reports whether p is associated with the current bubble.
//
//go:linkname synctest_isAssociated internal/synctest.isAssociated
func synctest_isAssociated(p unsafe.Pointer) bool {
	return getOrSetBubbleSpecial(p, getg().bubble.id, false) == bubbleAssocCurrentBubble
}
