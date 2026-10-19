// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package synctest provides support for testing concurrent code.
//
// See the testing/synctest package for function documentation.
package synctest

import (
	"internal/abi"
	"unsafe"
)

// Decision records one event within a bubble. Kind 0 is a schedule event:
// Index indexes ordered RunqBgids; ChosenBgid is the chosen goroutine. Kind 1
// is an initially enabled select event: Index is the runtime case index,
// RunqBgids encodes case index+1 (zero = default), ChosenBgid is the actor,
// and Site is the main-text-relative caller PC. ChosenSpawnPC is diagnostic
// only, process-local, and NOT a portable replay identity.
// Layout must match runtime.bubbleDecision exactly.
type Decision struct {
	Index         int32
	ChosenBgid    uint32
	ChosenSpawnPC uintptr
	RunqSize      int32
	RunqBgids     [16]uint32
	Step          int32
	// WaitReason is 255 for explicit Yield, otherwise reserved zero.
	// It does not count preemptions.
	WaitReason uint8
	Kind       uint8
	Site       uintptr
}

// BubbleState describes the state of a bubble at a scheduling decision point.
// Layout must match runtime.bubbleState exactly.
type BubbleState struct {
	Step         int32
	RunnableN    int32
	RunnableBgid [16]uint32
	RunnableGlob [16]bool
	Blocked      int32
	Idle         bool
	Now          int64
	// TimerCount counts heap entries, including timers pending removal.
	TimerCount   int32
	NextTimer    int64
	LastBgid     uint32
	External     int32
	ExternalWait int32
}

//go:linkname Run
func Run(f func())

// RunExplore runs f in a new bubble. If prefix is non-nil, the scheduler
// follows those decisions before making its own. Returns the full trace.
//
//go:linkname RunExplore
func RunExplore(f func(), prefix []Decision) []Decision

//go:linkname RunReplay
func RunReplay(f func(), trace []Decision) []Decision

//go:linkname Wait
func Wait()

// SetDecisionHook sets the onDecision hook for the current bubble.
// The hook is called by the root goroutine at each frontier decision point.
// Must be called from within a bubble.
//
//go:linkname SetDecisionHook
func SetDecisionHook(fn func(BubbleState) int32)

// MarkGlobal marks the current goroutine as global within its bubble.
// Children of a global goroutine inherit the global flag.
// When a global goroutine appears in the runq, the decision hook
// receives Global=true in the corresponding GoroutineInfo.
//
//go:linkname MarkGlobal
func MarkGlobal()

// incExternal increments the external counter for the current bubble.
//
//go:linkname incExternal
func incExternal()

// decExternal decrements the external counter for the current bubble.
//
//go:linkname decExternal
func decExternal()

// detachBubble detaches the current goroutine from its bubble, saving it
// in gp.bubbleHome. Channels created while detached are untagged.
//
//go:linkname detachBubble
func detachBubble()

// reattachBubble re-attaches the current goroutine to its bubble.
//
//go:linkname reattachBubble
func reattachBubble()

// incExternalWait increments the externalWait counter for the current bubble.
//
//go:linkname incExternalWait
func incExternalWait()

// decExternalWait decrements the externalWait counter for the current bubble.
//
//go:linkname decExternalWait
func decExternalWait()

// SetTime sets the bubble's fake clock to t (nanoseconds since epoch).
// If t is before the current time, the call is a no-op.
//
//go:linkname SetTime
func SetTime(t int64)

// External wraps fn with the external counter AND detaches the goroutine
// from the bubble. This means:
//   - Channels created inside fn are NOT tagged with the bubble.
//   - External servers can freely send/receive on those channels.
//   - The decision hook does not see this goroutine (gp.bubble is nil).
//
// Use External for operations like Redis GET/SET where the bubble
// needs to park but the orchestrator doesn't need to intercept.
func External(fn func()) {
	incExternal()
	detachBubble()
	defer func() { reattachBubble(); decExternal() }()
	fn()
}

// CallExternal marks the current goroutine as global and wraps fn
// with the external counter. Unlike External, the goroutine stays
// attached to the bubble — Gosched inside fn triggers decision hooks,
// and the orchestrator sees the goroutine as global.
//
// Use CallExternal for operations the orchestrator should intercept
// (e.g., RPC send/receive, inbox listen).
func CallExternal(fn func()) {
	incExternal()
	defer decExternal()
	MarkGlobal()
	fn()
}

// ExternalWait wraps fn with the externalWait counter AND detaches the
// goroutine from the bubble. Like External, channels created inside fn
// are NOT tagged with the bubble.
//
// Unlike External (which tracks generic external IO), ExternalWait
// increments the externalWait counter. When all goroutines are blocked
// with externalWait > 0, the bubble's decision hook fires with
// Idle: true, allowing the orchestrator to deliver a message or
// advance time.
//
// Use ExternalWait for operations on orchestrator-controlled channels
// (e.g., sending/receiving RPC messages via a mock transport).
func ExternalWait(fn func()) {
	incExternalWait()
	detachBubble()
	defer func() { reattachBubble(); decExternalWait() }()
	fn()
}

// IsInBubble reports whether the current goroutine is in a bubble.
//
//go:linkname IsInBubble
func IsInBubble() bool

// Association is the state of a pointer's bubble association.
type Association int

const (
	Unbubbled     = Association(iota) // not associated with any bubble
	CurrentBubble                     // associated with the current bubble
	OtherBubble                       // associated with a different bubble
)

// Associate attempts to associate p with the current bubble.
// It returns the new association status of p.
func Associate[T any](p *T) Association {
	// Ensure p escapes to permit us to attach a special to it.
	escapedP := abi.Escape(p)
	return Association(associate(unsafe.Pointer(escapedP)))
}

//go:linkname associate
func associate(p unsafe.Pointer) int

// Disassociate disassociates p from any bubble.
func Disassociate[T any](p *T) {
	disassociate(unsafe.Pointer(p))
}

//go:linkname disassociate
func disassociate(b unsafe.Pointer)

// IsAssociated reports whether p is associated with the current bubble.
func IsAssociated[T any](p *T) bool {
	return isAssociated(unsafe.Pointer(p))
}

//go:linkname isAssociated
func isAssociated(p unsafe.Pointer) bool

//go:linkname acquire
func acquire() any

//go:linkname release
func release(any)

//go:linkname inBubble
func inBubble(any, func())

// A Bubble is a synctest bubble.
//
// Not a public API. Used by syscall/js to propagate bubble membership through syscalls.
type Bubble struct {
	b any
}

// Acquire returns a reference to the current goroutine's bubble.
// The bubble will not become idle until Release is called.
func Acquire() *Bubble {
	if b := acquire(); b != nil {
		return &Bubble{b}
	}
	return nil
}

// Release releases the reference to the bubble,
// allowing it to become idle again.
func (b *Bubble) Release() {
	if b == nil {
		return
	}
	release(b.b)
	b.b = nil
}

// Run executes f in the bubble.
// The current goroutine must not be part of a bubble.
func (b *Bubble) Run(f func()) {
	if b == nil {
		f()
	} else {
		inBubble(b.b, f)
	}
}

// Ready is a one-use acknowledgement of owned runnable registration. It is
// not an acknowledgement that the callback or application work has completed.
// Create it with NewReady; the zero value is invalid.
type Ready struct{ token any }

func NewReady() *Ready       { return &Ready{token: newReady()} }
func (r *Ready) Ready() bool { return readyDone(r.token) }

// Wait parks the coordinator until registration. Call only outside a bubble.
func (r *Ready) Wait() { readyWait(r.token) }

// ExternalWaitReady is ExternalWait with a one-use owner-registration token.
// fn must perform one logical receive/close operation, not unrelated blocking
// work: the first wake acknowledges registration. For an immediate operation,
// wrapper exit cooperatively registers before returning to ordinary target code.
// Wait does not require the target to resume or finish its callback.
func ExternalWaitReady(fn func(), r *Ready) {
	incExternalWait()
	defer decExternalWait()
	armReady(r.token)
	detachBubble()
	defer func() { reattachBubble(); finishReady() }()
	fn()
}

//go:linkname newReady
func newReady() any

//go:linkname readyDone
func readyDone(any) bool

//go:linkname readyWait
func readyWait(any)

//go:linkname armReady
func armReady(any)

//go:linkname finishReady
func finishReady()

// SelectState describes initially enabled selectgo cases. Cases are runtime
// indices (send partition followed by receives), not source-case ordinals.
// -1 is default. Site is a main-module-relative PC in the exact pinned binary.
type SelectState struct {
	Step      int32
	Goroutine uint32
	Site      uintptr
	CaseN     int32
	Cases     [16]int32
}

//go:linkname SetSelectHook
func SetSelectHook(func(SelectState) int32)

//go:linkname Yield
func Yield()
