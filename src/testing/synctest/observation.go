// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package synctest

import "internal/synctest"

// Observation is one opt-in diagnostic event. Sequence orders reservations in
// one bubble, across scheduler picks, application marks, and GC stack scans.
// It is not a wall clock and does not prove when a resumed goroutine executes.
// NativeStep is the last recorded schedule step (-1 before the first pick
// observed after enablement). It is only correlation data, not replay identity.
// PC and StackPCs are process-local addresses. For a schedule event PC is the
// creation site, not its current instruction. User and scan stack PCs use the
// return-PC convention of runtime.Callers and should be resolved using
// runtime.CallersFrames. StackN can equal 16 when the stack is truncated.
// Stopped means suspendG must ready the target after scanning; false also
// includes scans of waiting or runnable goroutines. SelfScan identifies scans
// initiated by the target itself. StatusBeforeSuspend is the earlier runtime
// status sample and may differ from the state suspendG actually acquired.
// Scan stacks include Go frames only; foreign frames and registered cgo
// unwinders are deliberately omitted. A scan event alone does not establish
// interruption of application work.
//
// User events use caller-defined Tag values. Other kinds use the documented
// kind-specific tags below. Control* fields are queue/root diagnostics;
// StatusBeforeSuspend and STWReason use this runtime build's internal values.
// They are not portable replay identities or stable Go runtime state codes.
//
// This API and its struct layout are experimental Bubbles extensions. They
// can change between Bubbles versions. This API is separate from Decision data.
type Observation = synctest.Observation

// Observation kinds are diagnostic records, separate from Decision/replay.
const (
	ObservationSchedule    uint8 = 1
	ObservationUser        uint8 = 2
	ObservationGCScanBegin uint8 = 3
	// ObservationGCResumeRequested precedes resumeG. It does not establish
	// when the target resumes application execution.
	ObservationGCResumeRequested uint8 = 4
	// ObservationPreempt captures a runtime preemption while g0 owns the G.
	ObservationPreempt uint8 = 5
	// ObservationPreemptRequest is reserved for older diagnostic builds.
	// This runtime emits no remote request events: a requester does not own
	// the target's mutable membership or goroutine identity.
	ObservationPreemptRequest   uint8 = 6
	ObservationWorldStopWait    uint8 = 7
	ObservationWorldStopResumed uint8 = 8
	// ObservationWorldStopContinued resumes the interrupted STW continuation.
	// Its Tag is ObservationContinuationSTW.
	ObservationWorldStopContinued uint8 = 9
	// ObservationControlQueue snapshots the owner queue before a scheduling
	// decision or frontier hook. Tag is an ObservationControl* constant.
	ObservationControlQueue uint8 = 10
	// ObservationScanPark precedes relinquishing the collector/profiler scan
	// target. A later GC scan event is needed to attribute it to collection.
	ObservationScanPark uint8 = 11
	// ObservationRuntimeContinued resumes a reserved runtime continuation.
	// Tag is an ObservationContinuation* constant.
	ObservationRuntimeContinued uint8 = 12
	// ObservationGCInternalPause records a scoped GC yield or attempted park.
	// Tag is an ObservationGCPause* constant.
	ObservationGCInternalPause uint8 = 13
)

// Control-queue snapshot tags.
const (
	ObservationControlBeforeSchedule uint32 = 1
	ObservationControlBeforeFrontier uint32 = 2
)

// Continuation tags identify why runtime work paused an application actor.
const (
	ObservationContinuationSTW        uint32 = 1
	ObservationContinuationScan       uint32 = 2
	ObservationContinuationPreempt    uint32 = 3
	ObservationContinuationGCYield    uint32 = 4
	ObservationContinuationAssistDebt uint32 = 5
	ObservationContinuationGCWait     uint32 = 6
)

// Scoped GC pause tags. A generic wait callback can reject an attempted park;
// a matching continuation alone does not establish that the G was parked.
const (
	ObservationGCPauseYield uint32 = 1
	// ObservationGCPauseAssistDebt identifies the GC-assist queue park, whose
	// unlock callback commits it. This differs from merely doing assist work.
	ObservationGCPauseAssistDebt uint32 = 2
	ObservationGCPauseWait       uint32 = 3
)

// ObservationCapacity bounds retained events; ObservationStackDepth bounds
// captured Go frames. These limits do not bound workload or search size.
const (
	ObservationCapacity   = 4096
	ObservationStackDepth = 16
)

// EnableObservations allocates a fixed 4096-event buffer for the current bubble.
// Call once, before starting application actors. Recording is off by default.
// The enabled buffer uses about 2 MiB per bubble on 64-bit builds; its exact
// size depends on architecture and diagnostic layout. Disabled bubbles do not
// allocate this buffer.
// Enabling allocates memory; recording adds atomics, stack walks, and, for
// control snapshots, a runtime lock. These operations can perturb timing. This is diagnostic instrumentation.
// Stack capture stops once the buffer is full, and later attempts mark the
// snapshot incomplete. It panics outside a bubble or if already enabled.
func EnableObservations() { synctest.EnableObservations() }

// RecordObservation records tag, the actor, and its application call stack.
// Tags are caller-defined. This does not create an intentional yield point.
// It is a no-op when collection is disabled, and panics outside a bubble.
//
//go:noinline
func RecordObservation(tag uint32) { synctest.RecordObservation(tag) }

// Observations copies a snapshot of published observations in the current
// bubble. Call after application actors and requested GC activity have finished.
// Events generated while this function copies are outside its snapshot.
// The bool is true if the buffer overflowed or a reserved slot is unpublished;
// such a snapshot is incomplete and must not support an exact timeline claim.
// Overflow does not panic from the collector. Collection cannot be reset.
// It panics outside a bubble; when disabled it returns nil, false.
func Observations() ([]Observation, bool) { return synctest.Observations() }
