// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package synctest

import _ "unsafe"

// Observation is experimental diagnostic data; layout matches runtime.bubbleObservation.
type Observation struct {
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

//go:linkname EnableObservations
func EnableObservations()

//go:linkname RecordObservation
func RecordObservation(tag uint32)

//go:linkname Observations
func Observations() ([]Observation, bool)
