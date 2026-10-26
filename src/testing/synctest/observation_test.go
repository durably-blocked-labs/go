// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package synctest_test

import (
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"testing/synctest"
)

func TestObservationsDisabledAndCaller(t *testing.T) {
	processors := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(processors)
	gcPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gcPercent)
	runtime.GC()

	var disabled, recorded []synctest.Observation
	var disabledIncomplete, recordedIncomplete bool
	result := synctest.ExploreResult(t, func(*testing.T) {
		synctest.RecordObservation(17) // Disabled marks retain nothing.
		disabled, disabledIncomplete = synctest.Observations()
		synctest.EnableObservations()
		observationCallerMarker(23)
		recorded, recordedIncomplete = synctest.Observations()
	}, nil)
	if !result.OK || result.WorkerUnsafe {
		t.Fatalf("observation run failed: %v unsafe=%v", result.Cause, result.WorkerUnsafe)
	}
	if len(disabled) != 0 || disabledIncomplete || recordedIncomplete {
		t.Fatalf("disabled=%d disabledIncomplete=%v recordedIncomplete=%v", len(disabled), disabledIncomplete, recordedIncomplete)
	}
	for _, event := range recorded {
		if event.Kind != synctest.ObservationUser || event.Tag != 23 {
			continue
		}
		if event.StackN == 0 {
			t.Fatal("application mark has no caller stack")
		}
		frame, _ := runtime.CallersFrames(event.StackPCs[:event.StackN]).Next()
		if !strings.HasSuffix(frame.Function, ".observationCallerMarker") {
			t.Fatalf("mark identifies %q instead of its application caller", frame.Function)
		}
		return
	}
	t.Fatal("application mark was not retained")
}

//go:noinline
func observationCallerMarker(tag uint32) { synctest.RecordObservation(tag) }

func TestObservationsOverflowKeepsPublishedPrefix(t *testing.T) {
	processors := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(processors)
	gcPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gcPercent)
	runtime.GC()

	var first, second []synctest.Observation
	var firstIncomplete, secondIncomplete bool
	result := synctest.ExploreResult(t, func(*testing.T) {
		synctest.EnableObservations()
		for i := 0; i <= synctest.ObservationCapacity; i++ {
			synctest.RecordObservation(29)
		}
		first, firstIncomplete = synctest.Observations()
		// Later marks must not overwrite the published prefix or make an
		// incomplete observation stream appear complete again.
		for i := 0; i < 10; i++ {
			synctest.RecordObservation(31)
		}
		second, secondIncomplete = synctest.Observations()
	}, nil)
	if !result.OK || result.WorkerUnsafe {
		t.Fatalf("overflow run failed: %v unsafe=%v", result.Cause, result.WorkerUnsafe)
	}
	if !firstIncomplete || !secondIncomplete || len(first) != synctest.ObservationCapacity {
		t.Fatalf("overflow flags=%v/%v retained=%d", firstIncomplete, secondIncomplete, len(first))
	}
	for i, event := range first {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("noncontiguous published prefix at %d: sequence %d", i, event.Sequence)
		}
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("later overflow marks changed already-published observations")
	}
}
