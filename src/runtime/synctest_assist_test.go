// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"internal/synctest"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
)

// This uses real allocation debt and observes the hook parked inside an assist,
// not merely a hook waiting for another goroutine's GC. The existing GC test
// gate holds mark termination until the observer has checked disassociation.
func TestSynctestSelectHookAssistOwnership(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	runtime.GC()
	request, marking, gcDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	actor := make(chan *runtime.G, 1)
	observed := make(chan bool, 1)
	var ordinaryProgress atomic.Bool
	var retained []byte
	runtime.SetSpinInGCMarkDone(true)
	defer runtime.SetSpinInGCMarkDone(false)
	go func() {
		<-request
		runtime.GC()
		close(gcDone)
	}()
	go func() {
		gp := <-actor
		for !runtime.SynctestGCMarking() {
			runtime.Gosched()
		}
		close(marking)
		for {
			waiting, home := runtime.SynctestAssistWaiting(gp)
			if waiting {
				observed <- home && !ordinaryProgress.Load()
				runtime.SetSpinInGCMarkDone(false)
				return
			}
			runtime.Gosched()
		}
	}()
	synctest.Run(func() {
		synctest.SetSelectHook(func(s synctest.SelectState) int32 {
			actor <- runtime.Getg()
			close(request)
			<-marking
			runtime.SynctestSetAssistDebt()
			retained = make([]byte, 64<<10)
			if !runtime.SynctestOnOwner() {
				t.Error("allocating hook resumed without its owner or membership")
			}
			// Also exercise voluntary rescheduling after the real assist.
			runtime.Gosched()
			if !runtime.SynctestOnOwner() {
				t.Error("yielding hook lost owner")
			}
			return s.Cases[0]
		})
		a, b := make(chan int, 1), make(chan int, 1)
		a <- 1
		b <- 2
		go func() { ordinaryProgress.Store(true) }()
		select {
		case <-a:
		case <-b:
		}
		synctest.Wait()
	})
	if !<-observed {
		t.Fatal("parked assist lost home or ordinary target ran during hook")
	}
	<-gcDone
	runtime.KeepAlive(retained)
}
