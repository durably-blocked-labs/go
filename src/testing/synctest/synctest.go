// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package synctest provides support for testing concurrent code.
//
// The [Test] function runs a function in an isolated "bubble".
// Any goroutines started within the bubble are also part of the bubble.
//
// Each test should be entirely self-contained:
// The following guidelines should apply to most tests:
//
//   - Avoid interacting with goroutines not started from within the test.
//   - Avoid using the network. Use a fake network implementation as needed.
//   - Avoid interacting with external processes.
//   - Avoid leaking goroutines in background tasks.
//
// # Time
//
// Within a bubble, the [time] package uses a fake clock.
// Each bubble has its own clock.
// The initial time is midnight UTC 2000-01-01.
//
// Time in a bubble only advances when every goroutine in the
// bubble is durably blocked.
// See below for the exact definition of "durably blocked".
//
// For example, this test runs immediately rather than taking
// two seconds:
//
//	func TestTime(t *testing.T) {
//		synctest.Test(t, func(t *testing.T) {
//			start := time.Now() // always midnight UTC 2000-01-01
//			go func() {
//				time.Sleep(1 * time.Second)
//				t.Log(time.Since(start)) // always logs "1s"
//			}()
//			time.Sleep(2 * time.Second) // the goroutine above will run before this Sleep returns
//			t.Log(time.Since(start))    // always logs "2s"
//		})
//	}
//
// Time stops advancing when the root goroutine of the bubble exits.
//
// # Blocking
//
// A goroutine in a bubble is "durably blocked" when it is blocked
// and can only be unblocked by another goroutine in the same bubble.
// A goroutine which can be unblocked by an event from outside its
// bubble is not durably blocked.
//
// The [Wait] function blocks until all other goroutines in the
// bubble are durably blocked.
//
// For example:
//
//	func TestWait(t *testing.T) {
//		synctest.Test(t, func(t *testing.T) {
//			done := false
//			go func() {
//				done = true
//			}()
//			// Wait will block until the goroutine above has finished.
//			synctest.Wait()
//			t.Log(done) // always logs "true"
//		})
//	}
//
// When every goroutine in a bubble is durably blocked:
//
//   - [Wait] returns, if it has been called.
//   - Otherwise, time advances to the next time that will
//     unblock at least one goroutine, if there is such a time
//     and the root goroutine of the bubble has not exited.
//   - Otherwise, there is a deadlock and [Test] panics.
//
// The following operations durably block a goroutine:
//
//   - a blocking send or receive on a channel created within the bubble
//   - a blocking select statement where every case is a channel created
//     within the bubble
//   - [sync.Cond.Wait]
//   - [sync.WaitGroup.Wait], when [sync.WaitGroup.Add] was called within the bubble
//   - [time.Sleep]
//
// Operations not in the above list are not durably blocking.
// In particular, the following operations may block a goroutine,
// but are not durably blocking because the goroutine can be unblocked
// by an event occurring outside its bubble:
//
//   - locking a [sync.Mutex] or [sync.RWMutex]
//   - blocking on I/O, such as reading from a network socket
//   - system calls
//
// # Isolation
//
// A channel, [time.Timer], or [time.Ticker] created within a bubble
// is associated with it. Operating on a bubbled channel, timer, or
// ticker from outside the bubble panics.
//
// A [sync.WaitGroup] becomes associated with a bubble on the first
// call to Add or Go. Once a WaitGroup is associated with a bubble,
// calling Add or Go from outside that bubble is a fatal error.
// (As a technical limitation, a WaitGroup defined as a package
// variable, such as "var wg sync.WaitGroup", cannot be associated
// with a bubble and operations on it may not be durably blocking.
// This limitation does not apply to a *WaitGroup stored in a
// package variable, such as "var wg = new(sync.WaitGroup)".)
//
// [sync.Cond.Wait] is durably blocking. Waking a goroutine in a bubble
// blocked on Cond.Wait from outside the bubble is a fatal error.
//
// Cleanup functions and finalizers registered with
// [runtime.AddCleanup] and [runtime.SetFinalizer]
// run outside of any bubble.
//
// # Example: Context.AfterFunc
//
// This example demonstrates testing the [context.AfterFunc] function.
//
// AfterFunc registers a function to execute in a new goroutine
// after a context is canceled.
//
// The test verifies that the function is not run before the context is canceled,
// and is run after the context is canceled.
//
//	func TestContextAfterFunc(t *testing.T) {
//		synctest.Test(t, func(t *testing.T) {
//			// Create a context.Context which can be canceled.
//			ctx, cancel := context.WithCancel(t.Context())
//
//			// context.AfterFunc registers a function to be called
//			// when a context is canceled.
//			afterFuncCalled := false
//			context.AfterFunc(ctx, func() {
//				afterFuncCalled = true
//			})
//
//			// The context has not been canceled, so the AfterFunc is not called.
//			synctest.Wait()
//			if afterFuncCalled {
//				t.Fatalf("before context is canceled: AfterFunc called")
//			}
//
//			// Cancel the context and wait for the AfterFunc to finish executing.
//			// Verify that the AfterFunc ran.
//			cancel()
//			synctest.Wait()
//			if !afterFuncCalled {
//				t.Fatalf("after context is canceled: AfterFunc not called")
//			}
//		})
//	}
//
// # Example: Context.WithTimeout
//
// This example demonstrates testing the [context.WithTimeout] function.
//
// WithTimeout creates a context which is canceled after a timeout.
//
// The test verifies that the context is not canceled before the timeout expires,
// and is canceled after the timeout expires.
//
//	func TestContextWithTimeout(t *testing.T) {
//		synctest.Test(t, func(t *testing.T) {
//			// Create a context.Context which is canceled after a timeout.
//			const timeout = 5 * time.Second
//			ctx, cancel := context.WithTimeout(t.Context(), timeout)
//			defer cancel()
//
//			// Wait just less than the timeout.
//			time.Sleep(timeout - time.Nanosecond)
//			synctest.Wait()
//			if err := ctx.Err(); err != nil {
//				t.Fatalf("before timeout: ctx.Err() = %v, want nil\n", err)
//			}
//
//			// Wait the rest of the way until the timeout.
//			time.Sleep(time.Nanosecond)
//			synctest.Wait()
//			if err := ctx.Err(); err != context.DeadlineExceeded {
//				t.Fatalf("after timeout: ctx.Err() = %v, want DeadlineExceeded\n", err)
//			}
//		})
//	}
//
// # Example: HTTP 100 Continue
//
// This example demonstrates testing [http.Transport]'s 100 Continue handling.
//
// An HTTP client sending a request can include an "Expect: 100-continue" header
// to tell the server that the client has additional data to send.
// The server may then respond with an 100 Continue information response
// to request the data, or some other status to tell the client the data is not needed.
// For example, a client uploading a large file might use this feature to confirm
// that the server is willing to accept the file before sending it.
//
// This test confirms that when sending an "Expect: 100-continue" header
// the HTTP client does not send a request's content before the server requests it,
// and that it does send the content after receiving a 100 Continue response.
//
//	func TestHTTPTransport100Continue(t *testing.T) {
//		synctest.Test(t, func(*testing.T) {
//			// Create an in-process fake network connection.
//			// We cannot use a loopback network connection for this test,
//			// because goroutines blocked on network I/O prevent a synctest
//			// bubble from becoming idle.
//			srvConn, cliConn := net.Pipe()
//			defer cliConn.Close()
//			defer srvConn.Close()
//
//			tr := &http.Transport{
//				// Use the fake network connection created above.
//				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
//					return cliConn, nil
//				},
//				// Enable "Expect: 100-continue" handling.
//				ExpectContinueTimeout: 5 * time.Second,
//			}
//
//			// Send a request with the "Expect: 100-continue" header set.
//			// Send it in a new goroutine, since it won't complete until the end of the test.
//			body := "request body"
//			go func() {
//				req, _ := http.NewRequest("PUT", "http://test.tld/", strings.NewReader(body))
//				req.Header.Set("Expect", "100-continue")
//				resp, err := tr.RoundTrip(req)
//				if err != nil {
//					t.Errorf("RoundTrip: unexpected error %v\n", err)
//				} else {
//					resp.Body.Close()
//				}
//			}()
//
//			// Read the request headers sent by the client.
//			req, err := http.ReadRequest(bufio.NewReader(srvConn))
//			if err != nil {
//				t.Fatalf("ReadRequest: %v\n", err)
//			}
//
//			// Start a new goroutine copying the body sent by the client into a buffer.
//			// Wait for all goroutines in the bubble to block and verify that we haven't
//			// read anything from the client yet.
//			var gotBody bytes.Buffer
//			go io.Copy(&gotBody, req.Body)
//			synctest.Wait()
//			if got, want := gotBody.String(), ""; got != want {
//				t.Fatalf("before sending 100 Continue, read body: %q, want %q\n", got, want)
//			}
//
//			// Write a "100 Continue" response to the client and verify that
//			// it sends the request body.
//			srvConn.Write([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
//			synctest.Wait()
//			if got, want := gotBody.String(), body; got != want {
//				t.Fatalf("after sending 100 Continue, read body: %q, want %q\n", got, want)
//			}
//
//			// Finish up by sending the "200 OK" response to conclude the request.
//			srvConn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
//
//			// We started several goroutines during the test.
//			// The synctest.Test call will wait for all of them to exit before returning.
//		})
//	}
package synctest

import (
	"internal/synctest"
	"testing"
	_ "unsafe" // for linkname
)

// Decision records a schedule (Kind=0) or opt-in initially enabled select
// (Kind=1). For select, Index is a runtime case index, RunqBgids encodes case
// index+1 (zero = default), ChosenBgid is the actor, Site is main-text-relative.
// ChosenSpawnPC is process-local diagnostic information, not replay identity.
type Decision = synctest.Decision

// BubbleState describes the state of a bubble at a scheduling decision point.
type BubbleState = synctest.BubbleState

// Test executes f in a new bubble.
//
// Test waits for all goroutines in the bubble to exit before returning.
// If the goroutines in the bubble become deadlocked, the test fails.
// Returns the full scheduling trace (all decisions made during execution).
//
// If prefix is provided, the scheduler follows those decisions before
// making its own choices. Each call to Test creates a new bubble
// (a new interleaving exploration).
//
// Test must not be called from within a bubble.
//
// The [*testing.T] provided to f has the following properties:
//
//   - T.Cleanup functions run inside the bubble,
//     immediately before Test returns.
//   - T.Context returns a [context.Context] with a Done channel
//     associated with the bubble.
//   - T.Run, T.Parallel, and T.Deadline must not be called.
func Test(t *testing.T, f func(*testing.T), prefix ...[]Decision) []Decision {
	var pfx []Decision
	if len(prefix) > 0 {
		pfx = prefix[0]
	}
	var ok bool
	trace := synctest.RunExplore(func() {
		ok = testingSynctestTest(t, f)
	}, pfx)
	if !ok {
		// Fail the test outside the bubble,
		// so test durations get set using real time.
		t.FailNow()
	}
	return trace
}

//go:linkname testingSynctestTest testing/synctest.testingSynctestTest
func testingSynctestTest(t *testing.T, f func(*testing.T)) bool

// Explore executes f in a new bubble with the given prefix and returns
// the scheduling trace and whether the test passed.
// Unlike Test, Explore does not call t.FailNow() on failure,
// allowing the caller (e.g., an explorer) to handle failures.
// If the root panics (e.g., deadlock), returns (nil, false). Such a result
// makes the worker process unsafe to reuse. Prefer ExploreResult for the cause.
func Explore(t *testing.T, f func(*testing.T), prefix []Decision) (trace []Decision, ok bool) {
	r := ExploreResult(t, f, prefix)
	return r.Trace, r.OK
}

// Fixed capacities of the experimental runtime. Capacity exhaustion fails closed.
const (
	MaxDecisions        = 512
	MaxRunnable         = 16
	MaxSelectCases      = 16
	DecisionSchedule    = 0
	DecisionSelectReady = 1
	WaitReasonYield     = 255
)

// Result describes a completed exploration or strict replay. Cause retains a
// recovered root panic (including errors implementing Limit() string).
// WorkerUnsafe means the process MUST NOT execute another trial: recovering
// a panic does not cancel or join surviving goroutines. Dispose of the process.
// Fatal runtime throws cannot be recovered and terminate the process directly.
type Result struct {
	Trace        []Decision
	OK           bool
	Cause        any
	WorkerUnsafe bool
}

// ExploreResult follows a positional exploration prefix, then records new
// choices. Unlike strict replay, prefixes intentionally do not assert identity.
func ExploreResult(t *testing.T, f func(*testing.T), prefix []Decision) Result {
	return runResult(t, f, prefix, false)
}

// StrictReplay requires the exact ordered runnable identities and chosen
// identity at every event, and complete trace consumption. It is distinct from
// exploration. Trace PCs are process-local; portable source identity and final
// application outcome must additionally be checked by the worker harness.
func StrictReplay(t *testing.T, f func(*testing.T), trace []Decision) Result {
	return runResult(t, f, trace, true)
}

func runResult(t *testing.T, f func(*testing.T), prefix []Decision, strict bool) (r Result) {
	defer func() {
		if cause := recover(); cause != nil {
			r.OK = false
			r.Cause = cause
			r.WorkerUnsafe = true
		}
	}()
	f0 := func() { r.OK = testingSynctestTest(t, f) }
	if strict {
		r.Trace = synctest.RunReplay(f0, prefix)
	} else {
		r.Trace = synctest.RunExplore(f0, prefix)
	}
	return r
}

// SetDecisionHook registers a function that is called at each frontier
// scheduling decision point within the current bubble. The hook receives
// the bubble state (runnable goroutines, blocked count, fake time, etc.)
// and returns the index of the goroutine to schedule next (0 = FIFO default).
// When state.Idle is true, returning a negative value delegates back to the
// default synctest idle/time handling until a real frontier or ExternalWait idle
// event reclaims control (including across transient wakeups).
//
// SetDecisionHook must be called from within a bubble.
// Passing nil restores the default FIFO scheduling.
func SetDecisionHook(hook func(BubbleState) int32) {
	synctest.SetDecisionHook(hook)
}

// MarkGlobal marks the current goroutine as global within its bubble.
// Children of a global goroutine inherit the global flag.
// When a global goroutine appears in the runq, the decision hook
// receives Global=true in the corresponding GoroutineInfo, allowing
// the orchestrator to distinguish local vs. cross-node decisions.
//
// MarkGlobal must be called from within a bubble.
func MarkGlobal() {
	synctest.MarkGlobal()
}

// External wraps fn with the external counter so the bubble parks
// properly when fn blocks on an external channel. Does NOT mark the
// goroutine as global — the decision hook sees it as local (FIFO).
//
// Use this for external operations (e.g., redis calls) that need the
// bubble to park but should not be forwarded to the orchestrator.
//
// External must be called from within a bubble. External wrappers cannot nest;
// state is restored on return, panic, and runtime.Goexit.
func External(fn func()) {
	synctest.External(fn)
}

// CallExternal marks the current goroutine as global and wraps fn
// with the external counter. Global goroutines are forwarded to the
// orchestrator by the decision hook.
//
// Use this for RPC calls or other external operations whose scheduling
// decisions should be controlled by the orchestrator.
//
// CallExternal must be called from within a bubble.
func CallExternal(fn func()) {
	synctest.CallExternal(fn)
}

// ExternalWait wraps fn so the bubble correctly tracks goroutines
// waiting on orchestrator-controlled channels. The goroutine is
// detached from the bubble during fn (channels created inside fn
// are untagged).
//
// Unlike External (which tracks generic external IO), ExternalWait
// tells the bubble that the orchestrator CAN unblock this goroutine.
// When all goroutines are blocked with externalWait > 0, the decision
// hook fires with Idle: true, allowing the orchestrator to deliver a
// message or advance time.
//
// ExternalWait must be called from within a bubble.
func ExternalWait(fn func()) {
	synctest.ExternalWait(fn)
}

// SetTime sets the bubble's fake clock to t (nanoseconds since epoch).
// If t is before the current time, the call is a no-op.
//
// When the event loop next iterates, any timers with deadlines <= t
// fire automatically.
//
// SetTime is intended to be called from inside the decision hook
// (on the root goroutine) by the orchestrator, to synchronize time
// across multiple bubbles.
//
// When externalWait > 0 and a hook is set, timer auto-advancement is
// disabled — the orchestrator owns time via SetTime.
//
// SetTime must be called from within a bubble.
func SetTime(t int64) {
	synctest.SetTime(t)
}

// Wait blocks until every goroutine within the current bubble,
// other than the current goroutine, is durably blocked.
//
// Wait must not be called from outside a bubble.
// Wait must not be called concurrently by multiple goroutines
// in the same bubble.
func Wait() {
	synctest.Wait()
}

// Ready acknowledges owned runnable registration, NOT callback completion.
// The token is one-use; its zero value is invalid. Ready.Wait must be called
// outside a bubble and safely parks until registration, without polling.
type Ready = synctest.Ready

func NewReady() *Ready { return synctest.NewReady() }

// ExternalWaitReady performs one logical receive/close callback and acknowledges
// its owner-queue registration with r. No unrelated blocking work may occur in
// fn: its first wake signals r. An immediate/nonparking callback cooperatively
// queues on exit. Acknowledgement does not require application execution while
// the bubble is suspended. Reuse or nested external wrappers panic.
func ExternalWaitReady(fn func(), r *Ready) { synctest.ExternalWaitReady(fn, r) }

// SelectState describes enabled runtime cases, including -1 for default.
// Cases are runtime send/receive partition indices, NOT source-case ordinals.
// Site is relative to the main text module; replay requires the identical binary.
type SelectState = synctest.SelectState

// SetSelectHook opts into explicit initially enabled selectgo case choices.
// The hook returns a runtime case index from state.Cases[:state.CaseN], NOT an
// array offset. It runs outside channel locks, with ordinary target execution
// suspended. Readiness is revalidated under locks before committing; changed
// enabled sets fail closed. nil restores ordinary randomized select behavior
// and schedule-only traces. Only bubble-owned channels are supported (nil is
// ignored); closed sends remain eligible and panic normally if selected.
//
// Compiler-lowered selects (zero/one case and single-case-plus-default) do not
// call selectgo and are NOT controlled or recorded. reflect.Select and sites
// outside the main text module are rejected while control is enabled. A select
// that initially blocks has no explicit case event: its winning wake remains
// determined by controlled scheduling and channel operations. Case limits apply
// to all syntactic runtime cases, including nil cases. Hooks must not nest a
// controlled select. This is cooperative control, not arbitrary preemption.
func SetSelectHook(hook func(SelectState) int32) { synctest.SetSelectHook(hook) }

// Yield is a cooperative scheduling point equivalent to runtime.Gosched inside
// a target bubble goroutine. The next schedule records WaitReason=255. The
// yielding goroutine may legally be selected immediately; no fairness or
// preemption bound is implied. It must not be called from a decision hook.
func Yield() { synctest.Yield() }
