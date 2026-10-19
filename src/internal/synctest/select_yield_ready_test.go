package synctest_test

import (
	"encoding/json"
	"internal/synctest"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func chooseLast(s synctest.SelectState) int32 { return s.Cases[s.CaseN-1] }

func selectWork() {
	synctest.SetSelectHook(chooseLast)
	a, b := make(chan int, 1), make(chan int, 1)
	a <- 1
	b <- 2
	select {
	case <-a:
	case <-b:
	}
	synctest.Yield()
}

func TestRuntimeSelectFreshReplay(t *testing.T) {
	if encoded := os.Getenv("SYNCTEST_SELECT_TRACE"); encoded != "" {
		var trace []synctest.Decision
		if err := json.Unmarshal([]byte(encoded), &trace); err != nil {
			t.Fatal(err)
		}
		synctest.RunReplay(selectWork, trace)
		return
	}
	trace := synctest.RunExplore(selectWork, nil)
	selects, yields := 0, 0
	for _, d := range trace {
		if d.Kind == 1 {
			selects++
			if d.Index != 1 || d.RunqSize != 2 || d.Site == 0 {
				t.Fatalf("select event %+v", d)
			}
		}
		if d.WaitReason == 255 {
			yields++
		}
	}
	if selects != 1 || yields != 1 {
		t.Fatalf("selects=%d yields=%d", selects, yields)
	}
	encoded, _ := json.Marshal(trace)
	cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeSelectFreshReplay$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "SYNCTEST_SELECT_TRACE="+string(encoded))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestRuntimeSelectCases(t *testing.T) {
	synctest.Run(func() {
		count := 0
		synctest.SetSelectHook(func(s synctest.SelectState) int32 { count++; return chooseLast(s) })
		a, b := make(chan int, 1), make(chan int, 1)
		a <- 10
		b <- 20
		got := 0
		select {
		case got = <-a:
		case got = <-b:
		}
		if got != 10 {
			t.Fatalf("two ready receive chose %d", got)
		}
		// Duplicate channel occurrences remain separate enabled cases.
		dup := make(chan int, 1)
		dup <- 1
		which := 0
		select {
		case <-dup:
			which = 1
		case <-dup:
			which = 2
		}
		if which != 1 {
			t.Fatal("duplicate occurrence lost")
		}
		// Sends precede receives in runtime indexing.
		empty := make(chan int, 1)
		full := make(chan int, 1)
		full <- 3
		select {
		case empty <- 4:
			t.Fatal("selected send, wanted receive")
		case <-full:
		}
		var nilch chan int
		closed := make(chan int)
		close(closed)
		recvOK := true
		select {
		case <-nilch:
			t.Fatal("nil chosen")
		case _, recvOK = <-closed:
		}
		if recvOK {
			t.Fatal("closed receive reported value")
		}
		select {
		case <-a:
		case <-b:
		default:
			t.Fatal("default chosen despite enabled receive")
		}
		select {
		case <-a:
			t.Fatal("empty receive")
		case <-b:
			t.Fatal("empty receive")
		default:
		}
		// A closed send is enabled and retains ordinary send-on-closed panic.
		panicked := false
		func() {
			defer func() { panicked = recover() != nil }()
			select {
			case <-nilch:
			case closed <- 1:
			}
		}()
		if !panicked {
			t.Fatal("closed send accepted")
		}
		if count != 7 {
			t.Fatalf("hooks=%d", count)
		}
		synctest.SetSelectHook(nil)
	})
}

func TestRuntimeScheduleOnlyCompatibility(t *testing.T) {
	tr := synctest.RunExplore(func() {
		synctest.SetDecisionHook(func(s synctest.BubbleState) int32 {
			if s.Idle {
				return -1
			}
			return 0
		})
		a, b := make(chan int, 1), make(chan int, 1)
		a <- 1
		b <- 1
		select {
		case <-a:
		case <-b:
		}
		runtime.Gosched()
	}, nil)
	for _, d := range tr {
		if d.Kind != 0 {
			t.Fatal("unexpected select event without opt-in")
		}
	}
	synctest.Run(func() {
		calls := 0
		synctest.SetSelectHook(func(s synctest.SelectState) int32 { calls++; return chooseLast(s) })
		c := make(chan int, 1)
		c <- 1
		select {
		case <-c:
		default:
		} // compiler lowered, not controlled
		if calls != 0 {
			t.Fatal("lowered select unexpectedly instrumented")
		}
	})
}

func TestRuntimeSelectHookGC(t *testing.T) {
	old := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(old)
	request, release := make(chan struct{}), make(chan struct{})
	go func() { <-request; runtime.GC(); close(release) }()
	synctest.Run(func() {
		synctest.SetSelectHook(func(s synctest.SelectState) int32 { close(request); <-release; return chooseLast(s) })
		a, b := make(chan int, 1), make(chan int, 1)
		a <- 1
		b <- 2
		select {
		case <-a:
		case <-b:
		}
	})
}

func TestRuntimeReady(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("root failure: %#v", r)
		}
	}()
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	for _, mode := range []string{"blocked", "closed", "buffered", "immediate-close"} {
		t.Run(mode, func(t *testing.T) {
			immediate := mode == "buffered" || mode == "immediate-close"
			ch := make(chan int, 1)
			if mode == "buffered" {
				ch <- 1
			}
			if mode == "immediate-close" {
				close(ch)
			}
			token := synctest.NewReady()
			pause, resume, controllerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var processed atomic.Int32
			var started atomic.Bool
			var phase, executedPhase atomic.Int32
			type observation struct {
				idle      bool
				n         int32
				ready     bool
				processed int32
			}
			var observations []observation
			go func() {
				defer close(controllerDone)
				<-pause
				if !immediate {
					if mode == "closed" {
						close(ch)
					} else {
						ch <- 1
					}
				}
				token.Wait()
				if !token.Ready() {
					t.Error("Wait returned before ready")
				}
				if processed.Load() != 0 {
					t.Errorf("target ran before resume (phase=%d executedPhase=%d)", phase.Load(), executedPhase.Load())
				}
				runtime.GC()
				close(resume)
			}()
			trace := synctest.RunExplore(func() {
				paused := false
				synctest.SetDecisionHook(func(s synctest.BubbleState) int32 {
					observations = append(observations, observation{s.Idle, s.RunnableN, token.Ready(), processed.Load()})
					if !paused && started.Load() && token.Ready() == immediate {
						paused = true
						phase.Store(1)
						close(pause)
						<-resume
						phase.Store(2)
					}
					if s.Idle {
						return -1
					}
					return 0
				})
				var wg sync.WaitGroup
				wg.Go(func() {
					started.Store(true)
					synctest.ExternalWaitReady(func() { <-ch }, token)
					executedPhase.Store(phase.Load())
					processed.Add(1)
				})
				wg.Wait()
				if !paused {
					t.Error("bubble never frozen")
				}
			}, nil)
			<-controllerDone
			if t.Failed() {
				t.Logf("observations=%+v trace=%+v", observations, trace)
			}
			if processed.Load() != 1 {
				t.Fatalf("processing count=%d", processed.Load())
			}
		})
	}
}

func TestRuntimeReadyReuse(t *testing.T) {
	token := synctest.NewReady()
	synctest.Run(func() {
		synctest.ExternalWaitReady(func() {}, token)
		if !token.Ready() {
			t.Fatal("immediate callback not acknowledged")
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Error("reuse accepted")
				}
			}()
			synctest.ExternalWaitReady(func() {}, token)
		}()
		if !synctest.IsInBubble() {
			t.Fatal("reuse failure lost membership")
		}
	})
	token.Wait()
}

func TestRuntimeSelectFailure(t *testing.T) {
	if mode := os.Getenv("SYNCTEST_SELECT_FAILURE"); mode != "" {
		defer func() {
			if recover() == nil {
				t.Fatal("invalid select accepted")
			}
		}()
		synctest.Run(func() {
			// A target recovering a control error must still fail the entire bubble.
			defer func() { recover() }()
			a, b := make(chan int, 1), make(chan int, 1)
			a <- 1
			b <- 2
			synctest.SetSelectHook(func(s synctest.SelectState) int32 {
				if mode == "stale" {
					<-b
					return 0
				}
				if mode == "disabled" {
					return 99
				}
				return chooseLast(s)
			})
			switch mode {
			case "reflect":
				reflect.Select([]reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(a)}})
			case "external":
				synctest.External(func() {}) // separately create unbubbled channel
				var external chan int
				synctest.External(func() { external = make(chan int, 1); external <- 1 })
				select {
				case <-external:
				case <-b:
				}
			default:
				select {
				case <-a:
				case <-b:
				}
			}
		})
		return
	}
	for _, mode := range []string{"stale", "disabled", "reflect", "external"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeSelectFailure$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "SYNCTEST_SELECT_FAILURE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}
