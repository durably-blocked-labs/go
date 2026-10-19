package synctest_test

import (
	"internal/synctest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRuntimeDecisionStorage(t *testing.T) {
	runtime.GC()
	var before, after, plateau runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 80; i++ {
		synctest.Run(func() {})
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	// The old persistent array retained about 48KiB per completed bubble.
	if after.OtherSys > before.OtherSys+1<<20 {
		t.Fatalf("OtherSys grew %d", after.OtherSys-before.OtherSys)
	}
	for i := 0; i < 80; i++ {
		synctest.Run(func() {})
	}
	runtime.GC()
	runtime.ReadMemStats(&plateau)
	if plateau.HeapAlloc > after.HeapAlloc+1<<20 {
		t.Fatalf("post-GC heap did not plateau: %d", plateau.HeapAlloc-after.HeapAlloc)
	}
}

func TestRuntimeDecisionCapacity(t *testing.T) {
	for _, n := range []int{510, 511} {
		tr := synctest.RunExplore(func() {
			for i := 0; i < n; i++ {
				runtime.Gosched()
			}
		}, nil)
		if len(tr) != n+1 {
			t.Fatalf("trace=%d want %d", len(tr), n+1)
		}
	}
	synctest.Run(func() {
		for i := 0; i < 15; i++ {
			go func() {}()
		}
		runtime.Gosched()
	})
}

func TestRuntimeExternalBalanced(t *testing.T) {
	synctest.Run(func() {
		for _, wrap := range []func(func()){synctest.External, synctest.CallExternal, synctest.ExternalWait} {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("missing panic")
					}
				}()
				wrap(func() { panic("callback") })
			}()
			if !synctest.IsInBubble() {
				t.Fatal("membership lost")
			}
			wrap(func() {
				func() {
					defer func() {
						if recover() == nil {
							t.Error("nesting accepted")
						}
					}()
					synctest.External(func() {})
				}()
			})
			go wrap(func() { runtime.Goexit() })
		}
		synctest.Wait()
	})
}

func TestRuntimeSuspendedHookGC(t *testing.T) {
	old := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(old)
	request := make(chan struct{})
	release := make(chan struct{})
	go func() { <-request; runtime.GC(); close(release) }()
	called := false
	synctest.Run(func() {
		synctest.SetDecisionHook(func(s synctest.BubbleState) int32 {
			if s.Idle {
				return -1
			}
			if !called {
				called = true
				close(request)
				<-release
			}
			return 0
		})
		runtime.Gosched()
	})
	if !called {
		t.Fatal("hook not called")
	}
}

func TestRuntimeForeignWakeups(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	var count atomic.Int32
	synctest.Run(func() {
		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Go(func() {
				for j := 0; j < 8; j++ {
					synctest.ExternalWait(func() {
						ch := make(chan struct{})
						go func() { runtime.Gosched(); close(ch) }()
						<-ch
					})
					count.Add(1)
					runtime.Gosched()
				}
			})
		}
		wg.Wait()
	})
	if count.Load() != 48 {
		t.Fatalf("got %d wakeups", count.Load())
	}
}

func TestRuntimeStrictReplay(t *testing.T) {
	f := func() { go func() {}(); runtime.Gosched() }
	trace := synctest.RunExplore(f, nil)
	got := synctest.RunReplay(f, trace)
	if len(got) != len(trace) {
		t.Fatal("trace length mismatch")
	}
}

// Failed bubbles may leave live goroutines. Each negative regression is a
// fresh process and exits immediately after checking the cause.
func TestRuntimeFailure(t *testing.T) {
	if mode := os.Getenv("SYNCTEST_RUNTIME_FAILURE"); mode != "" {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("failure accepted")
			}
			if mode == "prefix" || mode == "decisions" || mode == "runnable" {
				e, ok := r.(interface{ Limit() string })
				if !ok || e.Limit() != mode {
					t.Fatalf("cause %T %v", r, r)
				}
			}
			if mode != "prefix" && mode != "decisions" && mode != "runnable" {
				e, ok := r.(error)
				if !ok || !strings.Contains(e.Error(), "synctest replay divergence: event mismatch or incomplete consumption") {
					t.Fatalf("wrong replay cause: %T %v", r, r)
				}
			}
		}()
		switch mode {
		case "prefix":
			synctest.RunExplore(func() {}, make([]synctest.Decision, 513))
		case "decisions":
			synctest.Run(func() {
				for i := 0; i < 513; i++ {
					runtime.Gosched()
				}
			})
		case "runnable":
			synctest.Run(func() {
				for i := 0; i < 16; i++ {
					go func() {}()
				}
				runtime.Gosched()
			})
		case "identity":
			f := func() { runtime.Gosched() }
			tr := synctest.RunExplore(f, nil)
			tr[0].ChosenBgid = 999
			synctest.RunReplay(f, tr)
		case "extra":
			synctest.RunReplay(func() { runtime.Gosched() }, nil)
		case "unconsumed":
			tr := synctest.RunExplore(func() { runtime.Gosched() }, nil)
			tr = append(tr, tr[len(tr)-1])
			synctest.RunReplay(func() { runtime.Gosched() }, tr)
		}
		return
	}
	for _, mode := range []string{"prefix", "decisions", "runnable", "identity", "extra", "unconsumed"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeFailure$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "SYNCTEST_RUNTIME_FAILURE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}

func TestRuntimeLockedExternalWakeup(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	for i := 0; i < 3; i++ {
		ch := make(chan struct{})
		started := make(chan struct{})
		go func() { <-started; close(ch) }()
		synctest.Run(func() {
			var wg sync.WaitGroup
			wg.Go(func() {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
				synctest.ExternalWait(func() { close(started); <-ch })
			})
			wg.Wait()
		})
	}
}
