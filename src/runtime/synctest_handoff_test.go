//go:build unix

package runtime_test

import (
	"internal/synctest"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
)

func TestSynctestInboundSyscallHandoff(t *testing.T) {
	if os.Getenv("SYNCTEST_HANDOFF_CHILD") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSynctestInboundSyscallHandoff$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), "SYNCTEST_HANDOFF_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		return
	}
	runtime.GOMAXPROCS(4)
	for _, withGC := range []bool{false, true} {
		var fds [2]int
		if err := syscall.Pipe(fds[:]); err != nil {
			t.Fatal(err)
		}
		ch := make(chan struct{})
		progress := make(chan struct{})
		started := make(chan *runtime.G)
		producerDone := make(chan struct{})
		token := synctest.NewReady()
		go func() {
			defer close(producerDone)
			syscallG := <-started
			// Observe actual syscall entry, not a host-time sleep/retry guess.
			for !runtime.SynctestInSyscall(syscallG) {
				runtime.Gosched()
			}
			close(ch)
			token.Wait() // owned inbound publication has happened
			<-progress   // requires owner P handoff/drain, BEFORE releasing the syscall
			if withGC {
				runtime.GC()
			}
			if _, err := syscall.Write(fds[1], []byte{1}); err != nil {
				t.Error(err)
			}
		}()
		synctest.Run(func() {
			go func() { synctest.ExternalWaitReady(func() { <-ch }, token); close(progress) }()
			// Let the receiver detach/park before entering the real blocking syscall.
			runtime.Gosched()
			started <- runtime.Getg()
			var b [1]byte
			if n, err := syscall.Read(fds[0], b[:]); err != nil || n != 1 {
				t.Errorf("read %d %v", n, err)
			}
		})
		<-producerDone
		syscall.Close(fds[0])
		syscall.Close(fds[1])
	}
}

// Bounded coverage of the unlocked-to-locked transition after publication.
// This does not deterministically pause the returning M after its queue unlock;
// the saved-lockedness ordering itself is also checked by source review.
func TestSynctestSyscallLockAfterReturn(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	for i := 0; i < 24; i++ {
		var fds [2]int
		if err := syscall.Pipe(fds[:]); err != nil {
			t.Fatal(err)
		}
		ch, progress := make(chan struct{}), make(chan struct{})
		started := make(chan *runtime.G)
		done := make(chan struct{})
		token := synctest.NewReady()
		go func() {
			defer close(done)
			gp := <-started
			for !runtime.SynctestInSyscall(gp) {
				runtime.Gosched()
			}
			close(ch)
			token.Wait()
			<-progress // require owner handoff before permitting syscall return
			if _, err := syscall.Write(fds[1], []byte{1}); err != nil {
				t.Error(err)
			}
		}()
		synctest.Run(func() {
			go func() { synctest.ExternalWaitReady(func() { <-ch }, token); close(progress) }()
			runtime.Gosched()
			started <- runtime.Getg()
			var b [1]byte
			n, err := syscall.Read(fds[0], b[:])
			runtime.LockOSThread()
			// Let other Ms run while this G is now locked to its resuming M.
			runtime.Gosched()
			runtime.UnlockOSThread()
			if err != nil || n != 1 {
				t.Errorf("read %d %v", n, err)
			}
		})
		<-done
		syscall.Close(fds[0])
		syscall.Close(fds[1])
	}
}

func TestSynctestExternalNetpoll(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	for i := 0; i < 3; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		started := make(chan *runtime.G)
		done := make(chan struct{})
		go func() {
			defer close(done)
			gp := <-started
			for !runtime.SynctestIOWaiting(gp) {
				runtime.Gosched()
			}
			if _, err := w.Write([]byte{1}); err != nil {
				t.Error(err)
			}
		}()
		synctest.Run(func() {
			synctest.External(func() {
				started <- runtime.Getg()
				var b [1]byte
				if n, err := r.Read(b[:]); err != nil || n != 1 {
					t.Errorf("read %d %v", n, err)
				}
			})
		})
		<-done
		r.Close()
		w.Close()
	}
}
