package synctest_test

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"testing/synctest"
)

func TestRuntimeResultFailure(t *testing.T) {
	if mode := os.Getenv("SYNCTEST_RESULT_CHILD"); mode != "" {
		marker := &struct{ message string }{"exact hook cause"}
		var result synctest.Result
		if mode == "hook" {
			result = synctest.ExploreResult(t, func(t *testing.T) {
				synctest.SetDecisionHook(func(s synctest.BubbleState) int32 { panic(marker) })
				synctest.Yield()
			}, nil)
		} else {
			result = synctest.ExploreResult(t, func(t *testing.T) {}, make([]synctest.Decision, 513))
		}
		valid := !result.OK && result.WorkerUnsafe && result.Cause != nil
		if mode == "hook" {
			valid = valid && result.Cause == marker
		} else {
			e, ok := result.Cause.(interface{ Limit() string })
			valid = valid && ok && e.Limit() == "prefix"
		}
		if !valid {
			fmt.Fprintf(os.Stderr, "bad result: %+v\n", result)
			os.Exit(1)
		}
		// A failed bubble is process terminal, even when the panic was recovered.
		os.Exit(0)
	}
	for _, mode := range []string{"hook", "prefix"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeResultFailure$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "SYNCTEST_RESULT_CHILD="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}
