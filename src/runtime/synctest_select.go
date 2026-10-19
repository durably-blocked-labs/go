// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import _ "unsafe" // for go:linkname

// Layout must match internal/synctest.SelectState.
type bubbleSelectState struct {
	step      int32
	goroutine uint32
	site      uintptr
	caseN     int32
	cases     [bubbleMaxRunq]int32
}

//go:linkname synctestSetSelectHook internal/synctest.SetSelectHook
func synctestSetSelectHook(hook func(bubbleSelectState) int32) {
	b := getg().bubble
	if b == nil {
		panic("synctest: SetSelectHook outside bubble")
	}
	b.onSelect = hook
}

//go:linkname synctestYield internal/synctest.Yield
func synctestYield() {
	b := getg().bubble
	if b == nil || b.rootInHook {
		panic("synctest: Yield requires a target bubble goroutine")
	}
	b.yieldReason = 255
	Gosched()
}

// Called only with all select channel locks held. Do not reserve a sudog while
// consulting the hook: reservation would strand its peer if the hook panicked.
func synctestSelectEnabled(state *bubbleSelectState, cases []scase, nsends int, block bool) {
	for i := range cases {
		c := cases[i].c
		if c == nil {
			continue
		}
		enabled := c.closed != 0
		if i < nsends {
			enabled = enabled || c.qcount < c.dataqsiz || synctestWaitqReady(&c.recvq)
		} else {
			enabled = enabled || c.qcount > 0 || synctestWaitqReady(&c.sendq)
		}
		if enabled {
			state.cases[state.caseN] = int32(i)
			state.caseN++
		}
	}
	if state.caseN == 0 && !block {
		state.cases[0] = -1
		state.caseN = 1
	}
}

func synctestWaitqReady(q *waitq) bool {
	for sg := q.first; sg != nil; sg = sg.next {
		if !sg.isSelect || sg.g.selectDone.Load() == 0 {
			return true
		}
	}
	return false
}

func synctestSelectFail(b *synctestBubble, reason string) {
	var cause any = plainError(reason)
	if reason == "synctest resource limit: select cases" {
		cause = synctestLimitError("select cases")
	}
	if reason == "synctest resource limit: decisions" {
		cause = synctestLimitError("decisions")
	}
	b.controlFailure = cause // a target recovering the panic cannot accept a partial trace
	panic(cause)
}

// Called without channel locks. While a hook blocks, the owning P can service
// safepoints/GC and only this hook goroutine, not ordinary target execution.
func synctestSelectChoice(b *synctestBubble, state bubbleSelectState) int32 {
	if b.rootInHook {
		synctestSelectFail(b, "synctest: nested controlled select in hook")
	}
	if state.site < firstmoduledata.text || state.site >= firstmoduledata.etext {
		synctestSelectFail(b, "synctest: controlled select outside main text module")
	}
	state.site -= firstmoduledata.text
	if b.decisionLen >= bubbleMaxDecisions {
		synctestSelectFail(b, "synctest resource limit: decisions")
	}
	n := b.decisionLen
	chosen := state.cases[0]
	if n < b.decisionFence {
		expected := b.decisions[n]
		if expected.kind != 1 {
			synctestSelectFail(b, "synctest replay divergence: expected event kind is not select")
		}
		chosen = expected.index
		if b.strictReplay {
			if expected.step != n || expected.site != state.site || expected.chosenBgid != state.goroutine || expected.runqSize != state.caseN || expected.waitReason != 0 {
				synctestSelectFail(b, "synctest replay divergence: select identity")
			}
			for i := int32(0); i < state.caseN; i++ {
				if expected.runqBgids[i] != uint32(state.cases[i]+1) {
					synctestSelectFail(b, "synctest replay divergence: enabled select set")
				}
			}
		}
	} else {
		if b.strictReplay {
			synctestSelectFail(b, "synctest replay divergence: extra select event")
		}
		func() {
			returned := false
			b.hookG = getg()
			b.rootInHook = true
			defer func() {
				b.rootInHook = false
				b.hookG = nil
				if !returned {
					b.controlFailure = plainError("synctest: select hook did not return")
				}
			}()
			chosen = b.onSelect(state)
			returned = true
		}()
	}
	valid := false
	for i := int32(0); i < state.caseN; i++ {
		if state.cases[i] == chosen {
			valid = true
		}
	}
	if !valid {
		synctestSelectFail(b, "synctest replay divergence: selected case not enabled")
	}
	d := bubbleDecision{kind: 1, step: n, index: chosen, chosenBgid: state.goroutine, site: state.site, runqSize: state.caseN}
	for i := int32(0); i < state.caseN; i++ {
		d.runqBgids[i] = uint32(state.cases[i] + 1)
	}
	b.decisions[n] = d
	b.decisionLen = n + 1
	return chosen
}
