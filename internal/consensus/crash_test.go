package consensus

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// TestCrashAtEveryWrite crashes a lone node after each possible number of filesystem writes, with snapshots every
// 1 or 3 entries and 4 clients proposing at once, and checks it restarts holding every write it acked
func TestCrashAtEveryWrite(t *testing.T) {
	for k := range 100 {
		for _, snapEntries := range []uint64{1, 3} {
			for seed := uint64(1); seed <= 3; seed++ {
				t.Run(fmt.Sprintf("after %d writes, snapshot every %d, seed %d", k, snapEntries, seed), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) { crashAfter(t, k, snapEntries, seed) })
				})
			}
		}
	}
}

func crashAfter(t *testing.T, writes int, snapEntries, seed uint64) {
	c := newStoppedCluster(t, 1, seed)
	c.fsSeed, c.snapEntries = seed, snapEntries
	c.start("n1")
	c.fsys["n1"].FreezeAfter(writes)

	var mu sync.Mutex
	var acked []string
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 4 {
				cmd := fmt.Sprintf("c%d-%d", w, i)
				err := c.propose("n1", cmd, time.Second)
				var nle *NotLeaderError
				for deadline := time.Now().Add(2 * time.Second); errors.As(err, &nle) && time.Now().Before(deadline); {
					time.Sleep(10 * time.Millisecond)
					err = c.propose("n1", cmd, time.Second)
				}
				if err != nil {
					return
				}
				mu.Lock()
				acked = append(acked, cmd)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	c.crash("n1")
	c.start("n1")
	defer c.stopAll()

	marker := c.commitAny("marker")
	c.waitApplied(marker)
	for _, cmd := range acked {
		if !c.log("n1").has(cmd) {
			t.Fatalf("acked %q is missing after the crash and restart; acked %v", cmd, acked)
		}
	}
}
