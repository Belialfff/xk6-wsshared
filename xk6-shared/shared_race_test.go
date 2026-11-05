package xk6_shared

import (
	"fmt"
	"sync"
	"testing"
)

// newInstance returns a ModuleInstance backed by the process-global store.
// (No k6 VU is needed for the store-level concurrency tests.)
func newInstance() *ModuleInstance {
	return &ModuleInstance{store: getStore()}
}

// resetStore wipes shared state between tests so they don't interfere.
func resetStore(mi *ModuleInstance) { mi.FlushAll() }

// TestRace_ScalarsConcurrent hammers scalar set/get/has/delete from many
// goroutines. Run with -race to detect data races.
func TestRace_ScalarsConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	const goroutines = 50
	const iters = 2000
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", id)
			for i := 0; i < iters; i++ {
				mi.Set(key, i)
				_ = mi.Get(key)
				_ = mi.Has(key)
				if i%100 == 0 {
					mi.Delete(key)
				}
				// Contended shared key touched by every goroutine.
				mi.Set("shared", id*1000+i)
				_ = mi.Get("shared")
			}
		}(g)
	}
	wg.Wait()
}

// TestRace_CountersConcurrent verifies atomic counters have no races AND are
// numerically exact under contention (no lost updates).
func TestRace_CountersConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	const goroutines = 64
	const iters = 5000
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				mi.Incr("counter")
				mi.Add("sum", 2)
			}
		}()
	}
	wg.Wait()

	wantIncr := int64(goroutines * iters)
	if got := mi.Count("counter"); got != wantIncr {
		t.Fatalf("Incr lost updates: got %d, want %d", got, wantIncr)
	}
	wantSum := int64(goroutines * iters * 2)
	if got := mi.Count("sum"); got != wantSum {
		t.Fatalf("Add lost updates: got %d, want %d", got, wantSum)
	}
}

// TestRace_PushUniqueConcurrent verifies pushUnique is race-free and that the
// dedup invariant holds under contention: N unique values pushed by many
// goroutines yield exactly N entries, and duplicates are all rejected.
func TestRace_PushUniqueConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	const goroutines = 40
	const uniquesPerG = 500
	var wg sync.WaitGroup
	var added int64
	var addedMu sync.Mutex

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			local := 0
			for i := 0; i < uniquesPerG; i++ {
				v := id*uniquesPerG + i // globally unique
				if mi.PushUnique("pool", v) {
					local++
				}
				// try to push a duplicate; must be rejected
				mi.PushUnique("pool", v)
			}
			addedMu.Lock()
			added += int64(local)
			addedMu.Unlock()
		}(g)
	}
	wg.Wait()

	wantN := goroutines * uniquesPerG
	if got := mi.Size("pool"); got != wantN {
		t.Fatalf("pool size wrong: got %d, want %d", got, wantN)
	}
	if added != int64(wantN) {
		t.Fatalf("added count wrong: got %d, want %d", added, wantN)
	}
}

// TestRace_ListMixedConcurrent mixes push/pop/shift/at/random/size/snapshot on
// the same list from many goroutines. Pure race detection (values are churned).
func TestRace_ListMixedConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	// seed so pops/shifts have something to take
	for i := 0; i < 1000; i++ {
		mi.Push("list", i)
	}

	const goroutines = 32
	const iters = 3000
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				switch i % 6 {
				case 0:
					mi.Push("list", id*100000+i)
				case 1:
					_ = mi.Pop("list")
				case 2:
					_ = mi.Shift("list")
				case 3:
					_ = mi.At("list", i%50)
				case 4:
					_ = mi.Random("list")
				case 5:
					_ = mi.Size("list")
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestRace_CompareAndSwapLeader verifies CAS elects EXACTLY ONE leader under
// a thundering herd, with no race.
func TestRace_CompareAndSwapLeader(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	mi.Set("leader", nil)

	const goroutines = 100
	var wg sync.WaitGroup
	var winners int64
	var winMu sync.Mutex
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start // release all at once for maximum contention
			if mi.CompareAndSwap("leader", nil, id) {
				winMu.Lock()
				winners++
				winMu.Unlock()
			}
		}(g)
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Fatalf("expected exactly 1 CAS winner, got %d", winners)
	}
}

// TestRace_SetIfAbsentOnce verifies setIfAbsent lets exactly one goroutine win.
func TestRace_SetIfAbsentOnce(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	const goroutines = 100
	var wg sync.WaitGroup
	var winners int64
	var winMu sync.Mutex
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			if mi.SetIfAbsent("once", id) {
				winMu.Lock()
				winners++
				winMu.Unlock()
			}
		}(g)
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Fatalf("expected exactly 1 setIfAbsent winner, got %d", winners)
	}
}

// TestRace_RandomNConcurrent stresses randomN + concurrent writers.
func TestRace_RandomNConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	for i := 0; i < 500; i++ {
		mi.Push("rp", i)
	}

	var wg sync.WaitGroup
	// readers
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				out := mi.RandomN("rp", 5)
				if len(out) > 5 {
					t.Errorf("randomN returned more than requested: %d", len(out))
				}
			}
		}()
	}
	// writers growing the pool concurrently
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				mi.Push("rp", 1000000+id*1000+i)
			}
		}(g)
	}
	wg.Wait()
}

// TestRace_EverythingAtOnce runs all operation families concurrently against a
// shared store — the broadest race surface.
func TestRace_EverythingAtOnce(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	var wg sync.WaitGroup
	families := []func(int){
		func(id int) { for i := 0; i < 3000; i++ { mi.Set(fmt.Sprintf("s%d", id), i); _ = mi.Get(fmt.Sprintf("s%d", id)) } },
		func(id int) { for i := 0; i < 3000; i++ { mi.Incr("gc"); mi.Add("ga", 3) } },
		func(id int) { for i := 0; i < 3000; i++ { mi.PushUnique("up", id*10000+i) } },
		func(id int) { for i := 0; i < 3000; i++ { mi.Push("lp", i); _ = mi.Random("lp"); _ = mi.Size("lp") } },
		func(id int) { for i := 0; i < 3000; i++ { _ = mi.CompareAndSwap("cas", nil, id); mi.Delete("cas") } },
		func(id int) { for i := 0; i < 500; i++ { _ = mi.Snapshot("lp"); _ = mi.Keys(); _ = mi.Lists() } },
	}
	for f, fn := range families {
		for r := 0; r < 6; r++ {
			wg.Add(1)
			go func(fn func(int), id int) { defer wg.Done(); fn(id) }(fn, f*100+r)
		}
	}
	wg.Wait()
}
