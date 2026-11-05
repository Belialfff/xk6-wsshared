package xk6_shared

import (
	"strconv"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Correctness under contention: many goroutines update the SAME key via the
// optimistic-retry core. Must have zero lost updates.
// ---------------------------------------------------------------------------

func TestUpdate_NoLostUpdatesConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	mi.Set("n", int64(0))

	const goroutines = 32
	const iters = 3000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				mi.updateValue("n", func(cur interface{}) interface{} {
					v, _ := cur.(int64)
					return v + 1
				})
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * iters)
	got, _ := mi.Get("n").(int64)
	if got != want {
		t.Fatalf("lost updates: got %d, want %d", got, want)
	}
}

// ===========================================================================
// Benchmarks. Run:
//   go test -bench=. -benchmem -run=^$ ./...
//   go test -bench=. -benchmem -cpuprofile cpu.prof -memprofile mem.prof -run=^$ ./...
// ===========================================================================

func BenchmarkSet(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mi.Set("k", i)
	}
}

func BenchmarkGet(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.Set("k", 42)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mi.Get("k")
	}
}

func BenchmarkIncr(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mi.Incr("c")
	}
}

func BenchmarkPush(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mi.Push("l", i)
	}
}

func BenchmarkPushUnique(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mi.PushUnique("u", i)
	}
}

func BenchmarkRandom(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	for i := 0; i < 100000; i++ {
		mi.Push("l", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mi.Random("l")
	}
}

func BenchmarkAt(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	for i := 0; i < 100000; i++ {
		mi.Push("l", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mi.At("l", i%100000)
	}
}

func BenchmarkGetPath(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.Flatten("t", map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 1}}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mi.GetPath("t", "a.b.c")
	}
}

func BenchmarkSetPath(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.Flatten("t", map[string]interface{}{"a": map[string]interface{}{"b": 0}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mi.SetPath("t", "a.b", i)
	}
}

// ------- Parallel benchmarks (simulate many VUs / goroutines) -------

func BenchmarkParallel_Incr(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mi.Incr("c")
		}
	})
}

func BenchmarkParallel_RandomRead(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	for i := 0; i < 100000; i++ {
		mi.Push("l", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = mi.Random("l")
		}
	})
}

func BenchmarkParallel_GetPath(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.Flatten("t", map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 1}}})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = mi.GetPath("t", "a.b.c")
		}
	})
}

func BenchmarkParallel_SetDistinctKeys(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	var ctr int64
	b.RunParallel(func(pb *testing.PB) {
		ctr++
		key := "k" + strconv.FormatInt(ctr, 10)
		i := 0
		for pb.Next() {
			mi.Set(key, i)
			i++
		}
	})
}

func BenchmarkParallel_MixedReadWrite(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	for i := 0; i < 10000; i++ {
		mi.Push("l", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%4 == 0 {
				mi.Push("l", i)
			} else {
				_ = mi.Random("l")
			}
			mi.Incr("ops")
			i++
		}
	})
}

// Contended update on ONE key — worst case for optimistic retry.
func BenchmarkParallel_UpdateSameKey(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.Set("n", int64(0))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mi.updateValue("n", func(cur interface{}) interface{} {
				v, _ := cur.(int64)
				return v + 1
			})
		}
	})
}
