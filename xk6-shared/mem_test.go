package xk6_shared

import (
	"fmt"
	"runtime"
	"testing"
)

func heapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / 1024 / 1024
}

// TestMemoryFootprint reports how much heap the store uses for large data.
// Run: go test -run TestMemoryFootprint -v ./...
func TestMemoryFootprint(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	base := heapMB()
	t.Logf("baseline heap: %.1f MB", base)

	// 1M integers in a pool
	mi.ClearList("ints")
	for i := 0; i < 1_000_000; i++ {
		mi.Push("ints", i)
	}
	afterInts := heapMB()
	t.Logf("1,000,000 ints in pool: +%.1f MB (%.2f bytes/elem)", afterInts-base, (afterInts-base)*1024*1024/1_000_000)

	// 1M small objects in a pool
	mi.ClearList("objs")
	for i := 0; i < 1_000_000; i++ {
		mi.Push("objs", map[string]interface{}{"id": i, "v": "xxxxxxxxxxxxxxxxxxxx"})
	}
	afterObjs := heapMB()
	t.Logf("1,000,000 small objects in pool: +%.1f MB (%.0f bytes/obj)", afterObjs-afterInts, (afterObjs-afterInts)*1024*1024/1_000_000)

	// 100k unique (pushUnique keeps an index too)
	mi.ClearList("uniq")
	for i := 0; i < 100_000; i++ {
		mi.PushUnique("uniq", i)
	}
	afterUniq := heapMB()
	t.Logf("100,000 pushUnique (list+index): +%.1f MB", afterUniq-afterObjs)

	// flattened tree with 100k leaves
	tree := make(map[string]interface{}, 100_000)
	for i := 0; i < 100_000; i++ {
		tree[fmt.Sprintf("key%d", i)] = i
	}
	before := heapMB()
	mi.Flatten("bigtree", tree)
	afterTree := heapMB()
	t.Logf("100,000-leaf flattened tree: +%.1f MB", afterTree-before)

	mi.FlushAll()
	t.Logf("after FlushAll: %.1f MB", heapMB())
}
