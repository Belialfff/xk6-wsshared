package xk6_shared

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func heapMBu() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / 1024 / 1024
}

// build a CSV string of n data rows: id,name,stake
func makeCsv(n int) string {
	var b strings.Builder
	b.WriteString("id,name,stake\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%d,user%d,%d\n", i, i, i%1000)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// USE CASE 1 — CSV load + per-thread unique take (shift)
// ---------------------------------------------------------------------------

func TestUC_CsvLoadMemory(t *testing.T) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		mi := newInstance()
		resetStore(mi)
		csv := makeCsv(n)
		before := heapMBu()
		size, err := mi.LoadCsv("rows", csv)
		if err != nil {
			t.Fatal(err)
		}
		after := heapMBu()
		t.Logf("CSV %d rows: loaded=%d, +%.1f MB (%.0f B/row), csv text=%.1f MB",
			n, size, after-before, (after-before)*1024*1024/float64(n), float64(len(csv))/1024/1024)
	}
}

func BenchmarkUC_CsvLoad100k(b *testing.B) {
	csv := makeCsv(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mi := newInstance()
		mi.ClearList("rows")
		mi.LoadCsv("rows", csv)
	}
}

// per-thread unique take: shift is O(?) — measure it
func BenchmarkUC_ShiftUnique(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	// refill enough for b.N shifts
	rows := make([]interface{}, b.N+10)
	for i := range rows {
		rows[i] = i
	}
	mi.PushMany("q", rows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mi.Shift("q")
	}
}

// concurrent unique take — many goroutines shift, must never get a duplicate
func TestUC_ShiftNoDuplicatesConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	const total = 100_000
	rows := make([]interface{}, total)
	for i := range rows {
		rows[i] = i
	}
	mi.PushMany("q", rows)

	seen := make(map[interface{}]bool, total)
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	done := make(chan int, 16)
	for g := 0; g < 16; g++ {
		go func() {
			cnt := 0
			for {
				v := mi.Shift("q")
				if v == nil {
					break
				}
				<-mu
				if seen[v] {
					t.Errorf("duplicate shift: %v", v)
				}
				seen[v] = true
				mu <- struct{}{}
				cnt++
			}
			done <- cnt
		}()
	}
	sum := 0
	for g := 0; g < 16; g++ {
		sum += <-done
	}
	if sum != total {
		t.Fatalf("took %d, want %d", sum, total)
	}
	t.Logf("16 goroutines took all %d rows, zero duplicates", total)
}

// ---------------------------------------------------------------------------
// USE CASE 2 — big JSON: 20k users, ~200MB, nested access
// ---------------------------------------------------------------------------

// build a JSON array of n users, each with nested wallet+prefs, ~targetKB each
func makeBigUsersJSON(n, padChars int) string {
	users := make([]map[string]interface{}, n)
	pad := strings.Repeat("x", padChars)
	for i := 0; i < n; i++ {
		users[i] = map[string]interface{}{
			"id":     i,
			"name":   fmt.Sprintf("user%d", i),
			"wallet": map[string]interface{}{"code": "EUR", "limit": i % 1000, "blob": pad},
			"prefs":  map[string]interface{}{"lang": "en", "theme": "dark"},
		}
	}
	b, _ := json.Marshal(users)
	return string(b)
}

func TestUC_BigJsonPoolMemory(t *testing.T) {
	// 20k users, padded so total ≈ 200MB
	const users = 20_000
	const padKB = 10 // each user ~10KB blob -> 20k*10KB ≈ 200MB
	jsonText := makeBigUsersJSON(users, padKB*1024)
	t.Logf("JSON text size = %.1f MB", float64(len(jsonText))/1024/1024)

	mi := newInstance()
	resetStore(mi)
	size, err := mi.LoadJsonPool("users", jsonText)
	if err != nil {
		t.Fatal(err)
	}
	jsonText = "" // release the 200MB source text so it doesn't skew the measure
	_ = jsonText
	after := heapMBu() // GC runs inside; only the store remains live
	t.Logf("LoadJsonPool: %d users, store heap now %.1f MB (source text freed)", size, after)

	// nested access via one element (thin): pull a user, read wallet.limit
	u := mi.At("users", 12345).(map[string]interface{})
	w := u["wallet"].(map[string]interface{})
	t.Logf("thin access users[12345].wallet.limit = %v", w["limit"])
}

// same data flattened for getPath access
func TestUC_BigJsonFlattenMemory(t *testing.T) {
	const users = 20_000
	const padKB = 10
	jsonText := makeBigUsersJSON(users, padKB*1024)

	mi := newInstance()
	resetStore(mi)
	leaves, err := mi.LoadJson("users", jsonText)
	if err != nil {
		t.Fatal(err)
	}
	jsonText = ""
	_ = jsonText
	after := heapMBu()
	t.Logf("LoadJson(flatten): %d leaves, store heap now %.1f MB (source text freed)", leaves, after)
	t.Logf("getPath users.12345.wallet.limit = %v", mi.GetPath("users", "12345.wallet.limit"))
}

func BenchmarkUC_GetPathNested(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.LoadJson("u", makeBigUsersJSON(20_000, 100))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mi.GetPath("u", "12345.wallet.limit")
	}
}

func BenchmarkUC_AtBigPool(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.LoadJsonPool("u", makeBigUsersJSON(20_000, 100))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mi.At("u", i%20_000)
	}
}

// ---------------------------------------------------------------------------
// USE CASE 3 — create / delete / mutate variables during the test
// ---------------------------------------------------------------------------

func BenchmarkUC_CreateVar(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mi.Set(fmt.Sprintf("k%d", i%10000), i)
	}
}

func BenchmarkUC_DeleteVar(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	for i := 0; i < 10000; i++ {
		mi.Set(fmt.Sprintf("k%d", i), i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mi.Delete(fmt.Sprintf("k%d", i%10000))
	}
}

func BenchmarkUC_MutateVar(b *testing.B) {
	mi := newInstance()
	resetStore(mi)
	mi.Set("k", 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mi.Set("k", i)
	}
}
