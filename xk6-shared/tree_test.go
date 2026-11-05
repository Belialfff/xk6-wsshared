package xk6_shared

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestFlatten_GetSetPath(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	obj := map[string]interface{}{
		"integration": "brand-a",
		"wallet":      map[string]interface{}{"code": "EUR", "limit": float64(500)},
		"events":      []interface{}{map[string]interface{}{"id": float64(1)}, map[string]interface{}{"id": float64(2)}},
	}
	n := mi.Flatten("cfg", obj)
	// leaves: integration, wallet.code, wallet.limit, events.0.id, events.1.id
	if n != 5 {
		t.Fatalf("leaf count: got %d, want 5", n)
	}

	if got := mi.GetPath("cfg", "wallet.code"); got != "EUR" {
		t.Fatalf("getPath wallet.code: got %v", got)
	}
	if got := mi.GetPath("cfg", "events.1.id"); got != float64(2) {
		t.Fatalf("getPath events.1.id: got %v", got)
	}
	if got := mi.GetPath("cfg", "missing.path"); got != nil {
		t.Fatalf("missing path should be nil, got %v", got)
	}

	// Point mutation — the thing At()/get() copy couldn't do.
	mi.SetPath("cfg", "wallet.limit", float64(999))
	if got := mi.GetPath("cfg", "wallet.limit"); got != float64(999) {
		t.Fatalf("setPath did not persist: got %v", got)
	}

	if !mi.HasPath("cfg", "integration") || mi.HasPath("cfg", "nope") {
		t.Fatal("hasPath wrong")
	}
	mi.DeletePath("cfg", "integration")
	if mi.HasPath("cfg", "integration") {
		t.Fatal("deletePath failed")
	}
}

func TestFlatten_KeysAndSearch(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	obj := map[string]interface{}{
		"a": map[string]interface{}{"eventId": float64(1), "name": "x"},
		"b": map[string]interface{}{"eventId": float64(2), "name": "y"},
	}
	mi.Flatten("t", obj)

	keys := mi.PathKeys("t", "")
	if len(keys) != 4 {
		t.Fatalf("pathKeys all: got %d want 4 (%v)", len(keys), keys)
	}
	pref := mi.PathKeys("t", "a.")
	sort.Strings(pref)
	if !reflect.DeepEqual(pref, []string{"a.eventId", "a.name"}) {
		t.Fatalf("prefix keys wrong: %v", pref)
	}
	found := mi.PathSearch("t", "eventId")
	sort.Strings(found)
	if !reflect.DeepEqual(found, []string{"a.eventId", "b.eventId"}) {
		t.Fatalf("pathSearch wrong: %v", found)
	}
}

func TestFlatten_Unflatten_RoundTrip(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	obj := map[string]interface{}{
		"wallet": map[string]interface{}{"code": "EUR", "limit": float64(5)},
		"ids":    []interface{}{float64(10), float64(20), float64(30)},
	}
	mi.Flatten("r", obj)
	back := mi.Unflatten("r")

	bm, ok := back.(map[string]interface{})
	if !ok {
		t.Fatalf("unflatten not a map: %T", back)
	}
	w := bm["wallet"].(map[string]interface{})
	if w["code"] != "EUR" || w["limit"] != float64(5) {
		t.Fatalf("wallet round-trip wrong: %v", w)
	}
	ids, ok := bm["ids"].([]interface{})
	if !ok {
		t.Fatalf("ids should be an array, got %T", bm["ids"])
	}
	if !reflect.DeepEqual(ids, []interface{}{float64(10), float64(20), float64(30)}) {
		t.Fatalf("ids round-trip wrong: %v", ids)
	}
}

func TestLoadJson_And_Pool(t *testing.T) {
	mi := newInstance()
	resetStore(mi)

	n, err := mi.LoadJson("j", `{"a":{"b":1},"c":[10,20]}`)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 { // a.b, c.0, c.1
		t.Fatalf("loadJson leaves: got %d want 3", n)
	}
	if mi.GetPath("j", "a.b") != float64(1) {
		t.Fatalf("loadJson getPath wrong")
	}

	size, err := mi.LoadJsonPool("jp", `[{"id":1},{"id":2},{"id":3}]`)
	if err != nil {
		t.Fatal(err)
	}
	if size != 3 || mi.Size("jp") != 3 {
		t.Fatalf("loadJsonPool size wrong: %d", size)
	}
	// invalid array
	if _, err := mi.LoadJsonPool("bad", `{"not":"array"}`); err == nil {
		t.Fatal("expected error for non-array")
	}
}

func TestLoadCsv(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	csvText := "id,name,stake\n1,alice,10\n2,bob,25\n"
	size, err := mi.LoadCsv("c", csvText)
	if err != nil {
		t.Fatal(err)
	}
	if size != 2 {
		t.Fatalf("csv rows: got %d want 2", size)
	}
	row0 := mi.At("c", 0).(map[string]interface{})
	if row0["name"] != "alice" || row0["stake"] != "10" {
		t.Fatalf("csv row0 wrong: %v", row0)
	}
}

func TestSetAt(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	mi.PushMany("l", []interface{}{"a", "b", "c"})
	if !mi.SetAt("l", 1, "B") {
		t.Fatal("setAt should succeed")
	}
	if mi.At("l", 1) != "B" {
		t.Fatalf("setAt did not persist")
	}
	if !mi.SetAt("l", -1, "Z") || mi.At("l", 2) != "Z" {
		t.Fatalf("setAt negative index failed")
	}
	if mi.SetAt("l", 99, "x") {
		t.Fatal("setAt out of range should return false")
	}
}

// Concurrency: many goroutines get/set distinct + shared paths on one tree.
func TestRace_TreeConcurrent(t *testing.T) {
	mi := newInstance()
	resetStore(mi)
	base := map[string]interface{}{"shared": float64(0)}
	mi.Flatten("tree", base)

	const goroutines = 40
	const iters = 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p := fmt.Sprintf("g%d.v", id)
			for i := 0; i < iters; i++ {
				mi.SetPath("tree", p, i)
				_ = mi.GetPath("tree", p)
				_ = mi.HasPath("tree", "shared")
				mi.SetPath("tree", "shared", id) // contended write
				if i%200 == 0 {
					_ = mi.PathKeys("tree", fmt.Sprintf("g%d", id))
				}
			}
		}(g)
	}
	wg.Wait()

	// Each goroutine's own path should hold its last write (iters-1).
	for g := 0; g < goroutines; g++ {
		if got := mi.GetPath("tree", fmt.Sprintf("g%d.v", g)); got != iters-1 {
			t.Fatalf("g%d.v: got %v want %d", g, got, iters-1)
		}
	}
}
