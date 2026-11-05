// Package xk6_shared provides in-process, cross-VU shared mutable state for k6.
//
// Unlike JS globals (which are isolated per-VU because every VU is its own
// goja runtime) this state lives once in the Go process and is visible to
// every VU of a single `k6 run`. It is the in-process analogue of Redis:
// no network, no serialization on the hot path, nanosecond access.
//
// Scope: a single generator (one `k6 run` process). It is NOT shared across
// distributed generators — for that keep using Redis.
//
// Design rule: keep heavy data in Go, return small values to JS. Prefer
// Random()/At()/Size()/PushUnique() over Get() of a whole pool, so that
// 30k VUs do not each marshal a large slice into their own isolate.
package xk6_shared

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/sobek"
	k6modules "go.k6.io/k6/js/modules"
)

// Register the extension. Importable from JS as "k6/x/shared".
func init() {
	k6modules.Register("k6/x/shared", new(RootModule))
}

// -----------------------------------------------------------------------------
// Process-global store. One per k6 process, shared by all VUs.
// -----------------------------------------------------------------------------

// shardCount is the number of stripes per data domain. Different keys hash to
// different shards, so writers to DIFFERENT keys/lists never contend on the
// same lock (like ConcurrentHashMap's lock striping). Must be a power of two so
// we can mask instead of modulo. 16 is plenty for thousands of VUs spread over
// many keys; the pathological "all VUs hit the SAME key" case still lands on
// one shard (that ceiling is physical — shard on the JS side: key+':'+__VU%N).
const shardCount = 16
const shardMask = shardCount - 1

// fnv1a hashes a key to pick its shard. Small, allocation-free, good spread.
func shardOf(key string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h & shardMask
}

// valShard holds one stripe of the scalar/object variables. Each entry carries
// its OWN version so update() only retries on a concurrent write to the SAME
// key — a write to any other key (even in the same shard) does not force a
// retry. This is the fix for update()'s false-retry storm under contention.
type valShard struct {
	mu   sync.RWMutex
	m    map[string]*valEntry
}

type valEntry struct {
	val     interface{}
	version uint64 // bumped on every write to THIS key
}

// listShard holds one stripe of the growable lists + their uniqueness indexes.
type listShard struct {
	mu       sync.RWMutex
	lists    map[string][]interface{}
	setIndex map[string]map[string]struct{}
}

// treeShard holds one stripe of the flattened trees. Each tree carries its own
// version for updatePath()'s per-tree optimistic CAS.
type treeShard struct {
	mu    sync.RWMutex
	trees map[string]*treeEntry
}

type treeEntry struct {
	m       map[string]interface{}
	version uint64
}

// -----------------------------------------------------------------------------
// Columnar tables (structure-of-arrays) — memory-lean storage for a large pool
// of UNIFORMLY-SHAPED flat records. Instead of N map[string]interface{} (each
// with a map header, bucket overhead and boxed scalars — ~376 B/record), a
// table stores one TYPED slice per column: []float64 / []string / []bool. Field
// names live once in the schema, numbers/bools sit unboxed and contiguous.
// Measured ~6.7x smaller than the live map representation on flat records.
//
// Use for big homogeneous pools (event pools, user rows). NOT for nested/ragged
// objects — those keep using set/push/flatten.
// -----------------------------------------------------------------------------

// soaCol is one typed column. Exactly one of the slices is non-nil per kind.
type soaCol struct {
	kind  byte // 'n' num (float64), 's' str, 'b' bool, 'a' any (boxed fallback)
	nums  []float64
	strs  []string
	bools []bool
	anys  []interface{}
}

type soaTable struct {
	fields []string          // column order (schema)
	cols   map[string]*soaCol
	rows   int
}

type soaShard struct {
	mu     sync.RWMutex
	tables map[string]*soaTable
}

// store holds every namespace's data, striped into shards per domain. Atomic
// counters stay lock-free on their own path.
//
// Lock ordering: a method takes at most ONE shard lock (of one domain) at a
// time, so there is no ordering to violate.
type store struct {
	valShards  [shardCount]valShard
	listShards [shardCount]listShard
	treeShards [shardCount]treeShard
	soaShards  [shardCount]soaShard

	counters sync.Map // name -> *int64, lock-free atomic counters
}

func (s *store) valShard(key string) *valShard   { return &s.valShards[shardOf(key)] }
func (s *store) listShard(key string) *listShard { return &s.listShards[shardOf(key)] }
func (s *store) treeShard(key string) *treeShard { return &s.treeShards[shardOf(key)] }
func (s *store) soaShard(key string) *soaShard   { return &s.soaShards[shardOf(key)] }

var (
	globalStore *store
	initOnce    sync.Once
)

// getStore lazily builds the single global store, initialising every shard map.
func getStore() *store {
	initOnce.Do(func() {
		globalStore = &store{}
		for i := 0; i < shardCount; i++ {
			globalStore.valShards[i].m = make(map[string]*valEntry)
			globalStore.listShards[i].lists = make(map[string][]interface{})
			globalStore.listShards[i].setIndex = make(map[string]map[string]struct{})
			globalStore.treeShards[i].trees = make(map[string]*treeEntry)
			globalStore.soaShards[i].tables = make(map[string]*soaTable)
		}
	})
	return globalStore
}

// -----------------------------------------------------------------------------
// Module wiring. k6 constructs a fresh module instance per VU; they all point
// at the same process-global store, which is where the sharing happens.
// -----------------------------------------------------------------------------

type RootModule struct{}

type ModuleInstance struct {
	vu    k6modules.VU
	store *store
}

var (
	_ k6modules.Module = &RootModule{}
)

func (*RootModule) NewModuleInstance(vu k6modules.VU) k6modules.Instance {
	return &ModuleInstance{vu: vu, store: getStore()}
}

// Exports maps Go methods to the JS namespace. The default export is the
// module instance itself, so JS does `import shared from 'k6/x/shared'`.
func (mi *ModuleInstance) Exports() k6modules.Exports {
	return k6modules.Exports{Default: mi}
}

// randIntn returns a random index in [0,n). Uses math/rand/v2's top-level
// generator, which is lock-free (per-P state in the runtime) and safe for
// concurrent use. This removes the single global RNG mutex that used to
// serialize every random()/randomN() across all VUs — the hot path for
// event-pool sampling — without introducing per-instance state.
func (mi *ModuleInstance) randIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}

// stringKey renders a value to a STABLE string used for set membership
// (PushUnique / ListHas). For scalars (the common case — ids, tokens) it takes
// a fast, allocation-light path. For composite values (objects/arrays) it must
// be order-independent: fmt's "%v" on a map iterates keys in random order, so
// the SAME object could hash to different strings and defeat uniqueness. We use
// canonical JSON (encoding/json sorts map keys) so equal objects collapse to
// the same key.
func stringKey(v interface{}) string {
	switch t := v.(type) {
	case string:
		return "s:" + t
	case bool:
		if t {
			return "b:1"
		}
		return "b:0"
	case int:
		return "n:" + strconv.FormatInt(int64(t), 10)
	case int64:
		return "n:" + strconv.FormatInt(t, 10)
	case float64:
		return "n:" + strconv.FormatFloat(t, 'g', -1, 64)
	case nil:
		return "z:"
	case map[string]interface{}, []interface{}:
		// canonical JSON: json.Marshal sorts object keys, giving a stable key.
		b, err := json.Marshal(t)
		if err != nil {
			// Non-serializable value (cycle, function). The old fallback used
			// fmt %v, which STACK-OVERFLOWS on a cyclic map (fatal). Signal the
			// caller with a sentinel so it can raise a catchable JS TypeError.
			panic(errNotSerializable{err})
		}
		return "j:" + string(b)
	default:
		return fmt.Sprintf("v:%v", v)
	}
}

// errNotSerializable is thrown by stringKey when a value can't be canonicalised
// (cyclic/function). The public methods that call stringKey recover it and
// re-raise a proper JS TypeError via the runtime, so it's catchable in JS.
type errNotSerializable struct{ err error }

// stringKeyJS is stringKey with the not-serializable panic converted into a JS
// TypeError on the given runtime, so `try { shared.pushUnique(...) } catch` works.
func (mi *ModuleInstance) stringKeyJS(v interface{}) (out string) {
	defer func() {
		if r := recover(); r != nil {
			if ns, ok := r.(errNotSerializable); ok {
				panic(mi.vu.Runtime().NewTypeError(
					"shared: value is not serializable for a unique/CAS key (cyclic or contains a function): " + ns.err.Error()))
			}
			panic(r)
		}
	}()
	return stringKey(v)
}

// stringKeySafe is stringKey that never panics — returns "" on a non-serializable
// value. Used on cleanup/read paths (Pop/Shift/ListHas) where a non-serializable
// value simply can't be in the uniqueness index, so a raw panic would be wrong.
func stringKeySafe(v interface{}) (k string) {
	defer func() { _ = recover() }()
	return stringKey(v)
}

// deepCopy returns a structural copy of v (maps/slices duplicated, scalars as
// is). Used by update*() so the JS mutator works on an ISOLATED copy — it must
// never mutate the live object in the store (that would be a data race on the
// shared map, bypassing the mutex). Scalars are immutable and returned as-is.
func deepCopy(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(t))
		for k, child := range t {
			m[k] = deepCopy(child)
		}
		return m
	case []interface{}:
		s := make([]interface{}, len(t))
		for i, child := range t {
			s[i] = deepCopy(child)
		}
		return s
	default:
		return v // string/number/bool/nil — immutable
	}
}

// =============================================================================
// Scalar variables — "global variable in the plain sense"
// =============================================================================

// Set stores a scalar or object value under key. Overwrites any prior value.
func (mi *ModuleInstance) Set(key string, value interface{}) {
	sh := mi.store.valShard(key)
	sh.mu.Lock()
	if e := sh.m[key]; e != nil {
		e.val = value
		e.version++
	} else {
		sh.m[key] = &valEntry{val: value, version: 1}
	}
	sh.mu.Unlock()
}

// Get returns the value at key, or null (undefined) if absent.
func (mi *ModuleInstance) Get(key string) interface{} {
	sh := mi.store.valShard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := sh.m[key]
	if e == nil {
		return nil
	}
	// deepCopy UNDER the read lock. Returning the live Go map would let JS mutate
	// mutating this very map from JS; copying after RUnlock would iterate a map
	// that is being written -> fatal "concurrent map iteration and map write"
	// (reproduced). Copying under RLock serializes against that write. Scalars
	// pass through deepCopy free, so the hot string/number path is unaffected.
	// the shared object bypassing all locks -> fatal concurrent map access.
	return deepCopy(e.val)
}

// Has reports whether a scalar variable exists.
func (mi *ModuleInstance) Has(key string) bool {
	sh := mi.store.valShard(key)
	sh.mu.RLock()
	_, ok := sh.m[key]
	sh.mu.RUnlock()
	return ok
}

// Delete removes a scalar variable.
func (mi *ModuleInstance) Delete(key string) {
	sh := mi.store.valShard(key)
	sh.mu.Lock()
	delete(sh.m, key)
	sh.mu.Unlock()
}

// SetIfAbsent stores value only if key is not present. Returns true if it
// wrote. Useful for "first VU wins" one-time initialization.
func (mi *ModuleInstance) SetIfAbsent(key string, value interface{}) bool {
	sh := mi.store.valShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[key]; ok {
		return false
	}
	sh.m[key] = &valEntry{val: value, version: 1}
	return true
}

// CompareAndSwap sets key to next only if its current value equals expected.
// Returns true on success. Enables leader election and one-shot latches
// without a mutex on the JS side.
func (mi *ModuleInstance) CompareAndSwap(key string, expected, next interface{}) bool {
	sh := mi.store.valShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.m[key]
	if !ok {
		// treat absent as matching only when expected is nil
		if expected == nil {
			sh.m[key] = &valEntry{val: next, version: 1}
			return true
		}
		return false
	}
	if mi.stringKeyJS(e.val) == mi.stringKeyJS(expected) {
		e.val = next
		e.version++
		return true
	}
	return false
}

// Update performs an atomic read-modify-write on a scalar/object variable.
// The JS mutator fn receives the current value and must RETURN the new value:
//
//	shared.update('cfg', c => { c.limit += 1; return c; });
//
// Safety model: fn runs OUTSIDE the store mutex (Go must not call JS while
// holding a lock). We use optimistic concurrency — snapshot the write version,
// run fn, then commit only if no other writer bumped the version meanwhile;
// otherwise we retry. So even with many VUs updating the SAME key there is no
// lost update. fn may run more than once, so it must be side-effect free apart
// from computing the new value. Returns the committed value.
func (mi *ModuleInstance) Update(key string, fn sobek.Value) interface{} {
	call, ok := sobek.AssertFunction(fn)
	if !ok {
		panic(mi.vu.Runtime().NewTypeError("update: second argument must be a function"))
	}
	rt := mi.vu.Runtime()
	return mi.updateValue(key, func(cur interface{}) interface{} {
		res, err := call(sobek.Undefined(), rt.ToValue(cur))
		if err != nil {
			panic(err)
		}
		return res.Export()
	})
}

// updateValue is the Go core of Update: optimistic read-modify-write on a
// scalar variable. mutate runs OUTSIDE the mutex and may be called more than
// once on contention. Exposed to tests via a Go mutator (no sobek needed).
func (mi *ModuleInstance) updateValue(key string, mutate func(cur interface{}) interface{}) interface{} {
	sh := mi.store.valShard(key)
	for {
		sh.mu.RLock()
		e := sh.m[key]
		var cur interface{}
		var ver uint64
		if e != nil {
			// Fast path: scalars (string/number/bool/nil) are immutable, so the
			// mutator can never race by mutating them — skip the deepCopy. Only
			// composite values (map/slice) need an isolated copy.
			cur = copyForUpdate(e.val)
			ver = e.version
		}
		sh.mu.RUnlock()

		next := mutate(cur)

		sh.mu.Lock()
		e = sh.m[key]
		curVer := uint64(0)
		if e != nil {
			curVer = e.version
		}
		// Per-KEY version: retry only if THIS key changed under us. A write to
		// any other key — even one in the same shard — does not force a retry.
		if curVer == ver {
			if e != nil {
				e.val = next
				e.version++
			} else {
				sh.m[key] = &valEntry{val: next, version: 1}
			}
			sh.mu.Unlock()
			return next
		}
		sh.mu.Unlock() // this key changed; retry with fresh value
	}
}

// copyForUpdate isolates a value for the update mutator. Scalars are immutable
// and returned as-is (no allocation); only maps/slices are deep-copied so the
// mutator cannot race on the store's live structure.
func copyForUpdate(v interface{}) interface{} {
	switch v.(type) {
	case map[string]interface{}, []interface{}:
		return deepCopy(v)
	default:
		return v // string/number/bool/nil — immutable, no copy needed
	}
}

// UpdatePath performs an atomic read-modify-write on one leaf of a tree.
//
//	shared.updatePath('cfg', 'wallet.limit', v => v + 1);
//
// Same optimistic-retry safety model as Update. Returns the committed value.
func (mi *ModuleInstance) UpdatePath(name, path string, fn sobek.Value) interface{} {
	call, ok := sobek.AssertFunction(fn)
	if !ok {
		panic(mi.vu.Runtime().NewTypeError("updatePath: third argument must be a function"))
	}
	rt := mi.vu.Runtime()
	sh := mi.store.treeShard(name)
	for {
		sh.mu.RLock()
		var cur interface{}
		var ver uint64
		if te := sh.trees[name]; te != nil {
			cur = copyForUpdate(te.m[path]) // isolated copy only if composite
			ver = te.version
		}
		sh.mu.RUnlock()

		res, err := call(sobek.Undefined(), rt.ToValue(cur))
		if err != nil {
			panic(err)
		}
		next := res.Export()

		sh.mu.Lock()
		te := sh.trees[name]
		curVer := uint64(0)
		if te != nil {
			curVer = te.version
		}
		// Per-TREE version: retry only if THIS tree changed under us.
		if curVer == ver {
			if te == nil {
				te = &treeEntry{m: make(map[string]interface{})}
				sh.trees[name] = te
			}
			te.m[path] = next
			te.version++
			sh.mu.Unlock()
			return next
		}
		sh.mu.Unlock()
	}
}

// =============================================================================
// Atomic counters — lock-free
// =============================================================================

func (mi *ModuleInstance) counterPtr(name string) *int64 {
	actual, _ := mi.store.counters.LoadOrStore(name, new(int64))
	return actual.(*int64)
}

// Incr atomically adds 1 and returns the new value.
func (mi *ModuleInstance) Incr(name string) int64 {
	return atomic.AddInt64(mi.counterPtr(name), 1)
}

// Add atomically adds delta and returns the new value.
func (mi *ModuleInstance) Add(name string, delta int64) int64 {
	return atomic.AddInt64(mi.counterPtr(name), delta)
}

// Count returns the current counter value (0 if never touched). Read-only: it
// does NOT create the counter — using Load, not LoadOrStore, so probing many
// distinct names (e.g. "lat:"+userId) never seeds the map (memory-leak fix).
func (mi *ModuleInstance) Count(name string) int64 {
	if p, ok := mi.store.counters.Load(name); ok {
		return atomic.LoadInt64(p.(*int64))
	}
	return 0
}

// ResetCounter sets a counter back to 0. If the counter never existed this is a
// no-op — it does NOT create it (avoids seeding the map with dead entries).
func (mi *ModuleInstance) ResetCounter(name string) {
	if p, ok := mi.store.counters.Load(name); ok {
		atomic.StoreInt64(p.(*int64), 0)
	}
}

// =============================================================================
// Pools / lists — growable, mutable at any time
// =============================================================================

// Push appends value to the named list (creating it if needed) and returns
// the new length.
func (mi *ModuleInstance) Push(name string, value interface{}) int {
	sh := mi.store.listShard(name)
	sh.mu.Lock()
	sh.lists[name] = append(sh.lists[name], value)
	n := len(sh.lists[name])
	sh.mu.Unlock()
	return n
}

// PushUnique appends value only if an equal value (by string form) is not
// already present. Returns true if it was added. O(1) via a membership index.
func (mi *ModuleInstance) PushUnique(name string, value interface{}) bool {
	sh := mi.store.listShard(name)
	key := mi.stringKeyJS(value)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	idx, ok := sh.setIndex[name]
	if !ok {
		idx = make(map[string]struct{})
		sh.setIndex[name] = idx
	}
	if _, exists := idx[key]; exists {
		return false
	}
	idx[key] = struct{}{}
	sh.lists[name] = append(sh.lists[name], value)
	return true
}

// PushMany appends every element of values and returns the new length. One
// lock acquisition for the whole batch — cheaper than N Push calls.
func (mi *ModuleInstance) PushMany(name string, values []interface{}) int {
	sh := mi.store.listShard(name)
	sh.mu.Lock()
	sh.lists[name] = append(sh.lists[name], values...)
	n := len(sh.lists[name])
	sh.mu.Unlock()
	return n
}

// Size returns the length of the named list.
func (mi *ModuleInstance) Size(name string) int {
	sh := mi.store.listShard(name)
	sh.mu.RLock()
	n := len(sh.lists[name])
	sh.mu.RUnlock()
	return n
}

// At returns the element at index i (0-based), or null if out of range.
// Negative indices count from the end, like JS slice semantics. Returns a COPY
// of composite elements (see Get) — safe to mutate.
// zero-copy element (read-only).
func (mi *ModuleInstance) At(name string, i int) interface{} {
	sh := mi.store.listShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	l := sh.lists[name]
	if i < 0 {
		i += len(l)
	}
	if i < 0 || i >= len(l) {
		return nil
	}
	return deepCopy(l[i]) // copy UNDER the lock (see Get)
}

// SetAt replaces the element at index i (negative counts from the end) in the
// named list. Returns true if the index was in range and the write happened.
// This is the mutation that At() cannot do: modifying a value returned by At()
// only changes the isolate's copy — SetAt writes into the Go-side slice.
func (mi *ModuleInstance) SetAt(name string, i int, value interface{}) bool {
	sh := mi.store.listShard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	l := sh.lists[name]
	if i < 0 {
		i += len(l)
	}
	if i < 0 || i >= len(l) {
		return false
	}
	l[i] = value
	return true
}

// Random returns a uniformly random element of the list, or null if empty.
// Hot-path read: touches only ONE element. Returns a COPY of a composite element
// (see Get) — safe to mutate.
func (mi *ModuleInstance) Random(name string) interface{} {
	sh := mi.store.listShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	l := sh.lists[name]
	n := len(l)
	if n == 0 {
		return nil
	}
	return deepCopy(l[mi.randIntn(n)]) // copy UNDER the lock (see Get)
}

// RandomN returns up to n distinct random elements (without replacement).
// If n >= size, returns a shuffled copy of the whole list.
func (mi *ModuleInstance) RandomN(name string, n int) []interface{} {
	sh := mi.store.listShard(name)
	sh.mu.RLock()
	l := sh.lists[name]
	size := len(l)
	if size == 0 || n <= 0 {
		sh.mu.RUnlock()
		return []interface{}{}
	}
	if n > size {
		n = size
	}
	out := make([]interface{}, 0, n)
	if n*2 <= size {
		// n is small vs the pool: rejection sampling — pick random indices and
		// skip duplicates via a tiny set. O(n) work, NOT O(size). This is the fix
		// for randomN being ~80x slower on a big pool: we no longer build an
		// index array over the WHOLE pool just to return a few elements.
		picked := make(map[int]struct{}, n)
		for len(out) < n {
			j := mi.randIntn(size)
			if _, dup := picked[j]; dup {
				continue
			}
			picked[j] = struct{}{}
			out = append(out, deepCopy(l[j]))
		}
	} else {
		// n is close to size: too many duplicates for rejection sampling, so use
		// partial Fisher-Yates on an index array (O(size), but n is large anyway).
		idx := make([]int, size)
		for i := range idx {
			idx[i] = i
		}
		for i := 0; i < n; i++ {
			j := i + mi.randIntn(size-i)
			idx[i], idx[j] = idx[j], idx[i]
			out = append(out, deepCopy(l[idx[i]]))
		}
	}
	sh.mu.RUnlock()
	return out
}

// Pop removes and returns the last element (LIFO), or null if empty.
func (mi *ModuleInstance) Pop(name string) interface{} {
	sh := mi.store.listShard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	l := sh.lists[name]
	if len(l) == 0 {
		return nil
	}
	v := l[len(l)-1]
	l[len(l)-1] = nil // drop reference so the popped element can be GC'd
	rest := l[:len(l)-1]
	// Compact like Shift: a list grown to 100k then popped down to a few would
	// otherwise pin the full 100k backing array. Drop it when empty; shrink it
	// when it's less than half full (and big enough to be worth copying).
	if len(rest) == 0 {
		delete(sh.lists, name)
		delete(sh.setIndex, name) // also drop the (now-empty) uniqueness index
	} else if cap(rest) > 64 && len(rest)*2 < cap(rest) {
		compact := make([]interface{}, len(rest))
		copy(compact, rest)
		sh.lists[name] = compact
	} else {
		sh.lists[name] = rest
	}
	if idx, ok := sh.setIndex[name]; ok {
		delete(idx, stringKeySafe(v))
	}
	return v
}

// Shift removes and returns the first element (FIFO / queue), or null if empty.
//
// Memory note: a naive `l = l[1:]` only advances the slice header — the backing
// array keeps growing on every Push and is never reclaimed, so a long-running
// producer/consumer queue (e.g. cashout) leaks unboundedly even at constant
// length. We compact: when the live length has shrunk to less than half of the
// backing capacity, copy the remainder into a fresh, right-sized slice so the
// old array (including the shifted-off head) can be GC'd.
func (mi *ModuleInstance) Shift(name string) interface{} {
	sh := mi.store.listShard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	l := sh.lists[name]
	if len(l) == 0 {
		return nil
	}
	v := l[0]
	l[0] = nil // drop reference so the shifted element can be GC'd
	rest := l[1:]
	if len(rest) == 0 {
		delete(sh.lists, name)    // empty queue: drop the backing array entirely
		delete(sh.setIndex, name) // also drop the (now-empty) uniqueness index
	} else if cap(rest) > 64 && len(rest)*2 < cap(rest) {
		compact := make([]interface{}, len(rest))
		copy(compact, rest)
		sh.lists[name] = compact
	} else {
		sh.lists[name] = rest
	}
	if idx, ok := sh.setIndex[name]; ok {
		delete(idx, stringKeySafe(v))
	}
	return v
}

// ListHas reports whether value is present in the named list's uniqueness
// index. Only meaningful for lists populated via PushUnique.
func (mi *ModuleInstance) ListHas(name string, value interface{}) bool {
	sh := mi.store.listShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	idx, ok := sh.setIndex[name]
	if !ok {
		return false
	}
	// A non-serializable value can never have been in the index — treat as absent
	// rather than panicking on a read.
	key := func() (k string) {
		defer func() { _ = recover() }()
		return stringKey(value)
	}()
	_, exists := idx[key]
	return exists
}

// Snapshot returns a shallow COPY of the whole list. Use sparingly — this is
// the one call that copies the full pool into the calling isolate. Fine in
// setup/teardown or debug; avoid on 30k VUs in the hot loop.
func (mi *ModuleInstance) Snapshot(name string) []interface{} {
	sh := mi.store.listShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	l := sh.lists[name]
	// deepCopy EACH element under the lock. A shallow copy(out,l) shares the
	// element pointers, so a pool of objects would hand JS live references into
	// the store — mutating them races (same crash as Get). Copy composites;
	// scalars pass through free.
	out := make([]interface{}, len(l))
	for i := range l {
		out[i] = deepCopy(l[i])
	}
	return out
}

// ClearList empties a list and its uniqueness index.
func (mi *ModuleInstance) ClearList(name string) {
	sh := mi.store.listShard(name)
	sh.mu.Lock()
	delete(sh.lists, name)
	delete(sh.setIndex, name)
	sh.mu.Unlock()
}

// =============================================================================
// Flattened trees — point access & mutation of a nested object by path.
//
// A nested object is flattened once into leaf-path -> scalar entries, e.g.
//   {a:{b:1}, list:[{x:2}]}  =>  "a.b"=1, "list.0.x"=2
// After that getPath/setPath are O(1) and DO NOT copy the whole object across
// the Go<->JS boundary — the fix for "mutating an At()/get() copy does nothing".
// =============================================================================

// flattenInto recursively walks v, writing each leaf into dst under the joined
// path. Arrays use numeric indices as path segments.
func flattenInto(dst map[string]interface{}, prefix string, v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		if len(t) == 0 {
			dst[prefix] = map[string]interface{}{} // preserve empty object leaf
			return
		}
		for k, child := range t {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			flattenInto(dst, p, child)
		}
	case []interface{}:
		if len(t) == 0 {
			dst[prefix] = []interface{}{} // preserve empty array leaf
			return
		}
		for i, child := range t {
			p := strconv.Itoa(i)
			if prefix != "" {
				p = prefix + "." + strconv.Itoa(i)
			}
			flattenInto(dst, p, child)
		}
	default:
		dst[prefix] = v // scalar leaf (string/number/bool/null)
	}
}

// Flatten stores a nested object under `name` as a path->value map. Overwrites
// any existing tree with the same name. Call once (e.g. in setup) for a large
// config; afterwards use getPath/setPath.
func (mi *ModuleInstance) Flatten(name string, obj interface{}) int {
	flat := make(map[string]interface{})
	flattenInto(flat, "", obj)
	sh := mi.store.treeShard(name)
	sh.mu.Lock()
	if te := sh.trees[name]; te != nil {
		te.m = flat
		te.version++
	} else {
		sh.trees[name] = &treeEntry{m: flat, version: 1}
	}
	n := len(flat)
	sh.mu.Unlock()
	return n
}

// GetPath returns the leaf value at path within the named tree, or null if the
// tree or path is absent. O(1), copies only that one leaf.
func (mi *ModuleInstance) GetPath(name, path string) interface{} {
	sh := mi.store.treeShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	te := sh.trees[name]
	if te == nil {
		return nil
	}
	v, ok := te.m[path]
	if !ok {
		return nil
	}
	// Leaves are usually scalars (deepCopy-free), but flatten keeps empty
	// object/array leaves and setPath can store a composite — copy UNDER the
	// lock (see Get) so JS can't mutate the stored leaf.
	return deepCopy(v)
}


// SetPath sets the leaf value at path within the named tree (creating the tree
// if needed). Returns true. This mutates the shared store in place, unlike
// mutating a copy returned by getPath.
func (mi *ModuleInstance) SetPath(name, path string, value interface{}) bool {
	sh := mi.store.treeShard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	te := sh.trees[name]
	if te == nil {
		te = &treeEntry{m: make(map[string]interface{})}
		sh.trees[name] = te
	}
	te.m[path] = value
	te.version++
	return true
}

// HasPath reports whether path exists in the named tree.
func (mi *ModuleInstance) HasPath(name, path string) bool {
	sh := mi.store.treeShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	te := sh.trees[name]
	if te == nil {
		return false
	}
	_, ok := te.m[path]
	return ok
}

// DeletePath removes a leaf from the named tree.
func (mi *ModuleInstance) DeletePath(name, path string) {
	sh := mi.store.treeShard(name)
	sh.mu.Lock()
	if te := sh.trees[name]; te != nil {
		delete(te.m, path)
		te.version++
	}
	sh.mu.Unlock()
}

// PathKeys returns all leaf paths in the named tree. If prefix is non-empty,
// only paths starting with it are returned. O(N) — use in setup, not hot loop.
func (mi *ModuleInstance) PathKeys(name, prefix string) []string {
	sh := mi.store.treeShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	te := sh.trees[name]
	if te == nil {
		return []string{}
	}
	out := make([]string, 0, len(te.m))
	for k := range te.m {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

// PathSearch returns all leaf paths in the named tree whose path CONTAINS the
// substring. O(N) full scan — intended for setup-time discovery, NOT the hot
// loop (cache the result into a pool if VUs need it per-iteration).
func (mi *ModuleInstance) PathSearch(name, substr string) []string {
	sh := mi.store.treeShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	te := sh.trees[name]
	if te == nil {
		return []string{}
	}
	out := make([]string, 0)
	for k := range te.m {
		if strings.Contains(k, substr) {
			out = append(out, k)
		}
	}
	return out
}

// TreeSize returns the number of leaves in the named tree.
func (mi *ModuleInstance) TreeSize(name string) int {
	sh := mi.store.treeShard(name)
	sh.mu.RLock()
	n := 0
	if te := sh.trees[name]; te != nil {
		n = len(te.m)
	}
	sh.mu.RUnlock()
	return n
}

// TreeClear drops a whole tree. Fills the gap where lists/tables/shards had a
// per-instance clear (clearList/tableClear/clearShard) but trees only had
// deletePath (single leaf); to drop a whole tree you previously needed flushAll.
func (mi *ModuleInstance) TreeClear(name string) {
	sh := mi.store.treeShard(name)
	sh.mu.Lock()
	delete(sh.trees, name)
	sh.mu.Unlock()
}

// Unflatten rebuilds the nested object from a flattened tree and returns it.
// Copies the whole structure into the isolate — use sparingly (teardown/debug).
func (mi *ModuleInstance) Unflatten(name string) interface{} {
	sh := mi.store.treeShard(name)
	sh.mu.RLock()
	te := sh.trees[name]
	if te == nil {
		sh.mu.RUnlock()
		return nil
	}
	// snapshot paths so we can release the lock before rebuilding. deepCopy each
	// value: composite leaves (empty object/array from flatten, or a composite
	// stored via setPath) would otherwise be handed to JS live and race on mutate.
	flat := make(map[string]interface{}, len(te.m))
	for k, v := range te.m {
		flat[k] = deepCopy(v)
	}
	sh.mu.RUnlock()
	return unflatten(flat)
}

// unflatten reconstructs nested maps/arrays from path->value entries.
func unflatten(flat map[string]interface{}) interface{} {
	root := make(map[string]interface{})
	for path, val := range flat {
		if path == "" {
			return val // whole tree was a single scalar
		}
		segs := strings.Split(path, ".")
		cur := root
		for i := 0; i < len(segs)-1; i++ {
			seg := segs[i]
			next, ok := cur[seg]
			if !ok {
				m := make(map[string]interface{})
				cur[seg] = m
				cur = m
			} else if m, ok := next.(map[string]interface{}); ok {
				cur = m
			} else {
				m := make(map[string]interface{})
				cur[seg] = m
				cur = m
			}
		}
		cur[segs[len(segs)-1]] = val
	}
	return arrayify(root)
}

// arrayify converts maps whose keys are the contiguous integers 0..n-1 into
// slices, so array paths ("list.0", "list.1") round-trip back to arrays.
func arrayify(v interface{}) interface{} {
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	for k, child := range m {
		m[k] = arrayify(child)
	}
	// is this map actually an array?
	if len(m) == 0 {
		return m
	}
	arr := make([]interface{}, len(m))
	for k := range m {
		idx, err := strconv.Atoi(k)
		if err != nil || idx < 0 || idx >= len(m) {
			return m // not a clean 0..n-1 array
		}
	}
	for k, child := range m {
		idx, _ := strconv.Atoi(k)
		arr[idx] = child
	}
	return arr
}

// =============================================================================
// Go-side loaders — parse a file's content directly in Go, so a large file
// never has to be parsed inside a JS isolate. Pass the file text via open().
// =============================================================================

// LoadJson parses JSON text and flattens it into the named tree. Returns the
// number of leaves. Parsing happens in Go, not goja.
func (mi *ModuleInstance) LoadJson(name, jsonText string) (int, error) {
	var v interface{}
	if err := json.Unmarshal([]byte(jsonText), &v); err != nil {
		return 0, fmt.Errorf("loadJson: %w", err)
	}
	return mi.Flatten(name, v), nil
}

// LoadJsonPool parses a JSON ARRAY and stores each element as a pool entry.
// Returns the pool size. For "array of records" files (JSON lines wrapped as
// an array, event pools, etc.). Errors if the JSON is not an array.
func (mi *ModuleInstance) LoadJsonPool(name, jsonText string) (int, error) {
	var arr []interface{}
	if err := json.Unmarshal([]byte(jsonText), &arr); err != nil {
		return 0, fmt.Errorf("loadJsonPool: %w", err)
	}
	return mi.PushMany(name, arr), nil
}

// LoadCsv parses CSV text into a pool of row objects keyed by the header row.
// Returns the pool size. Numbers are kept as strings (predictable for tests);
// convert in JS if needed. Parsing happens in Go, not goja.
func (mi *ModuleInstance) LoadCsv(name, csvText string) (int, error) {
	r := csv.NewReader(strings.NewReader(csvText))
	r.FieldsPerRecord = -1 // tolerate ragged rows
	records, err := r.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("loadCsv: %w", err)
	}
	if len(records) == 0 {
		return 0, nil
	}
	header := records[0]
	rows := make([]interface{}, 0, len(records)-1)
	for _, rec := range records[1:] {
		obj := make(map[string]interface{}, len(header))
		for i, h := range header {
			if i < len(rec) {
				obj[h] = rec[i]
			} else {
				obj[h] = ""
			}
		}
		rows = append(rows, obj)
	}
	return mi.PushMany(name, rows), nil
}

// =============================================================================
// Auto-sharded structures — SAME semantics as push/pushUnique/shift/random/
// update, but the store spreads writes across N internal sub-keys keyed by the
// calling VU, so thousands of writers do NOT all contend on one lock. The test
// code calls these exactly like the plain versions — no ':'+__VU%16 by hand.
//
// Layout: a logical name "queue" is stored as internal lists "queue\x00<0..N-1>".
// Writers pick their sub-list by VU id (fixed per VU → no cross-shard hopping).
// Readers (shiftAny/popAny/random/size/snapshot) fan out across all sub-lists.
// =============================================================================

// autoShards is how many internal sub-keys an auto-sharded name spreads over.
// Independent of the store's lock striping (shardCount) — this is the fan-out
// the WRITER chooses; different sub-keys then naturally land on different lock
// stripes too. 16 matches shardCount so the two align.
const autoShards = 16

// vuShard returns a stable shard index in [0,autoShards) for the calling VU, so
// a given VU always writes to the same sub-list (preserves per-VU ordering).
func (mi *ModuleInstance) vuShard() int {
	if mi.vu != nil {
		if st := mi.vu.State(); st != nil {
			return int(st.VUID % autoShards)
		}
	}
	return 0
}

// subKey builds the internal sub-list name for a logical name + shard index.
// Uses a NUL separator so it can't collide with a user-chosen name.
func subKey(name string, shard int) string {
	return name + "\x00" + strconv.Itoa(shard)
}

// PushShard appends to the VU's shard of the logical list. Same as Push but
// contention-free across VUs. Returns the new length of THAT shard.
func (mi *ModuleInstance) PushShard(name string, value interface{}) int {
	return mi.Push(subKey(name, mi.vuShard()), value)
}

// PushUniqueShard is PushUnique spread per VU shard. NOTE: uniqueness is checked
// WITHIN a shard only — the same value can exist once per shard. For strict
// global uniqueness use plain PushUnique. Good enough for "roughly dedup a
// high-write stream" without a global lock.
func (mi *ModuleInstance) PushUniqueShard(name string, value interface{}) bool {
	return mi.PushUnique(subKey(name, mi.vuShard()), value)
}

// SizeShard returns the TOTAL length across all shards of the logical list.
func (mi *ModuleInstance) SizeShard(name string) int {
	total := 0
	for i := 0; i < autoShards; i++ {
		total += mi.Size(subKey(name, i))
	}
	return total
}

// ShiftAny removes and returns one element from the logical queue, scanning
// shards until it finds a non-empty one. FIFO within a shard; across shards the
// order is round-robin-ish (NOT a strict global FIFO — the price of no-lock
// sharding). Returns null if every shard is empty. Starts the scan at the VU's
// own shard to spread consumer load.
func (mi *ModuleInstance) ShiftAny(name string) interface{} {
	start := mi.vuShard()
	for off := 0; off < autoShards; off++ {
		i := (start + off) % autoShards
		if v := mi.Shift(subKey(name, i)); v != nil {
			return v
		}
	}
	return nil
}

// PopAny removes and returns one element LIFO from any non-empty shard.
func (mi *ModuleInstance) PopAny(name string) interface{} {
	start := mi.vuShard()
	for off := 0; off < autoShards; off++ {
		i := (start + off) % autoShards
		if v := mi.Pop(subKey(name, i)); v != nil {
			return v
		}
	}
	return nil
}

// RandomShard returns a random element from the logical list across all shards,
// weighted by shard size. The size scan and the element fetch take each shard's
// lock separately (never together — the one-lock-at-a-time invariant), so this
// is NOT a consistent snapshot: if the pool is being drained CONCURRENTLY
// (shiftAny/popAny) the weighted pick can land past a shard that just shrank.
// We guard that with a fallback scan so a non-empty logical list still returns
// a non-nil element. For a pool filled once in setup and only read afterwards
// (the common case) the weighting is exact. Returns nil only if every shard is
// truly empty.
func (mi *ModuleInstance) RandomShard(name string) interface{} {
	sizes := make([]int, autoShards)
	total := 0
	for i := 0; i < autoShards; i++ {
		n := mi.Size(subKey(name, i))
		sizes[i] = n
		total += n
	}
	if total == 0 {
		return nil
	}
	pick := mi.randIntn(total)
	for i := 0; i < autoShards; i++ {
		if pick < sizes[i] {
			if v := mi.At(subKey(name, i), pick); v != nil {
				return v
			}
			break // that shard shrank under us — fall back below
		}
		pick -= sizes[i]
	}
	// Fallback: the weighted target shard changed size concurrently. Scan for any
	// non-empty shard and take a random element from it, so a non-empty logical
	// list is never reported as nil.
	for off := 0; off < autoShards; off++ {
		i := (mi.vuShard() + off) % autoShards
		if v := mi.Random(subKey(name, i)); v != nil {
			return v
		}
	}
	return nil
}

// SnapshotShard returns a COPY of the whole logical list, concatenating every
// shard. Use in setup/teardown/debug, not the hot loop.
func (mi *ModuleInstance) SnapshotShard(name string) []interface{} {
	// Pre-size the result from the total across shards so the concatenation does
	// not repeatedly reallocate a growing slice (was the main cost here).
	total := 0
	for i := 0; i < autoShards; i++ {
		total += mi.Size(subKey(name, i))
	}
	out := make([]interface{}, 0, total)
	for i := 0; i < autoShards; i++ {
		out = append(out, mi.Snapshot(subKey(name, i))...)
	}
	return out
}

// ClearShard removes every shard of the logical list.
func (mi *ModuleInstance) ClearShard(name string) {
	for i := 0; i < autoShards; i++ {
		mi.ClearList(subKey(name, i))
	}
}

// UpdateShard performs an atomic read-modify-write on the VU's OWN shard of a
// logical accumulator — so N VUs updating "acc" never contend. Each shard holds
// a partial value; combine them with ReadShard(mergeFn) when you need the whole.
//
//	// each VU accumulates into its shard, lock-free across VUs:
//	shared.updateShard('stats', s => { s = s || {n:0}; s.n++; return s; });
//	// later, merge all shards into the total:
//	const total = shared.readShard('stats', (acc, part) => {
//	    acc.n += (part?.n || 0); return acc;
//	}, () => ({n:0}));
func (mi *ModuleInstance) UpdateShard(name string, fn sobek.Value) interface{} {
	call, ok := sobek.AssertFunction(fn)
	if !ok {
		panic(mi.vu.Runtime().NewTypeError("updateShard: second argument must be a function"))
	}
	rt := mi.vu.Runtime()
	key := subKey(name, mi.vuShard())
	return mi.updateValue(key, func(cur interface{}) interface{} {
		res, err := call(sobek.Undefined(), rt.ToValue(cur))
		if err != nil {
			panic(err)
		}
		return res.Export()
	})
}

// ReadShard folds every shard of a logical accumulator into one value. seedFn
// produces the initial accumulator; mergeFn(acc, part) folds each shard's value
// in. Runs OUTSIDE any store lock (works on copies). Returns the merged value.
func (mi *ModuleInstance) ReadShard(name string, mergeFn, seedFn sobek.Value) interface{} {
	merge, ok := sobek.AssertFunction(mergeFn)
	if !ok {
		panic(mi.vu.Runtime().NewTypeError("readShard: second argument (merge) must be a function"))
	}
	rt := mi.vu.Runtime()
	var acc sobek.Value = sobek.Undefined()
	if seed, ok := sobek.AssertFunction(seedFn); ok {
		res, err := seed(sobek.Undefined())
		if err != nil {
			panic(err)
		}
		acc = res
	}
	for i := 0; i < autoShards; i++ {
		part := mi.Get(subKey(name, i))
		if part == nil {
			continue
		}
		res, err := merge(sobek.Undefined(), acc, rt.ToValue(part))
		if err != nil {
			panic(err)
		}
		acc = res
	}
	return acc.Export()
}

// =============================================================================
// Columnar tables — simple API. You push objects like a normal list; internally
// they are stored column-by-column (structure-of-arrays) to save ~6-7x memory
// on big pools of uniformly-shaped flat records. No schema to declare — the
// schema is inferred from the first object and GROWS automatically if a later
// object has a new field (older rows get a default for it). Missing fields read
// back as null. Only scalar fields (number/string/bool/null) are columnar; a
// nested object/array field falls back to a boxed column (still works, less
// lean). Best for homogeneous flat pools (event pools, user rows).
//
//   shared.tablePushMany('events', arr);   // schema inferred from arr[0], grows as needed
//   shared.tableRandom('events');          // -> a random row as a plain object
//   shared.tableRow('events', 3);          // -> row 3 as an object
//   shared.tableSize('events');            // row count
//   shared.tableClear('events');
// =============================================================================

// colKindOf picks the column kind for a JS value.
//   'n' number, 's' string, 'b' bool  — typed, memory-lean columns
//   'a' object/array                   — boxed column (composite: can't be typed)
//   '?' null                           — UNDETERMINED: null alone doesn't fix a
//                                        type; the first non-null value will.
// This means a leading null no longer permanently boxes an otherwise-numeric
// column (review finding #3, second half).
func colKindOf(v interface{}) byte {
	switch v.(type) {
	case float64, int, int64:
		return 'n'
	case string:
		return 's'
	case bool:
		return 'b'
	case nil:
		return '?' // undetermined until a non-null value arrives
	default:
		return 'a' // object / array -> boxed
	}
}

// promoteToAny migrates a typed column into a boxed 'a' column, preserving all
// existing values. Used when a field turns out to hold mixed types across rows
// (e.g. number in one object, string in another) — instead of silently coercing
// the later value to 0/"" (review finding #3), the whole column becomes boxed so
// every value round-trips faithfully. Memory-lean typing is lost for that field
// only. Caller holds the write lock.
func (c *soaCol) promoteToAny() {
	if c.kind == 'a' {
		return
	}
	var anys []interface{}
	switch c.kind {
	case 'n':
		anys = make([]interface{}, len(c.nums))
		for i, x := range c.nums {
			anys[i] = x
		}
		c.nums = nil
	case 's':
		anys = make([]interface{}, len(c.strs))
		for i, x := range c.strs {
			anys[i] = x
		}
		c.strs = nil
	case 'b':
		anys = make([]interface{}, len(c.bools))
		for i, x := range c.bools {
			anys[i] = x
		}
		c.bools = nil
	case '?':
		// undetermined column only ever appended nils
		anys = make([]interface{}, colLen(c))
	}
	c.kind = 'a'
	c.anys = anys
}

// colLen returns a column's current length regardless of kind.
func colLen(c *soaCol) int {
	switch c.kind {
	case 'n':
		return len(c.nums)
	case 's':
		return len(c.strs)
	case 'b':
		return len(c.bools)
	default:
		return len(c.anys)
	}
}

// valueKind returns the column kind a concrete value wants ('?' for nil).
func valueKind(v interface{}) byte { return colKindOf(v) }

// ensureCol returns the column for field, creating it (kind from sample) if
// absent, back-filled with `rows` defaults so every column stays exactly `rows`
// long BEFORE this row is appended. Caller holds the lock.
func (tb *soaTable) ensureCol(field string, sample interface{}, rows int) *soaCol {
	if c, ok := tb.cols[field]; ok {
		return c
	}
	c := &soaCol{kind: colKindOf(sample)}
	switch c.kind {
	case 'n':
		c.nums = make([]float64, rows)
	case 's':
		c.strs = make([]string, rows)
	case 'b':
		c.bools = make([]bool, rows)
	default:
		c.anys = make([]interface{}, rows)
	}
	tb.cols[field] = c
	tb.fields = append(tb.fields, field)
	return c
}

// appendRow adds one object as a new row. Invariant: on entry AND exit every
// column has exactly tb.rows / tb.rows+1 elements. Caller holds the write lock.
func (tb *soaTable) appendRow(obj map[string]interface{}) {
	// 1) create any NEW columns first, back-filled to the CURRENT row count, so
	//    all columns are tb.rows long before we append this row's values.
	for f, v := range obj {
		if _, known := tb.cols[f]; !known {
			tb.ensureCol(f, v, tb.rows)
		}
	}
	// 2) append exactly one value to EVERY column (default if field missing).
	for _, f := range tb.fields {
		v, present := obj[f]
		c := tb.cols[f]
		if present && v != nil {
			// A typed column meeting a value of a DIFFERENT type must not coerce
			// (that silently loses data — review finding #3). Resolve an
			// undetermined ('?') column to this value's type, or promote a
			// mismatched typed column to boxed 'a' so the value round-trips.
			vk := valueKind(v)
			if c.kind == '?' {
				c.retype(vk)
			} else if c.kind != 'a' && c.kind != vk {
				c.promoteToAny()
			}
		}
		c.appendVal(v, present)
	}
	tb.rows++
}

// retype converts a still-undetermined ('?') column (which so far holds only
// nils in its anys slice) to a concrete typed column of kind k, back-filling the
// existing nils with the type's zero value. Caller holds the write lock.
func (c *soaCol) retype(k byte) {
	if c.kind != '?' {
		return
	}
	n := len(c.anys)
	c.anys = nil
	c.kind = k
	switch k {
	case 'n':
		c.nums = make([]float64, n)
	case 's':
		c.strs = make([]string, n)
	case 'b':
		c.bools = make([]bool, n)
	default:
		c.kind = 'a'
		c.anys = make([]interface{}, n)
	}
}

func (c *soaCol) appendVal(v interface{}, present bool) {
	switch c.kind {
	case 'n':
		if present {
			c.nums = append(c.nums, toFloat(v))
		} else {
			c.nums = append(c.nums, 0)
		}
	case 's':
		if present {
			c.strs = append(c.strs, toStr(v))
		} else {
			c.strs = append(c.strs, "")
		}
	case 'b':
		b, _ := v.(bool)
		c.bools = append(c.bools, present && b)
	default:
		if present {
			c.anys = append(c.anys, v)
		} else {
			c.anys = append(c.anys, nil)
		}
	}
}


// rowAt reconstructs row i as a plain object. Caller holds the read lock.
func (tb *soaTable) rowAt(i int) map[string]interface{} {
	if i < 0 || i >= tb.rows {
		return nil
	}
	out := make(map[string]interface{}, len(tb.fields))
	for _, f := range tb.fields {
		c := tb.cols[f]
		switch c.kind {
		case 'n':
			out[f] = c.nums[i] // value-copied — safe
		case 's':
			out[f] = c.strs[i]
		case 'b':
			out[f] = c.bools[i]
		default:
			// boxed column may hold a map/slice — deep-copy so the returned row
			// never hands JS a live reference into the stored value (review
			// finding #6). Scalars in a boxed column pass through unchanged.
			out[f] = deepCopy(c.anys[i])
		}
	}
	return out
}

func toFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}
func toStr(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// TablePush appends one object as a row. Schema is inferred/grown automatically.
// Returns the new row count. Panics with a clear TypeError if the argument is not
// an object (JS null/number/array/string) — otherwise a null would silently add
// a row of defaults.
func (mi *ModuleInstance) TablePush(name string, obj map[string]interface{}) int {
	if obj == nil {
		panic(mi.vu.Runtime().NewTypeError("tablePush: argument must be an object"))
	}
	sh := mi.store.soaShard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	tb := sh.tables[name]
	if tb == nil {
		tb = &soaTable{cols: make(map[string]*soaCol)}
		sh.tables[name] = tb
	}
	tb.appendRow(obj)
	return tb.rows
}

// TablePushMany appends many objects. The first object seeds the schema; any new
// field in a later object grows the schema (older rows default). Returns the new
// row count. Non-object elements (number/string/null/array) are SKIPPED — if any
// were skipped a warning is logged so the count mismatch isn't silent (F2).
func (mi *ModuleInstance) TablePushMany(name string, objs []interface{}) int {
	sh := mi.store.soaShard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	tb := sh.tables[name]
	if tb == nil {
		tb = &soaTable{cols: make(map[string]*soaCol)}
		sh.tables[name] = tb
	}
	skipped := 0
	for _, o := range objs {
		if m, ok := o.(map[string]interface{}); ok && m != nil {
			tb.appendRow(m)
		} else {
			skipped++
		}
	}
	if skipped > 0 {
		fmt.Printf("[xk6-shared] tablePushMany(%q): skipped %d non-object element(s)\n", name, skipped)
	}
	return tb.rows
}

// TableRow returns row i as an object (negative counts from end), or null.
func (mi *ModuleInstance) TableRow(name string, i int) interface{} {
	sh := mi.store.soaShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	tb := sh.tables[name]
	if tb == nil {
		return nil
	}
	if i < 0 {
		i += tb.rows
	}
	r := tb.rowAt(i)
	if r == nil {
		return nil
	}
	return r
}

// TableRandom returns a random row as an object, or null if empty. Hot path:
// reconstructs ONE row, not the whole table.
func (mi *ModuleInstance) TableRandom(name string) interface{} {
	sh := mi.store.soaShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	tb := sh.tables[name]
	if tb == nil || tb.rows == 0 {
		return nil
	}
	r := tb.rowAt(mi.randIntn(tb.rows))
	if r == nil {
		return nil
	}
	return r
}

// TableField returns ONE field of row i without rebuilding the whole row — the
// cheapest possible read. null if absent.
func (mi *ModuleInstance) TableField(name string, i int, field string) interface{} {
	sh := mi.store.soaShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	tb := sh.tables[name]
	if tb == nil {
		return nil
	}
	if i < 0 {
		i += tb.rows
	}
	if i < 0 || i >= tb.rows {
		return nil
	}
	c := tb.cols[field]
	if c == nil {
		return nil
	}
	switch c.kind {
	case 'n':
		return c.nums[i]
	case 's':
		return c.strs[i]
	case 'b':
		return c.bools[i]
	default:
		// boxed column may hold a map/slice — deep-copy so we don't hand JS a
		// live reference into the stored value (same fix as rowAt; the RLock is
		// released once JS gets the value, so a live ref would be an unlocked
		// aliasing race). Scalars in a boxed column pass through unchanged.
		return deepCopy(c.anys[i])
	}
}

// TableSize returns the row count.
func (mi *ModuleInstance) TableSize(name string) int {
	sh := mi.store.soaShard(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if tb := sh.tables[name]; tb != nil {
		return tb.rows
	}
	return 0
}

// TableClear drops a table.
func (mi *ModuleInstance) TableClear(name string) {
	sh := mi.store.soaShard(name)
	sh.mu.Lock()
	delete(sh.tables, name)
	sh.mu.Unlock()
}

// =============================================================================
// Introspection
// =============================================================================

// Keys returns the names of all scalar variables (for debugging).
func (mi *ModuleInstance) Keys() []string {
	s := mi.store
	out := make([]string, 0)
	for i := 0; i < shardCount; i++ {
		sh := &s.valShards[i]
		sh.mu.RLock()
		for k := range sh.m {
			out = append(out, k)
		}
		sh.mu.RUnlock()
	}
	return out
}

// Lists returns the names of all lists (for debugging).
func (mi *ModuleInstance) Lists() []string {
	s := mi.store
	out := make([]string, 0)
	for i := 0; i < shardCount; i++ {
		sh := &s.listShards[i]
		sh.mu.RLock()
		for k := range sh.lists {
			out = append(out, k)
		}
		sh.mu.RUnlock()
	}
	return out
}

// FlushAll wipes the entire store. Intended for setup() at the very start of
// a run so state does not leak between iterations of the same process.
func (mi *ModuleInstance) FlushAll() {
	s := mi.store
	// Wipe every shard of every domain. FlushAll runs in setup() before any hot
	// traffic, so locking each shard in turn has no contention cost. Each shard
	// keeps its own lock — the "one lock per operation" invariant is preserved
	// (this loop is the one place that touches all shards, and only at setup).
	for i := 0; i < shardCount; i++ {
		vs := &s.valShards[i]
		vs.mu.Lock()
		vs.m = make(map[string]*valEntry)
		vs.mu.Unlock()

		ls := &s.listShards[i]
		ls.mu.Lock()
		ls.lists = make(map[string][]interface{})
		ls.setIndex = make(map[string]map[string]struct{})
		ls.mu.Unlock()

		ts := &s.treeShards[i]
		ts.mu.Lock()
		ts.trees = make(map[string]*treeEntry)
		ts.mu.Unlock()

		ss := &s.soaShards[i]
		ss.mu.Lock()
		ss.tables = make(map[string]*soaTable)
		ss.mu.Unlock()
	}
	s.counters.Range(func(k, _ interface{}) bool {
		s.counters.Delete(k)
		return true
	})
}
