/**
 * Type declarations for the `k6/x/shared` extension (xk6-shared).
 *
 * In-process, cross-VU shared mutable state for a single `k6 run`.
 * All VUs of one generator see the same store — the in-process analogue of
 * Redis, without network or serialization on the hot path.
 *
 * Scope: one generator process. NOT shared across distributed generators.
 *
 * Design rule: keep heavy data in Go. Prefer random()/at()/size()/pushUnique()
 * over snapshot()/get() of a whole pool so 30k VUs don't each copy it.
 *
 * SAFETY: get/at/random/getPath/tableRow/tableRandom/tableField/snapshot/
 * unflatten always return a COPY of composite values, so mutating the result is
 * always safe (it never touches the store) and a concurrent-map crash is
 * impossible. To WRITE back a change use set/setAt/setPath (replace under lock)
 * or update/updatePath (atomic read-modify-write) — do NOT expect a mutated
 * get() result to persist; it is a copy. Scalars (strings/numbers) copy free.
 *
 * Usage:
 *   import shared from 'k6/x/shared';
 *   shared.pushUnique('eventPool', eventId);
 *   const e = shared.random('eventPool');
 */
declare module 'k6/x/shared' {
  /** A value storable in the shared store. Marshaled across the Go<->JS boundary. */
  type SharedValue = string | number | boolean | null | SharedValue[] | { [k: string]: SharedValue };

  interface Shared {
    // --- scalar variables ("plain global variable") ---
    /** Store a scalar/object value under `key`, overwriting any prior value. */
    set(key: string, value: SharedValue): void;
    /** Read the value at `key` (a COPY of objects/arrays — safe to mutate), or null. */
    get(key: string): SharedValue;
    /** Whether a scalar variable exists. */
    has(key: string): boolean;
    /** Remove a scalar variable. */
    delete(key: string): void;
    /** Store only if `key` is absent. Returns true if it wrote ("first VU wins"). */
    setIfAbsent(key: string, value: SharedValue): boolean;
    /** Set `key` to `next` only if current value equals `expected`. Returns true on success. */
    compareAndSwap(key: string, expected: SharedValue, next: SharedValue): boolean;
    /**
     * Atomic read-modify-write. `fn` receives the current value and MUST return
     * the new value. Safe even when many VUs update the same key (no lost
     * updates). `fn` may run more than once on contention — keep it pure.
     *   shared.update('n', v => v + 1);
     *   shared.update('cfg', c => { c.limit++; return c; });
     */
    update(key: string, fn: (current: SharedValue) => SharedValue): SharedValue;

    // --- atomic counters (lock-free) ---
    /** Atomically +1, returns new value. */
    incr(name: string): number;
    /** Atomically add `delta`, returns new value. */
    add(name: string, delta: number): number;
    /** Current counter value (0 if never touched). */
    count(name: string): number;
    /** Reset a counter to 0. */
    resetCounter(name: string): void;

    // --- pools / lists (growable, mutable any time) ---
    /** Append `value`, returns new length. */
    push(name: string, value: SharedValue): number;
    /** Append only if not already present (by string form). Returns true if added. O(1). */
    pushUnique(name: string, value: SharedValue): boolean;
    /** Append every element in one lock. Returns new length. */
    pushMany(name: string, values: SharedValue[]): number;
    /** Length of the list. */
    size(name: string): number;
    /** Element at index `i` (negative counts from end) — a COPY, safe to mutate. Null if OOR. */
    at(name: string, i: number): SharedValue;
    /** Replace the element at index `i` (negative from end). Thread-safe. Returns true if in range. */
    setAt(name: string, i: number, value: SharedValue): boolean;
    /** Uniformly random element (a COPY — safe to mutate), or null if empty. HOT PATH. */
    random(name: string): SharedValue;
    /** Up to `n` distinct random elements (without replacement). */
    randomN(name: string, n: number): SharedValue[];
    /** Remove and return last element (LIFO), or null if empty. */
    pop(name: string): SharedValue;
    /** Remove and return first element (FIFO / queue), or null if empty. */
    shift(name: string): SharedValue;
    /** Whether `value` is in the list's uniqueness index (only for pushUnique-populated lists). */
    listHas(name: string, value: SharedValue): boolean;
    /** Shallow COPY of the whole list. Copies the full pool — avoid on the hot path. */
    snapshot(name: string): SharedValue[];
    /** Empty a list and its uniqueness index. */
    clearList(name: string): void;

    // --- auto-sharded structures: SAME semantics, but writes are spread across
    //     internal per-VU sub-keys so thousands of writers don't share one lock.
    //     Use these instead of push/update when MANY VUs write ONE logical name
    //     in the hot loop. No manual ":'+__VU" needed. ---
    /** Like push(), but into the VU's shard — contention-free across VUs. Returns that shard's length. */
    pushShard(name: string, value: SharedValue): number;
    /** Like pushUnique(), per VU shard. Uniqueness is WITHIN a shard only (use pushUnique for global). */
    pushUniqueShard(name: string, value: SharedValue): boolean;
    /** TOTAL length across all shards of the logical list. */
    sizeShard(name: string): number;
    /** Remove & return one element from the logical queue (FIFO within a shard, round-robin across). null if empty. */
    shiftAny(name: string): SharedValue;
    /** Remove & return one element LIFO from any non-empty shard. null if empty. */
    popAny(name: string): SharedValue;
    /** Uniform random element across all shards (size-weighted). null if empty. */
    randomShard(name: string): SharedValue;
    /** COPY of the whole logical list, all shards concatenated. Setup/debug, not hot loop. */
    snapshotShard(name: string): SharedValue[];
    /** Clear every shard of the logical list. */
    clearShard(name: string): void;
    /** Atomic read-modify-write on the VU's OWN shard of an accumulator — N VUs never contend.
     *  Combine shards with readShard() when you need the whole value. */
    updateShard(name: string, fn: (current: SharedValue) => SharedValue): SharedValue;
    /** Fold every shard of an accumulator into one: seedFn() → initial, mergeFn(acc, part) folds each shard. */
    readShard(name: string, mergeFn: (acc: SharedValue, part: SharedValue) => SharedValue, seedFn: () => SharedValue): SharedValue;

    // --- columnar tables (structure-of-arrays): ~4-6x LESS memory for big pools
    //     of uniformly-shaped FLAT records. Push objects like a normal list — the
    //     schema is inferred from the first object and grows automatically if a
    //     later object has a new field (older rows default; missing fields read
    //     back as null/0/""). No schema to declare. Best for flat homogeneous
    //     pools (event pools, user rows); NOT for deeply nested/ragged objects. ---
    /** Append one object as a row (schema inferred/grown automatically). Returns the new row count. */
    tablePush(name: string, obj: object): number;
    /** Append many objects as rows. First object seeds the schema; new fields grow it. Returns row count. */
    tablePushMany(name: string, objs: object[]): number;
    /** Row i as a plain object (negative counts from end), or null. */
    tableRow(name: string, i: number): SharedValue;
    /** A random row as a plain object, or null. Hot path: rebuilds ONE row, not the whole table. */
    tableRandom(name: string): SharedValue;
    /** One field of row i WITHOUT rebuilding the row (cheapest read). null if row/field absent. */
    tableField(name: string, i: number, field: string): SharedValue;
    /** Row count. */
    tableSize(name: string): number;
    /** Drop the table. */
    tableClear(name: string): void;

    // --- tree (flatten): point access to a nested object by path ---
    /** Flatten a nested object into path->value entries (e.g. "a.b.c"). Returns leaf count. Call once (setup). */
    flatten(name: string, obj: SharedValue): number;
    /** Value at `path` in the named tree (a COPY of the leaf — safe to mutate), or null. O(1). */
    getPath(name: string, path: string): SharedValue;
    /** Set the leaf at `path` (creates tree if needed). Thread-safe. Returns true. */
    setPath(name: string, path: string, value: SharedValue): boolean;
    /** Atomic read-modify-write of one leaf. Same safety as update(). Returns new value. */
    updatePath(name: string, path: string, fn: (current: SharedValue) => SharedValue): SharedValue;
    /** Whether `path` exists in the tree. */
    hasPath(name: string, path: string): boolean;
    /** Remove a leaf from the tree. */
    deletePath(name: string, path: string): void;
    /** Drop a WHOLE tree (the per-tree analogue of clearList/tableClear). */
    treeClear(name: string): void;
    /** Number of leaves in the tree. */
    treeSize(name: string): number;
    /** All leaf paths; if `prefix` non-empty, only those starting with it. O(N) — setup, not hot loop. */
    pathKeys(name: string, prefix: string): string[];
    /** All leaf paths CONTAINING `substr`. O(N) full scan — setup only, cache the result into a pool. */
    pathSearch(name: string, substr: string): string[];
    /** Rebuild the nested object from the tree (arrays restored). Copies all — sparing use. */
    unflatten(name: string): SharedValue;

    // --- Go-side loaders: parse a file in Go, not in the JS isolate ---
    /** Parse CSV text into a pool of row objects keyed by the header row. Returns pool size. */
    loadCsv(name: string, csvText: string): number;
    /** Parse JSON text and flatten it into the named tree. Returns leaf count. */
    loadJson(name: string, jsonText: string): number;
    /** Parse a JSON ARRAY and store each element as a pool entry. Returns pool size. */
    loadJsonPool(name: string, jsonText: string): number;

    // --- introspection / lifecycle ---
    /** Names of all scalar variables. */
    keys(): string[];
    /** Names of all lists. */
    lists(): string[];
    /** Wipe the entire store. Call in setup() to avoid state leaking between runs of the process. */
    flushAll(): void;
  }

  const shared: Shared;
  export default shared;
}
