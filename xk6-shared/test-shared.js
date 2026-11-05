// Smoke/verification test for xk6-shared.
//
// Verifies the core promise: state written by one VU is visible to all other
// VUs of the same run, is mutable at any time, and concurrent access is safe.
//
// Run:
//   ./k6-shared run test-shared.js
// (where k6-shared is the custom binary built with --with .../xk6-shared)

import shared from 'k6/x/shared';
import { check } from 'k6';
import exec from 'k6/execution';

export const options = {
    scenarios: {
        // Phase 1: a few VUs concurrently build a shared pool "in process".
        build: {
            executor: 'shared-iterations',
            vus: 50,
            iterations: 2000,          // 2000 pushUnique attempts across 50 VUs
            maxDuration: '30s',
            exec: 'buildPool',
        },
        // Phase 2: after the pool exists, many VUs hammer it with reads + writes.
        consume: {
            executor: 'constant-vus',
            vus: 200,
            duration: '5s',
            startTime: '2s',           // overlaps build on purpose (concurrency test)
            exec: 'consumePool',
        },
    },
};

// setup runs once, before any VU — the natural place to reset process state.
export function setup() {
    shared.flushAll();
    // Seed a scalar "global variable" and prove it is readable everywhere.
    shared.set('runConfig', { mode: 'demo', target: 12345 });
    shared.set('startedBy', exec.vu.idInTest || 0);
    console.log('[setup] store flushed, seed written');
    return {};
}

// Phase 1: each iteration contributes one unique event id to the pool.
export function buildPool() {
    // Unique id derived from VU + iteration so we get exactly N uniques.
    const id = exec.vu.idInInstance * 100000 + exec.vu.iterationInInstance;
    const added = shared.pushUnique('eventPool', id);
    if (added) {
        shared.incr('poolAdds');
    } else {
        shared.incr('poolDupes');
    }

    // Scalar config seeded in setup must be visible to every VU.
    const cfg = shared.get('runConfig');
    check(cfg, { 'runConfig visible to VU': (c) => c && c.mode === 'demo' });
}

// Phase 2: read random elements from the shared pool and keep mutating it.
export function consumePool() {
    const size = shared.size('eventPool');
    if (size > 0) {
        const e = shared.random('eventPool');           // hot path: one element only
        check(e, { 'random returns a pool element': (v) => v !== null });

        const few = shared.randomN('eventPool', 3);
        check(few, { 'randomN returns up to 3': (a) => a.length <= 3 });
    }

    // Prove writes during the run are visible: bump a shared counter and a list.
    shared.incr('reads');
    shared.pushUnique('consumerSeen', exec.vu.idInInstance);
}

// teardown runs once, after all VUs — assert final consistency.
export function teardown() {
    const poolSize = shared.size('eventPool');
    const adds = shared.count('poolAdds');
    const dupes = shared.count('poolDupes');
    const reads = shared.count('reads');
    const consumers = shared.size('consumerSeen');

    console.log('=========== xk6-shared verification ===========');
    console.log(`pool size (unique events) : ${poolSize}`);
    console.log(`pushUnique added          : ${adds}`);
    console.log(`pushUnique dupes rejected : ${dupes}`);
    console.log(`consume reads (counter)   : ${reads}`);
    console.log(`distinct consumer VUs     : ${consumers}`);
    console.log(`runConfig still present   : ${JSON.stringify(shared.get('runConfig'))}`);

    // Invariant: pushUnique added exactly as many as the list holds.
    check(null, {
        'pool size == unique adds': () => poolSize === adds,
        'reads happened across the run': () => reads > 0,
        'multiple consumer VUs contributed': () => consumers > 1,
        'scalar variable survived the whole run': () =>
            shared.get('runConfig') !== null,
    });
    console.log('================================================');
}
