// N users × several WebSockets each, triggered over HTTP, delivery counted.
//
//   go run ./examples/mockhub                       # ws://127.0.0.1:8091/hub/{orders,prices,alerts}
//   ./k6-wsshared.exe run -q --log-format=raw examples/hubs.js
//   ./k6-wsshared.exe run -q --log-format=raw -e USERS=20 -e PER_HUB=3 -e LOG=0 examples/hubs.js
//
// Each listener VU (= user):
//   1. opens PER_HUB sockets on every hub            → several WS per VU
//   2. subscribes each socket to [user-<VU>, all]
//   3. POSTs a personal message to every hub         → HTTP says how many sockets got it,
//      waits for it on each socket by msgId            the sockets confirm it
//      and prints the whole message + jsonPath cuts
//   4. collect(BCAST): the broadcasts sent by the broadcaster VU
//   5. latest value (track) and last N messages (buffers), then closes
// The broadcaster VU counts sockets (k6 side via xk6-shared vs server /api/stats),
// then broadcasts. teardown() compares: server delivered vs sockets received.
//
// Env: BASE, HUBS, USERS, PER_HUB, BCAST, FRAMING (plain | packed), FORMAT (json | msgpack | protobuf),
//      LOG = 0 summary only | 1 process (whole bodies from one VU) | 2 whole bodies from all VUs
import ws from 'k6/x/wsshared';
import shared from 'k6/x/shared';
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE || 'http://127.0.0.1:8091';
const WS_BASE = BASE.replace(/^http/, 'ws');
const HUBS = (__ENV.HUBS || 'orders,prices,alerts').split(',');
const USERS = Number(__ENV.USERS || 5);
const PER_HUB = Number(__ENV.PER_HUB || 2);
const BCAST = Number(__ENV.BCAST || 3);
const LOG = Number(__ENV.LOG ?? 1);
// FRAMING=packed pairs with: go run ./examples/mockhub -compression=deflate -varint -delimiter=rs
// FORMAT=msgpack|protobuf pairs with: go run ./examples/mockhub -format=msgpack (or protobuf)
//   (binary formats take no delimiter: with FRAMING=packed run the mock without -delimiter)
// The JSON view is identical in every mode, so CUTS below do not change.
const FORMAT = __ENV.FORMAT || 'json';
const PROTO = FORMAT === 'protobuf' ? open('./mockhub/hub.proto') : null;
const CODEC = Object.assign({},
    __ENV.FRAMING === 'packed' ? { compression: 'deflate', lengthPrefix: 'varint' } : {},
    __ENV.FRAMING === 'packed' && FORMAT === 'json' ? { delimiter: '\x1e' } : {},
    FORMAT === 'msgpack' ? { format: 'msgpack' } : {},
    FORMAT === 'protobuf' ? { format: 'protobuf', proto: { source: PROTO, file: 'hub.proto', message: 'mockhub.Envelope' } } : {},
);
const BCAST_START_S = 3;     // listeners are connected and subscribed by then
const BCAST_GAP_S = 0.3;

export const options = {
    scenarios: {
        listeners: { executor: 'per-vu-iterations', vus: USERS, iterations: 1, exec: 'listen', maxDuration: '60s' },
        broadcaster: { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'broadcast',
            startTime: `${BCAST_START_S}s`, maxDuration: '60s' },
    },
    thresholds: { checks: ['rate==1'] },
};

// What to cut out of each hub's message. Every hub has its own JSON shape.
const CUTS = {
    orders: {
        orderId: 'data.orderId', customer: 'data.customer.id', tier: 'data.customer.tier',
        'items[0].sku': 'data.items[0].sku', 'items[-1].price': 'data.items[-1].price', total: 'data.total',
    },
    prices: { symbol: 'data.symbol', bid: 'data.bid', 'levels[0].px': 'data.levels[0].px', 'levels[-1].px': 'data.levels[-1].px' },
    alerts: { level: 'data.level', text: 'data.text', 'tags[0]': 'data.tags[0]' },
};
const REGEX = { orders: '"sku":"(SKU-\\d+)"', prices: '"px":([\\d.]+)', alerts: '"(node-\\d+)"' };

const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };
const ms = (d) => (d === undefined || d === null ? '-' : d.toFixed(1));

function cuts(ev, hub) {
    const out = Object.entries(CUTS[hub] || {}).map(([label, path]) => `${label}=${ev.jsonPath(path)}`);
    if (REGEX[hub]) out.push(`regexAll=[${ev.matchAll(REGEX[hub]).join(',')}]`);
    return out.join(' ');
}

function logger(t0, who) {
    return (where, msg) => {
        if (LOG < 1) return;
        const t = `+${String(Date.now() - t0).padStart(5)}ms`;
        console.log(`${t} [${who}${where ? ' | ' + where : ''}] ${msg}`);
    };
}

export function setup() {
    shared.flushAll();
    const res = http.get(`${BASE}/api/stats`);
    if (res.status !== 200) {
        throw new Error(`mock is not up at ${BASE} (http ${res.status}): go run ./examples/mockhub`);
    }
    console.log(`setup: mock ${BASE} ok, format=${FORMAT} framing=${__ENV.FRAMING || 'plain'}, ` +
        `hubs=${HUBS.join(',')} users=${USERS} sockets/user=${HUBS.length * PER_HUB} ` +
        `→ ${USERS * HUBS.length * PER_HUB} sockets, broadcasts=${BCAST}/hub at +${BCAST_START_S}s`);
    // the mock outlives the test: remember its counters to report this run's delta
    const base = {};
    for (const hub of HUBS) {
        base[hub] = { opened: ws.jsonPath(res.body, `hubs.${hub}.opened`) || 0,
            closed: ws.jsonPath(res.body, `hubs.${hub}.closed`) || 0 };
    }
    return { t0: Date.now(), base };
}

export function listen(data) {
    const t0 = Date.now();
    const me = `user-${__VU}`;
    const say = logger(t0, `VU ${__VU}`);
    // whole bodies from the first listener that gets here (any VU id), or from all with LOG=2
    const wholeBodies = LOG >= 2 || (LOG >= 1 && shared.setIfAbsent('hubs:verbose', __VU));

    // 1. several sockets per VU -------------------------------------------------
    const socks = [];
    for (const hub of HUBS) {
        for (let i = 1; i <= PER_HUB; i++) {
            const name = `${hub}#${i}`;
            const tc = Date.now();
            let conn;
            try {
                conn = ws.connect({
                    url: `${WS_BASE}/hub/${hub}`,
                    query: { user: me },
                    codec: CODEC,
                    typePath: 'type',
                    keepBody: true,           // body()/jsonPath()/matchAll() on queued events
                    overflow: 'dropOldest',
                    autoReply: [{ match: { type: 'ping' }, reply: { type: 'pong' } }],  // answered in Go
                    fields: [
                        { name: 'ts', fromEnd: true, asTimestamp: true },   // → ev.delays.ts
                        { name: 'msgId' }, { name: 'topic' }, { name: 'conn' }, { name: 'code' },
                    ],
                    track: [   // the LATEST value, readable any time
                        { name: 'lastMsg', path: 'msgId', when: { type: 'event' } },
                        { name: 'lastValue', paths: ['data.total', 'data.bid', 'data.level'], when: { type: 'event' } },
                    ],
                    buffers: [{ name: 'events', size: 10, when: { type: 'event' } }],   // the LAST N
                    tags: { hub },
                });
            } catch (e) {
                say(name, `✗ connect failed: ${e}`);
                check(null, { 'ws connected': () => false });
                continue;
            }
            shared.incr(`hubs:open:${hub}`);
            const hello = conn.waitFor({ type: 'welcome' }, 3000);
            const id = hello ? hello.fields.conn : '?';
            check(hello, { 'ws connected': (h) => h !== null });
            say(name, `connected in ${Date.now() - tc}ms → server conn ${id}`);
            socks.push({ hub, name, id, conn });
        }
    }
    say('', `${socks.length} sockets open (${HUBS.length} hubs × ${PER_HUB})`);

    // 2. subscribe: send on all sockets first, then read the acks ----------------
    for (const s of socks) s.conn.send({ op: 'subscribe', topics: [me, 'all'] });
    for (const s of socks) {
        const ack = s.conn.waitFor({ type: 'ack' }, 3000);
        const ok = check(ack, { 'subscribe ack code=0': (a) => a !== null && a.fields.code === '0' });
        if (ok) shared.incr(`hubs:subscribed:${s.hub}`);
        say(s.name, ok ? `subscribed [${me}, all] on ${s.id}` : `✗ subscribe: ${ack ? ack.body() : 'timeout'}`);
        if (ok && wholeBodies && s.name === `${HUBS[0]}#1`) say(s.name, `   whole ack: ${ack.body()}`);
    }

    // 3. HTTP → personal message → find it on each socket by msgId -------------
    for (const hub of HUBS) {
        const res = http.post(`${BASE}/api/${hub}/publish`, JSON.stringify({ topic: me, event: 'personal' }),
            Object.assign({ tags: { name: 'publish' } }, JSON_HDR));
        const msgId = ws.jsonPath(res.body, 'msgId');           // jsonPath works on HTTP bodies too
        const delivered = ws.jsonPath(res.body, 'delivered');
        say('http', `POST /api/${hub}/publish topic=${me} → ${res.status} msgId=${msgId} ` +
            `delivered=${delivered} (${res.timings.duration.toFixed(1)}ms)`);
        check(delivered, { 'personal: server delivered to all my sockets': (d) => d === PER_HUB });
        shared.add('hubs:personal:delivered', delivered || 0);

        for (const s of socks.filter((x) => x.hub === hub)) {
            const ev = s.conn.waitFor({ field: 'msgId', equals: msgId }, 3000);
            if (!check(ev, { 'personal: arrived on socket': (e) => e !== null })) {
                say(s.name, `✗ ${msgId} did not arrive in 3s`);
                continue;
            }
            shared.incr('hubs:personal:received');
            say(s.name, `← ${msgId} (msg #${ev.seq} on this socket) ${ev.wireBytes}b wire → ${ev.size}b JSON ` +
                `delay=${ms(ev.delays.ts)}ms | ${cuts(ev, hub)}`);
            if (wholeBodies && s.name.endsWith('#1')) say(s.name, `   whole message: ${ev.body()}`);
        }
    }

    // 4. broadcasts: N of a kind, one JS wakeup per socket -----------------------
    const until = data.t0 + (BCAST_START_S + BCAST * BCAST_GAP_S + 3) * 1000;
    say('', `waiting for ${BCAST} broadcasts per socket (VU idle, Go reads all ${socks.length} sockets and answers pings)`);
    for (const s of socks) {
        const got = s.conn.collect(BCAST, Math.max(0, until - Date.now()), { field: 'topic', equals: 'all' });
        shared.add(`hubs:bcast:received:${s.hub}`, got.length);
        check(got, { 'broadcast: socket got all': (g) => g.length === BCAST });
        say(s.name, `collected ${got.length}/${BCAST}: ` +
            got.map((e) => `${e.fields.msgId}(${ms(e.delays.ts)}ms)`).join(', '));
        if (got.length && wholeBodies && s.name.endsWith('#1')) {
            say(s.name, `   last broadcast cuts: ${cuts(got[got.length - 1], s.hub)}`);
        }
    }

    // 5. latest value and last N, without touching the queue --------------------
    for (const s of socks) {
        const last = s.conn.trackedInfo('lastMsg');
        const value = s.conn.tracked('lastValue');
        const lastN = s.conn.buffer('events').slice(-3).map((e) => e.fields.msgId);
        const st = s.conn.stats();
        say(s.name, `latest=${last.value} (${last.updates} updates, ${last.found ? last.ageMs.toFixed(0) : '-'}ms ago) ` +
            `lastValue=${value} | last 3: [${lastN.join(', ')}] | in=${st.messagesReceived} out=${st.messagesSent} ` +
            `pings answered=${st.autoReplied} dropped=${st.dropped}`);
        s.conn.close();
    }
    say('', `done, ${socks.length} sockets closed`);
}

export function broadcast(data) {
    const say = logger(data.t0, 'broadcaster');

    // count sockets: what k6 opened (xk6-shared) vs what the server holds
    const st = http.get(`${BASE}/api/stats`);
    for (const hub of HUBS) {
        const opened = shared.count(`hubs:open:${hub}`);
        const subscribed = shared.count(`hubs:subscribed:${hub}`);
        const serverConns = ws.jsonPath(st.body, `hubs.${hub}.connections`);
        const serverAll = ws.jsonPath(st.body, `hubs.${hub}.topics.all`);
        say(hub, `sockets: k6 opened=${opened} subscribed=${subscribed} | server connections=${serverConns} ` +
            `subscribed to 'all'=${serverAll}`);
        check(null, { 'server sees every k6 socket': () => serverConns === opened && serverAll === subscribed });
    }

    for (let i = 1; i <= BCAST; i++) {
        for (const hub of HUBS) {
            const res = http.post(`${BASE}/api/${hub}/publish`, JSON.stringify({ topic: 'all', event: 'broadcast' }),
                Object.assign({ tags: { name: 'publish' } }, JSON_HDR));
            const delivered = res.json('delivered');
            shared.add(`hubs:bcast:delivered:${hub}`, delivered);
            shared.incr(`hubs:bcast:published:${hub}`);
            say(hub, `broadcast ${i}/${BCAST} → ${res.json('msgId')} delivered=${delivered}`);
        }
        sleep(BCAST_GAP_S);
    }
}

export function teardown(data) {
    sleep(0.5);   // let the server register the closes
    const st = http.get(`${BASE}/api/stats`);
    const rows = [];
    let lost = 0;
    let leaked = 0;
    let mismatch = 0;
    for (const hub of HUBS) {
        const delivered = shared.count(`hubs:bcast:delivered:${hub}`);
        const received = shared.count(`hubs:bcast:received:${hub}`);
        const k6Opened = shared.count(`hubs:open:${hub}`);
        const srvOpened = ws.jsonPath(st.body, `hubs.${hub}.opened`) - data.base[hub].opened;
        const srvClosed = ws.jsonPath(st.body, `hubs.${hub}.closed`) - data.base[hub].closed;
        lost += delivered - received;
        leaked += srvOpened - srvClosed;
        mismatch += Math.abs(srvOpened - k6Opened);
        rows.push([hub, k6Opened, `${srvOpened}/${srvClosed}`,
            shared.count(`hubs:bcast:published:${hub}`), delivered, received, delivered - received]);
    }
    const pd = shared.count('hubs:personal:delivered');
    const pr = shared.count('hubs:personal:received');

    const head = ['hub', 'k6 sockets', 'server open/closed (this run)', 'broadcasts', 'delivered(server)', 'received(sockets)', 'lost'];
    const w = head.map((h, i) => Math.max(h.length, ...rows.map((r) => String(r[i]).length)));
    const line = (r) => r.map((c, i) => String(c).padEnd(w[i])).join('  ');
    console.log(['', '=== delivery summary ===', line(head), ...rows.map(line),
        `personal: server delivered ${pd}, sockets received ${pr}`, ''].join('\n'));

    check(null, {
        'summary: no broadcast lost': () => lost === 0,
        'summary: server opened == k6 opened, all closed': () => mismatch === 0 && leaked === 0,
        'summary: personal delivered == received': () => pd === pr && pd === USERS * HUBS.length * PER_HUB,
    });
}
