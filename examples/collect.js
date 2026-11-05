// Getting exactly the messages you need, filtered in Go.
//
//   go run ./examples/mockws
//   ./k6-wsshared.exe run examples/collect.js
import ws from 'k6/x/wsshared';
import { check } from 'k6';

export const options = { vus: 1, iterations: 1 };

const URL = __ENV.WS_URL || 'ws://127.0.0.1:8090/ws';

export default function () {
    const conn = ws.connect({
        url: URL,
        typePath: 'type',
        keepBody: true,       // needed for body matchers and body()/jsonPath()/match()
        autoReply: [{ match: { type: 'ping' }, reply: { type: 'pong' } }],
        fields: [
            { name: 'ts', fromEnd: true, asTimestamp: true },
            { name: 'symbol', path: 'data.symbol' },          // jsonPath
            { name: 'seq', regex: '"seq":(\\d+)' },           // regex, group 1
            { name: 'code' },                                 // key scan, any depth
        ],
    });

    conn.send({ op: 'subscribe', channel: 'quotes.EURUSD' });
    conn.send({ op: 'subscribe', channel: 'quotes.BAD' });     // the mock refuses this one

    // --- waitFor: first message that matches, or null ------------------------
    const refused = conn.waitFor({ type: 'ack', field: 'code', equals: 404 }, 3000);
    check(refused, { 'refusal seen': (e) => e !== null });
    if (refused) console.log(`refused: ${refused.jsonPath('channel')}`);

    // --- collect: N matching messages, ONE JS wakeup -------------------------
    const quotes = conn.collect(5, 5000, { type: 'quote', path: 'data.symbol', equals: 'EURUSD' });
    check(quotes, { '5 quotes': (q) => q.length === 5 });
    for (const q of quotes) {
        // fields came pre-extracted; jsonPath/match resolve in Go on demand
        console.log(`seq=${q.fields.seq} sym=${q.fields.symbol} ` +
            `bid=${q.jsonPath('data.bid')} lastLevel=${q.jsonPath('data.levels[-1].px')} ` +
            `delay=${q.delays.ts.toFixed(2)}ms`);
    }

    // OR-matching and regex over the body
    const trades = conn.collect(2, 8000, {
        any: [
            { path: 'data.status', equals: 'filled' },
            { path: 'data.status', equals: 'rejected' },
        ],
        regex: '"id":"T-\\d+"',
    });
    check(trades, { 'trades collected': (t) => t.length === 2 });
    for (const t of trades) {
        console.log(`trade ${t.match('"id":"(T-\\d+)"')} status=${t.jsonPath('data.status')} ` +
            `all px values in body: ${t.matchAll('"px":([\\d.]+)').length}`);
    }

    // --- drain: whatever is queued right now, no waiting ---------------------
    const backlog = conn.drain();
    console.log(`drained ${backlog.length} queued messages`);

    // --- helpers work on any text, e.g. an HTTP body -------------------------
    const text = '{"result":{"items":[{"id":1},{"id":2},{"id":3}]}}';
    check(null, {
        'jsonPath helper': () => ws.jsonPath(text, '$.result.items[-1].id') === 3,
        'regex helper': () => ws.regex(text, '"id":(\\d)') === '1',
        'regexAll helper': () => ws.regexAll(text, '"id":(\\d)').length === 3,
    });

    conn.close();
}
