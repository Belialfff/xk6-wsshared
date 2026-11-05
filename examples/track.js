// A VU that is busy elsewhere (HTTP calls, think time) and only looks at the
// socket when it needs to. The read loop keeps going in Go the whole time:
// latest values are tracked, recent messages land in ring buffers.
//
//   go run ./examples/mockws
//   ./k6-wsshared.exe run examples/track.js
import ws from 'k6/x/wsshared';
import { check, sleep } from 'k6';

export const options = { vus: 1, iterations: 1 };

const URL = __ENV.WS_URL || 'ws://127.0.0.1:8090/ws';

export default function () {
    const conn = ws.connect({
        url: URL,
        typePath: 'type',
        autoReply: [{ match: { type: 'ping' }, reply: { type: 'pong' } }],
        fields: [{ name: 'ts', fromEnd: true, asTimestamp: true }],

        // Nobody calls next() here, so the queue is off entirely: no backlog,
        // no backpressure on the socket. (Alternative: overflow: 'dropOldest'.)
        queue: false,

        // Latest value, one slot per symbol.
        track: [
            { name: 'bid', path: 'data.bid', keyBy: 'data.symbol', when: { type: 'quote' } },
            { name: 'lastTrade', regex: '"id":"(T-\\d+)"', when: { type: 'trade' } },
        ],
        // Last N messages of a kind, readable at any time.
        buffers: [
            { name: 'trades', size: 20, when: { type: 'trade' }, keepBody: true },
            { name: 'rejects', size: 5, when: { path: 'data.status', equals: 'rejected' } },
        ],
    });

    conn.send({ op: 'subscribe', channel: 'quotes.EURUSD' }, { replay: true });
    conn.send({ op: 'subscribe', channel: 'quotes.GBPUSD' }, { replay: true });

    for (let step = 1; step <= 3; step++) {
        sleep(2);   // stands in for HTTP work; the socket is read in Go meanwhile

        const eur = conn.trackedInfo('bid', 'EURUSD');
        console.log(`step ${step}: EURUSD bid=${eur.value} updates=${eur.updates} ` +
            `age=${eur.found ? eur.ageMs.toFixed(0) : '-'}ms  all=${JSON.stringify(conn.trackedAll('bid'))}`);
    }

    const info = conn.bufferInfo('trades');
    const trades = conn.takeBuffer('trades');          // read and empty
    console.log(`trades buffered=${info.count} seen=${info.total} lastTrade=${conn.tracked('lastTrade')}`);
    for (const t of trades.slice(-3)) {
        console.log(`  ${t.jsonPath('data.id')} ${t.jsonPath('data.status')} qty=${t.jsonPath('data.qty')}`);
    }

    check(null, {
        'bid tracked per symbol': () => Object.keys(conn.trackedAll('bid')).length === 2,
        'trades buffered': () => trades.length > 0,
        'buffer emptied by take': () => conn.bufferInfo('trades').count === 0,
    });

    const st = conn.stats();
    console.log(`received=${st.messagesReceived} queued=${st.queued} autoReplied=${st.autoReplied}`);
    conn.close();
}
