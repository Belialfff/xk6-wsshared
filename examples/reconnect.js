// Automatic reconnect with replay.
//
//   go run ./examples/mockws -drop-after=3s
//   ./k6-wsshared.exe run examples/reconnect.js
import ws from 'k6/x/wsshared';
import { check } from 'k6';

export const options = { vus: 1, iterations: 1 };

const URL = __ENV.WS_URL || 'ws://127.0.0.1:8090/ws';

export default function () {
    const conn = ws.connect({
        url: URL,
        typePath: 'type',
        fields: [{ name: 'code' }],     // lets waitFor() match on it without keepBody
        onOpen: [{ op: 'hello' }],                            // re-sent on every reconnect
        autoReply: [{ match: { type: 'ping' }, reply: { type: 'pong' } }],
        reconnect: { maxAttempts: 5, minDelayMs: 200, maxDelayMs: 1000 },
        track: [{ name: 'seq', path: 'seq', when: { type: 'quote' } }],
    });

    // replay: true -> re-sent after onOpen on the new socket; otherwise the
    // reconnected socket would be alive but silent.
    conn.send({ op: 'subscribe', channel: 'quotes.EURUSD' }, { replay: true });
    conn.send({ op: 'subscribe', channel: 'quotes.BAD' }, { replay: true });

    // The server refuses .BAD - drop it from the replay list so a reconnect
    // does not keep asking for it.
    if (conn.waitFor({ type: 'ack', field: 'code', equals: 404 }, 3000)) {
        const n = conn.forgetReplay({ path: 'channel', equals: 'quotes.BAD' });
        console.log(`forgot ${n} refused subscription(s); replay list: ${JSON.stringify(conn.replay())}`);
    }

    const s = conn.run(8);
    console.log(`events=${s.events} reconnects=${s.reconnects} drop=${s.lastDropError || '-'} ` +
        `lastSeq=${conn.tracked('seq')}`);
    check(s, {
        'kept receiving': (x) => x.events > 0,
        'reconnected': (x) => x.reconnects > 0,
    });
    check(conn, { 'replay list pruned': (c) => c.replay().length === 1 });
    conn.close();
}
