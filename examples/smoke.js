// Minimal load shape: connect, say hello, subscribe, pump in Go, close.
//
//   go run ./examples/mockws
//   ./k6-wsshared.exe run examples/smoke.js
import ws from 'k6/x/wsshared';
import { check } from 'k6';

export const options = { vus: 2, iterations: 4 };

const URL = __ENV.WS_URL || 'ws://127.0.0.1:8090/ws';

export default function () {
    const conn = ws.connect({
        url: URL,
        query: { token: 'dummy-token' },          // or headers: { Authorization: ... }
        typePath: 'type',                         // -> ev.type and the messageType metric tag
        onOpen: [{ op: 'hello' }],                // sent on every (re)connect
        // Keep-alive handled in Go: the VU is never woken for it.
        autoReply: [{
            match: { type: 'ping' },
            reply: { type: 'pong' },
        }],
        fields: [
            { name: 'ts', fromEnd: true, asTimestamp: true },   // -> wsshared_message_delay_ms
        ],
        tags: { feed: 'quotes' },
    });

    check(conn.waitFor({ type: 'welcome' }, 3000), { 'welcome': (e) => e !== null });

    conn.send({ op: 'subscribe', channel: 'quotes.EURUSD' }, { replay: true });

    // The load-test mode: every message is decoded, measured and dropped in Go.
    const s = conn.run(3);
    check(s, { 'received events': (x) => x.events > 0 });

    const st = conn.stats();
    console.log(`VU ${__VU}: events=${s.events} autoReplied=${st.autoReplied} ` +
        `wire=${st.wireBytesIn}b parse=${st.parseMs.toFixed(1)}ms`);
    conn.close();
}
