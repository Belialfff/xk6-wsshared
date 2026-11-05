// Compressed, length-prefixed binary channel - all decoding in Go.
//
//   go run ./examples/mockws -compression=deflate -varint -delimiter=rs
//   ./k6-wsshared.exe run examples/framing.js
import ws from 'k6/x/wsshared';
import { check } from 'k6';

export const options = { vus: 1, iterations: 1 };

const URL = __ENV.WS_URL || 'ws://127.0.0.1:8090/ws';

// Must mirror the server: [varint length][raw deflate(JSON + 0x1e)]
const CODEC = { compression: 'deflate', lengthPrefix: 'varint', delimiter: '\x1e' };

export default function () {
    // The standalone codec: inspect what goes on the wire, no socket needed.
    const c = ws.codec(CODEC);
    const frame = c.encode({ op: 'subscribe', channel: 'quotes.EURUSD' });
    const back = c.decode(frame.bytes);
    console.log(`subscribe frame: ${frame.size} bytes, base64=${frame.base64}`);
    check(back, { 'codec round trip': (m) => JSON.parse(m[0].body).op === 'subscribe' });

    const conn = ws.connect({
        url: URL,
        codec: CODEC,              // outbound frames become binary automatically
        typePath: 'type',
        autoReply: [{ match: { type: 'ping' }, reply: { type: 'pong' } }],
        fields: [{ name: 'ts', fromEnd: true, asTimestamp: true }],
    });
    conn.send({ op: 'subscribe', channel: 'quotes.EURUSD' });

    const got = conn.collect(10, 5000, { type: 'quote' });
    check(got, { '10 quotes over a compressed channel': (g) => g.length === 10 });

    const st = conn.stats();
    const ratio = st.decodedBytesIn / Math.max(1, st.wireBytesIn);
    console.log(`wire=${st.wireBytesIn}b decoded=${st.decodedBytesIn}b ratio=${ratio.toFixed(1)}x ` +
        `fallbacks=${st.fallbacks} decodeErrors=${st.decodeErrors}`);
    check(st, { 'compression actually on': () => ratio > 1.5 });
    conn.close();
}
