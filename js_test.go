package wsshared

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
)

// JS-level tests: drive the module exactly as a script would, through sobek,
// so the binding layer (config export, argument conversion, thrown errors,
// returned objects, metric samples) is covered - not just the Go core.

type jsEnv struct {
	t       *testing.T
	rt      *modulestest.Runtime
	samples chan metrics.SampleContainer
}

func newJSEnv(t *testing.T, vuContext bool) *jsEnv {
	t.Helper()
	rt := modulestest.NewRuntime(t)
	mi := new(RootModule).NewModuleInstance(rt.VU)
	if err := rt.VU.Runtime().Set("ws", mi.Exports().Named); err != nil {
		t.Fatal(err)
	}
	env := &jsEnv{t: t, rt: rt, samples: make(chan metrics.SampleContainer, 1<<16)}
	if vuContext {
		reg := rt.VU.InitEnvField.Registry
		rt.MoveToVUContext(&lib.State{
			Samples:        env.samples,
			Tags:           lib.NewVUStateTags(reg.RootTagSet()),
			BuiltinMetrics: rt.BuiltinMetrics,
		})
	}
	return env
}

func (e *jsEnv) run(code string) sobek.Value {
	e.t.Helper()
	v, err := e.rt.VU.Runtime().RunString(code)
	if err != nil {
		e.t.Fatalf("js error: %v\n--- code ---\n%s", err, code)
	}
	return v
}

// mustThrow runs code and asserts it throws an error containing want.
func (e *jsEnv) mustThrow(code, want string) {
	e.t.Helper()
	_, err := e.rt.VU.Runtime().RunString(code)
	if err == nil {
		e.t.Fatalf("expected a throw containing %q\n--- code ---\n%s", want, code)
	}
	if !strings.Contains(err.Error(), want) {
		e.t.Fatalf("error %q does not contain %q", err.Error(), want)
	}
}

// metricTotals sums samples per metric name (and per name{tag=value} for the
// tags passed).
func (e *jsEnv) metricTotals(tagKeys ...string) map[string]float64 {
	out := map[string]float64{}
	for {
		select {
		case sc := <-e.samples:
			for _, s := range sc.GetSamples() {
				name := s.Metric.Name
				out[name] += s.Value
				out[name+"#n"]++
				for _, k := range tagKeys {
					if v, ok := s.Tags.Get(k); ok {
						key := fmt.Sprintf("%s{%s=%s}", name, k, v)
						out[key] += s.Value
						out[key+"#n"]++
					}
				}
			}
		default:
			return out
		}
	}
}

func TestJSConfigErrors(t *testing.T) {
	url := server(t, idle)
	env := newJSEnv(t, true)
	cases := []struct{ cfg, want string }{
		{`{}`, "url is required"},
		{`{url: U, codec: {compression: 'brotli'}}`, "unknown compression"},
		{`{url: U, codec: {lengthPrefix: 'u32'}}`, "unknown lengthPrefix"},
		{`{url: U, overflow: 'sometimes'}`, "unknown overflow"},
		{`{url: U, typePath: "a['b"}`, "unterminated quote"},
		{`{url: U, fields: [{}]}`, "field needs a name"},
		{`{url: U, fields: [{name: 'x', path: 'a', regex: 'b'}]}`, "exclusive"},
		{`{url: U, fields: [{name: 'x', regex: '('}]}`, "bad regex"},
		{`{url: U, fields: [{name: 'x', regex: 'a(b)', group: 2}]}`, "no group 2"},
		{`{url: U, track: [{name: 'x'}]}`, "needs a path"},
		{`{url: U, track: [{path: 'a'}]}`, "needs a name"},
		{`{url: U, track: [{name: 'x', path: 'a', when: {}}]}`, "empty match"},
		{`{url: U, buffers: [{name: 'b', size: 0}]}`, "size must be"},
		{`{url: U, buffers: [{name: 'b', size: 1}, {name: 'b', size: 1}]}`, "declared twice"},
		{`{url: U, autoReply: [{reply: 'x'}]}`, "needs a match"},
		{`{url: U, autoReply: [{match: {equals: 1}}]}`, "empty match"},
		{`{url: U, autoReply: [{match: {contains: 'x', equals: 1}}]}`, "need a path or a field"},
		{`{url: U, autoReply: [{match: {path: 'a', equals: {x: 1}}}]}`, "equals must be"},
		{`{url: U, heartbeat: {everyMs: 100}}`, "needs send or control"},
		{`{url: U, onOpen: [null]}`, "empty onOpen"},
		{`{url: 'ws://127.0.0.1:1/nothing'}`, "dial failed"},
		{`{url: '://bad'}`, "bad url"},
		{`{url: U, codec: {format: 'avro'}}`, "unknown format"},
		{`{url: U, codec: {format: 'protobuf'}}`, "needs codec.proto"},
		{`{url: U, codec: {format: 'msgpack', delimiter: '\n'}}`, "cannot be combined"},
		{`{url: U, codec: {format: 'protobuf', proto: {source: 'syntax = "proto3"; message M {}'}}}`, "message is required"},
		{`{url: U, codec: {format: 'protobuf', proto: {source: 'syntax = "proto3"; message M {}', message: 'M', descriptorSet: 'x'}}}`, "must be bytes"},
		// payloads are formatted when the config is read, not later in the read loop
		{`{url: U, codec: {format: 'protobuf', proto: {source: 'syntax = "proto3"; message M { string a = 1; }', message: 'M'}}, onOpen: [{b: 1}]}`, "does not fit"},
		{`{url: U, codec: {format: 'msgpack'}, autoReply: [{match: {type: 'x'}, reply: 'not json'}], typePath: 'type'}`, "not JSON"},
	}
	for _, c := range cases {
		env.mustThrow(fmt.Sprintf("var U = %q; ws.connect(%s)", url, c.cfg), c.want)
	}
	m := env.metricTotals()
	if m["wsshared_connect_success_rate#n"] != 2 || m["wsshared_connect_success_rate"] != 0 {
		t.Fatalf("failed dials must be recorded as connect failures: %v", m)
	}
}

func TestJSConnectInInitContext(t *testing.T) {
	env := newJSEnv(t, false)
	env.mustThrow(`ws.connect({url: 'ws://127.0.0.1:1/'})`, "not in the init context")
	// pure helpers work in init context
	if v := env.run(`ws.jsonPath('{"a":[1,2]}', 'a[-1]')`).Export(); v != 2.0 && v != int64(2) {
		t.Fatalf("jsonPath util: %v", v)
	}
}

func TestJSUtilities(t *testing.T) {
	env := newJSEnv(t, false)
	env.run(`
		const eq = (a, b, msg) => { if (a !== b) throw new Error(msg + ': ' + a + ' !== ' + b); };
		const text = '{"r":{"items":[{"id":1},{"id":2},{"id":3}]},"tok":"abc"}';
		eq(ws.jsonPath(text, '$.r.items[-1].id'), 3, 'jsonPath');
		eq(ws.jsonPath(text, 'r.nope'), null, 'jsonPath miss');
		eq(ws.regex(text, '"tok":"([a-z]+)"'), 'abc', 'regex');
		eq(ws.regex(text, 'zzz'), null, 'regex miss');
		eq(ws.regex(text, '"id":\\d', 0), '"id":1', 'regex group 0');
		eq(ws.regexAll(text, '"id":(\\d)').join(','), '1,2,3', 'regexAll');
		eq(ws.regexAll(text, '"id":(\\d)', 1, 2).length, 2, 'regexAll limit');

		const c = ws.codec({compression: 'deflate', lengthPrefix: 'varint', delimiter: '\x1e'});
		eq(c.binary, true, 'codec binary');
		const f = c.encode({op: 'x', n: 1});
		eq(f.bytes.byteLength, f.size, 'encode size');
		const back = c.decode(f.bytes);
		eq(JSON.parse(back[0].body).n, 1, 'decode');
		eq(c.decodeBase64(f.base64)[0].body, back[0].body, 'base64');
		eq(c.decode(new Uint8Array(f.bytes))[0].body, back[0].body, 'Uint8Array input');
		const plain = ws.codec();
		eq(plain.binary, false, 'plain codec');
		eq(plain.decode('{"a":1}')[0].body, '{"a":1}', 'string input');
	`)
	env.mustThrow(`ws.regex('x', '(')`, "bad regex")
	env.mustThrow(`ws.codec({compression: 'nope'})`, "unknown compression")
	env.mustThrow(`ws.codec({}).decodeBase64('!!!')`, "bad base64")
	env.mustThrow(`ws.codec({lengthPrefix: 'varint', strict: true}).decode('\x7f')`, "incomplete message")
	env.mustThrow(`ws.codec({lengthPrefix: 'varint', strict: true}).decode('\x80')`, "truncated")
	env.mustThrow(`ws.jsonPath('{}', "a['b")`, "unterminated")

	// binary formats through the standalone codec
	env.run(`{
		const eq = (a, b, msg) => { if (a !== b) throw new Error(msg + ': ' + a + ' !== ' + b); };
		const m = ws.codec({format: 'msgpack'});
		eq(m.format, 'msgpack', 'format name');
		eq(m.binary, true, 'msgpack is binary');
		eq(m.encode({a: 1}).base64, 'gaFhAQ==', 'msgpack bytes');
		eq(m.decodeBase64('gaFhAQ==')[0].body, '{"a":1}', 'msgpack -> JSON');
		eq(m.decode(m.encode({x: [1, 'y', null]}).bytes)[0].body, '{"x":[1,"y",null]}', 'round trip');
		eq(m.decode('{"t":1}')[0].fallback, true, 'not msgpack: fallback');

		const p = ws.codec({format: 'protobuf', lengthPrefix: 'varint', proto: {
			source: 'syntax = "proto3"; package t; message Env { string type = 1; int64 seq = 2; repeated string tags = 3; }',
			message: 't.Env'}});
		eq(p.format, 'protobuf', 'protobuf name');
		const f = p.encode({type: 'quote', seq: 5, tags: ['a']});
		eq(p.decode(f.bytes)[0].body, '{"type":"quote","seq":"5","tags":["a"]}', 'protobuf -> JSON');
		eq(ws.jsonPath(p.decode(f.bytes)[0].body, 'tags[0]'), 'a', 'jsonPath on the JSON view');
	}`)
	env.mustThrow(`ws.codec({format: 'msgpack'}).encode('not json')`, "not JSON")
}

// A binary-format channel end to end: a text frame bypasses the format, a
// msgpack ping is answered in Go with a msgpack pong, and fields, track,
// delays and jsonPath all work on the JSON view of msgpack messages.
func TestJSBinaryFormatChannel(t *testing.T) {
	codec, err := NewCodec(CodecConfig{Format: "msgpack", LengthPrefix: "varint"})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan string, 8)
	url := server(t, func(c *websocket.Conn) {
		send := func(js string) {
			f, err := codec.Encode([]byte(js))
			if err == nil {
				_ = c.WriteMessage(websocket.BinaryMessage, f)
			}
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		send(`{"type":"ping","n":1}`)
		send(`{"type":"quote","data":{"sym":"EURUSD","levels":[{"px":1.1},{"px":1.2}]},"ts":"` +
			time.Now().UTC().Format(time.RFC3339Nano) + `"}`)
		for i := 0; i < 2; i++ { // onOpen, then the pong
			mt, in, err := c.ReadMessage()
			if err != nil {
				return
			}
			res, _ := codec.DecodeFrame(in, mt == websocket.BinaryMessage)
			for _, m := range res.Messages {
				seen <- fmt.Sprintf("binary=%v %s", mt == websocket.BinaryMessage, m)
			}
		}
		idle(c)
	})

	env := newJSEnv(t, true)
	env.run(fmt.Sprintf(`
		const eq = (a, b, msg) => { if (a !== b) throw new Error(msg + ': ' + a + ' !== ' + b); };
		const c = ws.connect({
			url: %q,
			codec: {format: 'msgpack', lengthPrefix: 'varint'},
			typePath: 'type', keepBody: true,
			onOpen: [{op: 'hello', v: 1}],
			autoReply: [{match: {type: 'ping'}, reply: {type: 'pong'}}],
			fields: [{name: 'sym', path: 'data.sym'}, {name: 'ts', fromEnd: true, asTimestamp: true}],
			track: [{name: 'px', path: 'data.levels[-1].px'}],
		});
		const hello = c.next(2000);
		eq(hello.type, 'hello', 'text frame');
		eq(hello.binary, false, 'text frame type');
		const q = c.waitFor({type: 'quote'}, 2000);
		eq(q.binary, true, 'binary frame');
		eq(q.fields.sym, 'EURUSD', 'field from msgpack');
		eq(q.jsonPath('data.levels[-1].px'), 1.2, 'jsonPath on msgpack');
		eq(typeof q.delays.ts, 'number', 'delay from a msgpack timestamp');
		eq(q.wireBytes < q.size, true, 'msgpack on the wire is smaller than its JSON view');
		eq(q.matchAll('"px":([\\d.]+)').join(','), '1.1,1.2', 'regex on the JSON view');
		eq(c.tracked('px'), 1.2, 'track');
		const st = c.stats();
		eq(st.autoReplied, 1, 'ping answered');
		// the text hello has no length prefix: that layer falls back; the
		// format layer is not even tried on a text frame
		eq(st.fallbacks, 1, 'only the text frame falls back');
		eq(st.messagesReceived, 2, 'hello + quote (the ping was swallowed)');
		eq(st.decodeErrors, 0, 'no decode errors');
		c.close();
	`, url))

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case s := <-seen:
			got[s] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("server saw only %v", keys(got))
		}
	}
	for _, want := range []string{`binary=true {"op":"hello","v":1}`, `binary=true {"type":"pong"}`} {
		if !got[want] {
			t.Errorf("server did not see %s; saw %v", want, keys(got))
		}
	}
}

// fullServer: answers hello, acks subscriptions, streams typed messages,
// pings, and echoes whatever it is asked to.
func fullServer(t *testing.T, seen chan<- string) string {
	return server(t, func(c *websocket.Conn) {
		var mu sync.Mutex
		send := func(s string) {
			mu.Lock()
			defer mu.Unlock()
			_ = c.WriteMessage(websocket.TextMessage, []byte(s))
		}
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			n := 0
			tk := time.NewTicker(5 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tk.C:
					n++
					ts := time.Now().UTC().Format(time.RFC3339Nano)
					send(fmt.Sprintf(`{"type":"quote","seq":%d,"data":{"sym":"S%d","px":%d.5},"ts":"%s"}`, n, n%2, n, ts))
					if n%5 == 0 {
						send(fmt.Sprintf(`{"type":"ping","n":%d}`, n))
					}
					if n%3 == 0 {
						send(fmt.Sprintf(`{"type":"trade","seq":%d,"data":{"id":"T-%d","status":"filled"}}`, n, n))
					}
				}
			}
		}()
		for {
			mt, m, err := c.ReadMessage()
			if err != nil {
				return
			}
			s := string(m)
			if mt == websocket.BinaryMessage {
				s = "bin:" + s
			}
			select {
			case seen <- s:
			default:
			}
			if strings.Contains(s, `"op":"hello"`) {
				send(`{"type":"welcome"}`)
			}
			if strings.Contains(s, `"op":"sub"`) {
				code := 0
				if strings.Contains(s, "BAD") {
					code = 404
				}
				send(fmt.Sprintf(`{"type":"ack","code":%d}`, code))
			}
		}
	})
}

func TestJSFullAPI(t *testing.T) {
	seen := make(chan string, 1024)
	url := fullServer(t, seen)
	env := newJSEnv(t, true)
	env.run(fmt.Sprintf(`
		const eq = (a, b, msg) => { if (a !== b) throw new Error(msg + ': ' + a + ' !== ' + b); };
		const ok = (c, msg) => { if (!c) throw new Error(msg); };

		var conn = ws.connect({
			url: %q,
			query: {token: 'a b&c'},
			typePath: 'type',
			keepBody: true,
			onOpen: [{op: 'hello'}, {send: 'RAW-HELLO', raw: true}],
			autoReply: [{match: {type: 'ping'}, reply: {type: 'pong'}}],
			fields: [
				{name: 'ts', fromEnd: true, asTimestamp: true},
				{name: 'sym', path: 'data.sym'},
				{name: 'seq', regex: '"seq":(\\d+)'},
				{name: 'code'},
			],
			track: [
				{name: 'px', path: 'data.px', keyBy: 'data.sym', when: {type: 'quote'}},
				{name: 'tid', regex: '"id":"(T-\\d+)"'},
				{name: 'typo', path: 'data.nope', when: {type: 'quote'}},
			],
			buffers: [
				{name: 'trades', size: 3, when: {type: 'trade'}},
				{name: 'all', size: 1000},
			],
			tags: {feed: 'test'},
		});

		const welcome = conn.waitFor({type: 'welcome'}, 2000);
		ok(welcome, 'welcome');

		conn.send({op: 'sub', ch: 'A'}, {replay: true});
		conn.send({op: 'sub', ch: 'BAD'}, {replay: true});
		conn.send('plain-string');
		conn.send(new Uint8Array([1, 2, 3]).buffer, {raw: true});

		const refused = conn.waitFor({type: 'ack', field: 'code', equals: 404}, 2000);
		ok(refused, 'refused ack');
		eq(refused.fields.code, '404', 'field is a string');

		const qs = conn.collect(5, 3000, {type: 'quote', path: 'data.sym', oneOf: ['S0', 'S1']});
		eq(qs.length, 5, 'collect 5');
		const q = qs[0];
		eq(q.type, 'quote', 'type');
		ok(q.seq > 0 && q.size > 0 && q.wireBytes > 0 && q.receivedAt > 0, 'event numbers');
		eq(typeof q.delays.ts, 'number', 'delay');
		eq(q.jsonPath('data.sym'), q.fields.sym, 'jsonPath vs field');
		eq(q.jsonPath('nope'), null, 'jsonPath miss');
		eq(q.json().data.sym, q.fields.sym, 'json()');
		eq(q.match('"seq":(\\d+)'), q.fields.seq, 'match');
		eq(q.matchAll('"(\\w+)":').length > 3, true, 'matchAll');
		eq(q.bytes().byteLength, q.bodySize, 'bytes');
		eq(q.body().length, q.bodySize, 'body ascii');
		eq(q.test({path: 'type', equals: 'quote'}), true, 'test');
		eq(q.test({contains: 'zzz'}), false, 'test false');

		const none = conn.collect(3, 300, {type: 'never'});
		eq(none.length, 0, 'collect timeout');

		const tr = conn.collect(2, 3000, {type: 'trade'});
		eq(tr.length, 2, 'trades');

		const pinfo = conn.trackedInfo('px', 'S1');
		ok(pinfo.found && pinfo.updates > 0 && pinfo.ageMs >= 0, 'trackedInfo');
		eq(typeof conn.tracked('px'), 'number', 'tracked number');
		eq(Object.keys(conn.trackedAll('px')).length, 2, 'trackedAll keys');
		ok(/^T-\d+$/.test(conn.tracked('tid')), 'regex track');
		const typo = conn.trackedInfo('typo');
		ok(!typo.found && typo.misses > 0, 'misses visible');
		eq(conn.tracked('unknown'), null, 'unknown tracked');

		const bi = conn.bufferInfo('trades');
		ok(bi.size === 3 && bi.count <= 3 && bi.total >= bi.count, 'bufferInfo');
		const tb = conn.takeBuffer('trades');
		// the connection keeps bodies, so buffered events carry them too
		ok(tb.length > 0 && tb[0].jsonPath('type') === 'trade', 'buffer keeps body with keepBody');
		eq(conn.bufferInfo('trades').count, 0, 'take empties');
		ok(conn.buffer('all').length > 5, 'all buffer');

		const drained = conn.drain(3);
		ok(drained.length <= 3, 'drain max');

		eq(conn.replay().length, 2, 'replay list');
		eq(conn.forgetReplay({path: 'ch', equals: 'BAD'}), 1, 'forgetReplay');
		eq(conn.replay().length, 1, 'replay pruned');

		const r = conn.run(0.3);
		ok(r.events > 0 && r.errors === 0, 'run');

		const st = conn.stats();
		ok(st.messagesReceived > 10 && st.autoReplied > 0 && st.messagesSent >= 6, 'stats ' + JSON.stringify(st));
		eq(st.reconnects, 0, 'no reconnects');

		const cl = conn.close();
		ok(cl.messagesReceived > 0, 'close summary');
		eq(conn.close().alreadyClosed, true, 'close twice');
		eq(conn.next(10), null, 'next after close');
		eq(conn.collect(1, 10).length, 0, 'collect after close');
	`, url))

	env.mustThrow(`conn.send('x')`, "connection is closed")
	env.mustThrow(`conn.buffer('nope')`, `no buffer named "nope"`)

	// what reached the server
	got := map[string]bool{}
	for len(seen) > 0 {
		got[<-seen] = true
	}
	for _, want := range []string{`{"op":"hello"}`, "RAW-HELLO", "plain-string", "bin:\x01\x02\x03", `{"type":"pong"}`} {
		if !got[want] {
			t.Errorf("server did not receive %q (got %v)", want, keys(got))
		}
	}

	m := env.metricTotals("field", "messageType", "feed")
	for _, name := range []string{
		"wsshared_messages_received", "wsshared_messages_sent", "wsshared_bytes_sent",
		"wsshared_wire_bytes_received", "wsshared_auto_replied_total",
		// count, not sum: on Windows an in-process delay is often exactly 0
		"wsshared_message_delay_ms{field=ts}#n", "wsshared_messages_by_type{messageType=quote}",
		"wsshared_session_duration_ms#n", "wsshared_connect_duration_ms#n",
		"wsshared_messages_received{feed=test}",
	} {
		if m[name] <= 0 {
			t.Errorf("metric %s missing (have %d series)", name, len(m))
		}
	}
	if m["wsshared_messages_by_type{messageType=ping}"] != 0 {
		t.Error("swallowed pings must not be counted by type")
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestJSKeepBodyAndQueueGuards(t *testing.T) {
	url := fullServer(t, make(chan string, 1024))
	env := newJSEnv(t, true)
	env.run(fmt.Sprintf(`var a = ws.connect({url: %q, typePath: 'type'});
	                     var b = ws.connect({url: %q, queue: false, track: [{name: 's', path: 'seq'}]});`, url, url))
	env.mustThrow(`a.collect(1, 100, {path: 'seq'})`, "keepBody")
	env.mustThrow(`a.waitFor('quote', 100)`, "keepBody")
	env.run(`if (!a.waitFor({type: 'quote'}, 2000)) throw new Error('type match needs no body');`)
	env.mustThrow(`b.next(10)`, "queue: false")
	env.mustThrow(`b.collect(1, 10)`, "queue: false")
	env.run(`
		const r = b.run(0.2);
		if (r.events !== 0) throw new Error('no-queue run counts nothing');
		if (!(b.tracked('s') > 0)) throw new Error('tracking works without a queue');
		if (b.stats().messagesReceived === 0) throw new Error('messages still received');
		a.close(); b.close();
	`)
}

// Reconnect through the JS layer: onOpen and replay go out again on the new
// socket, tracked state survives, counters are continuous.
func TestJSReconnectReplay(t *testing.T) {
	var conns atomic.Int32
	inbound := make(chan string, 256)
	url := server(t, func(c *websocket.Conn) {
		n := conns.Add(1)
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			for {
				_, m, err := c.ReadMessage()
				if err != nil {
					return
				}
				inbound <- fmt.Sprintf("%d:%s", n, m)
			}
		}()
		for i := 1; i <= 20; i++ {
			if c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"c":%d,"i":%d}`, n, i))) != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		if n < 3 {
			return // drop: abnormal closure from the client's point of view
		}
		// gorilla allows ONE reader: wait for ours instead of reading here too.
		<-readerDone
	})
	env := newJSEnv(t, true)
	env.run(fmt.Sprintf(`
		var conn = ws.connect({
			url: %q,
			onOpen: ['HELLO'],
			reconnect: {maxAttempts: 5, minDelayMs: 10, maxDelayMs: 50},
			track: [{name: 'c', path: 'c'}],
		});
		conn.send('SUB', {replay: true});
		conn.send('ONCE');
		var r = conn.run(1.5);
		if (r.reconnects !== 2) throw new Error('reconnects: ' + JSON.stringify(r));
		if (!r.lastDropError) throw new Error('drop reason');
		if (conn.tracked('c') !== 3) throw new Error('tracked across reconnects: ' + conn.tracked('c'));
		var st = conn.stats();
		if (st.messagesReceived !== 60) throw new Error('continuous counters: ' + st.messagesReceived);
		conn.close();
	`, url))

	got := map[string]bool{}
	timeout := time.After(time.Second)
	for len(got) < 7 {
		select {
		case s := <-inbound:
			got[s] = true
		case <-timeout:
			t.Fatalf("inbound so far: %v", keys(got))
		}
	}
	for _, want := range []string{"1:HELLO", "1:SUB", "1:ONCE", "2:HELLO", "2:SUB", "3:HELLO", "3:SUB"} {
		if !got[want] {
			t.Errorf("missing %s in %v", want, keys(got))
		}
	}
	if got["2:ONCE"] || got["3:ONCE"] {
		t.Error("a send without replay must not be replayed")
	}
	m := env.metricTotals()
	if m["wsshared_reconnects_total"] != 2 || m["wsshared_messages_received"] != 60 {
		t.Errorf("metrics across reconnects: reconnects=%v received=%v",
			m["wsshared_reconnects_total"], m["wsshared_messages_received"])
	}
}

// A drop with reconnect disabled must not be retried, and the failure must be
// reported rather than hang.
func TestJSDropWithoutReconnect(t *testing.T) {
	url := server(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"x":1}`))
	})
	env := newJSEnv(t, true)
	env.run(fmt.Sprintf(`
		var conn = ws.connect({url: %q});
		var start = Date.now();
		var r = conn.run(5);
		if (Date.now() - start > 2000) throw new Error('run must end when the socket is gone');
		if (r.events !== 1 || r.reconnects !== 0 || !r.connectionError) throw new Error(JSON.stringify(r));
	`, url))
	env.mustThrow(`conn.send('x')`, "dropped connection")
	env.run(`conn.close()`)
}
