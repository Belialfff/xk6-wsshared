package wsshared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/altenar/xk6-wsshared/binfmt"
)

const sample = `{
  "type": "update",
  "channel": "orders.EURUSD",
  "data": {
    "id": 4711,
    "symbol": "EURUSD",
    "levels": [
      {"px": 1.0712, "qty": 5, "side": "bid"},
      {"px": 1.0715, "qty": 3, "side": "ask"},
      {"px": 1.0718, "qty": 9, "side": "ask"}
    ],
    "note": "line\nbreak é",
    "meta": {"a.b": "dotted"},
    "nothing": null,
    "ts": "2026-09-18T10:00:00.123Z"
  }
}`

func TestJSONPath(t *testing.T) {
	cases := []struct {
		path string
		want any
	}{
		{"type", "update"},
		{"$.type", "update"},
		{"data.id", 4711.0},
		{"data.levels.0.px", 1.0712},
		{"data.levels[1].side", "ask"},
		{"$.data.levels[-1].qty", 9.0},
		{"data.levels.-3.px", 1.0712},
		{"data['meta']['a.b']", "dotted"},
		{"data.note", "line\nbreak é"},
		{"data.nothing", nil},
		{"data.levels.1", `{"px": 1.0715, "qty": 3, "side": "ask"}`},
	}
	for _, c := range cases {
		raw, err := JSONPath([]byte(sample), c.path)
		if err != nil {
			t.Errorf("%s: %v", c.path, err)
			continue
		}
		if got := jsonValueToGo(raw); got != c.want {
			t.Errorf("%s: got %#v, want %#v", c.path, got, c.want)
		}
	}
	for _, p := range []string{"nope", "data.levels.9", "data.levels.-4", "data.id.x", "data[0]"} {
		if _, err := JSONPath([]byte(sample), p); err == nil {
			t.Errorf("%s: expected not found", p)
		}
	}
	if _, err := compilePath("a['b"); err == nil {
		t.Error("unterminated quote must fail")
	}
}

func TestCodecRoundTrip(t *testing.T) {
	// Compact: the multi-line sample would (correctly) be split by "\n".
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(sample)); err != nil {
		t.Fatal(err)
	}
	sample := buf.String()
	for _, cfg := range []CodecConfig{
		{},
		{Delimiter: "\n"},
		{Compression: "deflate"},
		{Compression: "zlib", Delimiter: "\x1e"},
		{Compression: "gzip", LengthPrefix: "varint"},
		{Compression: "deflate", LengthPrefix: "varint", Delimiter: "\x1e"},
	} {
		c, err := NewCodec(cfg)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := c.Encode([]byte(sample))
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.Decode(frame)
		if err != nil {
			t.Fatalf("%+v: %v", cfg, err)
		}
		if len(res.Messages) != 1 || string(res.Messages[0]) != sample {
			t.Fatalf("%+v: round trip mismatch: %d msgs", cfg, len(res.Messages))
		}
		if res.Fallback {
			t.Fatalf("%+v: unexpected fallback", cfg)
		}
		wantBinary := cfg.Compression != "" || cfg.LengthPrefix != ""
		if c.Binary() != wantBinary {
			t.Fatalf("%+v: binary=%v", cfg, c.Binary())
		}
	}
}

func TestCodecSeveralMessagesPerFrame(t *testing.T) {
	c, _ := NewCodec(CodecConfig{Compression: "deflate", LengthPrefix: "varint", Delimiter: "\n"})
	a, _ := c.Encode([]byte(`{"n":1}`))
	b, _ := c.Encode([]byte(`{"n":2}` + "\n" + `{"n":3}`))
	res, err := c.Decode(append(a, b...))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != 3 || string(res.Messages[2]) != `{"n":3}` {
		t.Fatalf("got %q", res.Messages)
	}
}

// A plain-text ack on a compressed, length-prefixed channel: taken as is
// unless strict.
func TestCodecFormatLayer(t *testing.T) {
	const js = `{"type":"quote","seq":7,"data":{"sym":"EURUSD","levels":[{"px":1.1},{"px":1.2}]},"ok":true}`
	for _, cfg := range []CodecConfig{
		{Format: "msgpack"},
		{Format: "msgpack", LengthPrefix: "varint"},
		{Format: "msgpack", Compression: "deflate", LengthPrefix: "varint"},
		{Format: "msgpack", Compression: "gzip"},
	} {
		c, err := NewCodec(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !c.Binary() || c.FormatName() != "msgpack" {
			t.Errorf("%+v: binary=%v format=%q", cfg, c.Binary(), c.FormatName())
		}
		frame, err := c.Encode([]byte(js))
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.Decode(frame)
		if err != nil || res.Fallback || len(res.Messages) != 1 {
			t.Fatalf("%+v: %v %+v", cfg, err, res)
		}
		// encoding/json order (as written) is preserved by both directions
		if string(res.Messages[0]) != js {
			t.Errorf("%+v: got %s", cfg, res.Messages[0])
		}
		// several length-prefixed records in one frame
		if cfg.LengthPrefix == "varint" {
			two := append(append([]byte(nil), frame...), frame...)
			if res, _ := c.Decode(two); len(res.Messages) != 2 {
				t.Errorf("%+v: two records -> %d messages", cfg, len(res.Messages))
			}
		}
	}

	c, _ := NewCodec(CodecConfig{Format: "msgpack"})
	// a text frame skips the format layer: '{' would otherwise decode as an integer
	res, err := c.DecodeFrame([]byte(`{"type":"ack"}`), false)
	if err != nil || res.Fallback || string(res.Messages[0]) != `{"type":"ack"}` {
		t.Fatalf("text frame: %v %+v", err, res)
	}
	// a binary frame that is not msgpack: kept as is, counted as a fallback
	res, err = c.DecodeFrame([]byte{0xc1, 0x01}, true)
	if err != nil || !res.Fallback || !bytes.Equal(res.Messages[0], []byte{0xc1, 0x01}) {
		t.Fatalf("fallback: %v %+v", err, res)
	}
	strict, _ := NewCodec(CodecConfig{Format: "msgpack", Strict: true})
	if _, err := strict.DecodeFrame([]byte{0xc1}, true); err == nil || !strings.Contains(err.Error(), "invalid type byte") {
		t.Fatalf("strict: %v", err)
	}
	// a plain-text ack in a binary frame on a length-prefixed channel: the
	// prefix does not parse, so no layer below it is attempted either
	vc, _ := NewCodec(CodecConfig{Format: "msgpack", LengthPrefix: "varint"})
	if res, err := vc.Decode([]byte("{}\x1e")); err != nil || !res.Fallback || string(res.Messages[0]) != "{}\x1e" {
		t.Fatalf("plain ack: %v %+v", err, res)
	}
	// sequence: concatenated values are separate messages
	seq, _ := NewCodec(CodecConfig{Format: "msgpack", Sequence: true})
	if res, _ := seq.Decode([]byte{0x81, 0xa1, 'a', 0x01, 0x81, 0xa1, 'a', 0x02}); len(res.Messages) != 2 ||
		string(res.Messages[1]) != `{"a":2}` {
		t.Fatalf("sequence: %q", res.Messages)
	}
	// a payload that is not JSON cannot be formatted
	if _, err := c.Encode([]byte("not json")); err == nil {
		t.Error("non-JSON payload must fail")
	}

	for _, bad := range []struct {
		cfg  CodecConfig
		want string
	}{
		{CodecConfig{Format: "avro"}, "unknown format"},
		{CodecConfig{Format: "protobuf"}, "needs codec.proto"},
		{CodecConfig{Format: "msgpack", Delimiter: "\n"}, "cannot be combined"},
		{CodecConfig{Format: "protobuf", Proto: &binfmt.ProtoConfig{Source: "nope", Message: "a.B"}}, "compiling"},
	} {
		if _, err := NewCodec(bad.cfg); err == nil || !strings.Contains(err.Error(), bad.want) {
			t.Errorf("%+v: %v, want %q", bad.cfg, err, bad.want)
		}
	}
}

func TestCodecProtobuf(t *testing.T) {
	c, err := NewCodec(CodecConfig{Format: "protobuf", LengthPrefix: "varint", Proto: &binfmt.ProtoConfig{
		Source:  `syntax = "proto3"; package t; message Env { string type = 1; int64 seq = 2; repeated string tags = 3; }`,
		Message: "t.Env",
	}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := c.Encode([]byte(`{"type":"quote","seq":5,"tags":["a","b"]}`))
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Decode(frame)
	if err != nil || len(res.Messages) != 1 || string(res.Messages[0]) != `{"type":"quote","seq":"5","tags":["a","b"]}` {
		t.Fatalf("%v %q", err, res.Messages)
	}
	// jsonPath works on the JSON view
	if v, err := JSONPath(res.Messages[0], "tags[-1]"); err != nil || string(v) != `"b"` {
		t.Errorf("jsonPath on protobuf: %s %v", v, err)
	}
}

func TestCodecFallback(t *testing.T) {
	c, _ := NewCodec(CodecConfig{Compression: "deflate", LengthPrefix: "varint", Delimiter: "\x1e"})
	res, err := c.Decode([]byte("{}\x1e"))
	if err != nil || !res.Fallback || len(res.Messages) != 1 || string(res.Messages[0]) != "{}" {
		t.Fatalf("fallback: %+v %v", res, err)
	}
	strict, _ := NewCodec(CodecConfig{Compression: "deflate", LengthPrefix: "varint", Strict: true})
	if _, err := strict.Decode([]byte("{}\x1e")); err == nil {
		t.Fatal("strict must fail")
	}
}

func TestVarIntMultiByte(t *testing.T) {
	big := strings.Repeat("x", 300)
	framed := prependVarInt([]byte(big))
	if framed[0]&0x80 == 0 {
		t.Fatal("300 needs a 2-byte prefix")
	}
	parts, err := parseVarIntFrames(framed)
	if err != nil || string(parts[0]) != big {
		t.Fatalf("%v", err)
	}
	if _, err := parseVarIntFrames(framed[:100]); err == nil {
		t.Fatal("truncated must fail")
	}
}

func mustField(t *testing.T, f FieldSpec) FieldSpec {
	t.Helper()
	if err := f.compile(); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestExtract(t *testing.T) {
	now, _ := time.Parse(time.RFC3339Nano, "2026-09-18T10:00:00.200Z")
	specs := []FieldSpec{
		mustField(t, FieldSpec{Name: "ts", FromEnd: true, AsTimestamp: true}),
		mustField(t, FieldSpec{Name: "sym", Key: "symbol"}),
		mustField(t, FieldSpec{Name: "best", Path: "data.levels.0.px"}),
		mustField(t, FieldSpec{Name: "pair", Regex: `orders\.([A-Z]{6})`}),
		mustField(t, FieldSpec{Name: "whole", Regex: `orders\.[A-Z]+`}),
		mustField(t, FieldSpec{Name: "missing"}),
	}
	got := map[string]Extracted{}
	for _, e := range Extract([]byte(sample), specs, now) {
		got[e.Name] = e
	}
	if e := got["ts"]; !e.HasDelay || e.DelayMs < 76.9 || e.DelayMs > 77.1 {
		t.Errorf("ts delay: %+v", e)
	}
	checks := map[string]string{"sym": "EURUSD", "best": "1.0712", "pair": "EURUSD", "whole": "orders.EURUSD"}
	for k, want := range checks {
		if got[k].Value != want {
			t.Errorf("%s: got %q want %q", k, got[k].Value, want)
		}
	}
	if _, ok := got["missing"]; ok {
		t.Error("missing must not be reported")
	}
}

// A key that also appears as a string VALUE must not confuse the key scan.
func TestExtractSkipsValueOccurrence(t *testing.T) {
	body := []byte(`{"kind":"price","price":42}`)
	f := mustField(t, FieldSpec{Name: "price"})
	e := Extract(body, []FieldSpec{f}, time.Now())
	if len(e) != 1 || e[0].Value != "42" {
		t.Fatalf("got %+v", e)
	}
}

func TestTimestamps(t *testing.T) {
	now := time.Date(2026, 9, 18, 10, 0, 1, 0, time.UTC)
	for _, v := range []string{
		"2026-09-18T10:00:00Z", "2026-09-18T10:00:00", "2026-09-18 10:00:00",
		"1789725600", "1789725600000", "1789725600000000", "1789725600000000000",
		"1789725600.5",
	} {
		ts, ok := parseTimestamp(v)
		if !ok {
			t.Errorf("%s: not parsed", v)
			continue
		}
		d, ok := delaySince(ts, now)
		if !ok || d < 499 || d > 1001 {
			t.Errorf("%s: delay %v ok=%v (ts=%v)", v, d, ok, ts)
		}
	}
	for _, v := range []string{"0001-01-01T00:00:00", "2020-01-01T00:00:00Z"} {
		ts, _ := parseTimestamp(v)
		if _, ok := delaySince(ts, now); ok {
			t.Errorf("%s: must be rejected", v)
		}
	}
}

func mustMatch(t *testing.T, s MatchSpec) *matcher {
	t.Helper()
	m, err := compileMatch(s)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMatcher(t *testing.T) {
	yes, no := true, false
	body := []byte(sample)
	fields := []Extracted{{Name: "code", Value: "0"}}
	cases := []struct {
		name string
		spec MatchSpec
		want bool
	}{
		{"contains", MatchSpec{Contains: `"EURUSD"`}, true},
		{"contains miss", MatchSpec{Contains: "GBPUSD"}, false},
		{"regex body", MatchSpec{Regex: `"qty":\s*9`}, true},
		{"path exists", MatchSpec{Path: "data.id"}, true},
		{"path absent", MatchSpec{Path: "data.zzz"}, false},
		{"exists false", MatchSpec{Path: "data.zzz", Exists: &no}, true},
		{"exists true", MatchSpec{Path: "data.id", Exists: &yes}, true},
		{"equals num", MatchSpec{Path: "data.id", Equals: int64(4711), HasEquals: true}, true},
		{"equals str num", MatchSpec{Path: "data.id", Equals: "4711", HasEquals: true}, true},
		{"equals str", MatchSpec{Path: "type", Equals: "update", HasEquals: true}, true},
		{"equals null", MatchSpec{Path: "data.nothing", Equals: nil, HasEquals: true}, true},
		{"oneOf", MatchSpec{Path: "data.levels.1.side", OneOf: []any{"bid", "ask"}}, true},
		{"oneOf miss", MatchSpec{Path: "data.levels.1.side", OneOf: []any{"x"}}, false},
		{"path regex", MatchSpec{Path: "channel", Regex: `^orders\.`}, true},
		{"field equals", MatchSpec{Field: "code", Equals: int64(0), HasEquals: true}, true},
		{"field miss", MatchSpec{Field: "other"}, false},
		{"type", MatchSpec{Types: []string{"snapshot", "update"}}, true},
		{"type miss", MatchSpec{Types: []string{"snapshot"}}, false},
		{"not", MatchSpec{Contains: "GBPUSD", Not: true}, true},
		{"any", MatchSpec{Any: []*matcher{
			mustMatch(t, MatchSpec{Contains: "nope"}),
			mustMatch(t, MatchSpec{Path: "data.symbol", Equals: "EURUSD", HasEquals: true}),
		}}, true},
		{"and", MatchSpec{Contains: "EURUSD", Types: []string{"snapshot"}}, false},
	}
	for _, c := range cases {
		if got := mustMatch(t, c.spec).match(body, "update", fields); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
	for _, bad := range []MatchSpec{
		{},
		{Equals: 1, HasEquals: true},
		{Path: "a", Field: "b"},
		{Path: "a", Equals: map[string]any{}, HasEquals: true},
		{Regex: "("},
	} {
		if _, err := compileMatch(bad); err == nil {
			t.Errorf("%+v: expected error", bad)
		}
	}
	if mustMatch(t, MatchSpec{Field: "code", Equals: "0", HasEquals: true}).needsBody {
		t.Error("a field matcher must not need the body")
	}
}

func TestTrackerPathsKeyByRegexWhen(t *testing.T) {
	tr, err := newTracker([]TrackSpec{
		{Name: "px", Paths: []string{"snapshot.px", "data.px"}, KeyBy: "data.sym"},
		{Name: "seq", Regex: `"seq":(\d+)`},
		{Name: "fills", Paths: []string{"data.px"}, When: mustMatch(t, MatchSpec{Path: "kind", Equals: "fill", HasEquals: true})},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tr.observe([]byte(`{"kind":"quote","seq":1,"data":{"sym":"A","px":1.5}}`), "", nil, now)
	tr.observe([]byte(`{"kind":"quote","seq":2,"data":{"sym":"B","px":7}}`), "", nil, now.Add(time.Millisecond))
	tr.observe([]byte(`{"kind":"fill","seq":3,"data":{"sym":"A","px":1.6}}`), "", nil, now.Add(2*time.Millisecond))
	tr.observe([]byte(`{"kind":"other"}`), "", nil, now.Add(3*time.Millisecond))

	if v, _ := tr.get("px", "A"); v.Value != 1.6 || v.Updates != 2 {
		t.Errorf("px A: %+v", v)
	}
	if v, _ := tr.get("px", "B"); v.Value != 7.0 {
		t.Errorf("px B: %+v", v)
	}
	if v, _ := tr.get("seq", ""); v.Value != "3" {
		t.Errorf("seq: %+v", v)
	}
	if v, _ := tr.get("fills", ""); v.Value != 1.6 || v.Updates != 1 {
		t.Errorf("fills: %+v", v)
	}
	if n := tr.missCount("px"); n != 1 {
		t.Errorf("px misses: %d", n)
	}
	if n := tr.missCount("fills"); n != 0 {
		t.Errorf("when-filtered messages must not count as misses: %d", n)
	}
	if len(tr.all("px")) != 2 {
		t.Error("two keys expected")
	}
}

func TestRingBuffer(t *testing.T) {
	bs, err := newBufferSet([]BufferSpec{
		{Name: "all", Size: 3},
		{Name: "odd", Size: 10, When: mustMatch(t, MatchSpec{Path: "n", OneOf: []any{1.0, 3.0, 5.0}}), KeepBody: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		raw := []byte(fmt.Sprintf(`{"n":%d}`, i))
		ev := Event{Seq: int64(i)}
		bs.observe(&ev, raw, false)
	}
	all, _ := bs.get("all", false)
	if len(all) != 3 || all[0].Seq != 3 || all[2].Seq != 5 || all[0].Body != nil {
		t.Fatalf("ring: %+v", all)
	}
	count, size, total, _ := bs.info("all")
	if count != 3 || size != 3 || total != 5 {
		t.Fatalf("info: %d %d %d", count, size, total)
	}
	odd, _ := bs.get("odd", true)
	if len(odd) != 3 || string(odd[1].Body) != `{"n":3}` {
		t.Fatalf("odd: %+v", odd)
	}
	if again, _ := bs.get("odd", false); len(again) != 0 {
		t.Fatal("take must empty the buffer")
	}
	if _, err := newBufferSet([]BufferSpec{{Name: "x", Size: 0}}); err == nil {
		t.Fatal("size 0 must fail")
	}
}

func TestReconnectBackoffJitter(t *testing.T) {
	r := ReconnectConfig{MinDelay: 100 * time.Millisecond, MaxDelay: time.Second}.withDefaults()
	for attempt := 1; attempt <= 8; attempt++ {
		for i := 0; i < 50; i++ {
			d := r.delayFor(attempt)
			if d < r.MinDelay || d > r.MaxDelay {
				t.Fatalf("attempt %d: %v out of bounds", attempt, d)
			}
		}
	}
}

func BenchmarkJSONPathDeep(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"data":{"levels":[`)
	for i := 0; i < 500; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"px":%d.25,"qty":%d,"side":"bid","tags":["a","b"]}`, i, i)
	}
	sb.WriteString(`],"ts":"2026-09-18T10:00:00Z"}}`)
	body := []byte(sb.String())
	jp, _ := compilePath("data.levels.400.px")
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := jp.lookup(body); err != nil {
			b.Fatal(err)
		}
	}
}
