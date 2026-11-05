package wsshared

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Fuzz targets for every byte-level parser. They scan untrusted server bytes
// with hand-written index arithmetic - exactly where an off-by-one becomes a
// panic that kills the whole k6 process on the generator.
//
//	go test -run=NONE -fuzz=FuzzJSONPath -fuzztime=30s

var fuzzSeeds = []string{
	sample,
	`{"a":[1,2,{"b":"c\"d"}],"e":null}`,
	`{"a":"\\","b":"\u00e9"}`,
	`[1,[2,[3]]]`,
	`{"a":`, `{"a"`, `"`, `"\`, `[`, `{`, `]`, `}`, ``,
	`{"ts":1789725600000,"x":"ts"}`,
}

func FuzzJSONPath(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s), "a.0.b")
		f.Add([]byte(s), "data.levels[-1].px")
		f.Add([]byte(s), "$['a']")
	}
	f.Fuzz(func(t *testing.T, data []byte, path string) {
		jp, err := compilePath(path)
		if err != nil {
			return
		}
		raw, err := jp.lookup(data)
		if err != nil {
			return
		}
		// A hit must be a sub-slice of the input and convert without panicking.
		if len(raw) > len(data) {
			t.Fatalf("result longer than input")
		}
		_ = jsonValueToGo(raw)
		_ = valueText(raw)
		// On valid JSON a hit must itself be valid JSON.
		if json.Valid(data) && !json.Valid(raw) {
			t.Fatalf("path %q on valid JSON returned invalid %q", path, raw)
		}
	})
}

func FuzzCodecDecode(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	f.Add(prependVarInt([]byte("abc")))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Add([]byte{0x80})
	codecs := []CodecConfig{
		{}, {Delimiter: "\x1e"}, {Compression: "deflate"}, {Compression: "zlib"},
		{Compression: "gzip"}, {LengthPrefix: "varint"},
		{Compression: "deflate", LengthPrefix: "varint", Delimiter: "\x1e"},
		{Compression: "deflate", LengthPrefix: "varint", Strict: true},
		{Format: "msgpack"}, {Format: "msgpack", LengthPrefix: "varint", Sequence: true},
		{Format: "msgpack", Compression: "deflate", Strict: true},
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		for _, cfg := range codecs {
			c, _ := NewCodec(cfg)
			res, err := c.Decode(frame)
			if err != nil {
				continue
			}
			for _, m := range res.Messages {
				if len(m) == 0 {
					t.Fatalf("%+v: empty message emitted", cfg)
				}
				// whatever the format layer accepted must come out as JSON
				if cfg.Format != "" && !res.Fallback && !json.Valid(m) {
					t.Fatalf("%+v: % x -> invalid JSON %s", cfg, frame, m)
				}
			}
		}
	})
}

func FuzzCodecFormatRoundTrip(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	codecs := []CodecConfig{
		{Format: "msgpack"},
		{Format: "msgpack", Compression: "deflate", LengthPrefix: "varint"},
		{Format: "msgpack", Compression: "gzip", LengthPrefix: "varint", Strict: true},
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		if !json.Valid(payload) || bytes.Contains(payload, []byte("$")) {
			return
		}
		for _, cfg := range codecs {
			c, _ := NewCodec(cfg)
			frame, err := c.Encode(payload)
			if err != nil {
				return // depth limit, float out of range
			}
			res, err := c.Decode(frame)
			if err != nil || len(res.Messages) != 1 || res.Fallback {
				t.Fatalf("%+v: %s: %v %+v", cfg, payload, err, res)
			}
			var a, b any
			if json.Unmarshal(payload, &a) != nil || json.Unmarshal(res.Messages[0], &b) != nil || !reflect.DeepEqual(a, b) {
				t.Fatalf("%+v: round trip changed the value:\n in  %s\n out %s", cfg, payload, res.Messages[0])
			}
		}
	})
}

func FuzzCodecRoundTrip(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	codecs := []CodecConfig{
		{Compression: "deflate"}, {Compression: "zlib"}, {Compression: "gzip"},
		{LengthPrefix: "varint"}, {Compression: "deflate", LengthPrefix: "varint"},
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) == 0 {
			return
		}
		for _, cfg := range codecs {
			c, _ := NewCodec(cfg)
			frame, err := c.Encode(payload)
			if err != nil {
				t.Fatal(err)
			}
			res, err := c.Decode(frame)
			if err != nil || len(res.Messages) != 1 || !bytes.Equal(res.Messages[0], payload) {
				t.Fatalf("%+v: round trip failed: %v", cfg, err)
			}
		}
	})
}

func FuzzExtractAndMatch(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	specs := []FieldSpec{
		{Name: "ts", FromEnd: true, AsTimestamp: true},
		{Name: "a"},
		{Name: "b", Path: "a.2.b"},
		{Name: "r", Regex: `"(\w+)":`},
		{Name: "e", AsTimestamp: true},
	}
	for i := range specs {
		if err := specs[i].compile(); err != nil {
			f.Fatal(err)
		}
	}
	yes := true
	matchers := []MatchSpec{
		{Contains: "a"}, {Regex: `\d+`}, {Path: "a.0", Equals: 1.0, HasEquals: true},
		{Path: "a", Exists: &yes}, {Field: "a", Regex: "."}, {Path: "e", Equals: nil, HasEquals: true},
	}
	var compiled []*matcher
	for _, s := range matchers {
		m, err := compileMatch(s)
		if err != nil {
			f.Fatal(err)
		}
		compiled = append(compiled, m)
	}
	tr, _ := newTracker([]TrackSpec{{Name: "x", Paths: []string{"a.0", "data.px"}, KeyBy: "a.1"}})
	bs, _ := newBufferSet([]BufferSpec{{Name: "b", Size: 2, When: compiled[0]}})
	now := time.Now()

	f.Fuzz(func(t *testing.T, body []byte) {
		fields := Extract(body, specs, now)
		for _, m := range compiled {
			_ = m.match(body, "t", fields)
		}
		tr.observe(body, "t", fields, now)
		ev := Event{Fields: fields}
		bs.observe(&ev, body, false)
		_, _ = parseTimestamp(string(body))
	})
}
