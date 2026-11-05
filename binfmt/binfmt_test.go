package binfmt

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func oneJSON(t *testing.T, f Format, rec []byte) string {
	t.Helper()
	out, err := f.ToJSON(nil, rec)
	if err != nil {
		t.Fatalf("ToJSON(% x): %v", rec, err)
	}
	if len(out) != 1 {
		t.Fatalf("ToJSON(% x): %d messages, want 1", rec, len(out))
	}
	if !json.Valid(out[0]) {
		t.Fatalf("ToJSON(% x): invalid JSON %s", rec, out[0])
	}
	return string(out[0])
}

// Byte strings straight from the MessagePack spec, independent of our encoder.
func TestMsgPackDecodeSpec(t *testing.T) {
	cases := []struct{ hex, want string }{
		{"c0", `null`},
		{"c2", `false`},
		{"c3", `true`},
		{"00", `0`},
		{"7f", `127`},
		{"e0", `-32`},
		{"ff", `-1`},
		{"cc ff", `255`},
		{"cd ffff", `65535`},
		{"ce ffffffff", `4294967295`},
		{"cf ffffffffffffffff", `18446744073709551615`},
		{"d0 80", `-128`},
		{"d1 8000", `-32768`},
		{"d2 80000000", `-2147483648`},
		{"d3 8000000000000000", `-9223372036854775808`},
		{"ca 3fc00000", `1.5`},
		{"cb 3ff8000000000000", `1.5`},
		{"cb 7ff8000000000000", `"NaN"`},
		{"cb 7ff0000000000000", `"Infinity"`},
		{"cb fff0000000000000", `"-Infinity"`},
		{"a0", `""`},
		{"a3 616263", `"abc"`},
		{"d9 03 616263", `"abc"`},
		{"da 0003 616263", `"abc"`},
		{"db 00000003 616263", `"abc"`},
		{"a5 22 5c 0a 01 7f", `"\"\\\n\u0001` + "\x7f" + `"`},
		{"a2 c328", `"�("`}, // invalid UTF-8
		{"a4 d0bfd180", `"пр"`},
		{"c4 03 010203", `"AQID"`},
		{"c5 0001 ff", `"/w=="`},
		{"90", `[]`},
		{"93 01 a1 78 c0", `[1,"x",null]`},
		{"dc 0002 01 02", `[1,2]`},
		{"dd 00000001 c3", `[true]`},
		{"80", `{}`},
		{"82 a1 61 01 a1 62 92 c2 c3", `{"a":1,"b":[false,true]}`},
		{"de 0001 a1 6b 80", `{"k":{}}`},
		{"81 01 a1 76", `{"1":"v"}`},                                    // int key
		{"81 c3 00", `{"true":0}`},                                      // bool key
		{"81 92 01 02 00", `{"[1,2]":0}`},                               // array key
		{"81 d9 01 6b 00", `{"k":0}`},                                   // str8 key
		{"81 da 0001 6b 00", `{"k":0}`},                                 // str16 key
		{"d6 ff 00000000", `"1970-01-01T00:00:00Z"`},                    // timestamp32
		{"d7 ff 00000004 00000001", `"1970-01-01T00:00:01.000000001Z"`}, // timestamp64: ns=1, s=1
		{"c7 0c ff 00000002 0000000000000003", `"1970-01-01T00:00:03.000000002Z"`}, // timestamp96
		{"d4 05 2a", `{"$ext":5,"$data":"Kg=="}`},
		{"c7 03 7f 010203", `{"$ext":127,"$data":"AQID"}`},
	}
	for _, c := range cases {
		if got := oneJSON(t, MsgPack{}, unhex(t, c.hex)); got != c.want {
			t.Errorf("% s: got %s, want %s", c.hex, got, c.want)
		}
	}
}

func TestMsgPackDecodeErrors(t *testing.T) {
	for _, h := range []string{
		"",                                      // empty
		"c1",                                    // never used
		"a3 6162",                               // str shorter than its header
		"92 01",                                 // array missing an element
		"dc ffff 01",                            // huge array header, little data
		"df ffffffff",                           // huge map header, no data
		"db ffffffff 61",                        // huge str
		"c6 ffffffff",                           // huge bin
		"c9 ffffffff 01",                        // huge ext
		"cb 3ff8",                               // truncated float
		"81 a1 61",                              // map value missing
		"01 02",                                 // trailing bytes (no Sequence)
		strings.Repeat("91", maxDepth+5) + "c0", // too deep
	} {
		if _, err := (MsgPack{}).ToJSON(nil, unhex(t, h)); err == nil {
			t.Errorf("%q: expected an error", h)
		}
	}
}

func TestMsgPackSequence(t *testing.T) {
	out, err := MsgPack{Sequence: true}.ToJSON([][]byte{[]byte("keep")}, unhex(t, "01 a1 78 81 a1 61 c3"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"keep", "1", `"x"`, `{"a":true}`}
	if len(out) != len(want) {
		t.Fatalf("got %q", out)
	}
	for i := range want {
		if string(out[i]) != want[i] {
			t.Errorf("[%d] = %s, want %s", i, out[i], want[i])
		}
	}
	// a broken tail rolls back everything this record added
	out, err = MsgPack{Sequence: true}.ToJSON([][]byte{[]byte("keep")}, unhex(t, "01 02 a3 61"))
	if err == nil || len(out) != 1 {
		t.Fatalf("broken tail: err=%v out=%q", err, out)
	}
}

func TestMsgPackEncode(t *testing.T) {
	cases := []struct{ js, hex string }{
		{`null`, "c0"},
		{`true`, "c3"},
		{`0`, "00"},
		{`127`, "7f"},
		{`128`, "cc 80"},
		{`-1`, "ff"},
		{`-33`, "d0 df"},
		{`65536`, "ce 00010000"},
		{`18446744073709551615`, "cf ffffffffffffffff"},
		{`-9223372036854775808`, "d3 8000000000000000"},
		{`1.5`, "cb 3ff8000000000000"},
		{`1e2`, "cb 4059000000000000"},
		{`"abc"`, "a3 616263"},
		{`[1,"x"]`, "92 01 a1 78"},
		{`{"b":1,"a":2}`, "82 a1 62 01 a1 61 02"}, // key order kept
		{`{"$bin":"AQID"}`, "c4 03 010203"},
		{`{"$ext":5,"$data":"Kg=="}`, "d4 05 2a"},
		{`{"$data":"AQID","$ext":-1}`, "c7 03 ff 010203"},
	}
	for _, c := range cases {
		got, err := MsgPack{}.FromJSON([]byte(c.js))
		if err != nil {
			t.Errorf("%s: %v", c.js, err)
			continue
		}
		if want := unhex(t, c.hex); !bytes.Equal(got, want) {
			t.Errorf("%s: got % x, want % x", c.js, got, want)
		}
	}
	long := strings.Repeat("x", 70000)
	b, err := MsgPack{}.FromJSON([]byte(`"` + long + `"`))
	if err != nil || b[0] != 0xdb || oneJSON(t, MsgPack{}, b) != `"`+long+`"` {
		t.Errorf("str32 round trip failed: %v", err)
	}
	for _, bad := range []string{``, `{`, `[1,]`, `1 2`, `{"$bin":"!!"}`, `{"$ext":999,"$data":""}`, `1e999`,
		strings.Repeat("[", maxDepth+5) + strings.Repeat("]", maxDepth+5)} {
		if _, err := (MsgPack{}).FromJSON([]byte(bad)); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func semanticEqual(t testing.TB, a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func TestMsgPackRoundTrip(t *testing.T) {
	in := `{"type":"event","seq":42,"neg":-7,"price":12.25,"ok":true,"none":null,` +
		`"items":[{"sku":"A-1","qty":3},{"sku":"Б-2","qty":1000000}],"big":9007199254740993,"nested":{"deep":[[[]]]}}`
	b, err := MsgPack{}.FromJSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if out := oneJSON(t, MsgPack{}, b); !semanticEqual(t, []byte(in), []byte(out)) {
		t.Fatalf("round trip:\n in  %s\n out %s", in, out)
	}
	if len(b) >= len(in) {
		t.Errorf("msgpack (%d bytes) should be smaller than JSON (%d)", len(b), len(in))
	}
}

const testProto = `
syntax = "proto3";
package shop;
import "google/protobuf/timestamp.proto";

enum Status { STATUS_UNKNOWN = 0; CREATED = 1; PAID = 2; }
message Item { string sku = 1; int32 qty = 2; double price = 3; }
message Order {
  string order_id = 1;
  repeated Item items = 2;
  Status status = 3;
  int64 total_cents = 4;
  bytes blob = 5;
  optional int32 code = 6;
  google.protobuf.Timestamp at = 7;
}
message Ping { int32 n = 1; }
`

func TestProtobufRoundTrip(t *testing.T) {
	p, err := NewProtobuf(ProtoConfig{Source: testProto, Message: "shop.Order"})
	if err != nil {
		t.Fatal(err)
	}
	in := `{"order_id":"O-1","items":[{"sku":"A","qty":2,"price":9.5}],"status":"PAID",` +
		`"total_cents":"1900","blob":"AQI=","code":0,"at":"2026-11-05T12:00:00Z"}`
	wire, err := p.FromJSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	got := oneJSON(t, p, wire)
	if !semanticEqual(t, []byte(in), []byte(got)) {
		t.Fatalf("round trip:\n in  %s\n out %s", in, got)
	}
	if strings.ContainsAny(got, " \n") {
		t.Errorf("output not compact: %s", got)
	}
	// proto3: zero values are omitted unless `optional` (code) or EmitDefaults
	wire, _ = p.FromJSON([]byte(`{"order_id":"O-2","code":0}`))
	if got := oneJSON(t, p, wire); got != `{"order_id":"O-2","code":0}` {
		t.Errorf("defaults: %s", got)
	}
	pd, _ := NewProtobuf(ProtoConfig{Source: testProto, Message: "shop.Order", EmitDefaults: true, JSONNames: true})
	if got := oneJSON(t, pd, wire); !strings.Contains(got, `"orderId":"O-2"`) || !strings.Contains(got, `"totalCents":"0"`) {
		t.Errorf("EmitDefaults+JSONNames: %s", got)
	}
	// input accepts both name styles
	if _, err := p.FromJSON([]byte(`{"orderId":"O-3"}`)); err != nil {
		t.Errorf("json name on input: %v", err)
	}
	if _, err := p.FromJSON([]byte(`{"nope":1}`)); err == nil {
		t.Error("unknown field should fail")
	}
	if _, err := p.ToJSON(nil, []byte{0xff, 0xff, 0xff}); err == nil {
		t.Error("garbage should fail")
	}
	// empty record = all defaults
	if got := oneJSON(t, p, nil); got != `{}` {
		t.Errorf("empty: %s", got)
	}
}

func TestProtobufSendMessageAndDescriptorSet(t *testing.T) {
	p, err := NewProtobuf(ProtoConfig{Source: testProto, Message: "shop.Order", SendMessage: "shop.Ping"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := p.FromJSON([]byte(`{"n":7}`))
	if err != nil || !bytes.Equal(w, []byte{0x08, 0x07}) {
		t.Fatalf("send as Ping: % x %v", w, err)
	}

	// a descriptor set built from the same schema
	find, err := schema(ProtoConfig{Source: testProto})
	if err != nil {
		t.Fatal(err)
	}
	set := descriptorSetFor(t, find, "shop.Order")
	pd, err := NewProtobuf(ProtoConfig{DescriptorSet: set, Message: "shop.Order"})
	if err != nil {
		t.Fatal(err)
	}
	w, _ = pd.FromJSON([]byte(`{"order_id":"X","items":[{"qty":1}]}`))
	if got := oneJSON(t, p, w); got != `{"order_id":"X","items":[{"qty":1}]}` {
		t.Errorf("descriptor set: %s", got)
	}
}

// descriptorSetFor builds what `protoc --descriptor_set_out --include_imports` would.
func descriptorSetFor(t *testing.T, find finder, msg string) []byte {
	t.Helper()
	d, err := find(protoreflect.FullName(msg))
	if err != nil {
		t.Fatal(err)
	}
	var set descriptorpb.FileDescriptorSet
	seen := map[string]bool{}
	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		for i := 0; i < fd.Imports().Len(); i++ {
			add(fd.Imports().Get(i).FileDescriptor)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	add(d.ParentFile())
	b, err := proto.Marshal(&set)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestProtobufConfigErrors(t *testing.T) {
	for _, c := range []struct {
		cfg  ProtoConfig
		want string
	}{
		{ProtoConfig{Source: testProto}, "message is required"},
		{ProtoConfig{Message: "a.B"}, "give proto.source"},
		{ProtoConfig{Source: "syntax = \"proto3\"; message {", Message: "a.B"}, "compiling"},
		{ProtoConfig{Source: testProto, Message: "shop.Nope"}, "not found"},
		{ProtoConfig{Source: testProto, Message: "shop.Status"}, "not a message"},
		{ProtoConfig{Source: testProto, Message: "shop.Order", SendMessage: "shop.Nope"}, "not found"},
		{ProtoConfig{DescriptorSet: []byte{0xff}, Message: "a.B"}, "not a FileDescriptorSet"},
		{ProtoConfig{Source: `syntax = "proto3"; import "missing.proto";`, Message: "a.B"}, "compiling"},
	} {
		_, err := NewProtobuf(c.cfg)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: err = %v, want %q", c.cfg.Message, err, c.want)
		}
	}
}

func TestProtobufImportsAndCache(t *testing.T) {
	cfg := ProtoConfig{
		FileName: "api.proto",
		Source:   `syntax = "proto3"; package api; import "common/money.proto"; message Price { common.Money m = 1; }`,
		Imports:  map[string]string{"common/money.proto": `syntax = "proto3"; package common; message Money { int64 units = 1; string cur = 2; }`},
		Message:  "api.Price",
	}
	p, err := NewProtobuf(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := p.FromJSON([]byte(`{"m":{"units":"5","cur":"EUR"}}`))
	if got := oneJSON(t, p, w); got != `{"m":{"units":"5","cur":"EUR"}}` {
		t.Errorf("imports: %s", got)
	}
	if testing.AllocsPerRun(5, func() { _, _ = NewProtobuf(cfg) }) > 200 {
		t.Error("schema cache does not seem to work: NewProtobuf allocates like a compile")
	}
}

// Many VUs connect at once with the same schema: the shared cache and the
// descriptors must hold up under -race.
func TestProtobufConcurrent(t *testing.T) {
	cfg := ProtoConfig{Source: testProto + "\n// concurrent", Message: "shop.Order"}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			p, err := NewProtobuf(cfg)
			if err != nil {
				errs <- err
				return
			}
			for i := 0; i < 50; i++ {
				w, err := p.FromJSON([]byte(fmt.Sprintf(`{"order_id":"O-%d-%d","items":[{"qty":%d}]}`, g, i, i+1)))
				if err != nil {
					errs <- err
					return
				}
				out, err := p.ToJSON(nil, w)
				if err != nil || !strings.Contains(string(out[0]), fmt.Sprintf(`"O-%d-%d"`, g, i)) {
					errs <- fmt.Errorf("round trip %d/%d: %v %s", g, i, err, out)
					return
				}
				if _, err := (MsgPack{}).ToJSON(nil, w); err == nil && len(w) == 0 {
					errs <- fmt.Errorf("impossible")
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

const benchOrder = `{"order_id":"O-000042","items":[{"sku":"SKU-1","qty":2,"price":9.5},{"sku":"SKU-2","qty":1,"price":19.99},` +
	`{"sku":"SKU-3","qty":5,"price":4.25}],"status":"PAID","total_cents":"6774","code":0,"at":"2026-11-05T12:00:00Z"}`

func BenchmarkToJSON(b *testing.B) {
	p, _ := NewProtobuf(ProtoConfig{Source: testProto, Message: "shop.Order"})
	pw, _ := p.FromJSON([]byte(benchOrder))
	mw, _ := MsgPack{}.FromJSON([]byte(benchOrder))
	for _, c := range []struct {
		name string
		f    Format
		w    []byte
	}{{"msgpack", MsgPack{}, mw}, {"protobuf", p, pw}} {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(int64(len(c.w)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := c.f.ToJSON(nil, c.w); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzMsgPackToJSON(f *testing.F) {
	for _, h := range []string{"c0", "93 01 a1 78 c0", "82 a1 61 01 a1 62 92 c2 c3", "d7 ff 00000004 00000001",
		"81 92 01 02 00", "c7 03 7f 010203", "cb 7ff8000000000000", "a2 c328"} {
		f.Add(unhex(f, h))
	}
	f.Fuzz(func(t *testing.T, rec []byte) {
		for _, m := range []MsgPack{{}, {Sequence: true}} {
			out, err := m.ToJSON(nil, rec)
			if err != nil {
				continue
			}
			for _, js := range out {
				if !json.Valid(js) {
					t.Fatalf("% x -> invalid JSON %s", rec, js)
				}
			}
		}
	})
}

func FuzzMsgPackRoundTrip(f *testing.F) {
	for _, s := range []string{`{"a":[1,-2,3.5,"x",null,true]}`, `"\u0000�"`, `[[[{}]]]`, `18446744073709551615`,
		`-9223372036854775809`, `{"k":"v","k":"w"}`, `0.1`, `1E+2`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, js []byte) {
		if !json.Valid(js) || bytes.Contains(js, []byte(`$`)) {
			return // $bin / $ext are one-way by design
		}
		b, err := MsgPack{}.FromJSON(js)
		if err != nil {
			return // depth limit, float out of range
		}
		out, err := MsgPack{}.ToJSON(nil, b)
		if err != nil {
			t.Fatalf("%s -> % x -> %v", js, b, err)
		}
		if !semanticEqual(t, js, out[0]) {
			t.Fatalf("round trip changed the value:\n in  %s\n out %s", js, out[0])
		}
	})
}

func FuzzProtobufToJSON(f *testing.F) {
	p, err := NewProtobuf(ProtoConfig{Source: testProto, Message: "shop.Order"})
	if err != nil {
		f.Fatal(err)
	}
	seed, _ := p.FromJSON([]byte(`{"order_id":"O-1","items":[{"sku":"A","qty":2,"price":9.5}],"status":"PAID","code":0}`))
	f.Add(seed)
	f.Add([]byte{})
	b64, _ := base64.StdEncoding.DecodeString("CgNPLTE=")
	f.Add(b64)
	f.Fuzz(func(t *testing.T, rec []byte) {
		out, err := p.ToJSON(nil, rec)
		if err != nil {
			return
		}
		if !json.Valid(out[0]) {
			t.Fatalf("% x -> invalid JSON %s", rec, out[0])
		}
		// whatever decoded must encode back
		if _, err := p.FromJSON(out[0]); err != nil {
			t.Fatalf("% x -> %s -> FromJSON: %v", rec, out[0], err)
		}
	})
}
