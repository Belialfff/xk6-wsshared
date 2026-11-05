package binfmt

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// MsgPack is MessagePack (https://msgpack.org) without a schema.
//
// Decoding writes JSON straight from the bytes, with no intermediate tree:
//
//	nil, bool, int, uint, float  ->  null, true/false, number
//	float NaN / ±Inf             ->  "NaN", "Infinity", "-Infinity" (JSON has no such numbers)
//	str                          ->  string (invalid UTF-8 -> U+FFFD)
//	bin                          ->  base64 string
//	array, map                   ->  array, object; a non-string map key becomes
//	                                 its JSON text as a string: 1 -> "1"
//	ext -1 (timestamp)           ->  RFC 3339 string, UTC
//	other ext                    ->  {"$ext": type, "$data": "<base64>"}
//
// Encoding takes JSON: integers go out in the smallest int/uint encoding,
// other numbers as float64, strings as str. {"$bin": "<base64>"} becomes bin
// and {"$ext": type, "$data": "<base64>"} becomes ext.
type MsgPack struct {
	// Sequence: a record may hold several concatenated values, each one a
	// message. Off by default, so that trailing garbage is an error rather
	// than a stream of small numbers.
	Sequence bool
}

// Name implements Format.
func (MsgPack) Name() string { return "msgpack" }

var (
	errTruncated = errors.New("msgpack: truncated")
	errDepth     = fmt.Errorf("msgpack: nesting deeper than %d", maxDepth)
)

// ToJSON implements Format.
func (m MsgPack) ToJSON(dst [][]byte, rec []byte) ([][]byte, error) {
	if len(rec) == 0 {
		return dst, errors.New("msgpack: empty record")
	}
	start := len(dst)
	d := mpDecoder{b: rec}
	for {
		// One value: size the buffer for the whole record. A sequence of many
		// small values must not allocate the remaining length per value -
		// that would be quadratic - so let append grow it.
		capHint := 64
		if !m.Sequence {
			capHint = len(rec) + len(rec)/2 + 8
		}
		out, err := d.value(make([]byte, 0, capHint), 0)
		if err != nil {
			return dst[:start], err
		}
		dst = append(dst, out)
		if d.off == len(rec) {
			return dst, nil
		}
		if !m.Sequence {
			return dst[:start], fmt.Errorf("msgpack: %d trailing bytes after the value", len(rec)-d.off)
		}
	}
}

type mpDecoder struct {
	b   []byte
	off int
}

func (d *mpDecoder) take(n int) ([]byte, error) {
	if n < 0 || n > len(d.b)-d.off {
		return nil, errTruncated
	}
	p := d.b[d.off : d.off+n]
	d.off += n
	return p, nil
}

func (d *mpDecoder) uintN(size int) (uint64, error) {
	p, err := d.take(size)
	if err != nil {
		return 0, err
	}
	switch size {
	case 1:
		return uint64(p[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(p)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(p)), nil
	default:
		return binary.BigEndian.Uint64(p), nil
	}
}

// length reads a size field and checks it against what is left, so a hostile
// header cannot make us allocate or loop past the input.
func (d *mpDecoder) length(size int) (int, error) {
	n, err := d.uintN(size)
	if err != nil {
		return 0, err
	}
	if n > uint64(len(d.b)-d.off) {
		return 0, errTruncated
	}
	return int(n), nil
}

func (d *mpDecoder) value(out []byte, depth int) ([]byte, error) {
	if depth > maxDepth {
		return nil, errDepth
	}
	p, err := d.take(1)
	if err != nil {
		return nil, err
	}
	c := p[0]
	switch {
	case c <= 0x7f:
		return strconv.AppendInt(out, int64(c), 10), nil
	case c >= 0xe0:
		return strconv.AppendInt(out, int64(int8(c)), 10), nil
	case c&0xf0 == 0x80:
		return d.mapBody(out, int(c&0x0f), depth)
	case c&0xf0 == 0x90:
		return d.arrayBody(out, int(c&0x0f), depth)
	case c&0xe0 == 0xa0:
		return d.str(out, int(c&0x1f))
	}

	switch c {
	case 0xc0:
		return append(out, "null"...), nil
	case 0xc2:
		return append(out, "false"...), nil
	case 0xc3:
		return append(out, "true"...), nil

	case 0xc4, 0xc5, 0xc6: // bin 8/16/32
		n, err := d.length(1 << (c - 0xc4))
		if err != nil {
			return nil, err
		}
		b, _ := d.take(n)
		return appendBase64String(out, b), nil

	case 0xc7, 0xc8, 0xc9: // ext 8/16/32
		n, err := d.uintN(1 << (c - 0xc7))
		if err != nil {
			return nil, err
		}
		return d.ext(out, n)
	case 0xd4, 0xd5, 0xd6, 0xd7, 0xd8: // fixext 1/2/4/8/16
		return d.ext(out, 1<<(c-0xd4))

	case 0xca:
		u, err := d.uintN(4)
		if err != nil {
			return nil, err
		}
		return appendFloat(out, float64(math.Float32frombits(uint32(u))), 32), nil
	case 0xcb:
		u, err := d.uintN(8)
		if err != nil {
			return nil, err
		}
		return appendFloat(out, math.Float64frombits(u), 64), nil

	case 0xcc, 0xcd, 0xce, 0xcf: // uint 8/16/32/64
		u, err := d.uintN(1 << (c - 0xcc))
		if err != nil {
			return nil, err
		}
		return strconv.AppendUint(out, u, 10), nil
	case 0xd0, 0xd1, 0xd2, 0xd3: // int 8/16/32/64
		size := 1 << (c - 0xd0)
		u, err := d.uintN(size)
		if err != nil {
			return nil, err
		}
		var v int64
		switch size {
		case 1:
			v = int64(int8(u))
		case 2:
			v = int64(int16(u))
		case 4:
			v = int64(int32(u))
		default:
			v = int64(u)
		}
		return strconv.AppendInt(out, v, 10), nil

	case 0xd9, 0xda, 0xdb: // str 8/16/32
		n, err := d.length(1 << (c - 0xd9))
		if err != nil {
			return nil, err
		}
		return d.str(out, n)
	case 0xdc, 0xdd: // array 16/32
		n, err := d.length(2 << (c - 0xdc))
		if err != nil {
			return nil, err
		}
		return d.arrayBody(out, n, depth)
	case 0xde, 0xdf: // map 16/32
		n, err := d.length(2 << (c - 0xde))
		if err != nil {
			return nil, err
		}
		return d.mapBody(out, n, depth)
	}
	return nil, fmt.Errorf("msgpack: invalid type byte 0x%02x at offset %d", c, d.off-1)
}

func (d *mpDecoder) str(out []byte, n int) ([]byte, error) {
	b, err := d.take(n)
	if err != nil {
		return nil, err
	}
	return appendJSONString(out, b), nil
}

func (d *mpDecoder) arrayBody(out []byte, n, depth int) ([]byte, error) {
	if n > len(d.b)-d.off { // every element takes at least one byte
		return nil, errTruncated
	}
	out = append(out, '[')
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		var err error
		if out, err = d.value(out, depth+1); err != nil {
			return nil, err
		}
	}
	return append(out, ']'), nil
}

func (d *mpDecoder) mapBody(out []byte, n, depth int) ([]byte, error) {
	if n > (len(d.b)-d.off)/2 { // key + value: at least two bytes
		return nil, errTruncated
	}
	out = append(out, '{')
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		var err error
		if out, err = d.key(out, depth+1); err != nil {
			return nil, err
		}
		out = append(out, ':')
		if out, err = d.value(out, depth+1); err != nil {
			return nil, err
		}
	}
	return append(out, '}'), nil
}

// key writes a map key as a JSON string. String keys are the norm; anything
// else is rendered as JSON and quoted.
func (d *mpDecoder) key(out []byte, depth int) ([]byte, error) {
	if d.off < len(d.b) {
		c := d.b[d.off]
		var n, hdr int
		switch {
		case c&0xe0 == 0xa0:
			n, hdr = int(c&0x1f), 1
		case c == 0xd9 && d.off+1 < len(d.b):
			n, hdr = int(d.b[d.off+1]), 2
		}
		if hdr > 0 {
			d.off += hdr
			return d.str(out, n)
		}
	}
	js, err := d.value(nil, depth)
	if err != nil {
		return nil, err
	}
	if len(js) > 0 && js[0] == '"' {
		return append(out, js...), nil
	}
	return appendJSONString(out, js), nil
}

func (d *mpDecoder) ext(out []byte, n uint64) ([]byte, error) {
	t, err := d.take(1)
	if err != nil {
		return nil, err
	}
	if n > uint64(len(d.b)-d.off) {
		return nil, errTruncated
	}
	data, _ := d.take(int(n))
	typ := int8(t[0])
	if typ == -1 {
		if ts, ok := timestampExt(data); ok {
			out = append(out, '"')
			out = ts.UTC().AppendFormat(out, time.RFC3339Nano)
			return append(out, '"'), nil
		}
	}
	out = append(out, `{"$ext":`...)
	out = strconv.AppendInt(out, int64(typ), 10)
	out = append(out, `,"$data":`...)
	out = appendBase64String(out, data)
	return append(out, '}'), nil
}

// timestampExt decodes the three timestamp layouts of the spec.
func timestampExt(b []byte) (time.Time, bool) {
	switch len(b) {
	case 4:
		return time.Unix(int64(binary.BigEndian.Uint32(b)), 0), true
	case 8:
		v := binary.BigEndian.Uint64(b)
		return time.Unix(int64(v&0x3ffffffff), int64(v>>34)), true
	case 12:
		ns := binary.BigEndian.Uint32(b)
		return time.Unix(int64(binary.BigEndian.Uint64(b[4:])), int64(ns)), true
	}
	return time.Time{}, false
}

func appendFloat(out []byte, f float64, bits int) []byte {
	switch {
	case math.IsNaN(f):
		return append(out, `"NaN"`...)
	case math.IsInf(f, 1):
		return append(out, `"Infinity"`...)
	case math.IsInf(f, -1):
		return append(out, `"-Infinity"`...)
	}
	return strconv.AppendFloat(out, f, 'g', -1, bits)
}

func appendBase64String(out, b []byte) []byte {
	out = append(out, '"')
	out = base64.StdEncoding.AppendEncode(out, b)
	return append(out, '"')
}

const hexDigits = "0123456789abcdef"

// appendJSONString quotes b as a JSON string. Invalid UTF-8 becomes U+FFFD,
// byte by byte, as encoding/json does.
func appendJSONString(out, b []byte) []byte {
	out = append(out, '"')
	for i := 0; i < len(b); {
		c := b[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				out = append(out, '\\', c)
			case c == '\n':
				out = append(out, '\\', 'n')
			case c == '\r':
				out = append(out, '\\', 'r')
			case c == '\t':
				out = append(out, '\\', 't')
			case c < 0x20:
				out = append(out, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			default:
				out = append(out, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size == 1 {
			out = append(out, `�`...)
		} else {
			out = append(out, b[i:i+size]...)
		}
		i += size
	}
	return append(out, '"')
}

// ---------------------------------------------------------------------------
// JSON -> MessagePack
// ---------------------------------------------------------------------------

// FromJSON implements Format.
func (MsgPack) FromJSON(js []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.UseNumber()
	n, err := readNode(dec, 0)
	if err != nil {
		return nil, fmt.Errorf("msgpack: payload is not JSON: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("msgpack: payload is not JSON: trailing data after the value")
	}
	return n.appendMsgPack(make([]byte, 0, len(js)))
}

// node is an order-preserving JSON tree: map key order survives the trip.
type node struct {
	kind byte // 'n' null, 'b' bool, '#' number, 's' string, 'a' array, 'o' object
	b    bool
	s    string // string value or number text
	keys []string
	kids []*node
}

func readNode(dec *json.Decoder, depth int) (*node, error) {
	if depth > maxDepth {
		return nil, errDepth
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case nil:
		return &node{kind: 'n'}, nil
	case bool:
		return &node{kind: 'b', b: v}, nil
	case json.Number:
		return &node{kind: '#', s: v.String()}, nil
	case string:
		return &node{kind: 's', s: v}, nil
	case json.Delim:
		n := &node{kind: 'a'}
		if v == '{' {
			n.kind = 'o'
		}
		for dec.More() {
			if n.kind == 'o' {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				n.keys = append(n.keys, kt.(string))
			}
			kid, err := readNode(dec, depth+1)
			if err != nil {
				return nil, err
			}
			n.kids = append(n.kids, kid)
		}
		if _, err := dec.Token(); err != nil { // closing delimiter
			return nil, err
		}
		return n, nil
	}
	return nil, fmt.Errorf("unexpected token %v", tok)
}

func (n *node) field(name string) *node {
	for i, k := range n.keys {
		if k == name {
			return n.kids[i]
		}
	}
	return nil
}

func (n *node) appendMsgPack(out []byte) ([]byte, error) {
	switch n.kind {
	case 'n':
		return append(out, 0xc0), nil
	case 'b':
		if n.b {
			return append(out, 0xc3), nil
		}
		return append(out, 0xc2), nil
	case '#':
		return appendNumber(out, n.s)
	case 's':
		return appendStr(out, n.s), nil
	case 'a':
		out = appendHeader(out, len(n.kids), 0x90, 16, 0xdc, 0xdd)
		for _, k := range n.kids {
			var err error
			if out, err = k.appendMsgPack(out); err != nil {
				return nil, err
			}
		}
		return out, nil
	}

	// object: the two escape hatches first
	if len(n.keys) == 1 && n.keys[0] == "$bin" && n.kids[0].kind == 's' {
		b, err := base64.StdEncoding.DecodeString(n.kids[0].s)
		if err != nil {
			return nil, fmt.Errorf("msgpack: $bin: %w", err)
		}
		return appendBin(out, b), nil
	}
	if len(n.keys) == 2 {
		t, d := n.field("$ext"), n.field("$data")
		if t != nil && d != nil && t.kind == '#' && d.kind == 's' {
			typ, err := strconv.ParseInt(t.s, 10, 8)
			if err != nil {
				return nil, fmt.Errorf("msgpack: $ext type: %w", err)
			}
			b, err := base64.StdEncoding.DecodeString(d.s)
			if err != nil {
				return nil, fmt.Errorf("msgpack: $data: %w", err)
			}
			return appendExt(out, int8(typ), b), nil
		}
	}
	out = appendHeader(out, len(n.keys), 0x80, 16, 0xde, 0xdf)
	for i, k := range n.keys {
		out = appendStr(out, k)
		var err error
		if out, err = n.kids[i].appendMsgPack(out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func appendHeader(out []byte, n int, fix byte, fixMax int, c16, c32 byte) []byte {
	switch {
	case n < fixMax:
		return append(out, fix|byte(n))
	case n <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(out, c16), uint16(n))
	default:
		return binary.BigEndian.AppendUint32(append(out, c32), uint32(n))
	}
}

func appendStr(out []byte, s string) []byte {
	n := len(s)
	switch {
	case n < 32:
		out = append(out, 0xa0|byte(n))
	case n <= math.MaxUint8:
		out = append(out, 0xd9, byte(n))
	case n <= math.MaxUint16:
		out = binary.BigEndian.AppendUint16(append(out, 0xda), uint16(n))
	default:
		out = binary.BigEndian.AppendUint32(append(out, 0xdb), uint32(n))
	}
	return append(out, s...)
}

func appendBin(out, b []byte) []byte {
	n := len(b)
	switch {
	case n <= math.MaxUint8:
		out = append(out, 0xc4, byte(n))
	case n <= math.MaxUint16:
		out = binary.BigEndian.AppendUint16(append(out, 0xc5), uint16(n))
	default:
		out = binary.BigEndian.AppendUint32(append(out, 0xc6), uint32(n))
	}
	return append(out, b...)
}

func appendExt(out []byte, typ int8, b []byte) []byte {
	switch n := len(b); {
	case n == 1:
		out = append(out, 0xd4)
	case n == 2:
		out = append(out, 0xd5)
	case n == 4:
		out = append(out, 0xd6)
	case n == 8:
		out = append(out, 0xd7)
	case n == 16:
		out = append(out, 0xd8)
	case n <= math.MaxUint8:
		out = append(out, 0xc7, byte(n))
	case n <= math.MaxUint16:
		out = binary.BigEndian.AppendUint16(append(out, 0xc8), uint16(n))
	default:
		out = binary.BigEndian.AppendUint32(append(out, 0xc9), uint32(n))
	}
	return append(append(out, byte(typ)), b...)
}

// appendNumber writes integers in the smallest encoding and everything else
// as float64.
func appendNumber(out []byte, s string) ([]byte, error) {
	if !strings.ContainsAny(s, ".eE") {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return appendInt(out, v), nil
		}
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return binary.BigEndian.AppendUint64(append(out, 0xcf), u), nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("msgpack: number %s: %w", s, err)
	}
	return binary.BigEndian.AppendUint64(append(out, 0xcb), math.Float64bits(f)), nil
}

func appendInt(out []byte, v int64) []byte {
	switch {
	case v >= 0 && v <= 0x7f:
		return append(out, byte(v))
	case v >= -32 && v < 0:
		return append(out, byte(int8(v)))
	case v >= 0 && v <= math.MaxUint8:
		return append(out, 0xcc, byte(v))
	case v >= 0 && v <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(out, 0xcd), uint16(v))
	case v >= 0 && v <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(out, 0xce), uint32(v))
	case v >= 0:
		return binary.BigEndian.AppendUint64(append(out, 0xcf), uint64(v))
	case v >= math.MinInt8:
		return append(out, 0xd0, byte(int8(v)))
	case v >= math.MinInt16:
		return binary.BigEndian.AppendUint16(append(out, 0xd1), uint16(int16(v)))
	case v >= math.MinInt32:
		return binary.BigEndian.AppendUint32(append(out, 0xd2), uint32(int32(v)))
	default:
		return binary.BigEndian.AppendUint64(append(out, 0xd3), uint64(v))
	}
}
