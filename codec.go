package wsshared

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"

	"github.com/altenar/xk6-wsshared/binfmt"
)

// Codec turns application payloads into WebSocket frames and back.
//
// It is protocol-agnostic: instead of knowing any particular wire format, it is
// assembled from independent, optional layers that cover most real services:
//
//	outbound:  JSON -> format  ->  + delimiter  ->  compress  ->  length prefix  -> frame
//	inbound:   frame  ->  split by length prefix  ->  decompress  ->  split by delimiter  ->  format -> JSON
//
// The format layer (MessagePack, protobuf) is what lets a binary protocol use
// everything that works on JSON: it runs once per message in the read loop.
//
// With every layer off (the default) one text frame is one message — plain
// JSON-over-WebSocket. Doing this in Go rather than in the k6 JS runtime is the
// point: goja has no JIT, so inflating and scanning large payloads per message
// per VU dominates generator cost.
type Codec struct {
	comp   compression
	varint bool
	delim  []byte
	binary bool
	strict bool
	// format converts binary records to JSON and back; nil = the records are
	// JSON (or text) already.
	format binfmt.Format
}

type compression int

const (
	compNone compression = iota
	compDeflate
	compZlib
	compGzip
)

// CodecConfig is the user-facing description of a codec.
type CodecConfig struct {
	// Compression: "" / "none", "deflate" (raw RFC 1951), "zlib", "gzip".
	Compression string
	// LengthPrefix: "" / "none", or "varint" (7-bit groups, LSB first).
	LengthPrefix string
	// Delimiter separates several messages inside one (decompressed) payload,
	// e.g. "\n" or "\x1e". Empty: the whole payload is one message.
	Delimiter string
	// Binary forces the outbound frame type. nil: binary when compression or
	// a length prefix is on (the output is not text then), text otherwise.
	Binary *bool
	// Strict turns decode failures into error events. Off by default: a frame
	// that does not decompress / does not parse as length-prefixed is taken as
	// plain bytes instead. Real servers do this — e.g. a plain-text ack sent
	// over an otherwise compressed binary channel.
	Strict bool

	// Format: "" / "json" (records are JSON already), "msgpack", "protobuf".
	Format string
	// Sequence (msgpack): one record may hold several concatenated values.
	Sequence bool
	// Proto is the schema for Format "protobuf".
	Proto *binfmt.ProtoConfig
}

// NewCodec builds a codec from its config.
func NewCodec(cfg CodecConfig) (*Codec, error) {
	c := &Codec{delim: []byte(cfg.Delimiter), strict: cfg.Strict}
	switch cfg.Format {
	case "", "json", "none":
	case "msgpack", "messagepack":
		c.format = binfmt.MsgPack{Sequence: cfg.Sequence}
	case "protobuf", "proto":
		if cfg.Proto == nil {
			return nil, errors.New("wsshared: format 'protobuf' needs codec.proto: { source | descriptorSet, message }")
		}
		p, err := binfmt.NewProtobuf(*cfg.Proto)
		if err != nil {
			return nil, fmt.Errorf("wsshared: %w", err)
		}
		c.format = p
	default:
		return nil, fmt.Errorf("wsshared: unknown format %q (want json, msgpack, protobuf)", cfg.Format)
	}
	if c.format != nil && len(c.delim) > 0 {
		// Binary records can contain any byte, the delimiter included, so
		// splitting on it would cut messages apart.
		return nil, fmt.Errorf("wsshared: format %q cannot be combined with a delimiter: "+
			"binary records may contain it; use lengthPrefix: 'varint' or one message per frame", cfg.Format)
	}
	switch cfg.Compression {
	case "", "none":
	case "deflate", "raw-deflate":
		c.comp = compDeflate
	case "zlib":
		c.comp = compZlib
	case "gzip":
		c.comp = compGzip
	default:
		return nil, fmt.Errorf("wsshared: unknown compression %q (want none, deflate, zlib, gzip)", cfg.Compression)
	}
	switch cfg.LengthPrefix {
	case "", "none":
	case "varint":
		c.varint = true
	default:
		return nil, fmt.Errorf("wsshared: unknown lengthPrefix %q (want none, varint)", cfg.LengthPrefix)
	}
	c.binary = c.comp != compNone || c.varint || c.format != nil
	if cfg.Binary != nil {
		c.binary = *cfg.Binary
	}
	return c, nil
}

// Binary reports whether encoded frames go out as binary WebSocket frames.
func (c *Codec) Binary() bool { return c.binary }

// FormatName is the format layer's name, "" without one.
func (c *Codec) FormatName() string {
	if c.format == nil {
		return ""
	}
	return c.format.Name()
}

// Encode turns one JSON payload into a wire frame: Format, then Frame.
func (c *Codec) Encode(payload []byte) ([]byte, error) {
	body, err := c.Format(payload)
	if err != nil {
		return nil, err
	}
	return c.Frame(body)
}

// Format applies the format layer to a JSON payload. Senders do this once,
// when the message is built, so a payload that does not fit the format fails
// at config time, not silently inside the read loop.
func (c *Codec) Format(payload []byte) ([]byte, error) {
	if c.format == nil {
		return payload, nil
	}
	return c.format.FromJSON(payload)
}

// Frame applies the framing layers - delimiter, compression, length prefix -
// to an already formatted payload.
func (c *Codec) Frame(payload []byte) ([]byte, error) {
	body := payload
	if len(c.delim) > 0 {
		body = make([]byte, 0, len(payload)+len(c.delim))
		body = append(body, payload...)
		body = append(body, c.delim...)
	}
	if c.comp != compNone {
		var err error
		body, err = compress(c.comp, body)
		if err != nil {
			return nil, err
		}
	}
	if c.varint {
		body = prependVarInt(body)
	}
	return body, nil
}

// DecodeResult is what one inbound frame turned into.
type DecodeResult struct {
	// Messages alias freshly allocated buffers (gorilla's ReadMessage and the
	// decompressor both allocate per frame), so they stay valid after the
	// next frame is read and can be kept without copying.
	Messages [][]byte
	// DecodedBytes is the payload size after decompression (before the
	// format layer: it describes the wire, not the JSON view).
	DecodedBytes int
	// Fallback reports that a layer did not apply and the bytes were taken as
	// they were (see CodecConfig.Strict).
	Fallback bool
}

// Decode splits one binary wire frame into messages.
func (c *Codec) Decode(frame []byte) (DecodeResult, error) {
	return c.DecodeFrame(frame, true)
}

// DecodeFrame splits one wire frame into messages. The format layer applies
// to binary frames only: a text frame on a binary-format channel is a
// plain-text message (an ack, an error), and decoding it as MessagePack would
// "succeed" - '{' is a valid small integer - and produce garbage.
func (c *Codec) DecodeFrame(frame []byte, binaryFrame bool) (DecodeResult, error) {
	var res DecodeResult
	if len(frame) == 0 {
		return res, nil
	}

	parts := [][]byte{frame}
	layered := true
	if c.varint {
		ps, err := parseVarIntFrames(frame)
		switch {
		case err == nil:
			parts = ps
		case c.strict:
			return res, err
		default:
			// Not length-prefixed after all: take the whole frame as plain
			// bytes, and do not try to decompress it either.
			res.Fallback = true
			layered = false
		}
	}

	for _, p := range parts {
		raw := p
		if c.comp != compNone && layered {
			out, err := decompress(c.comp, p)
			switch {
			case err == nil:
				raw = out
			case c.strict:
				return res, err
			default:
				res.Fallback = true
			}
		}
		res.DecodedBytes += len(raw)
		if c.format == nil || !layered || !binaryFrame {
			res.Messages = c.split(raw, res.Messages)
			continue
		}
		// no delimiter with a format (NewCodec refuses it): one part, one record
		if len(raw) == 0 {
			continue
		}
		msgs, err := c.format.ToJSON(res.Messages, raw)
		switch {
		case err == nil:
			res.Messages = msgs
		case c.strict:
			return res, fmt.Errorf("wsshared: %w", err)
		default:
			res.Fallback = true
			res.Messages = append(res.Messages, raw)
		}
	}
	return res, nil
}

func (c *Codec) split(raw []byte, out [][]byte) [][]byte {
	if len(c.delim) == 0 {
		if len(raw) > 0 {
			out = append(out, raw)
		}
		return out
	}
	for len(raw) > 0 {
		var rec []byte
		if i := bytes.Index(raw, c.delim); i >= 0 {
			rec, raw = raw[:i], raw[i+len(c.delim):]
		} else {
			rec, raw = raw, nil
		}
		if len(rec) > 0 {
			out = append(out, rec)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// compression
// ---------------------------------------------------------------------------

func compress(kind compression, b []byte) ([]byte, error) {
	var buf bytes.Buffer
	var w io.WriteCloser
	var err error
	switch kind {
	case compDeflate:
		w, err = flate.NewWriter(&buf, flate.DefaultCompression)
	case compZlib:
		w = zlib.NewWriter(&buf)
	case compGzip:
		w = gzip.NewWriter(&buf)
	default:
		return b, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompress(kind compression, b []byte) ([]byte, error) {
	var r io.ReadCloser
	var err error
	switch kind {
	case compDeflate:
		r = flate.NewReader(bytes.NewReader(b))
	case compZlib:
		r, err = zlib.NewReader(bytes.NewReader(b))
	case compGzip:
		r, err = gzip.NewReader(bytes.NewReader(b))
	default:
		return b, nil
	}
	if err != nil {
		return nil, err
	}
	defer r.Close() //nolint:errcheck
	out, err := io.ReadAll(r)
	// A sync-flushed stream without a final block is normal on WebSockets:
	// the sender flushes per message and never "finishes" the stream.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if err != nil && len(out) == 0 {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// length prefix
// ---------------------------------------------------------------------------

func prependVarInt(payload []byte) []byte {
	var prefix [5]byte
	n := 0
	size := len(payload)
	for {
		part := byte(size & 0x7f)
		size >>= 7
		if size > 0 {
			part |= 0x80
		}
		prefix[n] = part
		n++
		if size == 0 {
			break
		}
	}
	out := make([]byte, 0, n+len(payload))
	out = append(out, prefix[:n]...)
	return append(out, payload...)
}

func parseVarIntFrames(buf []byte) ([][]byte, error) {
	const maxPrefix = 5
	var out [][]byte
	for offset := 0; offset < len(buf); {
		size := 0
		shift := uint(0)
		n := 0
		for {
			if offset+n >= len(buf) {
				return nil, errors.New("wsshared: truncated length prefix")
			}
			b := buf[offset+n]
			size |= int(b&0x7f) << shift
			shift += 7
			n++
			if b&0x80 == 0 {
				break
			}
			if n >= maxPrefix {
				return nil, errors.New("wsshared: length prefix too long")
			}
		}
		if offset+n+size > len(buf) {
			return nil, fmt.Errorf("wsshared: incomplete message (want %d, have %d)",
				size, len(buf)-offset-n)
		}
		out = append(out, buf[offset+n:offset+n+size])
		offset += n + size
	}
	return out, nil
}
