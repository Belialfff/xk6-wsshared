package wsshared

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/grafana/sobek"
)

// Standalone helpers: the same Go-side machinery without a socket. Useful on
// HTTP response bodies (a 2MB JSON where you need one value), on frames
// captured from a browser, or to check what a payload encodes to.

// JSONPathUtil: wsshared.jsonPath(text, path) -> value | null.
func (mi *ModuleInstance) JSONPathUtil(text string, path string) (any, error) {
	raw, err := JSONPath([]byte(text), path)
	if errors.Is(err, errPathNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return jsonValueToGo(raw), nil
}

// RegexUtil: wsshared.regex(text, pattern, group?) -> string | null.
func (mi *ModuleInstance) RegexUtil(text string, pattern string, group sobek.Value) (any, error) {
	re, g, err := regexAndGroup(pattern, group)
	if err != nil {
		return nil, err
	}
	if s, ok := regexFind(re, g, []byte(text)); ok {
		return s, nil
	}
	return nil, nil
}

// RegexAllUtil: wsshared.regexAll(text, pattern, group?, limit?) -> string[].
func (mi *ModuleInstance) RegexAllUtil(text string, pattern string, group sobek.Value, limit int) ([]string, error) {
	re, g, err := regexAndGroup(pattern, group)
	if err != nil {
		return nil, err
	}
	return regexFindAll(re, g, []byte(text), limit), nil
}

// Codec returns a standalone encoder/decoder:
//
//	const c = wsshared.codec({ compression: 'deflate', lengthPrefix: 'varint' });
//	const f = c.encode({ op: 'ping' });       // { bytes, size, base64 }
//	const msgs = c.decode(f.bytes);           // [{ body, size }, ...]
func (mi *ModuleInstance) Codec(cfgVal sobek.Value) (map[string]any, error) {
	var j jsCodecConfig
	if !isAbsent(cfgVal) {
		if err := mi.vu.Runtime().ExportTo(cfgVal, &j); err != nil {
			return nil, fmt.Errorf("wsshared: bad codec config: %w", err)
		}
	}
	codecCfg, err := j.toConfig()
	if err != nil {
		return nil, err
	}
	c, err := NewCodec(codecCfg)
	if err != nil {
		return nil, err
	}
	rt := mi.vu.Runtime()

	encode := func(v sobek.Value) ([]byte, error) {
		data, _, err := payloadBytes(v)
		if err != nil {
			return nil, err
		}
		return c.Encode(data)
	}
	decode := func(frame []byte) ([]map[string]any, error) {
		res, err := c.Decode(frame)
		if err != nil {
			return nil, err
		}
		out := make([]map[string]any, 0, len(res.Messages))
		for i, m := range res.Messages {
			e := map[string]any{"body": string(m), "size": len(m)}
			if i == 0 {
				e["wireBytes"] = len(frame)
				e["decodedBytes"] = res.DecodedBytes
				e["fallback"] = res.Fallback
			}
			out = append(out, e)
		}
		return out, nil
	}

	return map[string]any{
		"binary": c.Binary(),
		"format": c.FormatName(),
		"encode": func(v sobek.Value) (map[string]any, error) {
			b, err := encode(v)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"bytes":  rt.NewArrayBuffer(b),
				"size":   len(b),
				"base64": base64.StdEncoding.EncodeToString(b),
			}, nil
		},
		"encodeBase64": func(v sobek.Value) (string, error) {
			b, err := encode(v)
			if err != nil {
				return "", err
			}
			return base64.StdEncoding.EncodeToString(b), nil
		},
		"decode": func(v sobek.Value) ([]map[string]any, error) {
			data, _, err := payloadBytes(v)
			if err != nil {
				return nil, err
			}
			return decode(data)
		},
		"decodeBase64": func(s string) ([]map[string]any, error) {
			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("wsshared: bad base64: %w", err)
			}
			return decode(raw)
		},
	}, nil
}
