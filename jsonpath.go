package wsshared

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Minimal JSON path lookup over raw bytes.
//
// The point is to reach into a message body WITHOUT unmarshalling it: on a
// large payload building a map[string]any costs more than everything else the
// extension does put together. Here we walk the bytes, skipping whole values we
// do not need, and return the one span that matches.
//
// Supported syntax, deliberately small:
//
//	data.items.3.price          object keys and array indices
//	$.data.items[3].price       the same, JSONPath-style
//	data.items[-1].price        negative index = from the end
//	data['key.with.dots']       quoted key
//
// What it does NOT do: wildcards, filters, recursive descent. Those need a real
// parser and a tree; a test that needs them takes the body into JS and pays the
// cost knowingly.

var errPathNotFound = errors.New("wsshared: path not found")

type pathSeg struct {
	key   []byte
	idx   int
	isIdx bool
}

// jpath is a pre-split path. Compiling once and reusing it avoids a
// strings.Split per message in the read loop.
type jpath struct {
	src  string
	segs []pathSeg
}

func compilePath(p string) (*jpath, error) {
	s := strings.TrimSpace(p)
	s = strings.TrimPrefix(s, "$")
	jp := &jpath{src: p}
	add := func(tok string) {
		seg := pathSeg{key: []byte(tok)}
		if n, err := strconv.Atoi(tok); err == nil {
			seg.idx, seg.isIdx = n, true
		}
		jp.segs = append(jp.segs, seg)
	}
	for i := 0; i < len(s); {
		switch s[i] {
		case '.':
			i++
		case '[':
			if i+1 < len(s) && (s[i+1] == '\'' || s[i+1] == '"') {
				q := s[i+1]
				k := strings.IndexByte(s[i+2:], q)
				if k < 0 {
					return nil, fmt.Errorf("wsshared: bad path %q: unterminated quote", p)
				}
				key := s[i+2 : i+2+k]
				end := i + 2 + k + 1
				if end >= len(s) || s[end] != ']' {
					return nil, fmt.Errorf("wsshared: bad path %q: expected ']'", p)
				}
				// A quoted segment is always a key, even if it looks numeric.
				jp.segs = append(jp.segs, pathSeg{key: []byte(key)})
				i = end + 1
			} else {
				k := strings.IndexByte(s[i:], ']')
				if k < 0 {
					return nil, fmt.Errorf("wsshared: bad path %q: unterminated '['", p)
				}
				add(strings.TrimSpace(s[i+1 : i+k]))
				i += k + 1
			}
		default:
			j := i
			for j < len(s) && s[j] != '.' && s[j] != '[' {
				j++
			}
			add(s[i:j])
			i = j
		}
	}
	return jp, nil
}

// lookup returns the raw bytes of the value at the path. The slice aliases data.
func (jp *jpath) lookup(data []byte) ([]byte, error) {
	cur := data
	for i := range jp.segs {
		seg := &jp.segs[i]
		cur = bytes.TrimLeft(cur, " \t\r\n")
		if len(cur) == 0 {
			return nil, errPathNotFound
		}
		var err error
		if seg.isIdx && cur[0] == '[' {
			cur, err = arrayIndex(cur, seg.idx)
		} else {
			cur, err = objectKey(cur, seg.key)
		}
		if err != nil {
			return nil, err
		}
	}
	return bytes.TrimSpace(cur), nil
}

// A small process-wide cache, so ev.jsonPath('...') called in a loop does not
// re-parse the same path string every time. Bounded: a script that builds paths
// dynamically must not grow it without limit.
const compileCacheLimit = 4096

var (
	pathCache  sync.Map
	pathCacheN atomic.Int64
)

func cachedPath(p string) (*jpath, error) {
	if v, ok := pathCache.Load(p); ok {
		return v.(*jpath), nil
	}
	jp, err := compilePath(p)
	if err != nil {
		return nil, err
	}
	if pathCacheN.Load() < compileCacheLimit {
		if _, loaded := pathCache.LoadOrStore(p, jp); !loaded {
			pathCacheN.Add(1)
		}
	}
	return jp, nil
}

// JSONPath returns the raw bytes of the value at `path`, or an error.
// The returned slice aliases `data`.
func JSONPath(data []byte, path string) ([]byte, error) {
	jp, err := cachedPath(path)
	if err != nil {
		return nil, err
	}
	return jp.lookup(data)
}

// objectKey finds `key` in the object starting at data[0] == '{' and returns
// its value. Values of non-matching keys are skipped whole, never parsed.
func objectKey(data []byte, key []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != '{' {
		return nil, errPathNotFound
	}
	i := 1
	for i < len(data) {
		i = skipSpace(data, i)
		if i >= len(data) {
			break
		}
		if data[i] == '}' || data[i] != '"' {
			return nil, errPathNotFound
		}
		// Compare the key in place: allocating a string for every skipped key
		// dominates the cost on large objects.
		keySpan, next, err := readStringSpan(data, i)
		if err != nil {
			return nil, err
		}
		matched := bytes.Equal(keySpan, key)
		i = skipSpace(data, next)
		if i >= len(data) || data[i] != ':' {
			return nil, errPathNotFound
		}
		i = skipSpace(data, i+1)
		end, err := skipValue(data, i)
		if err != nil {
			return nil, err
		}
		if matched {
			return data[i:end], nil
		}
		i = skipSpace(data, end)
		if i < len(data) && data[i] == ',' {
			i++
			continue
		}
		return nil, errPathNotFound
	}
	return nil, errPathNotFound
}

// arrayIndex returns element `idx` of the array starting at data[0] == '['.
// A negative index counts from the end (-1 is the last element).
func arrayIndex(data []byte, idx int) ([]byte, error) {
	if len(data) == 0 || data[0] != '[' {
		return nil, errPathNotFound
	}
	if idx < 0 {
		n, err := arrayLen(data)
		if err != nil {
			return nil, err
		}
		idx += n
		if idx < 0 {
			return nil, errPathNotFound
		}
	}
	i := 1
	for n := 0; i < len(data); n++ {
		i = skipSpace(data, i)
		if i >= len(data) || data[i] == ']' {
			return nil, errPathNotFound
		}
		end, err := skipValue(data, i)
		if err != nil {
			return nil, err
		}
		if n == idx {
			return data[i:end], nil
		}
		i = skipSpace(data, end)
		if i < len(data) && data[i] == ',' {
			i++
			continue
		}
		return nil, errPathNotFound
	}
	return nil, errPathNotFound
}

func arrayLen(data []byte) (int, error) {
	i, n := 1, 0
	for {
		i = skipSpace(data, i)
		if i >= len(data) {
			return 0, errPathNotFound
		}
		if data[i] == ']' {
			return n, nil
		}
		end, err := skipValue(data, i)
		if err != nil {
			return 0, err
		}
		n++
		i = skipSpace(data, end)
		if i < len(data) && data[i] == ',' {
			i++
			continue
		}
		if i < len(data) && data[i] == ']' {
			return n, nil
		}
		return 0, errPathNotFound
	}
}

// skipValue returns the index just past the JSON value starting at i.
func skipValue(data []byte, i int) (int, error) {
	if i >= len(data) {
		return 0, errPathNotFound
	}
	switch data[i] {
	case '"':
		_, next, err := readStringSpan(data, i)
		return next, err
	case '{', '[':
		open := data[i]
		closeCh := byte('}')
		if open == '[' {
			closeCh = ']'
		}
		depth := 0
		for ; i < len(data); i++ {
			switch data[i] {
			case '"':
				_, next, err := readStringSpan(data, i)
				if err != nil {
					return 0, err
				}
				i = next - 1
			case open:
				depth++
			case closeCh:
				depth--
				if depth == 0 {
					return i + 1, nil
				}
			}
		}
		return 0, errPathNotFound
	default:
		// number, true, false, null — runs until a structural character
		for ; i < len(data); i++ {
			c := data[i]
			if c == ',' || c == '}' || c == ']' || c == ' ' ||
				c == '\t' || c == '\r' || c == '\n' {
				return i, nil
			}
		}
		return len(data), nil
	}
}

// readStringSpan returns the RAW bytes between the quotes (escapes untouched)
// and the index past the closing quote. Allocation-free.
func readStringSpan(data []byte, i int) ([]byte, int, error) {
	if i >= len(data) || data[i] != '"' {
		return nil, 0, errPathNotFound
	}
	i++
	start := i
	for i < len(data) {
		switch data[i] {
		case '\\':
			i += 2
			continue
		case '"':
			return data[start:i], i + 1, nil
		}
		i++
	}
	return nil, 0, errPathNotFound
}

// readString reads the quoted string starting at data[i] == '"' and returns it
// unescaped, plus the index past it. The common no-escape case is a plain
// slice-to-string; escapes go through encoding/json so \uXXXX is right.
func readString(data []byte, i int) (string, int, error) {
	span, next, err := readStringSpan(data, i)
	if err != nil {
		return "", 0, err
	}
	if bytes.IndexByte(span, '\\') < 0 {
		return string(span), next, nil
	}
	var s string
	if err := json.Unmarshal(data[i:next], &s); err != nil {
		return string(span), next, nil
	}
	return s, next, nil
}

func skipSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

// jsonValueToGo turns a raw JSON value into something sobek can hand to JS:
// string, float64, bool, nil, or the raw JSON text for objects/arrays.
func jsonValueToGo(raw []byte) any {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	switch raw[0] {
	case '"':
		s, _, err := readString(raw, 0)
		if err != nil {
			return string(raw)
		}
		return s
	case '{', '[':
		// Composite: hand back the JSON text; JSON.parse it if needed.
		return string(raw)
	case 't':
		return true
	case 'f':
		return false
	case 'n':
		return nil
	default:
		if f, err := strconv.ParseFloat(string(raw), 64); err == nil {
			return f
		}
		return string(raw)
	}
}

// valueText is the textual form of a raw JSON value: an unquoted string for a
// JSON string, the literal text for anything else. Used where a string is
// needed (fields, keys, regex subjects).
func valueText(raw []byte) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		if s, _, err := readString(raw, 0); err == nil {
			return s
		}
	}
	return string(raw)
}
