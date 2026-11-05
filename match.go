package wsshared

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
)

// Matchers decide, in Go, whether a message is interesting: which messages to
// auto-reply to, which to keep in a buffer, which to track, which collect()
// returns. Evaluating them in Go is what lets a VU say "wake me for the 5th
// message with status=filled" instead of being woken for every message.
//
// A matcher picks a SUBJECT and checks CONDITIONS on it:
//
//	subject:    path: 'a.b'   (jsonPath into the body)
//	            field: 'x'    (a declared field, already extracted)
//	            none          (the whole body)
//	conditions: equals, oneOf, exists, regex
//
// plus `contains` (a substring of the body, the cheapest check), `type` (the
// value at the connection's typePath), `any` (OR of nested matchers) and `not`.
// Everything present must hold (AND).

// MatchSpec is the Go form of a matcher.
type MatchSpec struct {
	Contains  string
	Regex     string
	Path      string
	Field     string
	Types     []string
	Equals    any
	HasEquals bool
	OneOf     []any
	Exists    *bool
	Not       bool
	Any       []*matcher
}

type matcher struct {
	contains  []byte
	re        *regexp.Regexp
	path      *jpath
	field     string
	types     []string
	equals    any
	hasEquals bool
	oneOf     []any
	exists    *bool
	not       bool
	any       []*matcher
	// needsBody: some condition reads the raw body, so the matcher cannot be
	// evaluated on an event whose body was not kept.
	needsBody bool
}

func compileMatch(s MatchSpec) (*matcher, error) {
	if s.Contains == "" && s.Regex == "" && s.Path == "" && s.Field == "" &&
		len(s.Types) == 0 && len(s.Any) == 0 {
		return nil, errors.New("wsshared: empty match: set contains, regex, path, field, type or any")
	}
	if s.Path != "" && s.Field != "" {
		return nil, errors.New("wsshared: match: path and field are exclusive")
	}
	subjectConds := s.HasEquals || len(s.OneOf) > 0 || s.Exists != nil
	if subjectConds && s.Path == "" && s.Field == "" {
		return nil, errors.New("wsshared: match: equals/oneOf/exists need a path or a field")
	}
	if s.HasEquals && !isScalar(s.Equals) {
		return nil, errors.New("wsshared: match: equals must be a string, number, boolean or null")
	}
	for _, v := range s.OneOf {
		if !isScalar(v) {
			return nil, errors.New("wsshared: match: oneOf must hold strings, numbers, booleans or null")
		}
	}

	m := &matcher{
		field:     s.Field,
		types:     s.Types,
		equals:    s.Equals,
		hasEquals: s.HasEquals,
		oneOf:     s.OneOf,
		exists:    s.Exists,
		not:       s.Not,
		any:       s.Any,
	}
	if s.Contains != "" {
		m.contains = []byte(s.Contains)
		m.needsBody = true
	}
	if s.Regex != "" {
		re, err := compileRegex(s.Regex)
		if err != nil {
			return nil, err
		}
		m.re = re
		if s.Field == "" {
			m.needsBody = true // regex over the body or over a path in it
		}
	}
	if s.Path != "" {
		jp, err := cachedPath(s.Path)
		if err != nil {
			return nil, err
		}
		m.path = jp
		m.needsBody = true
	}
	for _, c := range s.Any {
		if c.needsBody {
			m.needsBody = true
		}
	}
	return m, nil
}

// match evaluates the matcher. body may be nil when the event did not keep
// it; callers check needsBody up front.
func (m *matcher) match(body []byte, typ string, fields []Extracted) bool {
	ok := m.eval(body, typ, fields)
	if m.not {
		return !ok
	}
	return ok
}

func (m *matcher) eval(body []byte, typ string, fields []Extracted) bool {
	if m.contains != nil && !bytes.Contains(body, m.contains) {
		return false
	}
	if len(m.types) > 0 {
		hit := false
		for _, t := range m.types {
			if t == typ {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if len(m.any) > 0 {
		hit := false
		for _, c := range m.any {
			if c.match(body, typ, fields) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	switch {
	case m.path != nil:
		raw, err := m.path.lookup(body)
		if err != nil {
			return m.subject(false, nil, "")
		}
		return m.subject(true, jsonValueToGo(raw), valueText(raw))
	case m.field != "":
		for i := range fields {
			if fields[i].Name == m.field {
				return m.subject(true, fields[i].Value, fields[i].Value)
			}
		}
		return m.subject(false, nil, "")
	default:
		if m.re != nil && !m.re.Match(body) {
			return false
		}
	}
	return true
}

// subject checks the per-subject conditions. A path/field with no conditions
// at all means "must exist".
func (m *matcher) subject(found bool, val any, text string) bool {
	if !found {
		return m.exists != nil && !*m.exists
	}
	if m.exists != nil && !*m.exists {
		return false
	}
	if m.hasEquals && !looseEqual(val, m.equals) {
		return false
	}
	if len(m.oneOf) > 0 {
		hit := false
		for _, want := range m.oneOf {
			if looseEqual(val, want) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if m.re != nil && !m.re.MatchString(text) {
		return false
	}
	return true
}

func isScalar(v any) bool {
	switch v.(type) {
	case nil, string, bool, float64, float32, int, int64, int32, uint64, uint32:
		return true
	default:
		return false
	}
}

// looseEqual compares the way a script expects: numbers compare by value
// regardless of int/float, and a numeric string equals the number it spells —
// a field extracted as "0" matches `equals: 0`.
func looseEqual(a, b any) bool {
	if !isScalar(a) || !isScalar(b) {
		return false
	}
	if a == b {
		return true
	}
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	return aok && bok && af == bf
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// ---------------------------------------------------------------------------
// regex helpers
// ---------------------------------------------------------------------------

var (
	reCache  sync.Map
	reCacheN atomic.Int64
)

// compileRegex compiles with a bounded process-wide cache. Go's regexp is RE2:
// linear time, no catastrophic backtracking — safe to run on every message.
func compileRegex(p string) (*regexp.Regexp, error) {
	if v, ok := reCache.Load(p); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(p)
	if err != nil {
		return nil, fmt.Errorf("wsshared: bad regex %q: %w", p, err)
	}
	if reCacheN.Load() < compileCacheLimit {
		if _, loaded := reCache.LoadOrStore(p, re); !loaded {
			reCacheN.Add(1)
		}
	}
	return re, nil
}

// resolveGroup picks the capture group to return: the requested one, or by
// default group 1 when the pattern has groups and the whole match otherwise.
func resolveGroup(re *regexp.Regexp, g int, set bool) (int, error) {
	if !set {
		if re.NumSubexp() >= 1 {
			return 1, nil
		}
		return 0, nil
	}
	if g < 0 || g > re.NumSubexp() {
		return 0, fmt.Errorf("wsshared: regex %q has no group %d", re.String(), g)
	}
	return g, nil
}

// regexFind returns the requested group of the first match.
func regexFind(re *regexp.Regexp, group int, data []byte) (string, bool) {
	loc := re.FindSubmatchIndex(data)
	if loc == nil || loc[2*group] < 0 {
		return "", false
	}
	return string(data[loc[2*group]:loc[2*group+1]]), true
}

// regexFindAll returns the requested group of every match, up to limit
// (<= 0 means no limit).
func regexFindAll(re *regexp.Regexp, group int, data []byte, limit int) []string {
	n := -1
	if limit > 0 {
		n = limit
	}
	all := re.FindAllSubmatchIndex(data, n)
	out := make([]string, 0, len(all))
	for _, loc := range all {
		if loc[2*group] < 0 {
			continue
		}
		out = append(out, string(data[loc[2*group]:loc[2*group+1]]))
	}
	return out
}
