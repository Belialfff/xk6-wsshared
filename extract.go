package wsshared

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Field extraction.
//
// The test declares which values it wants out of every message; the read loop
// pulls them out over the raw bytes, without building a JSON tree, and they
// arrive on the event already extracted. Three ways to address a value:
//
//	{ name: 'ts' }                         byte scan for the key "ts", any depth
//	{ name: 'price', path: 'data.px' }     jsonPath
//	{ name: 'id', regex: 'id=(\\d+)' }     regex over the body, capture group
//
// Values always come back as strings — fields are a cheap transport, not a
// typed decoder. `asTimestamp` additionally turns the value into a delivery
// delay measured against the moment the frame was read.

// FieldSpec declares one field to extract from a message body.
type FieldSpec struct {
	// Name is how the field is reported (ev.fields[name]).
	Name string
	// Key is the JSON key to scan for; defaults to Name.
	Key string
	// Path is a jsonPath; mutually exclusive with Regex.
	Path string
	// Regex is matched against the raw body; Group picks the capture group.
	Regex    string
	Group    int
	GroupSet bool
	// FromEnd scans backwards — for a key near the tail of a large body.
	// Applies to the key scan only.
	FromEnd bool
	// AsTimestamp parses the value as a time and reports the delay.
	AsTimestamp bool

	pattern []byte
	jp      *jpath
	re      *regexp.Regexp
	group   int
}

func (f *FieldSpec) compile() error {
	if f.Name == "" {
		return errors.New("wsshared: field needs a name")
	}
	if f.Path != "" && f.Regex != "" {
		return fmt.Errorf("wsshared: field %q: path and regex are exclusive", f.Name)
	}
	switch {
	case f.Path != "":
		jp, err := compilePath(f.Path)
		if err != nil {
			return err
		}
		f.jp = jp
	case f.Regex != "":
		re, err := compileRegex(f.Regex)
		if err != nil {
			return err
		}
		g, err := resolveGroup(re, f.Group, f.GroupSet)
		if err != nil {
			return fmt.Errorf("wsshared: field %q: %w", f.Name, err)
		}
		f.re, f.group = re, g
	default:
		key := f.Key
		if key == "" {
			key = f.Name
		}
		f.pattern = make([]byte, 0, len(key)+2)
		f.pattern = append(f.pattern, '"')
		f.pattern = append(f.pattern, key...)
		f.pattern = append(f.pattern, '"')
	}
	return nil
}

// Extracted holds one field's value.
type Extracted struct {
	Name  string
	Value string
	// DelayMs is set when the spec asked for a timestamp and it yielded a
	// plausible delay.
	DelayMs  float64
	HasDelay bool
	// BadTimestamp: found, asked to be a timestamp, but unusable (unparsable,
	// zero, absurd). Counted rather than silently dropped.
	BadTimestamp bool
}

// Extract pulls the declared (compiled) fields out of a message body.
// `now` is the receive time used for delay calculation.
func Extract(raw []byte, specs []FieldSpec, now time.Time) []Extracted {
	if len(specs) == 0 {
		return nil
	}
	out := make([]Extracted, 0, len(specs))
	for i := range specs {
		s := &specs[i]
		val, ok := s.find(raw)
		if !ok {
			continue
		}
		e := Extracted{Name: s.Name, Value: val}
		if s.AsTimestamp {
			if ts, parsed := parseTimestamp(val); parsed {
				if d, ok := delaySince(ts, now); ok {
					e.DelayMs, e.HasDelay = d, true
				} else {
					e.BadTimestamp = true
				}
			} else {
				e.BadTimestamp = true
			}
		}
		out = append(out, e)
	}
	return out
}

func (s *FieldSpec) find(raw []byte) (string, bool) {
	switch {
	case s.jp != nil:
		v, err := s.jp.lookup(raw)
		if err != nil {
			return "", false
		}
		return valueText(v), true
	case s.re != nil:
		return regexFind(s.re, s.group, raw)
	default:
		return findKey(raw, s.pattern, s.FromEnd)
	}
}

// findKey scans for `"key"` followed by ':' and returns the scalar after it.
// A composite value comes back as its JSON text.
func findKey(raw, pattern []byte, fromEnd bool) (string, bool) {
	// The same quoted text can occur as a string VALUE ("status":"ts"), so
	// keep searching until an occurrence is followed by ':'.
	start, end := 0, len(raw)
	for {
		var at int
		if fromEnd {
			at = bytes.LastIndex(raw[:end], pattern)
		} else {
			at = bytes.Index(raw[start:], pattern)
			if at >= 0 {
				at += start
			}
		}
		if at < 0 {
			return "", false
		}
		i := skipSpace(raw, at+len(pattern))
		if i < len(raw) && raw[i] == ':' {
			return scalarAt(raw, skipSpace(raw, i+1))
		}
		if fromEnd {
			end = at
		} else {
			start = at + 1
		}
	}
}

func scalarAt(raw []byte, i int) (string, bool) {
	if i >= len(raw) {
		return "", false
	}
	if raw[i] == '"' {
		s, _, err := readString(raw, i)
		return s, err == nil
	}
	end, err := skipValue(raw, i)
	if err != nil || end == i {
		return "", false
	}
	return string(raw[i:end]), true
}

// ---------------------------------------------------------------------------
// timestamps
// ---------------------------------------------------------------------------

// maxPlausibleDelay bounds what counts as a real delivery delay. Anything
// beyond this is not a slow message, it is a broken timestamp.
const maxPlausibleDelay = time.Hour

// delaySince returns the delay in milliseconds, or false when the timestamp is
// not usable. The guard matters: an unset date ("0001-01-01T00:00:00") parses
// fine and now.Sub() of it saturates at ~292 years — one such message poisons
// the whole p95.
func delaySince(ts, now time.Time) (float64, bool) {
	if ts.IsZero() || ts.Year() < 2000 {
		return 0, false
	}
	d := now.Sub(ts)
	if d < -time.Minute || d > maxPlausibleDelay {
		// Negative: clock skew between generator and server. Too large: the
		// timestamp is wrong, not the network.
		return 0, false
	}
	return float64(d.Microseconds()) / 1000.0, true
}

var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// parseTimestamp accepts ISO-8601 (with or without zone; zoneless is taken as
// UTC) and Unix epoch numbers. The epoch unit is inferred from magnitude:
// seconds, milliseconds, microseconds or nanoseconds.
func parseTimestamp(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	if c := v[0]; c >= '0' && c <= '9' && !strings.ContainsAny(v, "-:T ") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return time.Time{}, false
		}
		return fromEpoch(f), true
	}
	for _, layout := range timestampLayouts {
		if ts, err := time.Parse(layout, v); err == nil {
			return ts.UTC(), true
		}
	}
	return time.Time{}, false
}

func fromEpoch(f float64) time.Time {
	a := math.Abs(f)
	switch {
	case a < 1e11: // seconds (until year 5138)
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)).UTC()
	case a < 1e14: // milliseconds
		return time.UnixMicro(int64(f * 1e3)).UTC()
	case a < 1e17: // microseconds
		return time.UnixMicro(int64(f)).UTC()
	default: // nanoseconds
		return time.Unix(0, int64(f)).UTC()
	}
}
