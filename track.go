package wsshared

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Value tracking: "the current value of X, whenever I ask".
//
// A VU that mixes socket traffic with HTTP calls cannot sit in next(): while it
// is inside an HTTP request the event queue just grows. But usually the test
// does not want every message — it wants the CURRENT value of something, e.g.
// the latest price of the instrument it is about to order.
//
// Tracking moves that into the read loop: every message is checked against the
// declared specs and the last value is kept in a map. The VU reads it with one
// cheap call whenever it needs it, and is never woken per message. State lives
// on the JS handle, so it survives a reconnect.

// TrackSpec declares one value to keep the latest of.
type TrackSpec struct {
	// Name identifies the value: conn.tracked(name). Several specs may share a
	// name — e.g. the same value arriving in two message shapes — and then
	// write into the same slot.
	Name string
	// Paths are tried in order; the first that resolves wins. Lets one spec
	// cover a value that sits at different places in different messages.
	Paths []string
	// Regex is the alternative to Paths: a capture group over the body.
	Regex    string
	Group    int
	GroupSet bool
	// When restricts the spec to matching messages.
	When *matcher
	// KeyBy splits the value per key — a jsonPath whose value becomes the key,
	// e.g. 'symbol' gives one current price per instrument.
	KeyBy string

	jps   []*jpath
	re    *regexp.Regexp
	group int
	keyJP *jpath
}

func (s *TrackSpec) compile() error {
	if s.Name == "" {
		return errors.New("wsshared: track entry needs a name")
	}
	if len(s.Paths) == 0 && s.Regex == "" {
		return fmt.Errorf("wsshared: track %q needs a path, paths or regex", s.Name)
	}
	if len(s.Paths) > 0 && s.Regex != "" {
		return fmt.Errorf("wsshared: track %q: path and regex are exclusive", s.Name)
	}
	for _, p := range s.Paths {
		jp, err := compilePath(p)
		if err != nil {
			return err
		}
		s.jps = append(s.jps, jp)
	}
	if s.Regex != "" {
		re, err := compileRegex(s.Regex)
		if err != nil {
			return err
		}
		g, err := resolveGroup(re, s.Group, s.GroupSet)
		if err != nil {
			return fmt.Errorf("wsshared: track %q: %w", s.Name, err)
		}
		s.re, s.group = re, g
	}
	if s.KeyBy != "" {
		jp, err := compilePath(s.KeyBy)
		if err != nil {
			return err
		}
		s.keyJP = jp
	}
	return nil
}

// TrackedValue is one tracked value plus when it was seen.
type TrackedValue struct {
	// Value is the JS-ready value: number, string, bool, null, or JSON text
	// for a composite. A regex capture is always a string.
	Value any
	// Text is the textual form, as a field would report it.
	Text      string
	Key       string
	Type      string
	UpdatedAt time.Time
	// Updates counts writes to this slot. A value that stops moving is
	// visible here without diffing Value.
	Updates int64
}

// tracker is written by the read loop and read by the VU; the map is tiny and
// the critical section is an assignment, so a mutex is enough.
type tracker struct {
	specs []TrackSpec

	mu     sync.RWMutex
	values map[string]map[string]TrackedValue
	// misses counts messages the spec applied to but could not resolve. A path
	// typo otherwise looks exactly like a quiet socket.
	misses map[string]int64
}

func newTracker(specs []TrackSpec) (*tracker, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	t := &tracker{
		specs:  specs,
		values: make(map[string]map[string]TrackedValue, len(specs)),
		misses: make(map[string]int64, len(specs)),
	}
	for i := range t.specs {
		if err := t.specs[i].compile(); err != nil {
			return nil, err
		}
		if t.values[t.specs[i].Name] == nil {
			t.values[t.specs[i].Name] = make(map[string]TrackedValue, 4)
		}
	}
	return t, nil
}

// observe feeds one message to every applicable spec.
func (t *tracker) observe(raw []byte, typ string, fields []Extracted, at time.Time) {
	if t == nil || len(raw) == 0 {
		return
	}
	for i := range t.specs {
		s := &t.specs[i]
		if s.When != nil && !s.When.match(raw, typ, fields) {
			continue
		}
		var (
			val   any
			text  string
			found bool
		)
		if s.re != nil {
			text, found = regexFind(s.re, s.group, raw)
			val = text
		} else {
			for _, jp := range s.jps {
				if r, err := jp.lookup(raw); err == nil {
					val, text, found = jsonValueToGo(r), valueText(r), true
					break
				}
			}
		}
		if !found {
			t.mu.Lock()
			t.misses[s.Name]++
			t.mu.Unlock()
			continue
		}
		key := ""
		if s.keyJP != nil {
			if r, err := s.keyJP.lookup(raw); err == nil {
				key = valueText(r)
			}
		}

		t.mu.Lock()
		bucket := t.values[s.Name]
		prev := bucket[key]
		bucket[key] = TrackedValue{
			Value: val, Text: text, Key: key, Type: typ,
			UpdatedAt: at, Updates: prev.Updates + 1,
		}
		t.mu.Unlock()
	}
}

// get returns the latest value for a spec, optionally for one key. Without a
// key: the single slot of an unkeyed spec, or the most recently updated one.
func (t *tracker) get(name, key string) (TrackedValue, bool) {
	if t == nil {
		return TrackedValue{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	bucket, ok := t.values[name]
	if !ok {
		return TrackedValue{}, false
	}
	if key != "" {
		v, ok := bucket[key]
		return v, ok
	}
	var newest TrackedValue
	found := false
	for _, v := range bucket {
		if !found || v.UpdatedAt.After(newest.UpdatedAt) {
			newest, found = v, true
		}
	}
	return newest, found
}

func (t *tracker) all(name string) map[string]TrackedValue {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	bucket, ok := t.values[name]
	if !ok {
		return nil
	}
	out := make(map[string]TrackedValue, len(bucket))
	for k, v := range bucket {
		out[k] = v
	}
	return out
}

func (t *tracker) missCount(name string) int64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.misses[name]
}
