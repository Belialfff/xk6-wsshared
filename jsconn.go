package wsshared

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/grafana/sobek"
	"go.k6.io/k6/metrics"
)

// flushEvery throttles metric pushes from hot paths (next/run). Samples keep
// their receive timestamps, so batching does not skew time series.
const flushEvery = 100 * time.Millisecond

// jsConn is the JS-facing handle. Every method runs on the VU goroutine.
type jsConn struct {
	mi        *ModuleInstance
	conn      *Conn
	obs       *observer
	codec     *Codec
	tags      map[string]string
	startedAt time.Time
	closed    bool
	switches  metricSwitches
	queueOn   bool

	lastFlushed Stats
	lastFlushAt time.Time
	spare       []pendingSample

	dialCfg   Config
	reconnect ReconnectConfig
	// replayList: messages sent with { replay: true }, re-sent after every
	// reconnect (after onOpen). Without it a reconnected socket is alive but
	// silent - the server has no record of what this client asked for.
	replayList  []sendItem
	carried     Stats
	reconnects  int64
	lastDropErr string
}

func (h *jsConn) toObject(rt *sobek.Runtime) (*sobek.Object, error) {
	o := rt.NewObject()
	methods := map[string]any{
		"send":         h.jsSend,
		"next":         h.jsNext,
		"run":          h.jsRun,
		"collect":      h.jsCollect,
		"waitFor":      h.jsWaitFor,
		"drain":        h.jsDrain,
		"close":        h.jsClose,
		"stats":        h.jsStats,
		"tracked":      h.jsTracked,
		"trackedInfo":  h.jsTrackedInfo,
		"trackedAll":   h.jsTrackedAll,
		"buffer":       h.jsBuffer,
		"takeBuffer":   h.jsTakeBuffer,
		"bufferInfo":   h.jsBufferInfo,
		"replay":       h.jsReplay,
		"clearReplay":  h.jsClearReplay,
		"forgetReplay": h.jsForgetReplay,
	}
	for name, fn := range methods {
		if err := o.Set(name, rt.ToValue(fn)); err != nil {
			return nil, err
		}
	}
	if err := o.Set("subprotocol", h.conn.subprotocol); err != nil {
		return nil, err
	}
	return o, nil
}

// ---------------------------------------------------------------------------
// sending
// ---------------------------------------------------------------------------

// jsSend sends a payload: a string as is, an ArrayBuffer/Uint8Array as bytes,
// anything else as JSON. Options: { raw, binary, replay }.
func (h *jsConn) jsSend(data sobek.Value, opts sobek.Value) error {
	if h.closed {
		return errClosed
	}
	var o jsSendWrap
	if !isAbsent(opts) {
		if err := h.mi.vu.Runtime().ExportTo(opts, &o); err != nil {
			return fmt.Errorf("wsshared: bad send options: %w", err)
		}
	}
	it, err := makeItem(h.codec, data, o.Raw, o.Binary)
	if err != nil {
		return err
	}
	if o.Replay {
		h.replayList = append(h.replayList, it)
	}
	if h.dropped() && !h.tryReconnect() {
		return fmt.Errorf("wsshared: send on a dropped connection: %v", h.conn.Err())
	}
	return h.conn.sendItem(it)
}

// ---------------------------------------------------------------------------
// receiving
// ---------------------------------------------------------------------------

func (h *jsConn) requireQueue(method string) error {
	if !h.queueOn {
		return fmt.Errorf("wsshared: %s() needs the event queue, but it is off (queue: false)", method)
	}
	return nil
}

// pull takes one event, reconnecting once if the socket dropped. A zero or
// negative remaining budget only takes what is already queued.
func (h *jsConn) pull(remaining time.Duration) (Event, bool) {
	if remaining < 0 {
		remaining = 0
	}
	ev, ok := h.conn.Next(remaining)
	if ok {
		return ev, true
	}
	if h.dropped() && h.tryReconnect() {
		return h.conn.Next(remaining)
	}
	return Event{}, false
}

// jsNext returns the next event, or null on timeout/close.
func (h *jsConn) jsNext(timeoutMs int) (any, error) {
	if h.closed {
		return nil, nil
	}
	if err := h.requireQueue("next"); err != nil {
		return nil, err
	}
	ev, ok := h.pull(time.Duration(timeoutMs) * time.Millisecond)
	h.flush(false)
	if !ok {
		return nil, nil
	}
	return h.eventToJS(ev), nil
}

// jsRun pumps events for `seconds` entirely in Go: JS is not woken per
// message. Metrics, tracking, buffers and auto-replies all keep working.
func (h *jsConn) jsRun(seconds float64) (map[string]any, error) {
	if h.closed {
		return map[string]any{"closed": true}, nil
	}
	deadline := time.Now().Add(time.Duration(seconds * float64(time.Second)))

	var events, errs, reconnects int64
	var lastErr string
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if !h.queueOn {
			// No queue: everything happens in the read loop; just wait,
			// watching for a drop.
			t := time.NewTimer(remaining)
			select {
			case <-h.conn.done:
			case <-t.C:
			}
			t.Stop()
		} else if ev, ok := h.conn.Next(remaining); ok {
			events++
			if ev.Error != "" {
				errs++
				lastErr = ev.Error
			}
			h.flush(false)
			continue
		}
		if h.dropped() {
			if h.tryReconnect() {
				reconnects++
				continue
			}
		}
		if !h.queueOn && !h.dropped() {
			continue
		}
		break
	}
	h.flush(true)

	out := map[string]any{"events": events, "errors": errs, "reconnects": reconnects}
	if lastErr != "" {
		out["lastError"] = lastErr
	}
	if h.lastDropErr != "" {
		out["lastDropError"] = h.lastDropErr
	}
	if err := h.conn.Err(); err != nil {
		out["connectionError"] = err.Error()
	}
	return out, nil
}

// jsCollect returns up to `count` events matching `match`, waiting at most
// timeoutMs in total. The filtering happens in Go, so JS is woken once, with
// the result. Non-matching events are consumed and discarded (they are still
// measured, tracked and buffered). timeoutMs <= 0 looks only at what is queued.
func (h *jsConn) jsCollect(count int, timeoutMs int, match sobek.Value) ([]any, error) {
	if h.closed || count <= 0 {
		return []any{}, nil
	}
	if err := h.requireQueue("collect"); err != nil {
		return nil, err
	}
	m, err := h.eventMatcher(match)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, count)
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for len(out) < count {
		remaining := time.Until(deadline)
		if timeoutMs > 0 && remaining <= 0 {
			break
		}
		ev, ok := h.pull(remaining)
		if !ok {
			break
		}
		if m != nil && (ev.Error != "" || !m.match(ev.Body, ev.Type, ev.Fields)) {
			continue
		}
		out = append(out, h.eventToJS(ev))
	}
	h.flush(false)
	return out, nil
}

// jsWaitFor returns the first matching event within timeoutMs, or null.
func (h *jsConn) jsWaitFor(match sobek.Value, timeoutMs int) (any, error) {
	got, err := h.jsCollect(1, timeoutMs, match)
	if err != nil || len(got) == 0 {
		return nil, err
	}
	return got[0], nil
}

// jsDrain takes what is queued right now without waiting; max <= 0 = all.
func (h *jsConn) jsDrain(max int) ([]any, error) {
	if h.closed {
		return []any{}, nil
	}
	if err := h.requireQueue("drain"); err != nil {
		return nil, err
	}
	n := h.conn.Queued()
	if max > 0 && max < n {
		n = max
	}
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		ev, ok := h.conn.Next(0)
		if !ok {
			break
		}
		out = append(out, h.eventToJS(ev))
	}
	h.flush(false)
	return out, nil
}

// eventMatcher compiles a matcher for use on queued events, where the body is
// only present if it was kept.
func (h *jsConn) eventMatcher(v sobek.Value) (*matcher, error) {
	m, err := parseMatch(h.mi.vu.Runtime(), v)
	if err != nil || m == nil {
		return m, err
	}
	if m.needsBody && !h.obs.keepBody {
		return nil, errors.New("wsshared: this match reads the body (contains/regex/path), " +
			"which queued events only carry with keepBody: true; " +
			"turn keepBody on, or match on type / field instead")
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// tracked values
// ---------------------------------------------------------------------------

func (h *jsConn) jsTracked(name string, key string) any {
	h.flush(false)
	v, ok := h.obs.tracker.get(name, key)
	if !ok {
		return nil
	}
	return v.Value
}

func (h *jsConn) jsTrackedInfo(name string, key string) map[string]any {
	h.flush(false)
	v, ok := h.obs.tracker.get(name, key)
	misses := h.obs.tracker.missCount(name)
	if !ok {
		return map[string]any{"found": false, "misses": misses}
	}
	return map[string]any{
		"found":   true,
		"value":   v.Value,
		"text":    v.Text,
		"key":     v.Key,
		"type":    v.Type,
		"updates": v.Updates,
		"ageMs":   float64(time.Since(v.UpdatedAt).Nanoseconds()) / 1e6,
		"misses":  misses,
	}
}

func (h *jsConn) jsTrackedAll(name string) map[string]any {
	all := h.obs.tracker.all(name)
	out := make(map[string]any, len(all))
	for k, v := range all {
		out[k] = v.Value
	}
	return out
}

// ---------------------------------------------------------------------------
// buffers
// ---------------------------------------------------------------------------

func (h *jsConn) bufferEvents(name string, take bool) ([]any, error) {
	h.flush(false)
	evs, ok := h.obs.buffers.get(name, take)
	if !ok {
		return nil, fmt.Errorf("wsshared: no buffer named %q", name)
	}
	out := make([]any, len(evs))
	for i := range evs {
		out[i] = h.eventToJS(evs[i])
	}
	return out, nil
}

// jsBuffer returns the buffered events, oldest first, leaving them in place.
func (h *jsConn) jsBuffer(name string) ([]any, error) { return h.bufferEvents(name, false) }

// jsTakeBuffer returns the buffered events and empties the buffer.
func (h *jsConn) jsTakeBuffer(name string) ([]any, error) { return h.bufferEvents(name, true) }

func (h *jsConn) jsBufferInfo(name string) (map[string]any, error) {
	count, size, total, ok := h.obs.buffers.info(name)
	if !ok {
		return nil, fmt.Errorf("wsshared: no buffer named %q", name)
	}
	return map[string]any{
		"count": count, "size": size, "total": total,
		"overwritten": total - int64(count),
	}, nil
}

// ---------------------------------------------------------------------------
// replay list
// ---------------------------------------------------------------------------

func (h *jsConn) jsReplay() []map[string]any {
	out := make([]map[string]any, 0, len(h.replayList))
	for _, it := range h.replayList {
		out = append(out, map[string]any{"data": string(it.data), "raw": it.raw, "binary": it.binary})
	}
	return out
}

func (h *jsConn) jsClearReplay() { h.replayList = nil }

// jsForgetReplay removes recorded messages matching `match` — e.g. a
// subscription the server refused, which would otherwise be replayed on every
// reconnect only to fail again. Returns how many were removed.
func (h *jsConn) jsForgetReplay(match sobek.Value) (int, error) {
	m, err := parseMatch(h.mi.vu.Runtime(), match)
	if err != nil {
		return 0, err
	}
	if m == nil {
		n := len(h.replayList)
		h.replayList = nil
		return n, nil
	}
	kept := h.replayList[:0]
	removed := 0
	now := time.Now()
	for _, it := range h.replayList {
		fields := Extract(it.data, h.obs.fields, now)
		if m.match(it.data, h.obs.typeOf(it.data), fields) {
			removed++
			continue
		}
		kept = append(kept, it)
	}
	h.replayList = kept
	return removed, nil
}

// ---------------------------------------------------------------------------
// close / stats
// ---------------------------------------------------------------------------

func (h *jsConn) jsClose() map[string]any {
	if h.closed {
		return map[string]any{"alreadyClosed": true}
	}
	h.closed = true
	err := h.conn.Close()
	h.flush(true)
	if h.switches.session {
		h.pushOne(h.mi.metrics.sessionDur, float64(time.Since(h.startedAt).Microseconds())/1000.0)
	}

	out := map[string]any{}
	if err != nil {
		out["error"] = err.Error()
	}
	if rerr := h.conn.Err(); rerr != nil {
		out["readError"] = rerr.Error()
	}
	s := h.totalStats()
	out["messagesReceived"] = s.MessagesReceived
	out["messagesSent"] = s.MessagesSent
	out["wireBytesIn"] = s.WireBytesIn
	out["decodedBytesIn"] = s.DecodedBytesIn
	out["reconnects"] = h.reconnects
	return out
}

func (h *jsConn) jsStats() map[string]any {
	h.flush(false)
	s := h.totalStats()
	return map[string]any{
		"messagesReceived": s.MessagesReceived,
		"messagesSent":     s.MessagesSent,
		"wireBytesIn":      s.WireBytesIn,
		"decodedBytesIn":   s.DecodedBytesIn,
		"bytesOut":         s.BytesOut,
		"autoReplied":      s.AutoReplied,
		"controlPings":     s.ControlPings,
		"parseMs":          float64(s.ParseNanos) / 1e6,
		"readGapAvgMs":     avgMs(s.ReadGapNanos, s.ReadGapCount),
		"dropped":          s.Dropped,
		"decodeErrors":     s.DecodeErrors,
		"fallbacks":        s.Fallbacks,
		"queued":           h.conn.Queued(),
		"reconnects":       h.reconnects,
		"samplesDropped":   h.obs.droppedSamples(),
	}
}

func avgMs(totalNanos, count int64) float64 {
	if count <= 0 {
		return 0
	}
	return float64(totalNanos) / float64(count) / 1e6
}

// ---------------------------------------------------------------------------
// events -> JS
// ---------------------------------------------------------------------------

func (h *jsConn) eventToJS(ev Event) map[string]any {
	m := map[string]any{
		"seq":          ev.Seq,
		"binary":       ev.Binary,
		"size":         ev.Size,
		"wireBytes":    ev.WireBytes,
		"decodedBytes": ev.DecodedBytes,
		"receivedAt":   float64(ev.ReceivedAt.UnixNano()) / 1e6,
	}
	if ev.Type != "" {
		m["type"] = ev.Type
	}
	if ev.Error != "" {
		m["error"] = ev.Error
	}
	if len(ev.Fields) > 0 {
		fields := make(map[string]any, len(ev.Fields))
		delays := make(map[string]any)
		for _, f := range ev.Fields {
			fields[f.Name] = f.Value
			if f.HasDelay {
				delays[f.Name] = f.DelayMs
			}
		}
		m["fields"] = fields
		if len(delays) > 0 {
			m["delays"] = delays
		}
	}
	if ev.Body != nil {
		attachBody(m, ev.Body, h.mi.vu.Runtime())
	}
	return m
}

// attachBody adds the lazy body accessors. They are FUNCTIONS, not values: a
// script that never calls them never pays for moving the payload into JS.
func attachBody(m map[string]any, body []byte, rt *sobek.Runtime) {
	m["bodySize"] = len(body)

	// body(): the whole payload as a string - the expensive call.
	m["body"] = func() string { return string(body) }

	// bytes(): the payload as an ArrayBuffer, for binary messages.
	m["bytes"] = func() sobek.ArrayBuffer {
		return rt.NewArrayBuffer(append([]byte(nil), body...))
	}

	// json(): body() + JSON.parse, as a real JS object.
	m["json"] = func() (sobek.Value, error) {
		return jsonParse(rt, body)
	}

	// jsonPath(): resolved in Go; only the matched value crosses into JS.
	m["jsonPath"] = func(path string) (any, error) {
		raw, err := JSONPath(body, path)
		if errors.Is(err, errPathNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return jsonValueToGo(raw), nil
	}

	// match(re, group?): first match in Go, the capture group as a string.
	m["match"] = func(pattern string, group sobek.Value) (any, error) {
		re, g, err := regexAndGroup(pattern, group)
		if err != nil {
			return nil, err
		}
		if s, ok := regexFind(re, g, body); ok {
			return s, nil
		}
		return nil, nil
	}

	// matchAll(re, group?, limit?): every match.
	m["matchAll"] = func(pattern string, group sobek.Value, limit int) ([]string, error) {
		re, g, err := regexAndGroup(pattern, group)
		if err != nil {
			return nil, err
		}
		return regexFindAll(re, g, body, limit), nil
	}

	// test(match): evaluate a matcher spec on this message in Go.
	m["test"] = func(spec sobek.Value) (bool, error) {
		mt, err := parseMatch(rt, spec)
		if err != nil || mt == nil {
			return mt == nil, err
		}
		return mt.match(body, "", nil), nil
	}
}

func jsonParse(rt *sobek.Runtime, body []byte) (sobek.Value, error) {
	if rt == nil {
		return nil, errors.New("wsshared: no runtime for json()")
	}
	parse, ok := sobek.AssertFunction(rt.Get("JSON").ToObject(rt).Get("parse"))
	if !ok {
		return nil, errors.New("wsshared: JSON.parse unavailable")
	}
	return parse(sobek.Undefined(), rt.ToValue(string(body)))
}

func regexAndGroup(pattern string, group sobek.Value) (*regexp.Regexp, int, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, 0, err
	}
	g, set := 0, false
	if !isAbsent(group) {
		g, set = int(group.ToInteger()), true
	}
	g, err = resolveGroup(re, g, set)
	return re, g, err
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

func (h *jsConn) baseTags() *metrics.TagSet {
	state := h.mi.vu.State()
	if state == nil {
		return nil
	}
	tags := state.Tags.GetCurrentValues().Tags
	for k, v := range h.tags {
		tags = tags.With(k, v)
	}
	return tags
}

func (h *jsConn) push(m *metrics.Metric, value float64, tags *metrics.TagSet, at time.Time) {
	if m == nil {
		return
	}
	state := h.mi.vu.State()
	if state == nil {
		return
	}
	metrics.PushIfNotDone(h.mi.ctx(), state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: m, Tags: tags},
		Time:       at,
		Value:      value,
	})
}

func (h *jsConn) pushOne(m *metrics.Metric, v float64) {
	h.push(m, v, h.baseTags(), time.Now())
}

// flush pushes pending per-message samples and counter deltas. Throttled
// unless forced.
func (h *jsConn) flush(force bool) {
	if !force && time.Since(h.lastFlushAt) < flushEvery {
		return
	}
	h.lastFlushAt = time.Now()
	base := h.baseTags()

	pending := h.obs.swapPending(h.spare)
	mm := h.mi.metrics
	for _, s := range pending {
		switch s.kind {
		case sampleDelay:
			h.push(mm.msgDelay, s.value, base.With("field", s.label), s.at)
		case sampleBadTimestamp:
			h.push(mm.badTimestamps, s.value, base.With("field", s.label), s.at)
		case sampleByType:
			h.push(mm.byType, s.value, base.With("messageType", s.label), s.at)
		}
	}
	h.spare = pending
	h.flushCounters(base)
}

// flushCounters pushes the delta of each cumulative counter since the last
// call.
func (h *jsConn) flushCounters(base *metrics.TagSet) {
	if h.conn == nil {
		return
	}
	s := h.conn.Stats()
	prev := h.lastFlushed
	// Always advance the watermark, so a group switched off does not dump a
	// backlog later.
	defer func() { h.lastFlushed = s }()
	now := time.Now()
	mm := h.mi.metrics

	if h.switches.parse {
		if d := s.ParseNanos - prev.ParseNanos; d > 0 {
			h.push(mm.parseDuration, float64(d)/1e6, base, now)
		}
	}
	if h.switches.readGap {
		if n := s.ReadGapCount - prev.ReadGapCount; n > 0 {
			d := s.ReadGapNanos - prev.ReadGapNanos
			h.push(mm.readGap, float64(d)/float64(n)/1e6, base, now)
		}
	}
	if !h.switches.counters {
		return
	}
	pairs := []struct {
		m *metrics.Metric
		d int64
	}{
		{mm.msgsReceived, s.MessagesReceived - prev.MessagesReceived},
		{mm.msgsSent, s.MessagesSent - prev.MessagesSent},
		{mm.wireBytesIn, s.WireBytesIn - prev.WireBytesIn},
		{mm.decodedBytes, s.DecodedBytesIn - prev.DecodedBytesIn},
		{mm.bytesOut, s.BytesOut - prev.BytesOut},
		{mm.autoReplied, s.AutoReplied - prev.AutoReplied},
		{mm.dropped, s.Dropped - prev.Dropped},
		{mm.decodeErrors, s.DecodeErrors - prev.DecodeErrors},
	}
	for _, p := range pairs {
		if p.d > 0 {
			h.push(p.m, float64(p.d), base, now)
		}
	}
}
