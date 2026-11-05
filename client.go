package wsshared

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// readGapCapNanos bounds what counts as a read gap. An idle socket produces
// arbitrarily long waits that say nothing about generator health.
const readGapCapNanos = int64(20 * time.Millisecond)

// maxPending bounds metric samples waiting for the VU to flush them.
const maxPending = 1 << 16

// maxTypeLabels caps distinct messageType tag values: a typePath that points at
// an id instead of a kind would otherwise explode metric cardinality.
const maxTypeLabels = 64

var errClosed = errors.New("wsshared: connection is closed")

// ---------------------------------------------------------------------------
// observer: per-message work that must survive a reconnect
// ---------------------------------------------------------------------------

// sendItem is one prepared outbound message.
type sendItem struct {
	// data is the payload after the codec's format layer (see makeItem).
	data []byte
	// raw bypasses the codec: the bytes go out exactly as given.
	raw    bool
	binary bool
}

// autoReply answers matching messages inside the read loop — keep-alives,
// acks — so the VU is never woken for them.
type autoReply struct {
	when    *matcher
	reply   *sendItem
	swallow bool
}

type sampleKind uint8

const (
	sampleDelay sampleKind = iota
	sampleBadTimestamp
	sampleByType
)

type pendingSample struct {
	kind  sampleKind
	label string
	value float64
	at    time.Time
}

// observer holds everything the read loop does per message besides queueing:
// field extraction, typing, auto-replies, tracking, buffers and metric
// samples. It belongs to the JS handle, not to one socket, so tracked values,
// buffers and pending samples carry over a reconnect.
type observer struct {
	fields   []FieldSpec
	typePath *jpath
	tracker  *tracker
	buffers  *bufferSet
	replies  []autoReply
	keepBody bool
	switches metricSwitches

	seq atomic.Int64

	// Metric samples are produced here, on the reader goroutine, and pushed by
	// the VU (k6 expects samples from the VU side). Recording them at read
	// time means a message is measured whether or not the script ever reads
	// it - collect() skipping it, overflow dropping it, a VU that only uses
	// tracked() - and with its real receive timestamp.
	pmu            sync.Mutex
	pending        []pendingSample
	pendingDropped int64
	typeLabels     map[string]struct{}
}

func (o *observer) typeOf(raw []byte) string {
	if o.typePath == nil {
		return ""
	}
	v, err := o.typePath.lookup(raw)
	if err != nil {
		return ""
	}
	return valueText(v)
}

func (o *observer) autoReplyFor(raw []byte, typ string, fields []Extracted) *autoReply {
	for i := range o.replies {
		if o.replies[i].when.match(raw, typ, fields) {
			return &o.replies[i]
		}
	}
	return nil
}

func (o *observer) record(ev *Event) {
	byType := o.switches.byType && o.typePath != nil
	if !o.switches.delay && !byType {
		return
	}
	o.pmu.Lock()
	defer o.pmu.Unlock()
	add := func(s pendingSample) {
		if len(o.pending) >= maxPending {
			o.pendingDropped++
			return
		}
		o.pending = append(o.pending, s)
	}
	if byType {
		label := ev.Type
		if label == "" {
			label = "unknown"
		}
		if _, seen := o.typeLabels[label]; !seen {
			if o.typeLabels == nil {
				o.typeLabels = make(map[string]struct{})
			}
			if len(o.typeLabels) >= maxTypeLabels {
				label = "other"
			} else {
				o.typeLabels[label] = struct{}{}
			}
		}
		add(pendingSample{kind: sampleByType, label: label, value: 1, at: ev.ReceivedAt})
	}
	if o.switches.delay {
		for i := range ev.Fields {
			f := &ev.Fields[i]
			switch {
			case f.HasDelay:
				add(pendingSample{kind: sampleDelay, label: f.Name, value: f.DelayMs, at: ev.ReceivedAt})
			case f.BadTimestamp:
				add(pendingSample{kind: sampleBadTimestamp, label: f.Name, value: 1, at: ev.ReceivedAt})
			}
		}
	}
}

// swapPending hands the accumulated samples to the caller and installs buf
// (emptied) as the new accumulator — double buffering, no steady-state allocs.
func (o *observer) swapPending(buf []pendingSample) []pendingSample {
	o.pmu.Lock()
	out := o.pending
	o.pending = buf[:0]
	o.pmu.Unlock()
	return out
}

func (o *observer) droppedSamples() int64 {
	o.pmu.Lock()
	defer o.pmu.Unlock()
	return o.pendingDropped
}

// ---------------------------------------------------------------------------
// Conn
// ---------------------------------------------------------------------------

type overflowMode int

const (
	overflowBlock overflowMode = iota
	overflowDropNewest
	overflowDropOldest
	overflowNoQueue
)

func parseOverflow(s string, queue bool) (overflowMode, error) {
	if !queue {
		return overflowNoQueue, nil
	}
	switch s {
	case "", "block":
		return overflowBlock, nil
	case "dropNewest", "drop":
		return overflowDropNewest, nil
	case "dropOldest":
		return overflowDropOldest, nil
	default:
		return 0, fmt.Errorf("wsshared: unknown overflow %q (want block, dropNewest, dropOldest)", s)
	}
}

// Conn is one WebSocket connection. Exported methods are safe to call from the
// VU goroutine; the read loop runs in its own goroutine.
type Conn struct {
	codec *Codec
	ws    *websocket.Conn
	obs   *observer

	// writeMu serialises writes: gorilla permits one concurrent writer, and
	// the read loop (auto-replies) and heartbeat write too.
	writeMu sync.Mutex

	mu       sync.Mutex
	closed   bool
	closeErr error

	events   chan Event
	overflow overflowMode
	done     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once

	stats   Stats
	statsMu sync.Mutex

	subprotocol string
}

// Event is one decoded message handed to the VU.
type Event struct {
	Seq    int64
	Type   string
	Binary bool
	// Size is this message's decoded size.
	Size int
	// WireBytes / DecodedBytes describe the whole frame and are attributed to
	// its first delivered message only, so summing over events is exact.
	WireBytes    int
	DecodedBytes int
	Error        string
	Fields       []Extracted
	ReceivedAt   time.Time
	// Body is the message bytes, present only when kept. Zero-copy: the codec
	// output is never reused, so holding it is safe.
	Body []byte
}

// Stats accumulates per-connection counters.
type Stats struct {
	MessagesReceived int64
	MessagesSent     int64
	WireBytesIn      int64
	DecodedBytesIn   int64
	BytesOut         int64
	AutoReplied      int64
	ControlPings     int64
	ParseNanos       int64
	ReadGapNanos     int64
	ReadGapCount     int64
	Dropped          int64
	DecodeErrors     int64
	Fallbacks        int64
}

func (s Stats) add(o Stats) Stats {
	s.MessagesReceived += o.MessagesReceived
	s.MessagesSent += o.MessagesSent
	s.WireBytesIn += o.WireBytesIn
	s.DecodedBytesIn += o.DecodedBytesIn
	s.BytesOut += o.BytesOut
	s.AutoReplied += o.AutoReplied
	s.ControlPings += o.ControlPings
	s.ParseNanos += o.ParseNanos
	s.ReadGapNanos += o.ReadGapNanos
	s.ReadGapCount += o.ReadGapCount
	s.Dropped += o.Dropped
	s.DecodeErrors += o.DecodeErrors
	s.Fallbacks += o.Fallbacks
	return s
}

// HeartbeatConfig sends something periodically from Go.
type HeartbeatConfig struct {
	Every time.Duration
	// Item is an application-level message; nil for none.
	Item *sendItem
	// Control sends a WebSocket ping control frame.
	Control bool
}

// Config configures a connection.
type Config struct {
	URL               string
	Headers           map[string]string
	Subprotocols      []string
	HandshakeTimeout  time.Duration
	SkipTLSVerify     bool
	EnableCompression bool

	Codec *Codec

	EventBuffer int
	Overflow    overflowMode

	// OnOpen is sent right after every successful dial, reconnects included:
	// auth frames, protocol hellos.
	OnOpen    []sendItem
	Heartbeat HeartbeatConfig
}

// Dial opens a connection, sends OnOpen and starts the read loop.
func Dial(ctx context.Context, cfg Config, obs *observer) (*Conn, error) {
	codec := cfg.Codec
	if codec == nil {
		codec, _ = NewCodec(CodecConfig{})
	}
	if obs == nil {
		obs = &observer{}
	}

	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("wsshared: bad url: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}

	dialer := *websocket.DefaultDialer
	if cfg.HandshakeTimeout > 0 {
		dialer.HandshakeTimeout = cfg.HandshakeTimeout
	}
	if cfg.SkipTLSVerify {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	}
	dialer.Subprotocols = cfg.Subprotocols
	dialer.EnableCompression = cfg.EnableCompression

	header := http.Header{}
	for k, v := range cfg.Headers {
		header.Set(k, v)
	}

	ws, resp, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("wsshared: dial failed (http %d): %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("wsshared: dial failed: %w", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	buf := cfg.EventBuffer
	if buf <= 0 {
		buf = 256
	}
	c := &Conn{
		codec:       codec,
		ws:          ws,
		obs:         obs,
		events:      make(chan Event, buf),
		overflow:    cfg.Overflow,
		done:        make(chan struct{}),
		stop:        make(chan struct{}),
		subprotocol: ws.Subprotocol(),
	}

	// Answer control pings ourselves so they can be counted. Errors are
	// ignored: a broken socket is reported by the read loop.
	ws.SetPingHandler(func(data string) error {
		c.statsMu.Lock()
		c.stats.ControlPings++
		c.statsMu.Unlock()
		_ = ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
		return nil
	})

	for _, it := range cfg.OnOpen {
		if err := c.sendItem(it); err != nil {
			_ = ws.Close()
			return nil, fmt.Errorf("wsshared: onOpen send failed: %w", err)
		}
	}

	go c.readLoop()
	if cfg.Heartbeat.Every > 0 && (cfg.Heartbeat.Item != nil || cfg.Heartbeat.Control) {
		go c.heartbeat(cfg.Heartbeat)
	}
	return c, nil
}

func (c *Conn) writeRaw(msgType int, b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.WriteMessage(msgType, b)
}

// sendItem frames (unless raw) and writes one message. The format layer has
// already been applied when the item was built (makeItem).
func (c *Conn) sendItem(it sendItem) error {
	if c.isClosed() {
		return errClosed
	}
	frame := it.data
	if !it.raw {
		var err error
		if frame, err = c.codec.Frame(it.data); err != nil {
			return err
		}
	}
	mt := websocket.TextMessage
	if it.binary {
		mt = websocket.BinaryMessage
	}
	if err := c.writeRaw(mt, frame); err != nil {
		return err
	}
	c.statsMu.Lock()
	c.stats.MessagesSent++
	c.stats.BytesOut += int64(len(frame))
	c.statsMu.Unlock()
	return nil
}

func (c *Conn) heartbeat(hb HeartbeatConfig) {
	t := time.NewTicker(hb.Every)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-c.done:
			return
		case <-t.C:
			if hb.Control {
				_ = c.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
			if hb.Item != nil {
				_ = c.sendItem(*hb.Item)
			}
		}
	}
}

// readLoop owns the websocket reader. It exits on read error or Close and
// always closes `events` and `done` exactly once.
func (c *Conn) readLoop() {
	// Order matters: `done` must be closed BEFORE `events`. A consumer that
	// sees `events` closed immediately asks dropped(), which looks at `done`;
	// with the opposite order there is a window where the socket is gone but
	// not yet reported as dropped, and run()/next() return early instead of
	// reconnecting. (Two plain defers would run LIFO and get this backwards.)
	defer func() {
		close(c.done)
		close(c.events)
	}()

	o := c.obs
	// idleSince marks when this goroutine last became ready to read again;
	// the gap to the next frame is our own lag when frames are back-to-back.
	var idleSince time.Time

	for {
		select {
		case <-c.stop:
			return
		default:
		}

		mt, frame, err := c.ws.ReadMessage()
		if err != nil {
			c.mu.Lock()
			if !c.closed {
				c.closeErr = err
			}
			c.mu.Unlock()
			return
		}
		received := time.Now()

		var gapNanos int64 = -1
		if !idleSince.IsZero() {
			if g := received.Sub(idleSince).Nanoseconds(); g >= 0 && g <= readGapCapNanos {
				gapNanos = g
			}
		}

		// busy accumulates the generator's own work on this frame - decode,
		// extraction, matchers, tracking, buffers - but not the time spent
		// handing events to a slow consumer.
		busyFrom := received
		var busy time.Duration

		res, decErr := c.codec.DecodeFrame(frame, mt == websocket.BinaryMessage)

		c.statsMu.Lock()
		c.stats.WireBytesIn += int64(len(frame))
		c.stats.DecodedBytesIn += int64(res.DecodedBytes)
		if gapNanos >= 0 {
			c.stats.ReadGapNanos += gapNanos
			c.stats.ReadGapCount++
		}
		if res.Fallback {
			c.stats.Fallbacks++
		}
		if decErr != nil {
			c.stats.DecodeErrors++
		}
		c.statsMu.Unlock()

		binary := mt == websocket.BinaryMessage
		if decErr != nil {
			c.addParse(time.Since(busyFrom))
			c.emit(Event{
				Seq: o.seq.Add(1), Error: decErr.Error(), Binary: binary,
				WireBytes: len(frame), ReceivedAt: received,
			})
			idleSince = time.Now()
			continue
		}

		attributed := false
		for _, raw := range res.Messages {
			typ := o.typeOf(raw)
			var fields []Extracted
			if len(o.fields) > 0 {
				fields = Extract(raw, o.fields, received)
			}

			// Keep-alives and acks are handled here; the VU never sees them.
			if r := o.autoReplyFor(raw, typ, fields); r != nil {
				c.statsMu.Lock()
				c.stats.AutoReplied++
				c.statsMu.Unlock()
				if r.reply != nil {
					// A failed reply means the socket is going away; the
					// next ReadMessage reports it.
					_ = c.sendItem(*r.reply)
				}
				if r.swallow {
					continue
				}
			}

			ev := Event{
				Seq: o.seq.Add(1), Type: typ, Binary: binary, Size: len(raw),
				Fields: fields, ReceivedAt: received,
			}
			if !attributed {
				ev.WireBytes, ev.DecodedBytes = len(frame), res.DecodedBytes
				attributed = true
			}
			if o.keepBody {
				ev.Body = raw
			}

			// Tracking and buffers run before queueing, so their state is
			// current even if the VU never reads the queue.
			o.tracker.observe(raw, typ, fields, received)
			o.buffers.observe(&ev, raw, o.keepBody)
			o.record(&ev)

			c.statsMu.Lock()
			c.stats.MessagesReceived++
			c.statsMu.Unlock()

			busy += time.Since(busyFrom)
			c.emit(ev)
			busyFrom = time.Now()
		}
		busy += time.Since(busyFrom)
		c.addParse(busy)

		idleSince = time.Now()
	}
}

func (c *Conn) addParse(d time.Duration) {
	c.statsMu.Lock()
	c.stats.ParseNanos += d.Nanoseconds()
	c.statsMu.Unlock()
}

// emit queues an event according to the overflow policy. It never blocks
// forever: Close unblocks it.
func (c *Conn) emit(ev Event) {
	switch c.overflow {
	case overflowNoQueue:
		return
	case overflowDropNewest:
		select {
		case c.events <- ev:
		default:
			c.countDropped()
		}
	case overflowDropOldest:
		for {
			select {
			case c.events <- ev:
				return
			case <-c.stop:
				return
			default:
			}
			select {
			case <-c.events:
				c.countDropped()
			default:
			}
		}
	default: // block: backpressure onto the socket
		select {
		case c.events <- ev:
		case <-c.stop:
		}
	}
}

func (c *Conn) countDropped() {
	c.statsMu.Lock()
	c.stats.Dropped++
	c.statsMu.Unlock()
}

// Next returns the next event, waiting up to `timeout`. Zero takes whatever
// is queued and returns immediately; negative blocks until an event arrives.
// ok=false: timed out, or the connection is gone.
func (c *Conn) Next(timeout time.Duration) (Event, bool) {
	if timeout == 0 {
		select {
		case ev, ok := <-c.events:
			return ev, ok
		default:
			return Event{}, false
		}
	}
	if timeout < 0 {
		ev, ok := <-c.events
		return ev, ok
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case ev, ok := <-c.events:
		return ev, ok
	case <-t.C:
		return Event{}, false
	}
}

// Queued reports how many events wait in the queue.
func (c *Conn) Queued() int { return len(c.events) }

// Close shuts the connection down and waits for the read loop to finish.
// Idempotent and safe to call concurrently.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.done
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.stopOnce.Do(func() { close(c.stop) })

	_ = c.writeRaw(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	err := c.ws.Close()

	<-c.done
	return err
}

func (c *Conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Stats returns a snapshot of the counters.
func (c *Conn) Stats() Stats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return c.stats
}

// Err returns the error that terminated the read loop, if any.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}
