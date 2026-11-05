package wsshared

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// server runs handler per connection and returns a ws:// URL.
func server(t *testing.T, handler func(*websocket.Conn)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close() //nolint:errcheck
		handler(c)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func idle(c *websocket.Conn) {
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			return
		}
	}
}

func dial(t *testing.T, cfg Config, obs *observer) *Conn {
	t.Helper()
	c, err := Dial(context.Background(), cfg, obs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestOnOpenAutoReplyAndFields(t *testing.T) {
	got := make(chan string, 8)
	url := server(t, func(c *websocket.Conn) {
		_, hello, _ := c.ReadMessage()
		got <- string(hello)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"op":"ping","n":1}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"op":"data","px":1.5,"ts":"`+time.Now().UTC().Format(time.RFC3339Nano)+`"}`))
		_, pong, _ := c.ReadMessage()
		got <- string(pong)
		idle(c)
	})

	m := mustMatch(t, MatchSpec{Path: "op", Equals: "ping", HasEquals: true})
	ts := mustField(t, FieldSpec{Name: "ts", AsTimestamp: true})
	obs := &observer{
		typePath: mustPath(t, "op"),
		fields:   []FieldSpec{ts},
		replies:  []autoReply{{when: m, reply: &sendItem{data: []byte(`{"op":"pong"}`)}, swallow: true}},
		switches: metricSwitches{delay: true, byType: true},
	}
	c := dial(t, Config{URL: url, OnOpen: []sendItem{{data: []byte(`{"op":"hello"}`)}}}, obs)

	if h := <-got; h != `{"op":"hello"}` {
		t.Fatalf("onOpen: %q", h)
	}
	ev, ok := c.Next(2 * time.Second)
	if !ok || ev.Type != "data" {
		t.Fatalf("the ping must be swallowed, got %+v", ev)
	}
	if len(ev.Fields) != 1 || !ev.Fields[0].HasDelay {
		t.Fatalf("fields: %+v", ev.Fields)
	}
	if p := <-got; p != `{"op":"pong"}` {
		t.Fatalf("auto-reply: %q", p)
	}
	if s := c.Stats(); s.AutoReplied != 1 || s.MessagesReceived != 1 {
		t.Fatalf("stats: %+v", s)
	}
	pending := obs.swapPending(nil)
	if len(pending) != 2 { // one byType + one delay
		t.Fatalf("pending samples: %+v", pending)
	}
}

func mustPath(t *testing.T, p string) *jpath {
	t.Helper()
	jp, err := compilePath(p)
	if err != nil {
		t.Fatal(err)
	}
	return jp
}

// Compressed + length-prefixed binary channel, with a plain-text ack first.
func TestCompressedChannel(t *testing.T) {
	codec, _ := NewCodec(CodecConfig{Compression: "deflate", LengthPrefix: "varint", Delimiter: "\x1e"})
	url := server(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.BinaryMessage, []byte("{}\x1e"))
		_, in, _ := c.ReadMessage()
		res, err := codec.Decode(in)
		if err != nil || len(res.Messages) != 1 {
			return
		}
		echo, _ := codec.Encode([]byte(`{"echo":` + string(res.Messages[0]) + `}`))
		_ = c.WriteMessage(websocket.BinaryMessage, echo)
		idle(c)
	})
	c := dial(t, Config{URL: url, Codec: codec}, &observer{keepBody: true})

	ack, ok := c.Next(2 * time.Second)
	if !ok || string(ack.Body) != "{}" {
		t.Fatalf("ack: %+v", ack)
	}
	if err := c.sendItem(sendItem{data: []byte(`{"q":1}`), binary: codec.Binary()}); err != nil {
		t.Fatal(err)
	}
	ev, ok := c.Next(2 * time.Second)
	if !ok || string(ev.Body) != `{"echo":{"q":1}}` || !ev.Binary {
		t.Fatalf("echo: %+v %q", ev, ev.Body)
	}
	if s := c.Stats(); s.Fallbacks != 1 || s.DecodedBytesIn <= 0 {
		t.Fatalf("stats: %+v", s)
	}
}

func flood(n int) func(*websocket.Conn) {
	return func(c *websocket.Conn) {
		for i := 1; i <= n; i++ {
			_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"n":%d}`, i)))
		}
		idle(c)
	}
}

// dropOldest keeps the queue fresh and the reader moving; tracking sees
// everything regardless.
func TestOverflowDropOldestKeepsTracking(t *testing.T) {
	url := server(t, flood(500))
	tr, _ := newTracker([]TrackSpec{{Name: "n", Paths: []string{"n"}}})
	c := dial(t, Config{URL: url, EventBuffer: 8, Overflow: overflowDropOldest}, &observer{tracker: tr})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := tr.get("n", ""); v.Value == 500.0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v, _ := tr.get("n", ""); v.Value != 500.0 {
		t.Fatalf("tracker must see the last message without the VU reading: %+v", v)
	}
	if c.Queued() != 8 {
		t.Fatalf("queue should be full, got %d", c.Queued())
	}
	first, _ := c.Next(0)
	if first.Seq != 493 {
		t.Fatalf("oldest dropped: first queued seq should be 493, got %d", first.Seq)
	}
	if s := c.Stats(); s.Dropped != 492 {
		t.Fatalf("dropped: %d", s.Dropped)
	}
}

func TestOverflowDropNewestAndNoQueue(t *testing.T) {
	url := server(t, flood(100))
	c := dial(t, Config{URL: url, EventBuffer: 4, Overflow: overflowDropNewest}, &observer{})
	waitFor(t, func() bool { return c.Stats().MessagesReceived == 100 })
	if ev, _ := c.Next(0); ev.Seq != 1 {
		t.Fatalf("dropNewest keeps the first: %d", ev.Seq)
	}
	if c.Stats().Dropped != 96 {
		t.Fatalf("dropped %d", c.Stats().Dropped)
	}

	bs, _ := newBufferSet([]BufferSpec{{Name: "last", Size: 5}})
	url2 := server(t, flood(100))
	c2 := dial(t, Config{URL: url2, Overflow: overflowNoQueue}, &observer{buffers: bs})
	waitFor(t, func() bool { return c2.Stats().MessagesReceived == 100 })
	if c2.Queued() != 0 {
		t.Fatal("no-queue mode must not queue")
	}
	last, _ := bs.get("last", false)
	if len(last) != 5 || last[4].Seq != 100 {
		t.Fatalf("buffer: %+v", last)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestHeartbeat(t *testing.T) {
	var mu sync.Mutex
	var beats, pings int
	url := server(t, func(c *websocket.Conn) {
		c.SetPingHandler(func(string) error {
			mu.Lock()
			pings++
			mu.Unlock()
			return nil
		})
		for {
			_, m, err := c.ReadMessage()
			if err != nil {
				return
			}
			if string(m) == `{"op":"hb"}` {
				mu.Lock()
				beats++
				mu.Unlock()
			}
		}
	})
	_ = dial(t, Config{URL: url, Heartbeat: HeartbeatConfig{
		Every: 20 * time.Millisecond, Control: true,
		Item: &sendItem{data: []byte(`{"op":"hb"}`)},
	}}, &observer{})
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if beats < 3 || pings < 3 {
		t.Fatalf("beats=%d pings=%d", beats, pings)
	}
}

// Regression: once Next reports the queue closed, `done` must already be
// closed, or the drop is invisible to dropped() and no reconnect happens.
func TestQueueClosedImpliesDone(t *testing.T) {
	url := server(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"x":1}`))
	})
	for i := 0; i < 200; i++ {
		c, err := Dial(context.Background(), Config{URL: url}, &observer{})
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, ok := c.Next(-1); !ok {
				break
			}
		}
		select {
		case <-c.done:
		default:
			t.Fatalf("iteration %d: events closed but done still open", i)
		}
		_ = c.Close()
	}
}

func TestBlockingReaderUnblocksOnClose(t *testing.T) {
	url := server(t, flood(50))
	c, err := Dial(context.Background(), Config{URL: url, EventBuffer: 1}, &observer{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // reader now blocked in emit
	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close must unblock a reader stuck on a full queue")
	}
}

func TestConcurrentCloseAndNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	url := server(t, flood(10))
	for i := 0; i < 20; i++ {
		c, err := Dial(context.Background(), Config{URL: url,
			Heartbeat: HeartbeatConfig{Every: time.Millisecond, Control: true}}, &observer{})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() { defer wg.Done(); _ = c.Close(); c.Next(time.Millisecond) }()
		}
		wg.Wait()
		if err := c.sendItem(sendItem{data: []byte("x")}); err != errClosed {
			t.Fatalf("send after close: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Fatalf("goroutines: %d -> %d", before, after)
	}
}
