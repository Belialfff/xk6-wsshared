// Command mockws is a small WebSocket server for exercising the extension
// without a real environment. It speaks a made-up JSON protocol:
//
//	client -> {"op":"subscribe","channel":"quotes.EURUSD"}
//	server -> {"type":"ack","channel":"quotes.EURUSD","code":0}
//	server -> {"type":"quote","channel":"quotes.EURUSD","seq":17,
//	           "data":{"symbol":"EURUSD","bid":1.0712,"ask":1.0715,"levels":[...]},
//	           "ts":"2026-..."}                                   every -every
//	server -> {"type":"trade","channel":...,"data":{"status":"filled",...}}  now and then
//	server -> {"type":"ping","n":3}                                every 2s
//	client -> {"type":"pong","n":3}
//
// Framing is configurable, so the same server exercises every codec layer:
//
//	go run ./examples/mockws                                    # plain JSON text frames
//	go run ./examples/mockws -compression=deflate -varint -delimiter=rs
//	go run ./examples/mockws -drop-after=4s                     # to test reconnect
package main

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

type options struct {
	every     time.Duration
	levels    int
	dropAfter time.Duration
	deflate   bool
	varint    bool
	delimiter []byte
	verbose   bool
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "listen address")
	var o options
	flag.DurationVar(&o.every, "every", 200*time.Millisecond, "quote interval per subscription")
	flag.IntVar(&o.levels, "levels", 50, "order book levels per quote (payload size)")
	flag.DurationVar(&o.dropAfter, "drop-after", 0, "if >0, kill each connection this long after it opens")
	compression := flag.String("compression", "none", "none | deflate")
	flag.BoolVar(&o.varint, "varint", false, "VarInt length prefix on every frame")
	delim := flag.String("delimiter", "", "message delimiter: '' | nl | rs (0x1e)")
	flag.BoolVar(&o.verbose, "v", false, "log inbound messages")
	flag.Parse()

	o.deflate = *compression == "deflate"
	switch *delim {
	case "nl":
		o.delimiter = []byte("\n")
	case "rs":
		o.delimiter = []byte{0x1e}
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close() //nolint:errcheck
		serve(c, o, r.URL.Query().Get("token"))
	})
	log.Printf("mockws on ws://%s/ws (compression=%s varint=%v delimiter=%q)",
		*addr, *compression, o.varint, *delim)
	log.Fatal(http.ListenAndServe(*addr, nil)) //nolint:gosec
}

type conn struct {
	ws *websocket.Conn
	o  options
	mu sync.Mutex
}

func (c *conn) send(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	body = append(body, c.o.delimiter...)
	if c.o.deflate {
		var buf bytes.Buffer
		w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
		_, _ = w.Write(body)
		_ = w.Close()
		body = buf.Bytes()
	}
	if c.o.varint {
		body = varint(body)
	}
	mt := websocket.TextMessage
	if c.o.deflate || c.o.varint {
		mt = websocket.BinaryMessage
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.WriteMessage(mt, body)
}

func (c *conn) decode(frame []byte) [][]byte {
	parts := [][]byte{frame}
	if c.o.varint {
		parts = splitVarint(frame)
	}
	var out [][]byte
	for _, p := range parts {
		if c.o.deflate {
			r := flate.NewReader(bytes.NewReader(p))
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r)
			p = buf.Bytes()
		}
		if len(c.o.delimiter) == 0 {
			out = append(out, p)
			continue
		}
		for _, rec := range bytes.Split(p, c.o.delimiter) {
			if len(rec) > 0 {
				out = append(out, rec)
			}
		}
	}
	return out
}

func serve(ws *websocket.Conn, o options, token string) {
	c := &conn{ws: ws, o: o}
	subs := make(chan string, 16)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, frame, err := ws.ReadMessage()
			if err != nil {
				return
			}
			for _, rec := range c.decode(frame) {
				var m struct {
					Op      string `json:"op"`
					Type    string `json:"type"`
					Channel string `json:"channel"`
				}
				if json.Unmarshal(rec, &m) != nil {
					continue
				}
				if o.verbose {
					log.Printf("in: %s", rec)
				}
				switch m.Op {
				case "hello":
					_ = c.send(map[string]any{"type": "welcome", "token": token != ""})
				case "subscribe":
					code := 0
					if strings.HasSuffix(m.Channel, ".BAD") {
						code = 404 // lets a script exercise a refused subscription
					}
					_ = c.send(map[string]any{"type": "ack", "channel": m.Channel, "code": code})
					if code == 0 {
						subs <- m.Channel
					}
				}
			}
		}
	}()

	var drop <-chan time.Time
	if o.dropAfter > 0 {
		t := time.NewTimer(o.dropAfter)
		defer t.Stop()
		drop = t.C
	}
	quotes := time.NewTicker(o.every)
	defer quotes.Stop()
	pings := time.NewTicker(2 * time.Second)
	defer pings.Stop()

	var active []string
	seq, pingN := 0, 0
	for {
		select {
		case <-done:
			return
		case <-drop:
			log.Printf("dropping connection on purpose")
			return
		case ch := <-subs:
			active = append(active, ch)
		case <-pings.C:
			pingN++
			if c.send(map[string]any{"type": "ping", "n": pingN}) != nil {
				return
			}
		case <-quotes.C:
			for _, ch := range active {
				seq++
				if c.send(quote(ch, seq, o.levels)) != nil {
					return
				}
				if seq%7 == 0 {
					if c.send(trade(ch, seq)) != nil {
						return
					}
				}
			}
		}
	}
}

func symbolOf(ch string) string {
	if i := strings.LastIndexByte(ch, '.'); i >= 0 {
		return ch[i+1:]
	}
	return ch
}

func quote(ch string, seq, levels int) map[string]any {
	mid := 1 + rand.Float64()
	lv := make([]map[string]any, levels)
	for i := range lv {
		side := "bid"
		if i%2 == 1 {
			side = "ask"
		}
		lv[i] = map[string]any{"px": round(mid + float64(i)/10000), "qty": 1 + rand.IntN(50), "side": side}
	}
	return map[string]any{
		"type": "quote", "channel": ch, "seq": seq,
		"data": map[string]any{
			"symbol": symbolOf(ch), "bid": round(mid), "ask": round(mid + 0.0003), "levels": lv,
		},
		// at the tail on purpose: the awkward case for extraction
		"ts": time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func trade(ch string, seq int) map[string]any {
	status := "filled"
	if rand.IntN(4) == 0 {
		status = "rejected"
	}
	return map[string]any{
		"type": "trade", "channel": ch, "seq": seq,
		"data": map[string]any{
			"id": fmt.Sprintf("T-%06d", seq), "symbol": symbolOf(ch), "status": status,
			"qty": 1 + rand.IntN(10), "px": round(1 + rand.Float64()),
		},
		"ts": time.Now().UnixMilli(), // epoch millis: the other common timestamp shape
	}
}

func round(f float64) float64 { return float64(int(f*10000)) / 10000 }

func varint(payload []byte) []byte {
	var prefix []byte
	size := len(payload)
	for {
		part := byte(size & 0x7f)
		size >>= 7
		if size > 0 {
			part |= 0x80
		}
		prefix = append(prefix, part)
		if size == 0 {
			break
		}
	}
	return append(prefix, payload...)
}

func splitVarint(buf []byte) [][]byte {
	var out [][]byte
	for off := 0; off < len(buf); {
		size, n := 0, 0
		for {
			if off+n >= len(buf) {
				return out
			}
			b := buf[off+n]
			size |= int(b&0x7f) << (7 * n)
			n++
			if b&0x80 == 0 {
				break
			}
		}
		if off+n+size > len(buf) {
			return out
		}
		out = append(out, buf[off+n:off+n+size])
		off += n + size
	}
	return out
}
