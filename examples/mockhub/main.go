// Command mockhub is a multi-hub WebSocket server with an HTTP publish API —
// the "listen on sockets, trigger over HTTP, count what arrived" pattern.
//
// Hubs (one WebSocket endpoint each):
//
//	ws://127.0.0.1:8091/hub/{name}        name from -hubs (default orders,prices,alerts)
//
//	server -> {"type":"welcome","hub":"orders","conn":"orders-c7","ts":"..."}   right after open
//	client -> {"op":"subscribe","topics":["user-3","all"]}
//	server -> {"type":"ack","hub":"orders","conn":"orders-c7","topics":[...],"code":0}
//	          (a topic ending in ".BAD" is refused with code 404)
//	server -> {"type":"ping","n":3}                                               every -ping
//	client -> {"type":"pong","n":3}
//
// HTTP:
//
//	POST /api/{hub}/publish {"topic":"user-3","event":"orderCreated","data":{...}?}
//	  -> 200 {"msgId":"orders-m42","hub":"orders","topic":"user-3","delivered":2,"dropped":0}
//	  every socket of that hub subscribed to the topic gets
//	     {"type":"event","hub":"orders","topic":"user-3","event":"orderCreated",
//	      "msgId":"orders-m42","seq":42,"data":{...},"ts":"..."}
//	  without "data" the server makes a hub-specific payload (order / quote / alert).
//
//	GET /api/stats[?hub=orders]
//	  -> {"hubs":{"orders":{"connections":6,"opened":6,"closed":0,"subscribed":6,
//	      "topics":{"all":6,"user-1":2},"published":4,"delivered":14,"dropped":0}}}
//
// Framing matches examples/mockws: -compression=deflate -varint -delimiter=rs.
// Binary formats: -format=msgpack | -format=protobuf (schema: hub.proto,
// message mockhub.Envelope); field names are the same as in JSON.
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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "embed"

	"github.com/altenar/xk6-wsshared/binfmt"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

type options struct {
	ping      time.Duration
	deflate   bool
	varint    bool
	delimiter []byte
	format    binfmt.Format // nil = JSON
	verbose   bool
}

//go:embed hub.proto
var hubProto string

func main() {
	addr := flag.String("addr", "127.0.0.1:8091", "listen address")
	hubList := flag.String("hubs", "orders,prices,alerts", "comma-separated hub names")
	var o options
	flag.DurationVar(&o.ping, "ping", 2*time.Second, "app-level ping interval")
	compression := flag.String("compression", "none", "none | deflate")
	flag.BoolVar(&o.varint, "varint", false, "VarInt length prefix on every frame")
	delim := flag.String("delimiter", "", "message delimiter: '' | nl | rs (0x1e)")
	format := flag.String("format", "json", "json | msgpack | protobuf (schema: hub.proto, mockhub.Envelope)")
	flag.BoolVar(&o.verbose, "v", false, "log every connection open/subscribe/close")
	flag.Parse()

	o.deflate = *compression == "deflate"
	switch *delim {
	case "nl":
		o.delimiter = []byte("\n")
	case "rs":
		o.delimiter = []byte{0x1e}
	}
	switch *format {
	case "json":
	case "msgpack":
		o.format = binfmt.MsgPack{}
	case "protobuf":
		p, err := binfmt.NewProtobuf(binfmt.ProtoConfig{Source: hubProto, FileName: "hub.proto", Message: "mockhub.Envelope"})
		if err != nil {
			log.Fatal(err)
		}
		o.format = p
	default:
		log.Fatalf("unknown -format %q", *format)
	}
	if o.format != nil && len(o.delimiter) > 0 {
		log.Fatal("-format with -delimiter: binary records may contain the delimiter; use -varint")
	}

	srv := &server{o: o, hubs: map[string]*hub{}}
	for _, name := range strings.Split(*hubList, ",") {
		if name = strings.TrimSpace(name); name != "" {
			srv.hubs[name] = &hub{name: name, conns: map[*client]struct{}{}}
			srv.order = append(srv.order, name)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hub/{name}", srv.handleWS)
	mux.HandleFunc("POST /api/{hub}/publish", srv.handlePublish)
	mux.HandleFunc("GET /api/stats", srv.handleStats)

	log.Printf("mockhub on %s  hubs=%v  ws://%s/hub/{name}  POST /api/{hub}/publish  GET /api/stats  (format=%s compression=%s varint=%v delimiter=%q)",
		*addr, srv.order, *addr, *format, *compression, o.varint, *delim)
	go srv.reportLoop()
	log.Fatal(http.ListenAndServe(*addr, mux)) //nolint:gosec
}

type server struct {
	o     options
	hubs  map[string]*hub
	order []string
}

type hub struct {
	name      string
	mu        sync.Mutex
	conns     map[*client]struct{}
	connSeq   atomic.Int64
	msgSeq    atomic.Int64
	opened    atomic.Int64
	closed    atomic.Int64
	published atomic.Int64
	delivered atomic.Int64
	dropped   atomic.Int64
}

type client struct {
	id     string
	hub    *hub
	ws     *websocket.Conn
	o      options
	out    chan []byte // encoded frames; a slow reader drops instead of stalling publishers
	mu     sync.Mutex  // guards topics
	topics map[string]bool
}

func (c *client) subscribed(topic string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.topics[topic]
}

// enqueue hands an encoded frame to the writer goroutine; false when the
// client's buffer is full.
func (c *client) enqueue(frame []byte) bool {
	select {
	case c.out <- frame:
		return true
	default:
		return false
	}
}

// send encodes v and enqueues it; for one-off messages to one client.
func (c *client) send(v any) bool {
	f, err := encode(v, c.o)
	if err != nil {
		log.Printf("[%s] encode: %v", c.id, err)
		return false
	}
	return c.enqueue(f)
}

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	h := s.hubs[r.PathValue("name")]
	if h == nil {
		http.Error(w, "unknown hub", http.StatusNotFound)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{
		id:  fmt.Sprintf("%s-c%d", h.name, h.connSeq.Add(1)),
		hub: h, ws: ws, o: s.o,
		out:    make(chan []byte, 1024),
		topics: map[string]bool{},
	}
	h.mu.Lock()
	h.conns[c] = struct{}{}
	h.mu.Unlock()
	h.opened.Add(1)
	if s.o.verbose {
		log.Printf("[%s] open   %s  user=%q", h.name, c.id, r.URL.Query().Get("user"))
	}

	done := make(chan struct{})
	go func() { // single writer
		mt := websocket.TextMessage
		if s.o.deflate || s.o.varint || s.o.format != nil {
			mt = websocket.BinaryMessage
		}
		ping := time.NewTicker(s.o.ping)
		defer ping.Stop()
		n := 0
		for {
			select {
			case <-done:
				return
			case f := <-c.out:
				if ws.WriteMessage(mt, f) != nil {
					return
				}
			case <-ping.C:
				n++
				f, _ := encode(map[string]any{"type": "ping", "n": n}, s.o)
				if ws.WriteMessage(mt, f) != nil {
					return
				}
			}
		}
	}()

	c.send(map[string]any{"type": "welcome", "hub": h.name, "conn": c.id,
		"ts": time.Now().UTC().Format(time.RFC3339Nano)})

	defer func() {
		close(done)
		h.mu.Lock()
		delete(h.conns, c)
		h.mu.Unlock()
		h.closed.Add(1)
		_ = ws.Close()
		if s.o.verbose {
			log.Printf("[%s] close  %s", h.name, c.id)
		}
	}()

	for { // single reader
		mt, frame, err := ws.ReadMessage()
		if err != nil {
			return
		}
		for _, rec := range decode(frame, mt == websocket.BinaryMessage, s.o) {
			var m struct {
				Op     string   `json:"op"`
				Type   string   `json:"type"`
				Topics []string `json:"topics"`
			}
			if json.Unmarshal(rec, &m) != nil || m.Op != "subscribe" {
				continue
			}
			code := 0
			for _, t := range m.Topics {
				if strings.HasSuffix(t, ".BAD") {
					code = 404
				}
			}
			if code == 0 {
				c.mu.Lock()
				for _, t := range m.Topics {
					c.topics[t] = true
				}
				c.mu.Unlock()
			}
			c.send(map[string]any{"type": "ack", "hub": h.name, "conn": c.id, "topics": m.Topics, "code": code})
			if s.o.verbose {
				log.Printf("[%s] sub    %s  topics=%v code=%d", h.name, c.id, m.Topics, code)
			}
		}
	}
}

func (s *server) handlePublish(w http.ResponseWriter, r *http.Request) {
	h := s.hubs[r.PathValue("hub")]
	if h == nil {
		http.Error(w, `{"error":"unknown hub"}`, http.StatusNotFound)
		return
	}
	var req struct {
		Topic string          `json:"topic"`
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" {
		http.Error(w, `{"error":"body must be {\"topic\":...}"}`, http.StatusBadRequest)
		return
	}
	seq := h.msgSeq.Add(1)
	msgID := fmt.Sprintf("%s-m%d", h.name, seq)
	if req.Event == "" {
		req.Event = "update"
	}
	var data any = req.Data
	if len(req.Data) == 0 {
		data = payload(h.name, seq, req.Topic)
	}
	msg := map[string]any{
		"type": "event", "hub": h.name, "topic": req.Topic, "event": req.Event,
		"msgId": msgID, "seq": seq, "data": data,
		// at the tail on purpose: the awkward case for extraction
		"ts": time.Now().UTC().Format(time.RFC3339Nano),
	}
	// encode once, fan out the same bytes
	frame, err := encode(msg, s.o)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	h.mu.Lock()
	targets := make([]*client, 0, len(h.conns))
	for c := range h.conns {
		if c.subscribed(req.Topic) {
			targets = append(targets, c)
		}
	}
	h.mu.Unlock()

	delivered, dropped := 0, 0
	for _, c := range targets {
		if c.enqueue(frame) {
			delivered++
		} else {
			dropped++
		}
	}
	h.published.Add(1)
	h.delivered.Add(int64(delivered))
	h.dropped.Add(int64(dropped))
	log.Printf("[%s] publish %-12s topic=%-8s event=%-13s -> delivered=%d dropped=%d",
		h.name, msgID, req.Topic, req.Event, delivered, dropped)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"msgId": msgID, "hub": h.name, "topic": req.Topic,
		"delivered": delivered, "dropped": dropped,
	})
}

func (s *server) hubStats(h *hub) map[string]any {
	h.mu.Lock()
	topics := map[string]int{}
	subscribed := 0
	for c := range h.conns {
		c.mu.Lock()
		if len(c.topics) > 0 {
			subscribed++
		}
		for t := range c.topics {
			topics[t]++
		}
		c.mu.Unlock()
	}
	conns := len(h.conns)
	h.mu.Unlock()
	return map[string]any{
		"connections": conns, "subscribed": subscribed, "topics": topics,
		"opened": h.opened.Load(), "closed": h.closed.Load(),
		"published": h.published.Load(), "delivered": h.delivered.Load(), "dropped": h.dropped.Load(),
	}
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	for _, name := range s.order {
		if q := r.URL.Query().Get("hub"); q != "" && q != name {
			continue
		}
		out[name] = s.hubStats(s.hubs[name])
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"hubs": out})
}

// reportLoop prints one line per hub whenever the picture changes.
func (s *server) reportLoop() {
	last := ""
	for range time.Tick(time.Second) {
		var parts []string
		for _, name := range s.order {
			h := s.hubs[name]
			h.mu.Lock()
			n := len(h.conns)
			h.mu.Unlock()
			parts = append(parts, fmt.Sprintf("%s: %d conn, %d pub, %d delivered", name, n, h.published.Load(), h.delivered.Load()))
		}
		sort.Strings(parts)
		line := strings.Join(parts, " | ")
		if line != last {
			log.Printf("[stats] %s", line)
			last = line
		}
	}
}

// payload makes a hub-specific body so each hub has its own JSON shape.
func payload(hubName string, seq int64, topic string) any {
	user := strings.TrimPrefix(topic, "user-")
	switch hubName {
	case "orders":
		n := 1 + rand.IntN(4)
		items := make([]map[string]any, n)
		total := 0.0
		for i := range items {
			qty, price := 1+rand.IntN(5), round2(5+rand.Float64()*95)
			items[i] = map[string]any{"sku": fmt.Sprintf("SKU-%04d", rand.IntN(10000)), "qty": qty, "price": price}
			total += float64(qty) * price
		}
		tiers := []string{"basic", "silver", "gold"}
		return map[string]any{
			"orderId":  fmt.Sprintf("O-%06d", seq),
			"customer": map[string]any{"id": "u-" + user, "tier": tiers[rand.IntN(len(tiers))]},
			"items":    items, "total": round2(total), "currency": "EUR",
			"status": []string{"created", "paid", "shipped"}[rand.IntN(3)],
		}
	case "prices":
		mid := 1 + rand.Float64()
		lv := make([]map[string]any, 5)
		for i := range lv {
			lv[i] = map[string]any{"px": round4(mid + float64(i)/1000), "qty": 1 + rand.IntN(50)}
		}
		syms := []string{"EURUSD", "GBPUSD", "USDJPY"}
		return map[string]any{"symbol": syms[rand.IntN(len(syms))], "bid": round4(mid), "ask": round4(mid + 0.0003), "levels": lv}
	default:
		levels := []string{"info", "warn", "critical"}
		return map[string]any{
			"level": levels[rand.IntN(len(levels))],
			"text":  fmt.Sprintf("disk usage %d%% on node-%d", 60+rand.IntN(40), 1+rand.IntN(9)),
			"tags":  []string{"infra", "node-" + fmt.Sprint(1+rand.IntN(9))},
		}
	}
}

func round2(f float64) float64 { return float64(int(f*100)) / 100 }
func round4(f float64) float64 { return float64(int(f*10000)) / 10000 }

// encode: JSON -> format -> delimiter -> deflate -> varint (the extension's
// codec, mirrored).
func encode(v any, o options) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if o.format != nil {
		if body, err = o.format.FromJSON(body); err != nil {
			return nil, err
		}
	}
	body = append(body, o.delimiter...)
	if o.deflate {
		var buf bytes.Buffer
		w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
		_, _ = w.Write(body)
		_ = w.Close()
		body = buf.Bytes()
	}
	if o.varint {
		body = varint(body)
	}
	return body, nil
}

// decode returns JSON records. A text frame is taken as plain JSON.
func decode(frame []byte, binaryFrame bool, o options) [][]byte {
	if !binaryFrame && o.format != nil {
		return [][]byte{frame}
	}
	parts := [][]byte{frame}
	if o.varint {
		parts = splitVarint(frame)
	}
	var out [][]byte
	for _, p := range parts {
		if o.deflate {
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(flate.NewReader(bytes.NewReader(p)))
			p = buf.Bytes()
		}
		if o.format != nil {
			js, err := o.format.ToJSON(nil, p)
			if err != nil {
				log.Printf("decode %s: %v", o.format.Name(), err)
				continue
			}
			out = append(out, js...)
			continue
		}
		if len(o.delimiter) == 0 {
			out = append(out, p)
			continue
		}
		for _, rec := range bytes.Split(p, o.delimiter) {
			if len(rec) > 0 {
				out = append(out, rec)
			}
		}
	}
	return out
}

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
