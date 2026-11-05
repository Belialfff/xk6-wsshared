// Package wsshared is a generic, Go-side WebSocket client for k6, exposed to
// scripts as "k6/x/wsshared".
//
// Why it exists: with k6's own WebSocket API every message wakes the JS
// runtime, and goja has no JIT. Decompressing, splitting and scanning large
// payloads in JS for every message of every VU saturates the generator long
// before the server. Here the framing, decompression, keep-alive, field
// extraction, jsonPath, regex and message filtering all run in Go; JS only
// drives the scenario and is woken when the script actually asks for data.
//
// It knows no application protocol. Framing (compression, length prefix,
// delimiter), what to auto-answer, what to extract and what counts as an
// interesting message are all declared by the script.
package wsshared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/altenar/xk6-wsshared/binfmt"
	"github.com/grafana/sobek"
	k6modules "go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

// Version identifies the build, so a script can tell whether the binary on
// the generator is the one just built.
const Version = "0.1.0"

// Features lists what this build supports.
var Features = []string{
	"codec", "fields", "jsonPath", "regex", "match", "collect", "waitFor",
	"drain", "track", "buffers", "autoReply", "heartbeat", "onOpen",
	"replay", "reconnect", "overflow", "readGap",
}

func init() {
	k6modules.Register("k6/x/wsshared", new(RootModule))
}

// RootModule is the module bootstrap, instantiated once per process.
type RootModule struct{}

// ModuleInstance is created per VU.
type ModuleInstance struct {
	vu      k6modules.VU
	metrics *builtinMetrics
}

var (
	_ k6modules.Module   = &RootModule{}
	_ k6modules.Instance = &ModuleInstance{}
)

// NewModuleInstance implements k6modules.Module.
func (*RootModule) NewModuleInstance(vu k6modules.VU) k6modules.Instance {
	m, err := registerMetrics(vu)
	if err != nil {
		panic(vu.Runtime().NewGoError(err))
	}
	return &ModuleInstance{vu: vu, metrics: m}
}

// Exports implements k6modules.Instance.
func (mi *ModuleInstance) Exports() k6modules.Exports {
	return k6modules.Exports{
		Named: map[string]any{
			"connect": mi.Connect,
			// Pure helpers, no socket: the same Go-side machinery for HTTP
			// bodies or captured frames.
			"codec":    mi.Codec,
			"jsonPath": mi.JSONPathUtil,
			"regex":    mi.RegexUtil,
			"regexAll": mi.RegexAllUtil,
			"version":  Version,
			"features": Features,
		},
	}
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

type builtinMetrics struct {
	connDuration  *metrics.Metric
	connSuccess   *metrics.Metric
	sessionDur    *metrics.Metric
	msgDelay      *metrics.Metric
	badTimestamps *metrics.Metric
	parseDuration *metrics.Metric
	msgsReceived  *metrics.Metric
	msgsSent      *metrics.Metric
	wireBytesIn   *metrics.Metric
	decodedBytes  *metrics.Metric
	bytesOut      *metrics.Metric
	byType        *metrics.Metric
	autoReplied   *metrics.Metric
	dropped       *metrics.Metric
	decodeErrors  *metrics.Metric
	readGap       *metrics.Metric

	reconnects        *metrics.Metric
	reconnectSuccess  *metrics.Metric
	reconnectFailed   *metrics.Metric
	reconnectDuration *metrics.Metric
}

// MetricPrefix is prepended to every built-in metric. It is deliberately not
// "ws_": scripts commonly define their own ws_* metrics, and k6 refuses to
// register one name with two different types.
const MetricPrefix = "wsshared_"

func registerMetrics(vu k6modules.VU) (*builtinMetrics, error) {
	r := vu.InitEnv().Registry
	m := &builtinMetrics{}
	var err error
	reg := func(name string, typ metrics.MetricType, unit metrics.ValueType) *metrics.Metric {
		if err != nil {
			return nil
		}
		var mm *metrics.Metric
		mm, err = r.NewMetric(MetricPrefix+name, typ, unit)
		return mm
	}

	m.connDuration = reg("connect_duration_ms", metrics.Trend, metrics.Time)
	m.connSuccess = reg("connect_success_rate", metrics.Rate, metrics.Default)
	m.sessionDur = reg("session_duration_ms", metrics.Trend, metrics.Time)
	m.msgDelay = reg("message_delay_ms", metrics.Trend, metrics.Time)
	m.badTimestamps = reg("bad_timestamps_total", metrics.Counter, metrics.Default)
	m.parseDuration = reg("parse_duration_ms", metrics.Trend, metrics.Time)
	m.msgsReceived = reg("messages_received", metrics.Counter, metrics.Default)
	m.msgsSent = reg("messages_sent", metrics.Counter, metrics.Default)
	m.wireBytesIn = reg("wire_bytes_received", metrics.Counter, metrics.Data)
	m.decodedBytes = reg("decoded_bytes_received", metrics.Counter, metrics.Data)
	m.bytesOut = reg("bytes_sent", metrics.Counter, metrics.Data)
	m.byType = reg("messages_by_type", metrics.Counter, metrics.Default)
	m.autoReplied = reg("auto_replied_total", metrics.Counter, metrics.Default)
	m.dropped = reg("events_dropped_total", metrics.Counter, metrics.Default)
	m.decodeErrors = reg("decode_errors_total", metrics.Counter, metrics.Default)
	m.readGap = reg("read_gap_ms", metrics.Trend, metrics.Time)
	m.reconnects = reg("reconnects_total", metrics.Counter, metrics.Default)
	m.reconnectSuccess = reg("reconnect_success_rate", metrics.Rate, metrics.Default)
	m.reconnectFailed = reg("reconnect_exhausted_total", metrics.Counter, metrics.Default)
	m.reconnectDuration = reg("reconnect_duration_ms", metrics.Trend, metrics.Time)

	if err != nil {
		return nil, fmt.Errorf("wsshared: registering metrics: %w", err)
	}
	return m, nil
}

// jsMetricsConfig switches groups of built-in metrics off. Pointers so that an
// omitted field means "leave the default".
type jsMetricsConfig struct {
	Enabled  *bool `js:"enabled"`
	Delay    *bool `js:"delay"`
	Counters *bool `js:"counters"`
	Parse    *bool `js:"parse"`
	ByType   *bool `js:"byType"`
	Session  *bool `js:"session"`
	ReadGap  *bool `js:"readGap"`
}

type metricSwitches struct {
	delay, counters, parse, byType, session, readGap bool
}

func resolveMetricSwitches(cfg *jsMetricsConfig) metricSwitches {
	// readGap defaults to false: a diagnostic, one extra sample per flush.
	s := metricSwitches{delay: true, counters: true, parse: true, byType: true, session: true}
	if cfg == nil {
		return s
	}
	if cfg.Enabled != nil && !*cfg.Enabled {
		return metricSwitches{}
	}
	pick := func(dst *bool, v *bool) {
		if v != nil {
			*dst = *v
		}
	}
	pick(&s.delay, cfg.Delay)
	pick(&s.counters, cfg.Counters)
	pick(&s.parse, cfg.Parse)
	pick(&s.byType, cfg.ByType)
	pick(&s.session, cfg.Session)
	pick(&s.readGap, cfg.ReadGap)
	return s
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

type jsConfig struct {
	URL                string            `js:"url"`
	Headers            map[string]string `js:"headers"`
	Query              map[string]string `js:"query"`
	Subprotocols       []string          `js:"subprotocols"`
	SkipTLSVerify      bool              `js:"skipTLSVerify"`
	HandshakeTimeoutMs int               `js:"handshakeTimeoutMs"`
	EnableCompression  bool              `js:"enableCompression"`

	Codec    *jsCodecConfig `js:"codec"`
	TypePath string         `js:"typePath"`

	EventBuffer int    `js:"eventBuffer"`
	Overflow    string `js:"overflow"`
	Queue       *bool  `js:"queue"`
	KeepBody    bool   `js:"keepBody"`

	Fields    []jsField     `js:"fields"`
	Track     []jsTrack     `js:"track"`
	Buffers   []jsBuffer    `js:"buffers"`
	AutoReply []jsAutoReply `js:"autoReply"`
	Heartbeat *jsHeartbeat  `js:"heartbeat"`
	OnOpen    []sobek.Value `js:"onOpen"`
	Reconnect sobek.Value   `js:"reconnect"`

	Tags    map[string]string `js:"tags"`
	Metrics *jsMetricsConfig  `js:"metrics"`
}

type jsCodecConfig struct {
	Compression  string         `js:"compression"`
	LengthPrefix string         `js:"lengthPrefix"`
	Delimiter    string         `js:"delimiter"`
	Binary       *bool          `js:"binary"`
	Strict       bool           `js:"strict"`
	Format       string         `js:"format"`
	Sequence     bool           `js:"sequence"`
	Proto        *jsProtoConfig `js:"proto"`
}

type jsProtoConfig struct {
	Source        string            `js:"source"`
	File          string            `js:"file"`
	Imports       map[string]string `js:"imports"`
	DescriptorSet sobek.Value       `js:"descriptorSet"` // open(path, 'b')
	Message       string            `js:"message"`
	SendMessage   string            `js:"sendMessage"`
	JSONNames     bool              `js:"jsonNames"`
	EmitDefaults  bool              `js:"emitDefaults"`
}

func (j *jsCodecConfig) toConfig() (CodecConfig, error) {
	if j == nil {
		return CodecConfig{}, nil
	}
	cfg := CodecConfig{
		Compression: j.Compression, LengthPrefix: j.LengthPrefix,
		Delimiter: j.Delimiter, Binary: j.Binary, Strict: j.Strict,
		Format: j.Format, Sequence: j.Sequence,
	}
	if p := j.Proto; p != nil {
		cfg.Proto = &binfmt.ProtoConfig{
			Source: p.Source, FileName: p.File, Imports: p.Imports,
			Message: p.Message, SendMessage: p.SendMessage,
			JSONNames: p.JSONNames, EmitDefaults: p.EmitDefaults,
		}
		if !isAbsent(p.DescriptorSet) {
			b, isBytes, err := payloadBytes(p.DescriptorSet)
			if err != nil || !isBytes {
				return cfg, errors.New("wsshared: codec.proto.descriptorSet must be bytes: open(path, 'b')")
			}
			cfg.Proto.DescriptorSet = b
		}
	}
	return cfg, nil
}

type jsField struct {
	Name        string `js:"name"`
	Key         string `js:"key"`
	Path        string `js:"path"`
	Regex       string `js:"regex"`
	Group       *int   `js:"group"`
	FromEnd     bool   `js:"fromEnd"`
	AsTimestamp bool   `js:"asTimestamp"`
}

type jsTrack struct {
	Name  string      `js:"name"`
	Path  string      `js:"path"`
	Paths []string    `js:"paths"`
	Regex string      `js:"regex"`
	Group *int        `js:"group"`
	When  sobek.Value `js:"when"`
	KeyBy string      `js:"keyBy"`
}

type jsBuffer struct {
	Name     string      `js:"name"`
	Size     int         `js:"size"`
	When     sobek.Value `js:"when"`
	KeepBody bool        `js:"keepBody"`
}

type jsAutoReply struct {
	Match   sobek.Value `js:"match"`
	Reply   sobek.Value `js:"reply"`
	Raw     bool        `js:"raw"`
	Binary  *bool       `js:"binary"`
	Swallow *bool       `js:"swallow"`
}

type jsHeartbeat struct {
	EveryMs int         `js:"everyMs"`
	Send    sobek.Value `js:"send"`
	Raw     bool        `js:"raw"`
	Binary  *bool       `js:"binary"`
	Control bool        `js:"control"`
}

// jsSendWrap is the explicit form of an onOpen entry / send options.
type jsSendWrap struct {
	Send   sobek.Value `js:"send"`
	Raw    bool        `js:"raw"`
	Binary *bool       `js:"binary"`
	Replay bool        `js:"replay"`
}

type jsMatch struct {
	Contains string        `js:"contains"`
	Regex    string        `js:"regex"`
	Path     string        `js:"path"`
	Field    string        `js:"field"`
	Type     sobek.Value   `js:"type"`
	Equals   sobek.Value   `js:"equals"`
	OneOf    []any         `js:"oneOf"`
	Exists   *bool         `js:"exists"`
	Not      bool          `js:"not"`
	Any      []sobek.Value `js:"any"`
}

type jsReconnect struct {
	Enabled     *bool `js:"enabled"`
	MaxAttempts int   `js:"maxAttempts"`
	MinDelayMs  int   `js:"minDelayMs"`
	MaxDelayMs  int   `js:"maxDelayMs"`
}

func isAbsent(v sobek.Value) bool {
	return v == nil || sobek.IsUndefined(v) || sobek.IsNull(v)
}

// parseMatch turns a JS matcher into a compiled one. nil/undefined -> nil
// (match everything). A bare string is shorthand for { contains: '...' }.
func parseMatch(rt *sobek.Runtime, v sobek.Value) (*matcher, error) {
	if isAbsent(v) {
		return nil, nil
	}
	if s, ok := v.Export().(string); ok {
		return compileMatch(MatchSpec{Contains: s})
	}
	var j jsMatch
	if err := rt.ExportTo(v, &j); err != nil {
		return nil, fmt.Errorf("wsshared: bad match: %w", err)
	}
	spec := MatchSpec{
		Contains: j.Contains, Regex: j.Regex, Path: j.Path, Field: j.Field,
		OneOf: j.OneOf, Exists: j.Exists, Not: j.Not,
	}
	if j.Equals != nil && !sobek.IsUndefined(j.Equals) {
		spec.Equals, spec.HasEquals = j.Equals.Export(), true
	}
	if !isAbsent(j.Type) {
		switch t := j.Type.Export().(type) {
		case string:
			spec.Types = []string{t}
		case []any:
			for _, x := range t {
				spec.Types = append(spec.Types, fmt.Sprint(x))
			}
		default:
			spec.Types = []string{fmt.Sprint(t)}
		}
	}
	for _, a := range j.Any {
		child, err := parseMatch(rt, a)
		if err != nil {
			return nil, err
		}
		if child != nil {
			spec.Any = append(spec.Any, child)
		}
	}
	return compileMatch(spec)
}

// payloadBytes converts what a script hands over into bytes: a string as is,
// an ArrayBuffer / Uint8Array as bytes, anything else as JSON. isBytes reports
// binary input, which defaults a raw send to a binary frame.
func payloadBytes(v sobek.Value) (data []byte, isBytes bool, err error) {
	if isAbsent(v) {
		return nil, false, errors.New("wsshared: nothing to send")
	}
	switch x := v.Export().(type) {
	case string:
		return []byte(x), false, nil
	case []byte:
		return append([]byte(nil), x...), true, nil
	case sobek.ArrayBuffer:
		return append([]byte(nil), x.Bytes()...), true, nil
	case *sobek.ArrayBuffer:
		return append([]byte(nil), x.Bytes()...), true, nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return nil, false, fmt.Errorf("wsshared: cannot encode payload as JSON: %w", err)
		}
		return b, false, nil
	}
}

// makeItem prepares a message. The frame type defaults to the codec's for an
// encoded send, and to "binary if the input was bytes" for a raw one.
func makeItem(codec *Codec, v sobek.Value, raw bool, binary *bool) (sendItem, error) {
	data, isBytes, err := payloadBytes(v)
	if err != nil {
		return sendItem{}, err
	}
	it := sendItem{data: data, raw: raw}
	if raw {
		it.binary = isBytes
	} else {
		it.binary = codec.Binary()
		if it.data, err = codec.Format(data); err != nil {
			return sendItem{}, err
		}
	}
	if binary != nil {
		it.binary = *binary
	}
	return it, nil
}

// parseSendEntry accepts a payload (string, bytes, object) or the explicit
// wrapper { send, raw?, binary? }. An object is taken as the wrapper only when
// it has a `send` key — a payload that itself has a top-level "send" field has
// to be wrapped: { send: { send: ... } }.
func parseSendEntry(rt *sobek.Runtime, codec *Codec, v sobek.Value) (sendItem, error) {
	if isAbsent(v) {
		return sendItem{}, errors.New("wsshared: empty onOpen entry")
	}
	if obj, ok := v.(*sobek.Object); ok && obj.Get("send") != nil {
		var w jsSendWrap
		if err := rt.ExportTo(v, &w); err != nil || isAbsent(w.Send) {
			return sendItem{}, errors.New("wsshared: onOpen entry: bad { send, raw?, binary? }")
		}
		return makeItem(codec, w.Send, w.Raw, w.Binary)
	}
	return makeItem(codec, v, false, nil)
}

func parseReconnect(rt *sobek.Runtime, v sobek.Value) (ReconnectConfig, error) {
	var out ReconnectConfig
	if isAbsent(v) {
		return out, nil
	}
	if b, ok := v.Export().(bool); ok {
		out.Enabled = b
		return out, nil
	}
	var j jsReconnect
	if err := rt.ExportTo(v, &j); err != nil {
		return out, fmt.Errorf("wsshared: bad reconnect config: %w", err)
	}
	out.Enabled = j.Enabled == nil || *j.Enabled
	out.MaxAttempts = j.MaxAttempts
	out.MinDelay = time.Duration(j.MinDelayMs) * time.Millisecond
	out.MaxDelay = time.Duration(j.MaxDelayMs) * time.Millisecond
	return out, nil
}

// buildConnect validates the JS config and produces everything Dial needs.
func buildConnect(rt *sobek.Runtime, cfg *jsConfig) (Config, *observer, ReconnectConfig, error) {
	var zero ReconnectConfig
	if cfg.URL == "" {
		return Config{}, nil, zero, errors.New("wsshared: url is required")
	}
	codecCfg, err := cfg.Codec.toConfig()
	if err != nil {
		return Config{}, nil, zero, err
	}
	codec, err := NewCodec(codecCfg)
	if err != nil {
		return Config{}, nil, zero, err
	}
	queue := cfg.Queue == nil || *cfg.Queue
	overflow, err := parseOverflow(cfg.Overflow, queue)
	if err != nil {
		return Config{}, nil, zero, err
	}

	obs := &observer{keepBody: cfg.KeepBody, switches: resolveMetricSwitches(cfg.Metrics)}

	if cfg.TypePath != "" {
		if obs.typePath, err = compilePath(cfg.TypePath); err != nil {
			return Config{}, nil, zero, err
		}
	}

	for _, f := range cfg.Fields {
		fs := FieldSpec{
			Name: f.Name, Key: f.Key, Path: f.Path, Regex: f.Regex,
			FromEnd: f.FromEnd, AsTimestamp: f.AsTimestamp,
		}
		if f.Group != nil {
			fs.Group, fs.GroupSet = *f.Group, true
		}
		if err := fs.compile(); err != nil {
			return Config{}, nil, zero, err
		}
		obs.fields = append(obs.fields, fs)
	}

	tracks := make([]TrackSpec, 0, len(cfg.Track))
	for _, t := range cfg.Track {
		ts := TrackSpec{Name: t.Name, Paths: t.Paths, Regex: t.Regex, KeyBy: t.KeyBy}
		if t.Path != "" {
			ts.Paths = append([]string{t.Path}, ts.Paths...)
		}
		if t.Group != nil {
			ts.Group, ts.GroupSet = *t.Group, true
		}
		if ts.When, err = parseMatch(rt, t.When); err != nil {
			return Config{}, nil, zero, err
		}
		tracks = append(tracks, ts)
	}
	if obs.tracker, err = newTracker(tracks); err != nil {
		return Config{}, nil, zero, err
	}

	bufs := make([]BufferSpec, 0, len(cfg.Buffers))
	for _, b := range cfg.Buffers {
		bs := BufferSpec{Name: b.Name, Size: b.Size, KeepBody: b.KeepBody}
		if bs.When, err = parseMatch(rt, b.When); err != nil {
			return Config{}, nil, zero, err
		}
		bufs = append(bufs, bs)
	}
	if obs.buffers, err = newBufferSet(bufs); err != nil {
		return Config{}, nil, zero, err
	}

	for _, r := range cfg.AutoReply {
		m, err := parseMatch(rt, r.Match)
		if err != nil {
			return Config{}, nil, zero, err
		}
		if m == nil {
			return Config{}, nil, zero, errors.New("wsshared: autoReply entry needs a match")
		}
		ar := autoReply{when: m, swallow: r.Swallow == nil || *r.Swallow}
		if !isAbsent(r.Reply) {
			it, err := makeItem(codec, r.Reply, r.Raw, r.Binary)
			if err != nil {
				return Config{}, nil, zero, err
			}
			ar.reply = &it
		}
		obs.replies = append(obs.replies, ar)
	}

	dial := Config{
		URL:               cfg.URL,
		Headers:           cfg.Headers,
		Subprotocols:      cfg.Subprotocols,
		HandshakeTimeout:  time.Duration(cfg.HandshakeTimeoutMs) * time.Millisecond,
		SkipTLSVerify:     cfg.SkipTLSVerify,
		EnableCompression: cfg.EnableCompression,
		Codec:             codec,
		EventBuffer:       cfg.EventBuffer,
		Overflow:          overflow,
	}
	if len(cfg.Query) > 0 {
		dial.URL = appendQuery(dial.URL, cfg.Query)
	}

	for _, v := range cfg.OnOpen {
		it, err := parseSendEntry(rt, codec, v)
		if err != nil {
			return Config{}, nil, zero, err
		}
		dial.OnOpen = append(dial.OnOpen, it)
	}

	if hb := cfg.Heartbeat; hb != nil && hb.EveryMs > 0 {
		dial.Heartbeat = HeartbeatConfig{
			Every:   time.Duration(hb.EveryMs) * time.Millisecond,
			Control: hb.Control,
		}
		if !isAbsent(hb.Send) {
			it, err := makeItem(codec, hb.Send, hb.Raw, hb.Binary)
			if err != nil {
				return Config{}, nil, zero, err
			}
			dial.Heartbeat.Item = &it
		}
		if dial.Heartbeat.Item == nil && !dial.Heartbeat.Control {
			return Config{}, nil, zero, errors.New("wsshared: heartbeat needs send or control: true")
		}
	}

	rec, err := parseReconnect(rt, cfg.Reconnect)
	if err != nil {
		return Config{}, nil, zero, err
	}
	return dial, obs, rec, nil
}

// Connect opens a connection and returns its JS handle.
func (mi *ModuleInstance) Connect(cfgVal sobek.Value) (*sobek.Object, error) {
	rt := mi.vu.Runtime()
	if mi.vu.State() == nil {
		return nil, errors.New("wsshared: connect() is only available in VU code, not in the init context")
	}

	var cfg jsConfig
	if err := rt.ExportTo(cfgVal, &cfg); err != nil {
		return nil, fmt.Errorf("wsshared: bad config: %w", err)
	}
	dialCfg, obs, rec, err := buildConnect(rt, &cfg)
	if err != nil {
		return nil, err
	}

	h := &jsConn{
		mi:        mi,
		obs:       obs,
		codec:     dialCfg.Codec,
		tags:      cfg.Tags,
		switches:  obs.switches,
		dialCfg:   dialCfg,
		reconnect: rec,
		queueOn:   dialCfg.Overflow != overflowNoQueue,
	}

	start := time.Now()
	conn, err := Dial(mi.ctx(), dialCfg, obs)
	h.startedAt = start
	if err != nil {
		h.pushOne(mi.metrics.connSuccess, 0)
		return nil, err
	}
	h.conn = conn
	h.pushOne(mi.metrics.connSuccess, 1)
	h.pushOne(mi.metrics.connDuration, float64(time.Since(start).Microseconds())/1000.0)
	return h.toObject(rt)
}

func appendQuery(base string, q map[string]string) string {
	sep := "?"
	for i := 0; i < len(base); i++ {
		if base[i] == '?' {
			sep = "&"
			break
		}
	}
	out := base
	for k, v := range q {
		out += sep + urlEncode(k) + "=" + urlEncode(v)
		sep = "&"
	}
	return out
}

// urlEncode percent-encodes a query component. net/url.QueryEscape turns
// spaces into '+', which some gateways mishandle in tokens.
func urlEncode(s string) string {
	const hex = "0123456789ABCDEF"
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			buf = append(buf, c)
			continue
		}
		buf = append(buf, '%', hex[c>>4], hex[c&0x0f])
	}
	return string(buf)
}

func (mi *ModuleInstance) ctx() context.Context {
	if c := mi.vu.Context(); c != nil {
		return c
	}
	return context.Background()
}
