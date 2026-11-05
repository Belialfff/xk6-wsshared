package wsshared

import (
	"errors"
	"fmt"
	"sync"
)

// Named ring buffers: "keep the last N messages of this kind, I will look when
// I am ready".
//
// collect() is the blocking way to get N messages. Buffers are the
// non-blocking one: the read loop files every matching message into a ring of
// fixed size, and the VU reads it whenever it gets round to it — between HTTP
// calls, at the end of the iteration, after a check failed. The ring is
// bounded, so a VU that never looks costs a fixed amount of memory, not a
// growing queue. Buffers do not consume the event queue: a message can be in a
// buffer AND be returned by next().

const maxBufferSize = 100_000

// BufferSpec declares one ring buffer.
type BufferSpec struct {
	Name string
	Size int
	// When restricts the buffer to matching messages; nil keeps everything.
	When *matcher
	// KeepBody keeps the body on buffered events (body()/json()/jsonPath()),
	// even when the connection itself does not.
	KeepBody bool
}

type ring struct {
	items []Event
	start int
	count int
	// total counts every message ever filed, so total - count is how many
	// were overwritten.
	total int64
}

func (r *ring) push(ev Event) {
	size := len(r.items)
	if r.count < size {
		r.items[(r.start+r.count)%size] = ev
		r.count++
	} else {
		r.items[r.start] = ev
		r.start = (r.start + 1) % size
	}
	r.total++
}

func (r *ring) snapshot() []Event {
	out := make([]Event, r.count)
	for i := 0; i < r.count; i++ {
		out[i] = r.items[(r.start+i)%len(r.items)]
	}
	return out
}

func (r *ring) reset() {
	for i := range r.items {
		r.items[i] = Event{} // release bodies
	}
	r.start, r.count = 0, 0
}

type bufferSet struct {
	specs []BufferSpec
	mu    sync.Mutex
	rings map[string]*ring
}

func newBufferSet(specs []BufferSpec) (*bufferSet, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	b := &bufferSet{specs: specs, rings: make(map[string]*ring, len(specs))}
	for _, s := range specs {
		if s.Name == "" {
			return nil, errors.New("wsshared: buffer needs a name")
		}
		if s.Size <= 0 || s.Size > maxBufferSize {
			return nil, fmt.Errorf("wsshared: buffer %q: size must be 1..%d", s.Name, maxBufferSize)
		}
		if _, dup := b.rings[s.Name]; dup {
			return nil, fmt.Errorf("wsshared: buffer %q declared twice", s.Name)
		}
		b.rings[s.Name] = &ring{items: make([]Event, s.Size)}
	}
	return b, nil
}

// observe files one event into every buffer that wants it. raw is the body,
// available here even when the event does not carry it.
func (b *bufferSet) observe(ev *Event, raw []byte, connKeepsBody bool) {
	if b == nil {
		return
	}
	for i := range b.specs {
		s := &b.specs[i]
		if s.When != nil && !s.When.match(raw, ev.Type, ev.Fields) {
			continue
		}
		e := *ev
		if s.KeepBody || connKeepsBody {
			e.Body = raw
		} else {
			e.Body = nil
		}
		b.mu.Lock()
		b.rings[s.Name].push(e)
		b.mu.Unlock()
	}
}

func (b *bufferSet) get(name string, take bool) ([]Event, bool) {
	if b == nil {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.rings[name]
	if !ok {
		return nil, false
	}
	out := r.snapshot()
	if take {
		r.reset()
	}
	return out, true
}

func (b *bufferSet) info(name string) (count, size int, total int64, ok bool) {
	if b == nil {
		return 0, 0, 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, found := b.rings[name]
	if !found {
		return 0, 0, 0, false
	}
	return r.count, len(r.items), r.total, true
}
