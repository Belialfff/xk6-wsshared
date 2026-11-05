package wsshared

import (
	"context"
	"math/rand/v2"
	"time"
)

// ReconnectConfig controls automatic reconnection after an unexpected drop.
// A deliberate close() never reconnects.
type ReconnectConfig struct {
	Enabled bool
	// MaxAttempts caps consecutive attempts; 0 = default (5), negative = no cap.
	MaxAttempts int
	// MinDelay / MaxDelay bound the jittered exponential backoff. Jitter
	// matters at scale: without it, thousands of VUs dropped by one server
	// restart reconnect in the same instant.
	MinDelay time.Duration
	MaxDelay time.Duration
}

func (r ReconnectConfig) withDefaults() ReconnectConfig {
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 5
	}
	if r.MinDelay <= 0 {
		r.MinDelay = 500 * time.Millisecond
	}
	if r.MaxDelay < r.MinDelay {
		r.MaxDelay = 5 * time.Second
		if r.MaxDelay < r.MinDelay {
			r.MaxDelay = r.MinDelay
		}
	}
	return r
}

// delayFor returns the backoff for attempt n (1-based): full jitter over
// [MinDelay, MinDelay*2^(n-1)] capped at MaxDelay.
func (r ReconnectConfig) delayFor(attempt int) time.Duration {
	d := r.MinDelay
	for i := 1; i < attempt && d < r.MaxDelay; i++ {
		d *= 2
	}
	if d > r.MaxDelay {
		d = r.MaxDelay
	}
	span := d - r.MinDelay
	if span <= 0 {
		return r.MinDelay
	}
	return r.MinDelay + time.Duration(rand.Int64N(int64(span)+1))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// dropped reports whether the connection went away on its own (as opposed to
// being closed by the script).
func (h *jsConn) dropped() bool {
	if h.closed {
		return false
	}
	select {
	case <-h.conn.done:
		return true
	default:
		return false
	}
}

// tryReconnect re-dials a dropped connection: onOpen is re-sent by Dial, then
// the replay list. Tracked values, buffers and pending samples live in the
// observer and carry over; counters are folded so totals stay continuous.
func (h *jsConn) tryReconnect() bool {
	if h.closed || !h.reconnect.Enabled {
		return false
	}
	if err := h.conn.Err(); err != nil {
		h.lastDropErr = err.Error()
	}
	h.flush(true)
	h.carried = h.carried.add(h.conn.Stats())
	h.lastFlushed = Stats{} // the new connection counts from zero

	cfg := h.reconnect.withDefaults()
	ctx := h.mi.ctx()
	mm := h.mi.metrics

	for attempt := 1; cfg.MaxAttempts < 0 || attempt <= cfg.MaxAttempts; attempt++ {
		if !sleepCtx(ctx, cfg.delayFor(attempt)) {
			return false // VU is shutting down
		}
		start := time.Now()
		conn, err := Dial(ctx, h.dialCfg, h.obs)
		if err != nil {
			if h.switches.counters {
				h.pushOne(mm.reconnectSuccess, 0)
			}
			continue
		}
		h.conn = conn
		h.reconnects++
		if h.switches.counters {
			h.pushOne(mm.reconnectSuccess, 1)
			h.pushOne(mm.reconnects, 1)
		}
		if h.switches.session {
			h.pushOne(mm.reconnectDuration, float64(time.Since(start).Microseconds())/1000.0)
		}
		for _, it := range h.replayList {
			if err := h.conn.sendItem(it); err != nil {
				break
			}
		}
		return true
	}
	if h.switches.counters {
		h.pushOne(mm.reconnectFailed, 1)
	}
	return false
}

// totalStats returns counters across every connection this handle has had.
func (h *jsConn) totalStats() Stats {
	return h.carried.add(h.conn.Stats())
}
