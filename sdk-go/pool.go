package keystone

import (
	"context"
	"errors"
	"sync"
)

// ErrPoolClosed is returned by Acquire after Pool.Close.
var ErrPoolClosed = errors.New("keystone: pool is closed")

// PoolConfig configures a Pool.
type PoolConfig struct {
	// Addr is the "host:port" of the Keystone node.
	Addr string
	// Params are startup parameters forwarded to Connect (e.g. "user").
	Params map[string]string
	// MinIdle connections are opened eagerly in NewPool and are never
	// closed by the pool itself (see "What this pool does NOT do" below).
	MinIdle int
	// MaxOpen caps the total number of connections (idle + checked out)
	// the pool will ever have open at once. Acquire blocks (respecting
	// ctx) once this many are open and all are checked out. Must be >= 1;
	// values <= 0 are treated as 1.
	MaxOpen int
}

// Pool hands out pooled *Conn connections to Keystone.
//
// # What this pool does
//   - Bounds total open connections to MaxOpen.
//   - Reuses idle connections across Acquire/Release calls.
//   - Health-checks a connection on checkout: if a Conn's Broken() flag was
//     set (by a prior context cancellation or I/O error — see Conn's doc
//     comment), it is discarded and a fresh one opened in its place instead
//     of being handed to the caller. This is a cheap flag check, not a live
//     round-trip ping (e.g. it will not detect a connection the server
//     silently dropped without the client noticing via a failed read/write).
//   - Pre-warms MinIdle connections at construction time.
//
// # What this pool does NOT do (explicit scope cuts)
//   - No idle connection reaping / max-lifetime eviction — a connection
//     that goes idle stays in the pool until used again or the pool is
//     closed. For a long-running server process against a database that
//     recycles connections server-side, add a reaper; out of scope here.
//   - No exponential backoff / retry on dial failure inside Acquire — a
//     failed dial is returned to the caller as-is.
//   - Acquire's "wait for a free slot" path uses a sync.Cond woken on every
//     Release and by a per-call goroutine watching ctx.Done(); that
//     goroutine lives until either the wait resolves or ctx completes, so
//     it does not leak past ctx's own lifetime, but a ctx with no deadline
//     that's also never canceled would leave that one goroutine parked for
//     as long as the caller keeps waiting (i.e. no different from a normal
//     blocking wait, just implemented via a goroutine rather than a
//     channel select on the net.Conn itself).
type Pool struct {
	cfg PoolConfig

	mu      sync.Mutex
	cond    *sync.Cond
	idle    []*Conn
	numOpen int
	closed  bool
}

// NewPool opens cfg.MinIdle connections eagerly and returns a ready Pool.
// If any eager connection fails, already-opened ones are closed and the
// error is returned.
func NewPool(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	if cfg.MaxOpen <= 0 {
		cfg.MaxOpen = 1
	}
	if cfg.MinIdle > cfg.MaxOpen {
		cfg.MinIdle = cfg.MaxOpen
	}
	p := &Pool{cfg: cfg}
	p.cond = sync.NewCond(&p.mu)

	for i := 0; i < cfg.MinIdle; i++ {
		c, err := Connect(ctx, cfg.Addr, cloneParams(cfg.Params))
		if err != nil {
			for _, ic := range p.idle {
				_ = ic.Close()
			}
			return nil, err
		}
		p.idle = append(p.idle, c)
		p.numOpen++
	}
	return p, nil
}

func cloneParams(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Acquire returns a Conn from the pool, opening a new one if under MaxOpen,
// or waiting for one to be Released otherwise. The returned Conn must be
// handed back via Release (not Close) so it can be reused; call Close
// yourself only if you want to permanently remove it from the pool (e.g.
// after deciding it's unhealthy for reasons the pool can't detect itself).
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	// Wake this waiter's cond.Wait() if ctx completes while we're parked.
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	for {
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return nil, err
		}

		// Drain idle list, skipping (and closing) broken connections.
		for len(p.idle) > 0 {
			c := p.idle[len(p.idle)-1]
			p.idle = p.idle[:len(p.idle)-1]
			if c.Broken() {
				_ = c.Close()
				p.numOpen--
				continue
			}
			p.mu.Unlock()
			return c, nil
		}

		if p.numOpen < p.cfg.MaxOpen {
			p.numOpen++
			p.mu.Unlock()
			c, err := Connect(ctx, p.cfg.Addr, cloneParams(p.cfg.Params))
			if err != nil {
				p.mu.Lock()
				p.numOpen--
				p.mu.Unlock()
				return nil, err
			}
			return c, nil
		}

		// At MaxOpen with none idle: wait for a Release or ctx completion.
		p.cond.Wait()
	}
}

// Release returns c to the pool for reuse, or discards it (and opens room
// for a replacement on the next Acquire) if it's Broken().
func (p *Pool) Release(c *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || c.Broken() {
		_ = c.Close()
		p.numOpen--
		p.cond.Signal()
		return
	}
	p.idle = append(p.idle, c)
	p.cond.Signal()
}

// Close closes every idle connection and marks the pool closed; Conns
// currently checked out are unaffected until Released, at which point they
// are closed rather than pooled.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var firstErr error
	for _, c := range p.idle {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.idle = nil
	p.cond.Broadcast()
	return firstErr
}

// Stats reports point-in-time pool occupancy.
type Stats struct {
	NumOpen int
	NumIdle int
}

func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{NumOpen: p.numOpen, NumIdle: len(p.idle)}
}
