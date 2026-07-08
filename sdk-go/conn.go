// Package keystone is a hand-written Go client SDK for TPT Keystone, a
// Postgres-wire-compatible database engine (see the tpt-keystone crate in
// this repo). It speaks the wire protocol directly over net.Conn — no
// lib/pq, jackc/pgx, or any other Postgres driver dependency — mirroring
// the Rust client SDK at tpt-sdk/src/keystone.
package keystone

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrClosed is returned by operations on a Conn that has already been
// closed (explicitly, or because a context was canceled/timed out during a
// prior operation — see Conn's doc comment on cancellation semantics).
var ErrClosed = errors.New("keystone: connection is closed")

// ServerError wraps the human-readable message TPT Keystone sends back in
// an ErrorResponse.
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string { return "keystone: server error: " + e.Message }

// Conn is a single connection to a Keystone node. It is not safe for
// concurrent use by multiple goroutines — use a Pool (pool.go) for
// concurrent access.
//
// # Context cancellation
//
// Every network-touching method takes a context.Context. Internally each
// call to the wire layer sets the underlying net.Conn's deadline from
// ctx.Deadline() (if any) *and* races the operation against ctx.Done() via
// a watcher goroutine, so a context.WithCancel with no deadline is also
// respected, not just context.WithTimeout/WithDeadline. If the context is
// canceled or times out mid-operation, the underlying TCP connection is
// force-closed (the wire protocol has no way to "skip" a message cleanly
// once a Parse/Bind/Execute sequence is in flight) and every subsequent
// call on this Conn returns ErrClosed. A Pool checks this via Conn.Broken()
// and discards + replaces such a Conn instead of returning it to the idle
// list.
type Conn struct {
	w      *wireConn
	nc     net.Conn
	mu     sync.Mutex // guards broken/closed; network I/O itself is not goroutine-safe and is the caller's job to serialize
	broken bool
	closed bool
}

// Connect dials addr (host:port) and performs the startup handshake.
// tpt-keystone's startup handshake auto-approves (no auth), so any `user`
// param is accepted; params may be nil.
func Connect(ctx context.Context, addr string, params map[string]string) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Conn{w: newWireConn(nc), nc: nc}

	if params == nil {
		params = map[string]string{}
	}
	if _, ok := params["user"]; !ok {
		params["user"] = "tpt_sdk_go"
	}

	err = c.withContext(ctx, func() error {
		c.w.writeStartup(params)
		if err := c.w.flush(); err != nil {
			return err
		}
		for {
			msg, err := c.w.readMessage()
			if err != nil {
				return err
			}
			switch msg.Kind {
			case msgAuthenticationOk:
				// no-op; no auth types are supported server-side
			case msgReadyForQuery:
				return nil
			case msgErrorResponse:
				return &ServerError{Message: msg.ErrMsg}
			default:
				// ParameterStatus / BackendKeyData etc — ignored
			}
		}
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// Broken reports whether this Conn's underlying socket was force-closed by
// a canceled/timed-out context or an I/O error, and can no longer be used.
func (c *Conn) Broken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broken || c.closed
}

// Close terminates the connection gracefully (best-effort Terminate
// message) and closes the socket.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	if !c.Broken() {
		c.w.writeTerminate()
		_ = c.w.flush() // best-effort; ignore errors on the way out
	}
	return c.nc.Close()
}

// withContext runs fn (which performs blocking network I/O via c.w) with
// the underlying socket's deadline derived from ctx, and force-closes the
// socket if ctx is done before fn returns. This is the single choke point
// all network-touching methods route through, so ctx handling only needs
// to be gotten right once.
func (c *Conn) withContext(ctx context.Context, fn func() error) error {
	if c.Broken() {
		return ErrClosed
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.nc.SetDeadline(deadline)
	} else {
		_ = c.nc.SetDeadline(time.Time{})
	}

	done := make(chan error, 1)
	go func() { done <- fn() }()

	select {
	case err := <-done:
		if err != nil {
			c.mu.Lock()
			c.broken = true
			c.mu.Unlock()
			// Prefer surfacing the context error if it raced with an I/O
			// error caused by the deadline actually firing.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		c.broken = true
		c.mu.Unlock()
		_ = c.nc.Close() // unblocks the goroutine's in-flight Read/Write
		<-done           // avoid leaking the goroutine
		return ctx.Err()
	}
}

// Exec runs sql via the extended query protocol with args bound as
// parameters ($1, $2, ...) and discards any result rows, returning the
// server's command tag (e.g. "INSERT 0 1") and best-effort rows-affected
// count parsed from it.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) (CommandTag string, rowsAffected int64, err error) {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return "", 0, err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	tag := rows.CommandTag()
	return tag, parseRowsAffected(tag), nil
}

func parseRowsAffected(tag string) int64 {
	parts := strings.Fields(tag)
	if len(parts) == 0 {
		return 0
	}
	// "INSERT <oid> <n>" | "UPDATE <n>" | "DELETE <n>" | "SELECT <n>" | ...
	n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// Query runs sql via the extended query protocol (Parse/Bind/Describe/
// Execute/Sync — all text format) with args bound as $1, $2, ... The
// returned *Rows streams results directly off the socket: each call to
// Rows.Next reads exactly the next DataRow message rather than buffering
// the whole result set up front, so this is safe to use for large result
// sets. See rows.go's doc comment for the exact streaming semantics.
func (c *Conn) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	if c.Broken() {
		return nil, ErrClosed
	}
	params := make([]*[]byte, len(args))
	for i, a := range args {
		enc, isNull := encodeParam(a)
		if isNull {
			params[i] = nil
		} else {
			params[i] = &enc
		}
	}

	err := c.withContext(ctx, func() error {
		c.w.writeParse("", sql, nil)
		c.w.writeBind("", "", params)
		c.w.writeDescribePortal("")
		c.w.writeExecute("", 0) // 0 == no row limit; stream to completion
		c.w.writeSync()
		return c.w.flush()
	})
	if err != nil {
		return nil, err
	}

	r := &Rows{conn: c, ctx: ctx}
	// Pull messages up to (and including) RowDescription/NoData so Columns()
	// is populated before the first Next() call, matching database/sql's
	// Query returning with column metadata already available.
	if err := r.primeColumns(); err != nil {
		return nil, err
	}
	return r, nil
}

// SimpleQuery runs sql over the simple query protocol (no parameters
// supported). Unlike Query, this buffers the full result into memory — it
// exists for parity with the Rust SDK's simple `query()` and for
// multi-statement SQL text (each ';'-separated statement gets its own
// CommandComplete; only the last statement's rows are returned, matching
// the simple query protocol's semantics).
func (c *Conn) SimpleQuery(ctx context.Context, sql string) (columns []string, rows [][]*[]byte, commandTag string, err error) {
	if c.Broken() {
		return nil, nil, "", ErrClosed
	}
	err = c.withContext(ctx, func() error {
		c.w.writeQuery(sql)
		return c.w.flush()
	})
	if err != nil {
		return nil, nil, "", err
	}

	for {
		var msg kindAndPayload
		err = c.withContext(ctx, func() error {
			m, e := c.w.readMessage()
			msg = m
			return e
		})
		if err != nil {
			return nil, nil, "", err
		}
		switch msg.Kind {
		case msgRowDescription:
			columns = make([]string, len(msg.Fields))
			for i, f := range msg.Fields {
				columns[i] = f.Name
			}
			rows = nil
		case msgDataRow:
			rows = append(rows, msg.Row)
		case msgCommandComplete:
			commandTag = msg.CommandTag
		case msgEmptyQueryResponse, msgNoticeResponse:
			// ignored
		case msgErrorResponse:
			return nil, nil, "", &ServerError{Message: msg.ErrMsg}
		case msgReadyForQuery:
			return columns, rows, commandTag, nil
		}
	}
}
