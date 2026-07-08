//go:build live

// These tests require a live tpt-keystone instance listening on
// 127.0.0.1:5432 (unlike tpt-keystone's own storage::phase3_tests, which
// spin up an in-process Database, this SDK has no way to embed the Rust
// engine) — run explicitly with `go test -tags live ./...`. Excluded from a
// plain `go test ./...` so a fresh clone doesn't fail for lack of a running
// server.
package keystone

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLiveSmoke(t *testing.T) {
	ctx := context.Background()
	c, err := Connect(ctx, "127.0.0.1:5432", nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	table := fmt.Sprintf("sdkgo_smoke_%d", time.Now().UnixNano())
	if _, _, err := c.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int8, name text, score float8)", table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, n, err := c.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", table), 1, "alice", 9.5); err != nil || n != 1 {
		t.Fatalf("insert 1: n=%d err=%v", n, err)
	}
	if _, n, err := c.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", table), 2, "bob", nil); err != nil || n != 1 {
		t.Fatalf("insert 2: n=%d err=%v", n, err)
	}

	rows, err := c.Query(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var got []string
	for rows.Next() {
		var id int64
		var name string
		var score any
		if err := rows.Scan(&id, &name, &score); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, fmt.Sprintf("%d/%s/%v", id, name, score))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	t.Logf("streamed rows: %v", got)
	if len(got) != 2 || got[0] != "1/alice/9.5" || got[1] != "2/bob/<nil>" {
		t.Fatalf("unexpected rows: %v", got)
	}
}

func TestContextCancellation(t *testing.T) {
	ctx := context.Background()
	c, err := Connect(ctx, "127.0.0.1:5432", nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // ensure the deadline has definitely passed

	_, err = c.Query(timeoutCtx, "SELECT 1")
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
	t.Logf("got expected error: %v", err)
	if !c.Broken() {
		t.Fatal("expected Conn to be marked Broken after context cancellation")
	}
}

func TestPoolConcurrent(t *testing.T) {
	ctx := context.Background()
	pool, err := NewPool(ctx, PoolConfig{Addr: "127.0.0.1:5432", MinIdle: 1, MaxOpen: 4})
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer pool.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				errs <- err
				return
			}
			defer pool.Release(conn)
			rows, err := conn.Query(ctx, "SELECT $1::int8 AS n", n)
			if err != nil {
				errs <- err
				return
			}
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
	stats := pool.Stats()
	t.Logf("pool stats after run: %+v", stats)
	if stats.NumOpen > 4 {
		t.Fatalf("pool exceeded MaxOpen: %+v", stats)
	}
}
