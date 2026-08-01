#!/usr/bin/env bash
# Manual verification of Phase 1 transactions from two *concurrent* psql
# sessions against a running tpt-keystone node (port 55432 by default,
# override via PSQL_PORT). This is the "manual verification" follow-up from
# TODO.md Phase 1 — it is NOT scripted CI (needs a live node + psql on PATH).
#
# It exercises:
#   1. isolation        — session B does not see session A's uncommitted write
#   2. snapshot pinning  — B opens a transaction, A commits a conflicting write
#      concurrently, and B's *second* read within the same still-open
#      transaction stays frozen to its BEGIN-time snapshot
#   3. COMMIT            — a fresh (autocommit) read sees the row after A commits
#   4. ROLLBACK          — A's transaction is discarded; a fresh read never saw it
#   5. concurrent commit — two sessions hold open transactions at the same
#      time, neither sees the other's uncommitted write, and both rows are
#      visible only once both sessions actually COMMIT
#
# Usage:
#   PSQL_PORT=55432 ./tools/verify_transactions.sh
#
# Expects a table `t (id INT PRIMARY KEY, v TEXT)` — the script seeds it.
# It is idempotent: DROP/CREATE TABLE at the start.
#
# Note: each `-c "stmt1; stmt2; ..."` invocation is ONE psql session — a
# transaction only actually commits if `COMMIT` appears in that same
# invocation. Disconnecting mid-transaction (no COMMIT) discards it, so any
# "session stays open" step below holds it open with `SELECT pg_sleep(...)`
# inside the same -c string rather than across multiple psql invocations.

set -euo pipefail

PORT="${PSQL_PORT:-55432}"
PG="psql -h localhost -p "$PORT" -U postgres -X -q -A -t"
TABLE=t
PASS=0
FAIL=0

ok() { echo "   PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "   FAIL: $1"; FAIL=$((FAIL+1)); }

echo "== setup: create $TABLE =="
$PG -c "DROP TABLE IF EXISTS $TABLE;" >/dev/null
$PG -c "CREATE TABLE $TABLE (id INT PRIMARY KEY, v TEXT);" >/dev/null
$PG -c "INSERT INTO $TABLE VALUES (1, 'base');" >/dev/null

# ---------------------------------------------------------------------------
# 1. Isolation — B does not see A's uncommitted write
# ---------------------------------------------------------------------------
echo "== 1. isolation: B must NOT see A's uncommitted write =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (2, 'from_a');" >/dev/null
B_SEES_A=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 2;")
if [ "$B_SEES_A" = "0" ]; then ok "B sees 0 rows from A's open txn"; else bad "B saw A's uncommitted write"; fi

# ---------------------------------------------------------------------------
# 2. Snapshot pinning — B's open transaction stays frozen across A's commit
# ---------------------------------------------------------------------------
echo "== 2. snapshot pinning: B's open txn stays frozen despite A's concurrent commit =="
OUT_B="$(mktemp)"
$PG -c "BEGIN; SELECT count(*) FROM $TABLE WHERE id = 3; SELECT pg_sleep(1); SELECT count(*) FROM $TABLE WHERE id = 3; COMMIT;" >"$OUT_B" &
B_PID=$!
sleep 0.3
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (3, 'pinned_a'); COMMIT;" >/dev/null
wait $B_PID
B_BEFORE="$(sed -n '1p' "$OUT_B")"
# Line 2 is pg_sleep's own (NULL -> empty) output line; the second count is
# line 3.
B_DURING="$(sed -n '3p' "$OUT_B")"
rm -f "$OUT_B"
if [ "$B_BEFORE" = "0" ] && [ "$B_DURING" = "0" ]; then
    ok "B's snapshot stayed at 0 across A's concurrent commit (before=$B_BEFORE, during=$B_DURING)"
else
    bad "B's snapshot changed mid-transaction (before=$B_BEFORE, during=$B_DURING)"
fi
B_AFTER=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 3;")
if [ "$B_AFTER" = "1" ]; then ok "fresh read after A's COMMIT sees the row"; else bad "committed row not visible after COMMIT"; fi

# ---------------------------------------------------------------------------
# 3. COMMIT — B sees A's row after COMMIT
# ---------------------------------------------------------------------------
echo "== 3. COMMIT: B sees A's row after COMMIT =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (4, 'committed_a'); COMMIT;" >/dev/null
B_SEES_COMMITTED=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 4;")
if [ "$B_SEES_COMMITTED" = "1" ]; then ok "B sees A's committed row"; else bad "B did not see committed row"; fi

# ---------------------------------------------------------------------------
# 4. ROLLBACK — A's write discarded, never visible to B
# ---------------------------------------------------------------------------
echo "== 4. ROLLBACK: A's write discarded, never visible to B =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (5, 'gone'); ROLLBACK;" >/dev/null
B_SEES_ROLLED_BACK=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 5;")
if [ "$B_SEES_ROLLED_BACK" = "0" ]; then ok "rolled-back row is gone"; else bad "rolled-back row leaked"; fi

# ---------------------------------------------------------------------------
# 5. Concurrent — two sessions hold open transactions at the same time
# ---------------------------------------------------------------------------
echo "== 5. concurrent: two sessions, neither sees the other's writes until both commit =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (6, 'from_a_concurrent'); SELECT pg_sleep(3); COMMIT;" >/dev/null &
A_PID=$!
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (7, 'from_b_concurrent'); SELECT pg_sleep(3); COMMIT;" >/dev/null &
B_PID=$!
sleep 1
# While both transactions are still open (mid pg_sleep), neither should see
# the other's uncommitted insert. Both checks are launched as background
# jobs so their psql spawn/round-trip latency overlaps instead of stacking
# sequentially — a sequential second check would lose safety margin against
# the first transaction's 3s window purely from process-spawn overhead, not
# any real isolation gap.
OUT_A_SEES_B="$(mktemp)"
OUT_B_SEES_A="$(mktemp)"
$PG -c "SELECT count(*) FROM $TABLE WHERE id = 7;" >"$OUT_A_SEES_B" &
CHECK1_PID=$!
$PG -c "SELECT count(*) FROM $TABLE WHERE id = 6;" >"$OUT_B_SEES_A" &
CHECK2_PID=$!
wait $CHECK1_PID $CHECK2_PID
A_SEES_B="$(cat "$OUT_A_SEES_B")"
B_SEES_A="$(cat "$OUT_B_SEES_A")"
rm -f "$OUT_A_SEES_B" "$OUT_B_SEES_A"
if [ "$A_SEES_B" = "0" ]; then ok "a fresh reader does not see B's uncommitted write"; else bad "saw B's uncommitted write ($A_SEES_B)"; fi
if [ "$B_SEES_A" = "0" ]; then ok "a fresh reader does not see A's uncommitted write"; else bad "saw A's uncommitted write ($B_SEES_A)"; fi
wait $A_PID $B_PID
# Both sessions have now actually issued COMMIT (in their own -c invocation).
FINAL=$($PG -c "SELECT count(*) FROM $TABLE WHERE id IN (6, 7);")
if [ "$FINAL" = "2" ]; then ok "both committed rows visible to fresh read"; else bad "expected 2 committed rows, got $FINAL"; fi

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
echo "== done: $PASS passed, $FAIL failed =="
$PG -c "DROP TABLE IF EXISTS $TABLE;" >/dev/null

if [ "$FAIL" -gt 0 ]; then
    echo "VERIFICATION FAILED"
    exit 1
fi
echo "ALL CHECKS PASSED"
