#!/usr/bin/env bash
# Manual verification of Stage 1 read-committed transactions from two concurrent
# psql sessions against a running tpt-keystone node (port 55432 by default).
#
# This is the "manual verification" follow-up from TODO.md Phase 1. It is NOT
# scripted CI — it needs a live node (cargo run in tpt-keystone) and the psql
# client on PATH. It exercises:
#   1. isolation  — session B does not see session A's uncommitted write
#   2. COMMIT      — session B sees the row after session A commits
#   3. ROLLBACK    — session A's write is discarded, session B never saw it
#
# Usage:
#   PSQL_PORT=55432 ./tools/verify_transactions.sh
#
# Expects a table `t (id INT PRIMARY KEY, v TEXT)` to already exist (the script
# seeds it). It is idempotent: it DROPs/CREATEs `t` at the start.

set -euo pipefail

PORT="${PSQL_PORT:-55432}"
PG="psql -h localhost -p "$PORT" -U postgres -X -q -A -t"
TABLE=t

echo "== setting up $TABLE =="
$PG -c "DROP TABLE IF EXISTS $TABLE;" >/dev/null
$PG -c "CREATE TABLE $TABLE (id INT PRIMARY KEY, v TEXT);" >/dev/null
$PG -c "INSERT INTO $TABLE VALUES (1, 'base');" >/dev/null

echo "== 1. isolation: B must NOT see A's uncommitted write =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (2, 'from_a');" >/dev/null
B_SEES_A=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 2;")
if [ "$B_SEES_A" = "0" ]; then echo "   PASS: B sees 0 rows from A's open txn"; else echo "   FAIL: B saw A's uncommitted write"; fi
$PG -c "ROLLBACK;" >/dev/null

echo "== 2. COMMIT: B sees A's row after COMMIT =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (3, 'committed_a'); COMMIT;" >/dev/null
B_SEES_COMMITTED=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 3;")
if [ "$B_SEES_COMMITTED" = "1" ]; then echo "   PASS: B sees A's committed row"; else echo "   FAIL: B did not see committed row"; fi

echo "== 3. ROLLBACK: A's write discarded, never visible to B =="
$PG -c "BEGIN; INSERT INTO $TABLE VALUES (4, 'gone');" >/dev/null
$PG -c "ROLLBACK;" >/dev/null
B_SEES_ROLLED_BACK=$($PG -c "SELECT count(*) FROM $TABLE WHERE id = 4;")
if [ "$B_SEES_ROLLED_BACK" = "0" ]; then echo "   PASS: rolled-back row is gone"; else echo "   FAIL: rolled-back row leaked"; fi

echo "== done =="
$PG -c "DROP TABLE IF EXISTS $TABLE;" >/dev/null
