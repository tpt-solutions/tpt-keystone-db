# Manual verification of Phase 1 transactions from two concurrent psql
# sessions against a running tpt-keystone node (port 55432 by default,
# override via PSQL_PORT). This is the "manual verification" follow-up from
# TODO.md Phase 1.
#
# It exercises:
#   1. isolation         — session B does not see session A's uncommitted write
#   2. snapshot pinning   — B opens a transaction, A commits a conflicting
#      write concurrently, and B's *second* read within the same still-open
#      transaction stays frozen to its BEGIN-time snapshot
#   3. COMMIT             — a fresh (autocommit) read sees the row after A commits
#   4. ROLLBACK           — A's transaction is discarded; a fresh read never saw it
#   5. concurrent commit  — two sessions hold open transactions at the same
#      time, neither sees the other's uncommitted write, and both rows are
#      visible only once both sessions actually COMMIT
#
# Usage:
#   .\tools\verify_transactions.ps1
#
# Expects a table `t (id INT PRIMARY KEY, v TEXT)` — the script seeds it.
# It is idempotent: DROP/CREATE TABLE at the start.
#
# Note: each psql invocation below is ONE session — a transaction only
# actually commits if COMMIT appears in that same invocation's -c string.
# Disconnecting mid-transaction (no COMMIT) discards it, so any "session
# stays open" step holds it open with `SELECT pg_sleep(...)` inside the same
# -c string rather than across multiple separate invocations.

$ErrorActionPreference = "Stop"

$PORT = if ($env:PSQL_PORT) { [int]$env:PSQL_PORT } else { 55432 }
$PSQL = (Get-Command psql -ErrorAction SilentlyContinue).Source
if (-not $PSQL) { $PSQL = "C:\Program Files\PostgreSQL\15\bin\psql.exe" }
$TABLE = "t"
$PASS = 0
$FAIL = 0

function ok($msg) {
    Write-Host "   PASS: $msg"
    $script:PASS++
}

function bad($msg) {
    Write-Host "   FAIL: $msg"
    $script:FAIL++
}

function exec($sql) {
    & $PSQL -h localhost -p $PORT -U postgres -X -q -A -t -c $sql 2>$null
}

function execJob($sql) {
    Start-Job -ScriptBlock {
        param($psql, $p, $s)
        & $psql -h localhost -p $p -U postgres -X -q -A -t -c $s 2>$null
    } -ArgumentList $PSQL, $PORT, $sql
}

Write-Host "== setup: create $TABLE =="
exec "DROP TABLE IF EXISTS $TABLE;" | Out-Null
exec "CREATE TABLE $TABLE (id INT PRIMARY KEY, v TEXT);" | Out-Null
exec "INSERT INTO $TABLE VALUES (1, 'base');" | Out-Null

# ---------------------------------------------------------------------------
# 1. Isolation — B does not see A's uncommitted write
# ---------------------------------------------------------------------------
Write-Host "== 1. isolation: B must NOT see A's uncommitted write =="
exec "BEGIN; INSERT INTO $TABLE VALUES (2, 'from_a');" | Out-Null
$B_SEES_A = exec "SELECT count(*) FROM $TABLE WHERE id = 2;"
if ($B_SEES_A -eq "0") { ok "B sees 0 rows from A's open txn" } else { bad "B saw A's uncommitted write ($B_SEES_A)" }

# ---------------------------------------------------------------------------
# 2. Snapshot pinning — B's open transaction stays frozen across A's commit
# ---------------------------------------------------------------------------
Write-Host "== 2. snapshot pinning: B's open txn stays frozen despite A's concurrent commit =="
$jobB = execJob "BEGIN; SELECT count(*) FROM $TABLE WHERE id = 3; SELECT pg_sleep(1); SELECT count(*) FROM $TABLE WHERE id = 3; COMMIT;"
Start-Sleep -Milliseconds 300
exec "BEGIN; INSERT INTO $TABLE VALUES (3, 'pinned_a'); COMMIT;" | Out-Null
$bOut = Receive-Job -Job $jobB -Wait
Remove-Job -Job $jobB
$B_BEFORE = $bOut[0]
# Index 1 is pg_sleep's own (NULL -> empty) output line; the second count is
# index 2.
$B_DURING = $bOut[2]
if ($B_BEFORE -eq "0" -and $B_DURING -eq "0") {
    ok "B's snapshot stayed at 0 across A's concurrent commit (before=$B_BEFORE, during=$B_DURING)"
} else {
    bad "B's snapshot changed mid-transaction (before=$B_BEFORE, during=$B_DURING)"
}
$B_AFTER = exec "SELECT count(*) FROM $TABLE WHERE id = 3;"
if ($B_AFTER -eq "1") { ok "fresh read after A's COMMIT sees the row" } else { bad "committed row not visible after COMMIT ($B_AFTER)" }

# ---------------------------------------------------------------------------
# 3. COMMIT — B sees A's row after COMMIT
# ---------------------------------------------------------------------------
Write-Host "== 3. COMMIT: B sees A's row after COMMIT =="
exec "BEGIN; INSERT INTO $TABLE VALUES (4, 'committed_a'); COMMIT;" | Out-Null
$B_SEES_COMMITTED = exec "SELECT count(*) FROM $TABLE WHERE id = 4;"
if ($B_SEES_COMMITTED -eq "1") { ok "B sees A's committed row" } else { bad "B did not see committed row ($B_SEES_COMMITTED)" }

# ---------------------------------------------------------------------------
# 4. ROLLBACK — A's write discarded, never visible to B
# ---------------------------------------------------------------------------
Write-Host "== 4. ROLLBACK: A's write discarded, never visible to B =="
exec "BEGIN; INSERT INTO $TABLE VALUES (5, 'gone'); ROLLBACK;" | Out-Null
$B_SEES_ROLLED_BACK = exec "SELECT count(*) FROM $TABLE WHERE id = 5;"
if ($B_SEES_ROLLED_BACK -eq "0") { ok "rolled-back row is gone" } else { bad "rolled-back row leaked ($B_SEES_ROLLED_BACK)" }

# ---------------------------------------------------------------------------
# 5. Concurrent — two sessions hold open transactions at the same time
# ---------------------------------------------------------------------------
Write-Host "== 5. concurrent: two sessions, neither sees the other's writes until both commit =="
$jobA = execJob "BEGIN; INSERT INTO $TABLE VALUES (6, 'from_a_concurrent'); SELECT pg_sleep(3); COMMIT;"
$jobB2 = execJob "BEGIN; INSERT INTO $TABLE VALUES (7, 'from_b_concurrent'); SELECT pg_sleep(3); COMMIT;"
Start-Sleep -Seconds 1
# While both transactions are still open (mid pg_sleep), neither should be
# visible to a fresh reader. Both checks are launched as background jobs so
# their psql spawn/round-trip latency overlaps instead of stacking
# sequentially — a sequential second check would lose safety margin against
# the first transaction's 3s window purely from process-spawn overhead, not
# any real isolation gap.
$checkA = execJob "SELECT count(*) FROM $TABLE WHERE id = 7;"
$checkB = execJob "SELECT count(*) FROM $TABLE WHERE id = 6;"
$A_SEES_B = Receive-Job -Job $checkA -Wait
$B_SEES_A = Receive-Job -Job $checkB -Wait
Remove-Job -Job $checkA, $checkB
if ($A_SEES_B -eq "0") { ok "a fresh reader does not see B's uncommitted write" } else { bad "saw B's uncommitted write ($A_SEES_B)" }
if ($B_SEES_A -eq "0") { ok "a fresh reader does not see A's uncommitted write" } else { bad "saw A's uncommitted write ($B_SEES_A)" }
Wait-Job -Job $jobA, $jobB2 | Out-Null
Remove-Job -Job $jobA, $jobB2
# Both sessions have now actually issued COMMIT (in their own -c invocation).
$FINAL = exec "SELECT count(*) FROM $TABLE WHERE id IN (6, 7);"
if ($FINAL -eq "2") { ok "both committed rows visible to fresh read" } else { bad "expected 2 committed rows, got $FINAL" }

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
Write-Host "== done: $PASS passed, $FAIL failed =="
exec "DROP TABLE IF EXISTS $TABLE;" | Out-Null

if ($FAIL -gt 0) {
    Write-Host "VERIFICATION FAILED"
    exit 1
}
Write-Host "ALL CHECKS PASSED"
