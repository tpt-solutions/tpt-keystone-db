## Phase 2 — Keystone: SQL Engine Implementation Plan

### Current State Analysis
- SQL Parser: Already supports SELECT (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET), INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX, DROP TABLE
- Executor: Basic SELECT (no WHERE filtering, no GROUP BY, no ORDER BY, no LIMIT/OFFSET), INSERT, UPDATE, DELETE (stub implementations)
- Storage: LSM engine with MVCC, B-Tree indexes, but no transaction integration in executor
- Wire Protocol: Extended query protocol (Parse/Bind/Execute) is stubbed

### Implementation Order

#### 1. Full SELECT Implementation
- [ ] Implement WHERE clause filtering in execute_select
- [ ] Implement GROUP BY with aggregation
- [ ] Implement HAVING clause
- [ ] Implement ORDER BY sorting
- [ ] Implement LIMIT/OFFSET

#### 2. JOINs Implementation
- [ ] Add JOIN AST nodes (JoinExpr, JoinType)
- [ ] Add JOIN parsing to parser
- [ ] Implement hash join
- [ ] Implement merge join
- [ ] Implement nested loop join

#### 3. INSERT/UPDATE/DELETE with MVCC
- [ ] Integrate transaction context in session
- [ ] Use MVCC for INSERT/UPDATE/DELETE operations
- [ ] Implement proper WHERE clause evaluation for UPDATE/DELETE

#### 4. DDL: ALTER TABLE
- [ ] Add ALTER TABLE AST node
- [ ] Add ALTER TABLE parsing
- [ ] Implement ALTER TABLE execution

#### 5. Subqueries + CTEs
- [ ] Add subquery AST nodes
- [ ] Add WITH clause parsing
- [ ] Implement CTE execution

#### 6. Window Functions
- [ ] Add window function AST nodes
- [ ] Implement window function evaluation

#### 7. Prepared Statements
- [ ] Implement full extended query protocol
- [ ] Add prepared statement storage
- [ ] Add parameter binding

#### 8. Query Planner
- [ ] Add planner module
- [ ] Implement cost-based optimization
- [ ] Add plan execution