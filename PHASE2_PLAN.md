## Phase 2 — Keystone: SQL Engine Implementation Plan

### Current State Analysis
- SQL Parser: Already supports SELECT (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET), INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX, DROP TABLE
- Executor: Basic SELECT (no WHERE filtering, no GROUP BY, no ORDER BY, no LIMIT/OFFSET), INSERT, UPDATE, DELETE (stub implementations)
- Storage: LSM engine with MVCC, B-Tree indexes, but no transaction integration in executor
- Wire Protocol: Extended query protocol (Parse/Bind/Execute) is stubbed

### Implementation Order

#### 1. Full SELECT Implementation
- [x] Implement WHERE clause filtering in execute_select
- [x] Implement GROUP BY with aggregation
- [x] Implement HAVING clause
- [x] Implement ORDER BY sorting
- [x] Implement LIMIT/OFFSET

#### 2. JOINs Implementation
- [x] Add JOIN AST nodes (JoinExpr, JoinType)
- [x] Add JOIN parsing to parser
- [x] Implement hash join
- [x] Implement merge join
- [x] Implement nested loop join

#### 3. INSERT/UPDATE/DELETE with MVCC
- [x] Integrate transaction context in session
- [x] Use MVCC for INSERT/UPDATE/DELETE operations
- [x] Implement proper WHERE clause evaluation for UPDATE/DELETE

#### 4. DDL: ALTER TABLE
- [x] Add ALTER TABLE AST node
- [x] Add ALTER TABLE parsing
- [x] Implement ALTER TABLE execution

#### 5. Subqueries + CTEs
- [ ] Add WITH clause parsing to lexer and parser
- [ ] Implement CTE execution
- [ ] Implement subquery execution

#### 6. Window Functions
- [ ] Implement window function evaluation

#### 7. Prepared Statements
- [ ] Implement full extended query protocol
- [ ] Add prepared statement storage
- [ ] Add parameter binding

#### 8. Query Planner
- [ ] Add planner module
- [ ] Implement cost-based optimization
- [ ] Add plan execution