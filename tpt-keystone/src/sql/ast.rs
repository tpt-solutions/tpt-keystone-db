/// Top-level statement.
#[derive(Debug, Clone)]
pub enum Stmt {
    Select(SelectStmt),
    Insert(InsertStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    AlterTable(AlterTableStmt),
    Set(SetStmt),
    Show(ShowStmt),
    Begin,
    Commit,
    Rollback,
    DeclareCursor(DeclareCursorStmt),
    Fetch(FetchStmt),
    MoveCursor(FetchStmt),
    CloseCursor(String),
    Listen(String),
    Notify(String, Option<String>),
    Unlisten(String),
    CopyIn(CopyStmt),
    CopyOut(CopyStmt),
    CreateFunction(CreateFunctionStmt),
    CreateSequence(CreateSequenceStmt),
    CreateTopic(CreateTopicStmt),
    /// `ANALYZE [table]` — `None` means every table in `public`.
    Analyze(Option<String>),
    Match(MatchStmt),
    /// `CREATE ROLE` / `ALTER ROLE` / `DROP ROLE` (RBAC DDL, Phase 2 §9).
    /// Full enforcement (catalog writes, superuser checks, membership/privilege
    /// grants) is Phase 20 — this engine only parses these shapes today.
    CreateRole(CreateRoleStmt),
    AlterRole(AlterRoleStmt),
    DropRole(DropRoleStmt),
    /// `GRANT ... TO ...` / `REVOKE ... FROM ...` (RBAC DDL, Phase 2 §9).
    /// `is_role_grant` distinguishes `GRANT role TO role` (membership) from
    /// `GRANT priv ON obj TO role` (object privilege) — the two parse very
    /// differently and must not be confused.
    Grant(GrantStmt),
    Revoke(RevokeStmt),
}

/// Plexus's GQL-subset pattern-matching statement:
/// `MATCH (a)-[:REL]->(b)-[:REL2]-(c) ON table(from_column) [WHERE var = 'lit'] RETURN a, b [LIMIT n]`
///
/// Scope cut (see `graph::mod`'s module doc / `TODO.md`): a real GQL grammar
/// (arbitrary WHERE expressions over bound variables, OPTIONAL MATCH,
/// multiple disjoint patterns, pattern comprehensions, `CREATE`/`MERGE`)
/// is a separate, large grammar-and-planner effort comparable in scope to
/// the SQL parser itself. What's implemented is a real, working single
/// linear-chain pattern against one existing `CREATE INDEX ... USING GRAPH`
/// index: a sequence of node variables connected by typed, directed edges,
/// an optional single equality filter that pins the first node's value (the
/// search starting point), and a `RETURN` list of the pattern's own node
/// variables (not arbitrary expressions over them).
#[derive(Debug, Clone)]
pub struct MatchStmt {
    /// Node variable names in pattern order, e.g. `["a", "b", "c"]` for
    /// `(a)-[:R1]->(b)-[:R2]->(c)`. Always `hops.len() + 1` long.
    pub nodes: Vec<String>,
    pub hops: Vec<MatchHop>,
    /// `ON table(from_column)` — which graph index this pattern traverses.
    pub table: String,
    pub column: String,
    /// `WHERE <first node var> = '<literal>'` — pins the search's starting
    /// vertex. `None` means every vertex the index knows about is a
    /// starting candidate (can be expensive on a large graph; a documented
    /// tradeoff of not having a real WHERE-clause planner here).
    pub start_filter: Option<String>,
    pub returns: Vec<String>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchDirection {
    Out,
    In,
    /// `-[:REL]-` with no arrowhead: either direction.
    Both,
}

#[derive(Debug, Clone)]
pub struct MatchHop {
    pub rel_type: Option<String>,
    pub direction: MatchDirection,
}

/// `CREATE TOPIC [IF NOT EXISTS] name [WITH (partitions = n, retention =
/// '<interval>', retention_bytes = n)]` (Flux, Phase 11). Reuses the same
/// generic `WITH (...)` options grammar as `CreateIndexStmt`/
/// `CreateTableStmt` — `partitions`/`retention`/`retention_bytes` are parsed
/// out of `options` at execution time (`executor::execute_create_topic`),
/// same division of labor as Chronos's `interval`/`retention` index options.
#[derive(Debug, Clone)]
pub struct CreateTopicStmt {
    pub name: String,
    pub if_not_exists: bool,
    pub options: Vec<(String, String)>,
}

/// `CREATE SEQUENCE [IF NOT EXISTS] name [START [WITH] n] [INCREMENT [BY] n]`.
/// Any trailing clauses real `pg_dump` output emits (`NO MINVALUE`,
/// `CACHE 1`, `OWNED BY ...`, etc.) are tolerated by the parser but not
/// modeled — sequences here are just a monotonic counter.
#[derive(Debug, Clone)]
pub struct CreateSequenceStmt {
    pub name: String,
    pub start: i64,
    pub increment: i64,
}

/// `CREATE FUNCTION name(arg type, ...) RETURNS type LANGUAGE wasm AS '<base64>'`.
/// Only WASM UDFs are supported — `language` must be `"wasm"`.
#[derive(Debug, Clone)]
pub struct CreateFunctionStmt {
    pub name: String,
    pub args: Vec<(String, String)>, // (arg_name, declared type name)
    pub return_type: String,
    pub language: String,
    pub body_base64: String,
}

/// `CREATE ROLE [IF NOT EXISTS] name
///   [SUPERUSER | NOSUPERUSER]
///   [LOGIN | NOLOGIN]
///   [PASSWORD '...']
///   [IN ROLE role, ...]`.
/// `IF NOT EXISTS` is accepted syntactically but not modeled (role creation
/// is single-statement and idempotent-by-name at the catalog level in Phase
/// 20); `SUPERUSER`/`NOLOGIN` default to the Postgres defaults
/// (`NOSUPERUSER LOGIN`).
#[derive(Debug, Clone)]
pub struct CreateRoleStmt {
    pub name: String,
    pub superuser: bool,
    pub can_login: bool,
    pub password: Option<String>,
    /// `IN ROLE a, b` — roles this new role is a *member* of at creation.
    pub in_role: Vec<String>,
}

/// `ALTER ROLE name
///   [SUPERUSER | NOSUPERUSER]
///   [LOGIN | NOLOGIN]
///   [PASSWORD '...']`.
/// Every clause is optional and applied independently; omitted attributes are
/// left unchanged (modeled as `None`).
#[derive(Debug, Clone)]
pub struct AlterRoleStmt {
    pub name: String,
    pub superuser: Option<bool>,
    pub can_login: Option<bool>,
    pub password: Option<String>,
}

/// `DROP ROLE [IF EXISTS] name`.
#[derive(Debug, Clone)]
pub struct DropRoleStmt {
    pub if_exists: bool,
    pub name: String,
}

/// A grantable object privilege.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Usage,
}

/// The object a privilege is granted on (or `*`/`ALL` whole-instance,
/// represented as `Database` in this schema-less engine — there is no
/// namespace concept, so `ON DATABASE` is the only whole-instance form).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantObject {
    /// `ON [TABLE] name` — a single relation. `table` already has any
    /// schema qualifier stripped (this engine has one implicit schema).
    Table(String),
    /// `ON DATABASE` — the whole instance.
    Database,
}

/// `GRANT ... TO ...` (RBAC DDL, Phase 2 §9).
#[derive(Debug, Clone)]
pub struct GrantStmt {
    /// `true` ⇒ `GRANT role [, ...] TO role [, ...]` (membership);
    /// `false` ⇒ `GRANT priv [, ...] ON obj TO role [, ...]` (privilege).
    pub is_role_grant: bool,
    /// Populated when `is_role_grant` is true.
    pub roles: Vec<String>,
    /// Populated when `is_role_grant` is false.
    pub privileges: Vec<Privilege>,
    pub object: GrantObject,
    pub grantees: Vec<String>,
}

/// `REVOKE ... FROM ...` (RBAC DDL, Phase 2 §9). Mirrors `GrantStmt`.
#[derive(Debug, Clone)]
pub struct RevokeStmt {
    pub is_role_grant: bool,
    pub roles: Vec<String>,
    pub privileges: Vec<Privilege>,
    pub object: GrantObject,
    pub grantees: Vec<String>,
}

/// `COPY table [(columns)] FROM STDIN` / `COPY table [(columns)] TO STDOUT`.
/// Only the default Postgres text format is supported (tab-delimited rows,
/// `\N` for NULL) — `WITH (...)` options are not parsed.
#[derive(Debug, Clone)]
pub struct CopyStmt {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeclareCursorStmt {
    pub name: String,
    pub query: SelectStmt,
}

/// Shared shape for `FETCH`/`MOVE`: how many rows, from which cursor.
#[derive(Debug, Clone)]
pub struct FetchStmt {
    pub cursor: String,
    pub count: FetchCount,
}

#[derive(Debug, Clone)]
pub enum FetchCount {
    Next,
    All,
    Count(i64),
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub table_constraints: Vec<TableConstraint>,
    /// `WITH (key = 'value', ...)` trailing the column/constraint list —
    /// currently used by Canopy's JSON Schema validation (`json_schema_col`,
    /// `json_schema`, `json_schema_mode`), mirroring `CreateIndexStmt`'s
    /// generic `WITH (...)` options grammar. Empty for a plain table.
    pub options: Vec<(String, String)>,
}

/// A table-level constraint, e.g. `UNIQUE (a, b)` or
/// `FOREIGN KEY (a) REFERENCES other(b)`. Table-level `PRIMARY KEY (...)` is
/// applied directly to the named columns' `is_pk` at parse time instead of
/// being modeled here. Foreign keys are single-column only — composite FKs
/// are a documented scope cut.
#[derive(Debug, Clone)]
pub enum TableConstraint {
    Unique(Vec<String>),
    ForeignKey {
        column: String,
        ref_table: String,
        ref_column: String,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: String,
    pub nullable: bool,
    pub default: Option<Expr>,
    pub is_pk: bool,
    pub is_unique: bool,
    pub references: Option<(String, String)>, // (ref_table, ref_column)
    /// `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` shorthand — `col_type` has already
    /// been rewritten to the underlying integer type; this just tells
    /// `execute_create_table` to also create a backing sequence and an
    /// implicit `nextval(...)` default.
    pub is_serial: bool,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub table: String,
    pub column: String,
    pub name: Option<String>,
    /// `USING <method>` (e.g. `USING SPATIAL`/`USING GIST`/`USING TIME`) —
    /// `None` means the default B-Tree. `SPATIAL`/`GIST` (aliased) select
    /// Meridian's S2-inspired spatial index, `TIME`/`CHRONOS` (aliased)
    /// select Chronos's time-bucketed index; any other method name is
    /// rejected at execution time rather than silently falling back to
    /// B-Tree.
    pub using: Option<String>,
    /// `WITH (key = 'value', ...)` — generic index options, e.g. Chronos's
    /// `interval`/`retention`/`value` keys. Empty for a plain B-Tree/spatial
    /// index.
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table: String,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    AlterColumn { name: String, action: ColumnAction },
}

#[derive(Debug, Clone)]
pub enum ColumnAction {
    SetDefault(Expr),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub ctes: Vec<Cte>,
    pub distinct: bool,
    pub projections: Vec<Projection>,
    pub from: Option<TableWithJoins>,
    pub where_: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub union: Option<(UnionOp, Box<SelectStmt>)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnionOp {
    Union,
    UnionAll,
}

/// Common Table Expression (CTE) definition.
#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub columns: Vec<String>,
    pub subquery: SelectStmt,
    pub recursive: bool,
}

#[derive(Debug, Clone)]
pub enum Projection {
    Wildcard,
    WildcardTable(String), // table.*
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    /// Present for a derived table (`(SELECT ...) AS alias`) in the FROM
    /// clause or a JOIN. When set, `name` holds the required alias.
    pub subquery: Option<Box<SelectStmt>>,
    /// Present for a table-valued function call in the FROM clause (e.g.
    /// `graph_bfs('edges', 'from_id', '1', 3)`) — the Plexus graph-traversal
    /// entry points are exposed this way rather than through a GQL `MATCH`
    /// pattern grammar (not implemented, see `graph` module docs). When set,
    /// `name` holds the function name and this holds its argument
    /// expressions, evaluated as constants (no row/outer-query context) at
    /// FROM-resolution time.
    pub func_args: Option<Vec<Expr>>,
}

/// Represents a FROM clause with optional JOINs.
#[derive(Debug, Clone)]
pub struct TableWithJoins {
    pub primary: TableRef,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub struct SetStmt {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct ShowStmt {
    pub name: String,
}

/// Expression tree.
#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Literal),
    Ident(String),
    QualifiedIdent(String, String), // table.column
    BinaryOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    UnaryOp {
        op: UnOp,
        expr: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    IsTrue {
        expr: Box<Expr>,
        negated: bool,
    },
    IsFalse {
        expr: Box<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    In {
        expr: Box<Expr>,
        list: InList,
        negated: bool,
    },
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    Cast {
        expr: Box<Expr>,
        ty: String,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Param(u32), // $1
    Subquery(Box<SelectStmt>),
    Window {
        func: String,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderBy>,
        frame: Option<WindowFrame>,
    },
}

#[derive(Debug, Clone)]
pub enum InList {
    Exprs(Vec<Expr>),
    Subquery(Box<SelectStmt>),
}

#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub frame_type: FrameType,
    pub start: FrameBound,
    pub end: Option<FrameBound>,
}

#[derive(Debug, Clone, Copy)]
pub enum FrameType {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone)]
pub struct FrameBound {
    pub bound_type: FrameBoundType,
    pub offset: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Copy)]
pub enum FrameBoundType {
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
}

#[derive(Debug, Clone)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Concat,          // ||
    Arrow,           // ->
    LongArrow,       // ->>
    Contains,        // @> (JSON/JSONB containment)
    HashArrow,       // #> (JSON path extraction, array-literal path)
    HashLongArrow,   // #>> (JSON path extraction as text)
    RegexMatch,      // ~  (POSIX regex match)
    RegexNotMatch,   // !~ (POSIX regex non-match)
    RegexMatchCI,    // ~* (case-insensitive POSIX regex match)
    RegexNotMatchCI, // !~* (case-insensitive POSIX regex non-match)
}

#[derive(Debug, Clone, Copy)]
pub enum UnOp {
    Neg,
    Not,
}
