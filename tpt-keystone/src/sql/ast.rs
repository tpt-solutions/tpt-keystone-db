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
    Set(SetStmt),
    Show(ShowStmt),
    Begin,
    Commit,
    Rollback,
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
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: String,
    pub nullable: bool,
    pub default: Option<Expr>,
    pub is_pk: bool,
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
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub distinct: bool,
    pub projections: Vec<Projection>,
    pub from: Option<TableRef>,
    pub where_: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum Projection {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
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
    BinaryOp { op: BinOp, lhs: Box<Expr>, rhs: Box<Expr> },
    UnaryOp { op: UnOp, expr: Box<Expr> },
    IsNull { expr: Box<Expr>, negated: bool },
    IsTrue { expr: Box<Expr>, negated: bool },
    IsFalse { expr: Box<Expr>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },
    In { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Cast { expr: Box<Expr>, ty: String },
    Function { name: String, args: Vec<Expr>, distinct: bool },
    Case { operand: Option<Box<Expr>>, branches: Vec<(Expr, Expr)>, else_: Option<Box<Expr>> },
    Param(u32), // $1
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
    Add, Sub, Mul, Div, Mod,
    Eq, NotEq, Lt, Lte, Gt, Gte,
    And, Or,
    Concat,    // ||
    Arrow,     // ->
    LongArrow, // ->>
}

#[derive(Debug, Clone, Copy)]
pub enum UnOp {
    Neg,
    Not,
}
