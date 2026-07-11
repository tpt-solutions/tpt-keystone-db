use super::ast::*;
use super::{parse, parse_expr_text};

fn parse_ok(sql: &str) -> Stmt {
    parse(sql).unwrap_or_else(|e| panic!("expected {sql:?} to parse, got error: {e}"))
}

fn expr_ok(text: &str) -> Expr {
    parse_expr_text(text).unwrap_or_else(|e| panic!("expected {text:?} to parse, got error: {e}"))
}

#[test]
fn parses_simple_select_with_where_order_limit() {
    let stmt = parse_ok("SELECT a, b FROM t WHERE a > 1 ORDER BY b DESC LIMIT 10 OFFSET 5");
    match stmt {
        Stmt::Select(s) => {
            assert_eq!(s.projections.len(), 2);
            assert!(s.from.is_some());
            assert!(s.where_.is_some());
            assert_eq!(s.order_by.len(), 1);
            assert!(!s.order_by[0].asc);
            assert!(s.limit.is_some());
            assert!(s.offset.is_some());
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn parses_select_joins() {
    let stmt = parse_ok(
        "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON a.id = c.id CROSS JOIN d",
    );
    match stmt {
        Stmt::Select(s) => {
            let fj = s.from.expect("from");
            assert_eq!(fj.joins.len(), 3);
            assert_eq!(fj.joins[0].join_type, JoinType::Inner);
            assert_eq!(fj.joins[1].join_type, JoinType::Left);
            assert_eq!(fj.joins[2].join_type, JoinType::Cross);
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn parses_select_with_cte() {
    let stmt = parse_ok("WITH x AS (SELECT 1) SELECT * FROM x");
    match stmt {
        Stmt::Select(s) => {
            assert_eq!(s.ctes.len(), 1);
            assert_eq!(s.ctes[0].name, "x");
            assert!(!s.ctes[0].recursive);
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn parses_recursive_cte() {
    let stmt = parse_ok("WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x");
    match stmt {
        Stmt::Select(s) => {
            assert_eq!(s.ctes.len(), 1);
            assert!(s.ctes[0].recursive);
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn parses_select_union() {
    let stmt = parse_ok("SELECT 1 UNION ALL SELECT 2");
    match stmt {
        Stmt::Select(s) => match s.union {
            Some((UnionOp::UnionAll, _)) => {}
            other => panic!("expected UnionAll, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn parses_window_function_and_frame() {
    let stmt = parse_ok(
        "SELECT sum(a) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
    );
    match stmt {
        Stmt::Select(s) => match &s.projections[0] {
            Projection::Expr {
                expr: Expr::Window {
                    partition_by,
                    order_by,
                    frame,
                    ..
                },
                ..
            } => {
                assert_eq!(partition_by.len(), 1);
                assert_eq!(order_by.len(), 1);
                let frame = frame.as_ref().expect("frame");
                assert!(matches!(frame.frame_type, FrameType::Rows));
                assert!(matches!(
                    frame.start.bound_type,
                    FrameBoundType::UnboundedPreceding
                ));
                assert!(matches!(
                    frame.end.as_ref().unwrap().bound_type,
                    FrameBoundType::CurrentRow
                ));
            }
            other => panic!("expected Window projection, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn parses_insert_update_delete() {
    match parse_ok("INSERT INTO t (a, b) VALUES (1, 2), (3, 4)") {
        Stmt::Insert(s) => {
            assert_eq!(s.table, "t");
            assert_eq!(s.columns, vec!["a", "b"]);
            assert_eq!(s.values.len(), 2);
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    match parse_ok("UPDATE t SET a = 1, b = 2 WHERE c = 3") {
        Stmt::Update(s) => {
            assert_eq!(s.table, "t");
            assert_eq!(s.assignments.len(), 2);
            assert!(s.where_.is_some());
        }
        other => panic!("expected Update, got {other:?}"),
    }
    match parse_ok("DELETE FROM t WHERE a = 1") {
        Stmt::Delete(s) => {
            assert_eq!(s.table, "t");
            assert!(s.where_.is_some());
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn parses_create_table_with_options() {
    let stmt = parse_ok(
        "CREATE TABLE IF NOT EXISTS t (id int PRIMARY KEY, name text NOT NULL) WITH (json_schema = 'strict')",
    );
    match stmt {
        Stmt::CreateTable(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.columns.len(), 2);
            assert!(s.columns[0].is_pk);
            assert!(!s.columns[0].nullable);
            assert!(!s.columns[1].nullable);
            assert_eq!(s.options, vec![("json_schema".to_string(), "strict".to_string())]);
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_variants() {
    match parse_ok("ALTER TABLE t ADD COLUMN a int") {
        Stmt::AlterTable(s) => {
            assert!(matches!(s.action, AlterTableAction::AddColumn(_)));
        }
        other => panic!("expected AlterTable, got {other:?}"),
    }
    match parse_ok("ALTER TABLE t DROP COLUMN a") {
        Stmt::AlterTable(s) => {
            assert!(matches!(s.action, AlterTableAction::DropColumn(_)));
        }
        other => panic!("expected AlterTable, got {other:?}"),
    }
    match parse_ok("ALTER TABLE t ALTER COLUMN a SET NOT NULL") {
        Stmt::AlterTable(s) => match s.action {
            AlterTableAction::AlterColumn { action, .. } => {
                assert!(matches!(action, ColumnAction::SetNotNull));
            }
            other => panic!("expected AlterColumn, got {other:?}"),
        },
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn parses_create_and_drop_index() {
    let stmt = parse_ok("CREATE INDEX idx_a ON t USING SPATIAL (a) WITH (interval = '1h')");
    match stmt {
        Stmt::CreateIndex(s) => {
            assert_eq!(s.table, "t");
            assert_eq!(s.column, "a");
            assert_eq!(s.name.as_deref(), Some("idx_a"));
            assert_eq!(s.using.as_deref(), Some("SPATIAL"));
            assert_eq!(s.options, vec![("interval".to_string(), "1h".to_string())]);
        }
        other => panic!("expected CreateIndex, got {other:?}"),
    }
    // DROP INDEX is parsed but explicitly rejected — not yet supported.
    assert!(parse("DROP INDEX idx_a").is_err());

    match parse_ok("DROP TABLE IF EXISTS t") {
        Stmt::DropTable(s) => {
            assert_eq!(s.table, "t");
            assert!(s.if_exists);
        }
        other => panic!("expected DropTable, got {other:?}"),
    }
}

#[test]
fn parses_transaction_control() {
    assert!(matches!(parse_ok("BEGIN"), Stmt::Begin));
    assert!(matches!(parse_ok("COMMIT"), Stmt::Commit));
    assert!(matches!(parse_ok("ROLLBACK"), Stmt::Rollback));
}

#[test]
fn parses_listen_notify_unlisten() {
    assert!(matches!(parse_ok("LISTEN chan"), Stmt::Listen(c) if c == "chan"));
    assert!(matches!(parse_ok("UNLISTEN *"), Stmt::Unlisten(c) if c == "*"));
    match parse_ok("NOTIFY chan, 'payload'") {
        Stmt::Notify(c, Some(p)) => {
            assert_eq!(c, "chan");
            assert_eq!(p, "payload");
        }
        other => panic!("expected Notify with payload, got {other:?}"),
    }
    match parse_ok("NOTIFY chan") {
        Stmt::Notify(c, None) => assert_eq!(c, "chan"),
        other => panic!("expected Notify without payload, got {other:?}"),
    }
}

#[test]
fn parses_copy_stmt() {
    match parse_ok("COPY t (a, b) FROM STDIN") {
        Stmt::CopyIn(s) => {
            assert_eq!(s.table, "t");
            assert_eq!(s.columns, vec!["a", "b"]);
        }
        other => panic!("expected CopyIn, got {other:?}"),
    }
    match parse_ok("COPY t TO STDOUT") {
        Stmt::CopyOut(s) => assert_eq!(s.table, "t"),
        other => panic!("expected CopyOut, got {other:?}"),
    }
}

#[test]
fn parses_create_function_sequence_topic() {
    match parse_ok("CREATE FUNCTION f(x int) RETURNS int LANGUAGE wasm AS 'YWJj'") {
        Stmt::CreateFunction(s) => {
            assert_eq!(s.name, "f");
            assert_eq!(s.args, vec![("x".to_string(), "int".to_string())]);
            assert_eq!(s.return_type, "int");
            assert_eq!(s.language, "wasm");
            assert_eq!(s.body_base64, "YWJj");
        }
        other => panic!("expected CreateFunction, got {other:?}"),
    }
    match parse_ok("CREATE SEQUENCE IF NOT EXISTS seq START WITH 10 INCREMENT BY 2") {
        Stmt::CreateSequence(s) => {
            assert_eq!(s.name, "seq");
            assert_eq!(s.start, 10);
            assert_eq!(s.increment, 2);
        }
        other => panic!("expected CreateSequence, got {other:?}"),
    }
    match parse_ok("CREATE TOPIC t WITH (partitions = '4')") {
        Stmt::CreateTopic(s) => {
            assert_eq!(s.name, "t");
            assert_eq!(s.options, vec![("partitions".to_string(), "4".to_string())]);
        }
        other => panic!("expected CreateTopic, got {other:?}"),
    }
}

#[test]
fn parses_analyze() {
    assert!(matches!(parse_ok("ANALYZE"), Stmt::Analyze(None)));
    match parse_ok("ANALYZE t") {
        Stmt::Analyze(Some(name)) => assert_eq!(name, "t"),
        other => panic!("expected Analyze(Some), got {other:?}"),
    }
}

#[test]
fn parses_show_and_set() {
    match parse_ok("SHOW search_path") {
        Stmt::Show(s) => assert_eq!(s.name, "search_path"),
        other => panic!("expected Show, got {other:?}"),
    }
    match parse_ok("SET search_path = public") {
        Stmt::Set(s) => assert_eq!(s.name, "search_path"),
        other => panic!("expected Set, got {other:?}"),
    }
}

#[test]
fn pratt_parser_operator_precedence() {
    // 1 + 2 * 3 = 1 + (2 * 3), not (1 + 2) * 3.
    let expr = expr_ok("1 + 2 * 3");
    match expr {
        Expr::BinaryOp {
            op: BinOp::Add,
            rhs,
            ..
        } => {
            assert!(matches!(
                *rhs,
                Expr::BinaryOp {
                    op: BinOp::Mul,
                    ..
                }
            ));
        }
        other => panic!("expected top-level Add, got {other:?}"),
    }

    // AND binds tighter than OR.
    let expr = expr_ok("a OR b AND c");
    match expr {
        Expr::BinaryOp {
            op: BinOp::Or, rhs, ..
        } => {
            assert!(matches!(
                *rhs,
                Expr::BinaryOp {
                    op: BinOp::And,
                    ..
                }
            ));
        }
        other => panic!("expected top-level Or, got {other:?}"),
    }
}

#[test]
fn parses_posix_regex_operators() {
    assert!(matches!(
        expr_ok("a ~ 'x'"),
        Expr::BinaryOp {
            op: BinOp::RegexMatch,
            ..
        }
    ));
    assert!(matches!(
        expr_ok("a !~ 'x'"),
        Expr::BinaryOp {
            op: BinOp::RegexNotMatch,
            ..
        }
    ));
    assert!(matches!(
        expr_ok("a ~* 'x'"),
        Expr::BinaryOp {
            op: BinOp::RegexMatchCI,
            ..
        }
    ));
    assert!(matches!(
        expr_ok("a !~* 'x'"),
        Expr::BinaryOp {
            op: BinOp::RegexNotMatchCI,
            ..
        }
    ));
    // `~` binds tighter than AND, so `a ~ 'x' AND b ~ 'y'` groups each match.
    assert!(matches!(
        expr_ok("a ~ 'x' AND b ~ 'y'"),
        Expr::BinaryOp {
            op: BinOp::And,
            ..
        }
    ));
}

#[test]
fn parses_between_like_in_exists() {
    assert!(matches!(expr_ok("a BETWEEN 1 AND 10"), Expr::Between { negated: false, .. }));
    assert!(matches!(
        expr_ok("a NOT BETWEEN 1 AND 10"),
        Expr::Between { negated: true, .. }
    ));
    assert!(matches!(expr_ok("a LIKE 'x%'"), Expr::Like { negated: false, .. }));
    match expr_ok("a IN (1, 2, 3)") {
        Expr::In {
            list: InList::Exprs(items),
            negated: false,
            ..
        } => assert_eq!(items.len(), 3),
        other => panic!("expected In with expr list, got {other:?}"),
    }
    match expr_ok("a IN (SELECT b FROM t)") {
        Expr::In {
            list: InList::Subquery(_),
            ..
        } => {}
        other => panic!("expected In with subquery, got {other:?}"),
    }
    assert!(matches!(expr_ok("EXISTS (SELECT 1)"), Expr::Exists { negated: false, .. }));
    assert!(matches!(
        expr_ok("NOT EXISTS (SELECT 1)"),
        Expr::Exists { negated: true, .. }
    ));
}

#[test]
fn parses_cast_and_case_expr() {
    match expr_ok("CAST(a AS int)") {
        Expr::Cast { ty, .. } => assert_eq!(ty, "int"),
        other => panic!("expected Cast, got {other:?}"),
    }
    match expr_ok("a::int") {
        Expr::Cast { ty, .. } => assert_eq!(ty, "int"),
        other => panic!("expected postfix Cast, got {other:?}"),
    }
    match expr_ok("CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END") {
        Expr::Case {
            operand,
            branches,
            else_,
        } => {
            assert!(operand.is_some());
            assert_eq!(branches.len(), 2);
            assert!(else_.is_some());
        }
        other => panic!("expected Case, got {other:?}"),
    }
}

#[test]
fn parses_json_operators() {
    let cases = [
        ("a -> 'x'", BinOp::Arrow),
        ("a ->> 'x'", BinOp::LongArrow),
        ("a @> b", BinOp::Contains),
        ("a #> b", BinOp::HashArrow),
        ("a #>> b", BinOp::HashLongArrow),
    ];
    for (text, expected) in cases {
        match expr_ok(text) {
            Expr::BinaryOp { op, .. } => assert_eq!(op, expected, "for {text:?}"),
            other => panic!("expected BinaryOp for {text:?}, got {other:?}"),
        }
    }
}

#[test]
fn parses_params_and_numeric_literals() {
    assert!(matches!(expr_ok("$1"), Expr::Param(1)));
    assert!(matches!(expr_ok("42"), Expr::Literal(Literal::Int(42))));
    assert!(matches!(expr_ok("4.5"), Expr::Literal(Literal::Float(f)) if f == 4.5));
    assert!(matches!(expr_ok("1e10"), Expr::Literal(Literal::Float(f)) if f == 1e10));
}

#[test]
fn parses_string_escaping() {
    match expr_ok("'it''s'") {
        Expr::Literal(Literal::Text(s)) => assert_eq!(s, "it's"),
        other => panic!("expected Text literal, got {other:?}"),
    }
}

#[test]
fn skips_comments() {
    let stmt = parse_ok("SELECT 1 -- trailing comment\n");
    assert!(matches!(stmt, Stmt::Select(_)));
    let stmt = parse_ok("SELECT /* inline */ 1");
    assert!(matches!(stmt, Stmt::Select(_)));
}

#[test]
fn malformed_sql_returns_err() {
    assert!(parse("SELECT ~ FROM t").is_err());
    assert!(parse("SELECT 'unterminated").is_err());
    assert!(parse("SELECT \"unterminated").is_err());
    assert!(parse("").is_err());
}
