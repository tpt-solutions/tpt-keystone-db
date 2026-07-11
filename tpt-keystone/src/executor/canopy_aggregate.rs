//! Canopy's "MongoDB-compatible aggregation pipeline" roadmap item:
//! `$match`/`$group`/`$project`/`$sort`/`$limit`/`$skip` stages over a
//! table's rows treated as documents, driven by a real JSON pipeline
//! (`serde_json`, already a dependency via `storage::jsonb`) rather than a
//! from-scratch Mongo-syntax grammar. Exposed to SQL as the
//! `aggregate('table', '<json pipeline array>')` table-valued function
//! (`executor::graph_fn`), following the exact precedent
//! `graph_neighbors`/`vector_search`/`hybrid_search` already established
//! for "a non-relational query shape surfaced as a FROM-clause function
//! that returns ordinary rows and composes with `WHERE`/`JOIN`/`ORDER BY`."
//!
//! Honest scope, matching the roadmap note this replaces: SQL's own
//! `WHERE`/`GROUP BY`/projection plus `->>`/`#>>` JSON operators already
//! cover most of what these stages do for a single `Json` column's
//! extracted scalars — this pipeline is for the cases where the
//! Mongo-shaped *pipeline itself* (a JSON array of stages, e.g. generated
//! by a client library or ported from an existing Mongo aggregation) is
//! what a caller has in hand, not a claim that every Mongo operator exists
//! here. Implemented: `$match` (equality plus `$eq`/`$ne`/`$gt`/`$gte`/
//! `$lt`/`$lte`/`$in`), `$group` (`_id` as a `$field` reference or a
//! sub-document of multiple field references, accumulators `$sum`/`$avg`/
//! `$min`/`$max`/`$count`/`$push`/`$first`), `$project` (field inclusion/
//! exclusion/rename via `$field` references, or a literal value), `$sort`
//! (multi-key, `1`/`-1`), `$limit`, `$skip`. NOT implemented: computed
//! expression operators inside `$project` (`$add`/`$multiply`/`$concat`/
//! etc.), `$unwind`, `$lookup`, `$facet`, and every other pipeline stage —
//! an unrecognized stage name errors clearly rather than silently
//! no-op'ing.

use anyhow::{bail, Result};
use serde_json::{Map, Value as Json};
use std::cmp::Ordering;

pub type Doc = Map<String, Json>;

/// Runs `stages` (a JSON array of one-key stage objects, e.g.
/// `[{"$match": {...}}, {"$group": {...}}]`) over `docs` in order.
pub fn run_pipeline(mut docs: Vec<Doc>, stages: &[Json]) -> Result<Vec<Doc>> {
    for stage in stages {
        let obj = stage
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("aggregate: each pipeline stage must be a JSON object"))?;
        anyhow::ensure!(
            obj.len() == 1,
            "aggregate: each pipeline stage must have exactly one operator key, got {}",
            obj.len()
        );
        let (op, spec) = obj.iter().next().unwrap();
        docs = match op.as_str() {
            "$match" => apply_match(docs, spec)?,
            "$group" => apply_group(docs, spec)?,
            "$project" => apply_project(docs, spec)?,
            "$sort" => apply_sort(docs, spec)?,
            "$limit" => apply_limit(docs, spec)?,
            "$skip" => apply_skip(docs, spec)?,
            other => bail!("aggregate: unsupported pipeline stage \"{other}\""),
        };
    }
    Ok(docs)
}

// ---------------------------------------------------------------- $match --

fn apply_match(docs: Vec<Doc>, spec: &Json) -> Result<Vec<Doc>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("aggregate: $match spec must be an object"))?;
    Ok(docs.into_iter().filter(|d| doc_matches(d, spec)).collect())
}

fn doc_matches(doc: &Doc, spec: &Map<String, Json>) -> bool {
    spec.iter().all(|(field, cond)| {
        let actual = doc.get(field).cloned().unwrap_or(Json::Null);
        match cond {
            Json::Object(ops) if ops.keys().any(|k| k.starts_with('$')) => {
                ops.iter().all(|(op, rhs)| eval_match_op(op, &actual, rhs))
            }
            other => &actual == other,
        }
    })
}

fn eval_match_op(op: &str, actual: &Json, rhs: &Json) -> bool {
    match op {
        "$eq" => actual == rhs,
        "$ne" => actual != rhs,
        "$gt" => json_cmp(actual, rhs) == Some(Ordering::Greater),
        "$gte" => matches!(json_cmp(actual, rhs), Some(Ordering::Greater | Ordering::Equal)),
        "$lt" => json_cmp(actual, rhs) == Some(Ordering::Less),
        "$lte" => matches!(json_cmp(actual, rhs), Some(Ordering::Less | Ordering::Equal)),
        "$in" => rhs.as_array().is_some_and(|a| a.contains(actual)),
        "$nin" => rhs.as_array().is_none_or(|a| !a.contains(actual)),
        _ => false,
    }
}

/// Numeric-or-string ordering between two JSON scalars; `None` for
/// incomparable types (e.g. comparing an object to a number).
fn json_cmp(a: &Json, b: &Json) -> Option<Ordering> {
    match (a, b) {
        (Json::Number(x), Json::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
        (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// ---------------------------------------------------------------- $group --

/// Resolves a `_id`/accumulator-argument expression against one document:
/// a `"$field"` string dereferences that field, an object recursively
/// resolves each of its values (for compound `_id` specs), anything else is
/// a literal.
fn resolve_ref(doc: &Doc, expr: &Json) -> Json {
    match expr {
        Json::String(s) if s.starts_with('$') => {
            doc.get(&s[1..]).cloned().unwrap_or(Json::Null)
        }
        Json::Object(fields) => {
            let mut out = Map::new();
            for (k, v) in fields {
                out.insert(k.clone(), resolve_ref(doc, v));
            }
            Json::Object(out)
        }
        other => other.clone(),
    }
}

fn apply_group(docs: Vec<Doc>, spec: &Json) -> Result<Vec<Doc>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("aggregate: $group spec must be an object"))?;
    let id_expr = spec
        .get("_id")
        .ok_or_else(|| anyhow::anyhow!("aggregate: $group requires an \"_id\" field"))?;
    let accumulators: Vec<(&String, &Json)> = spec.iter().filter(|(k, _)| *k != "_id").collect();

    // Group while preserving first-seen group order (a stable, deterministic
    // result given the input row order — a caller wanting a specific final
    // order composes this with an outer `$sort` stage or SQL `ORDER BY`,
    // same "this function returns ordinary rows" precedent as
    // `graph_neighbors`).
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, (Json, Vec<&Doc>)> = std::collections::HashMap::new();
    for doc in &docs {
        let key_json = resolve_ref(doc, id_expr);
        let key = serde_json::to_string(&key_json).unwrap_or_default();
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups
            .entry(key)
            .or_insert_with(|| (key_json, Vec::new()))
            .1
            .push(doc);
    }

    let mut out = Vec::with_capacity(order.len());
    for key in order {
        let (id_val, members) = groups.remove(&key).unwrap();
        let mut result = Map::new();
        result.insert("_id".to_string(), id_val);
        for (field, acc_spec) in &accumulators {
            result.insert((*field).clone(), apply_accumulator(acc_spec, &members)?);
        }
        out.push(result);
    }
    Ok(out)
}

fn apply_accumulator(acc_spec: &Json, members: &[&Doc]) -> Result<Json> {
    let obj = acc_spec
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("aggregate: accumulator spec must be an object, e.g. {{\"$sum\": \"$amount\"}}"))?;
    anyhow::ensure!(obj.len() == 1, "aggregate: accumulator spec must have exactly one operator");
    let (op, arg) = obj.iter().next().unwrap();

    let numeric_values = || -> Vec<f64> {
        members
            .iter()
            .filter_map(|d| resolve_ref(d, arg).as_f64().or_else(|| arg.as_f64()))
            .collect()
    };

    match op.as_str() {
        "$sum" => {
            // `{"$sum": 1}` (a literal, not a `$field` ref) counts members,
            // matching Mongo's idiomatic "count via $sum" pattern.
            let total: f64 = if let Some(c) = arg.as_f64() {
                if matches!(arg, Json::String(_)) {
                    numeric_values().into_iter().sum()
                } else {
                    c * members.len() as f64
                }
            } else {
                numeric_values().into_iter().sum()
            };
            Ok(num(total))
        }
        "$avg" => {
            let vals = numeric_values();
            if vals.is_empty() {
                Ok(Json::Null)
            } else {
                Ok(num(vals.iter().sum::<f64>() / vals.len() as f64))
            }
        }
        "$min" => Ok(members
            .iter()
            .map(|d| resolve_ref(d, arg))
            .min_by(|a, b| json_cmp(a, b).unwrap_or(Ordering::Equal))
            .unwrap_or(Json::Null)),
        "$max" => Ok(members
            .iter()
            .map(|d| resolve_ref(d, arg))
            .max_by(|a, b| json_cmp(a, b).unwrap_or(Ordering::Equal))
            .unwrap_or(Json::Null)),
        "$count" => Ok(num(members.len() as f64)),
        "$first" => Ok(members.first().map(|d| resolve_ref(d, arg)).unwrap_or(Json::Null)),
        "$push" => Ok(Json::Array(members.iter().map(|d| resolve_ref(d, arg)).collect())),
        other => bail!("aggregate: unsupported $group accumulator \"{other}\""),
    }
}

fn num(f: f64) -> Json {
    serde_json::Number::from_f64(f)
        .map(Json::Number)
        .unwrap_or(Json::Null)
}

// -------------------------------------------------------------- $project --

fn apply_project(docs: Vec<Doc>, spec: &Json) -> Result<Vec<Doc>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("aggregate: $project spec must be an object"))?;
    Ok(docs.into_iter().map(|d| project_one(&d, spec)).collect())
}

fn project_one(doc: &Doc, spec: &Map<String, Json>) -> Doc {
    let mut out = Map::new();
    for (key, directive) in spec {
        match directive {
            Json::Bool(true) => {
                if let Some(v) = doc.get(key) {
                    out.insert(key.clone(), v.clone());
                }
            }
            Json::Number(n) if n.as_f64() == Some(1.0) => {
                if let Some(v) = doc.get(key) {
                    out.insert(key.clone(), v.clone());
                }
            }
            Json::Bool(false) => {}
            Json::Number(n) if n.as_f64() == Some(0.0) => {}
            Json::String(s) if s.starts_with('$') => {
                if let Some(v) = doc.get(&s[1..]) {
                    out.insert(key.clone(), v.clone());
                }
            }
            literal => {
                out.insert(key.clone(), literal.clone());
            }
        }
    }
    // Mongo default: `_id` passes through unless the spec explicitly
    // mentions it (inclusion or exclusion).
    if !spec.contains_key("_id") {
        if let Some(id) = doc.get("_id") {
            out.insert("_id".to_string(), id.clone());
        }
    }
    out
}

// ----------------------------------------------------------------- $sort --

fn apply_sort(mut docs: Vec<Doc>, spec: &Json) -> Result<Vec<Doc>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("aggregate: $sort spec must be an object"))?;
    let keys: Vec<(String, i64)> = spec
        .iter()
        .map(|(k, v)| (k.clone(), v.as_i64().unwrap_or(1)))
        .collect();
    docs.sort_by(|a, b| {
        for (field, dir) in &keys {
            let av = a.get(field).cloned().unwrap_or(Json::Null);
            let bv = b.get(field).cloned().unwrap_or(Json::Null);
            if let Some(ord) = json_cmp(&av, &bv) {
                let ord = if *dir < 0 { ord.reverse() } else { ord };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
        }
        Ordering::Equal
    });
    Ok(docs)
}

// ---------------------------------------------------------- $limit/$skip --

fn apply_limit(docs: Vec<Doc>, spec: &Json) -> Result<Vec<Doc>> {
    let n = spec
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("aggregate: $limit spec must be a non-negative integer"))?
        as usize;
    Ok(docs.into_iter().take(n).collect())
}

fn apply_skip(docs: Vec<Doc>, spec: &Json) -> Result<Vec<Doc>> {
    let n = spec
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("aggregate: $skip spec must be a non-negative integer"))?
        as usize;
    Ok(docs.into_iter().skip(n).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(v: Json) -> Doc {
        v.as_object().unwrap().clone()
    }

    fn sample_docs() -> Vec<Doc> {
        vec![
            doc(json!({"dept": "eng", "name": "a", "amount": 100})),
            doc(json!({"dept": "eng", "name": "b", "amount": 200})),
            doc(json!({"dept": "sales", "name": "c", "amount": 50})),
            doc(json!({"dept": "sales", "name": "d", "amount": 150})),
        ]
    }

    #[test]
    fn match_filters_by_equality_and_operators() {
        let stages = vec![json!({"$match": {"dept": "eng"}})];
        let out = run_pipeline(sample_docs(), &stages).unwrap();
        assert_eq!(out.len(), 2);

        let stages = vec![json!({"$match": {"amount": {"$gt": 100}}})];
        let out = run_pipeline(sample_docs(), &stages).unwrap();
        let names: Vec<&str> = out.iter().map(|d| d["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["b", "d"]);
    }

    #[test]
    fn group_sums_and_counts_per_key() {
        let stages = vec![json!({
            "$group": {
                "_id": "$dept",
                "total": {"$sum": "$amount"},
                "n": {"$count": {}},
                "avg": {"$avg": "$amount"},
            }
        })];
        let out = run_pipeline(sample_docs(), &stages).unwrap();
        assert_eq!(out.len(), 2);
        let eng = out.iter().find(|d| d["_id"] == json!("eng")).unwrap();
        assert_eq!(eng["total"], json!(300.0));
        assert_eq!(eng["n"], json!(2.0));
        assert_eq!(eng["avg"], json!(150.0));
    }

    #[test]
    fn group_count_via_sum_one_literal() {
        let stages = vec![json!({
            "$group": {"_id": "$dept", "n": {"$sum": 1}}
        })];
        let out = run_pipeline(sample_docs(), &stages).unwrap();
        let eng = out.iter().find(|d| d["_id"] == json!("eng")).unwrap();
        assert_eq!(eng["n"], json!(2.0));
    }

    #[test]
    fn project_includes_renames_and_keeps_id() {
        let grouped = run_pipeline(
            sample_docs(),
            &[json!({"$group": {"_id": "$dept", "total": {"$sum": "$amount"}}})],
        )
        .unwrap();
        let projected = run_pipeline(
            grouped,
            &[json!({"$project": {"department": "$_id", "total": 1}})],
        )
        .unwrap();
        for d in &projected {
            assert!(d.contains_key("department"));
            assert!(d.contains_key("total"));
            assert!(d.contains_key("_id")); // not excluded, so passes through
        }
    }

    #[test]
    fn sort_limit_skip_compose() {
        let stages = vec![
            json!({"$sort": {"amount": -1}}),
            json!({"$skip": 1}),
            json!({"$limit": 2}),
        ];
        let out = run_pipeline(sample_docs(), &stages).unwrap();
        let names: Vec<&str> = out.iter().map(|d| d["name"].as_str().unwrap()).collect();
        // Sorted desc by amount: b(200), d(150), a(100), c(50) -> skip 1, take 2
        assert_eq!(names, vec!["d", "a"]);
    }

    #[test]
    fn full_pipeline_match_group_sort() {
        let stages = vec![
            json!({"$match": {"amount": {"$gte": 100}}}),
            json!({"$group": {"_id": "$dept", "total": {"$sum": "$amount"}}}),
            json!({"$sort": {"total": -1}}),
        ];
        let out = run_pipeline(sample_docs(), &stages).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["_id"], json!("eng")); // 300 > 150
        assert_eq!(out[0]["total"], json!(300.0));
    }

    #[test]
    fn unknown_stage_errors_rather_than_silently_passing_through() {
        let stages = vec![json!({"$unwind": "$tags"})];
        let err = run_pipeline(sample_docs(), &stages).unwrap_err();
        assert!(err.to_string().contains("$unwind"));
    }
}
