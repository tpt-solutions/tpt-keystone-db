//! Canopy (Phase 10) JSON Schema validation — a hand-written subset of the
//! JSON Schema spec (not the full `draft-2020-12` conformance suite, same
//! "real but scoped" discipline as Meridian's OGC subset): `type`,
//! `required`, `properties`, `items`, `enum`, `minimum`/`maximum`,
//! `minLength`/`maxLength`. Anything else in a schema document is ignored
//! rather than rejected, so a schema copied from a real JSON Schema tool
//! still loads (with the unsupported keywords silently unenforced).

use serde_json::Value;

/// How strictly a table's `json_schema_mode` enforces its rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Every violation (type mismatch, missing required field, enum
    /// mismatch, out-of-range) is rejected.
    Strict,
    /// Only a top-level `type` mismatch is rejected — nested
    /// `properties`/`required`/`enum` rules are not checked. Lets a
    /// collection accept documents that are "roughly" schema-shaped
    /// without a hard reject on unanticipated fields.
    Relaxed,
    /// The rule is stored (round-trips through `\d`/`pg_dump`-style
    /// introspection) but never evaluated.
    Off,
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "strict" => Some(Self::Strict),
            "relaxed" => Some(Self::Relaxed),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

/// Validate `instance` against `schema` under `mode`. Returns a list of
/// human-readable violation messages — empty means valid. `mode == Off`
/// always returns no violations.
pub fn validate(schema: &Value, instance: &Value, mode: Mode) -> Vec<String> {
    if mode == Mode::Off {
        return Vec::new();
    }
    let mut errors = Vec::new();
    validate_at(schema, instance, "$", mode, &mut errors);
    errors
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn type_matches(declared: &str, instance: &Value) -> bool {
    match declared {
        "integer" => matches!(instance, Value::Number(n) if n.is_i64() || n.is_u64()),
        "number" => matches!(instance, Value::Number(_)),
        "string" => matches!(instance, Value::String(_)),
        "boolean" => matches!(instance, Value::Bool(_)),
        "array" => matches!(instance, Value::Array(_)),
        "object" => matches!(instance, Value::Object(_)),
        "null" => matches!(instance, Value::Null),
        _ => true, // unknown declared type name: don't reject
    }
}

fn validate_at(schema: &Value, instance: &Value, path: &str, mode: Mode, errors: &mut Vec<String>) {
    let Value::Object(schema) = schema else {
        return;
    };

    if let Some(Value::String(t)) = schema.get("type") {
        if !type_matches(t, instance) {
            errors.push(format!(
                "{path}: expected type \"{t}\", got \"{}\"",
                json_type_name(instance)
            ));
            return; // further checks on a type-mismatched value aren't meaningful
        }
    }

    if mode == Mode::Relaxed {
        return; // only the top-level type check above applies in relaxed mode
    }

    if let Some(Value::Array(allowed)) = schema.get("enum") {
        if !allowed.iter().any(|v| v == instance) {
            errors.push(format!(
                "{path}: value is not one of the allowed enum values"
            ));
        }
    }

    if let Value::Object(obj) = instance {
        if let Some(Value::Array(required)) = schema.get("required") {
            for req in required {
                if let Value::String(name) = req {
                    if !obj.contains_key(name) {
                        errors.push(format!("{path}: missing required property \"{name}\""));
                    }
                }
            }
        }
        if let Some(Value::Object(props)) = schema.get("properties") {
            for (key, sub_schema) in props {
                if let Some(sub_instance) = obj.get(key) {
                    validate_at(
                        sub_schema,
                        sub_instance,
                        &format!("{path}.{key}"),
                        mode,
                        errors,
                    );
                }
            }
        }
    }

    if let Value::Array(items) = instance {
        if let Some(item_schema) = schema.get("items") {
            for (i, item) in items.iter().enumerate() {
                validate_at(item_schema, item, &format!("{path}[{i}]"), mode, errors);
            }
        }
    }

    if let Value::Number(n) = instance {
        if let Some(min) = schema.get("minimum").and_then(|v| v.as_f64()) {
            if n.as_f64().is_some_and(|v| v < min) {
                errors.push(format!("{path}: {n} is less than minimum {min}"));
            }
        }
        if let Some(max) = schema.get("maximum").and_then(|v| v.as_f64()) {
            if n.as_f64().is_some_and(|v| v > max) {
                errors.push(format!("{path}: {n} is greater than maximum {max}"));
            }
        }
    }

    if let Value::String(s) = instance {
        if let Some(min_len) = schema.get("minLength").and_then(|v| v.as_u64()) {
            if (s.len() as u64) < min_len {
                errors.push(format!("{path}: string shorter than minLength {min_len}"));
            }
        }
        if let Some(max_len) = schema.get("maxLength").and_then(|v| v.as_u64()) {
            if (s.len() as u64) > max_len {
                errors.push(format!("{path}: string longer than maxLength {max_len}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> Value {
        json!({
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "age": {"type": "integer", "minimum": 0, "maximum": 150},
                "role": {"type": "string", "enum": ["admin", "user"]},
            }
        })
    }

    #[test]
    fn strict_accepts_valid_document() {
        let doc = json!({"name": "Ada", "age": 30, "role": "admin"});
        assert!(validate(&schema(), &doc, Mode::Strict).is_empty());
    }

    #[test]
    fn strict_rejects_missing_required_field() {
        let doc = json!({"name": "Ada"});
        let errors = validate(&schema(), &doc, Mode::Strict);
        assert!(errors.iter().any(|e| e.contains("age")));
    }

    #[test]
    fn strict_rejects_type_mismatch_and_enum_violation() {
        let doc = json!({"name": "Ada", "age": "thirty", "role": "superuser"});
        let errors = validate(&schema(), &doc, Mode::Strict);
        assert!(errors.iter().any(|e| e.contains("age")));
        assert!(errors.iter().any(|e| e.contains("role")));
    }

    #[test]
    fn strict_rejects_out_of_range_number() {
        let doc = json!({"name": "Ada", "age": 200});
        let errors = validate(&schema(), &doc, Mode::Strict);
        assert!(errors.iter().any(|e| e.contains("maximum")));
    }

    #[test]
    fn relaxed_ignores_missing_required_and_enum() {
        let doc = json!({"name": "Ada"});
        assert!(validate(&schema(), &doc, Mode::Relaxed).is_empty());
    }

    #[test]
    fn relaxed_still_rejects_top_level_type_mismatch() {
        let errors = validate(&schema(), &json!("not an object"), Mode::Relaxed);
        assert!(!errors.is_empty());
    }

    #[test]
    fn off_mode_never_reports_violations() {
        assert!(validate(&schema(), &json!("not an object"), Mode::Off).is_empty());
    }
}
