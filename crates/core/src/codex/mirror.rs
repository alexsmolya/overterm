//! A local copy of one conversation, kept current from the owner's stream.
//!
//! The desktop app keeps its conversation state in immer and publishes the
//! patches immer produces, so applying them here follows immer's rules:
//! paths are lists of object keys and array indices, `add` on an array
//! inserts, and a `replace` of an array's `length` truncates it.

use serde_json::Value;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MirrorError {
    #[error("patches arrived before any snapshot")]
    NoSnapshot,
    #[error("patches were based on revision {got}, but the mirror is at {expected}")]
    RevisionGap { expected: u64, got: u64 },
    #[error("patch does not fit the mirrored state: {0}")]
    BadPatch(String),
    #[error("stream change is malformed: {0}")]
    Malformed(String),
}

#[derive(Debug, Default)]
pub struct Mirror {
    current: Option<(u64, Value)>,
}

impl Mirror {
    pub fn state(&self) -> Option<&Value> {
        self.current.as_ref().map(|(_, state)| state)
    }

    pub fn revision(&self) -> Option<u64> {
        self.current.as_ref().map(|(revision, _)| *revision)
    }

    /// Apply the `change` object of a `thread-stream-state-changed`
    /// broadcast.
    ///
    /// On any error the mirror is emptied rather than left half-patched.
    /// The only way back is a fresh snapshot, which the owner sends when a
    /// follower asks it for the thread's history again.
    pub fn apply(&mut self, change: &Value) -> Result<(), MirrorError> {
        let result = self.try_apply(change);
        if result.is_err() {
            self.current = None;
        }
        result
    }

    fn try_apply(&mut self, change: &Value) -> Result<(), MirrorError> {
        let revision = number(change, "revision")?;
        match change.get("type").and_then(Value::as_str) {
            Some("snapshot") => {
                let state = change
                    .get("conversationState")
                    .ok_or_else(|| MirrorError::Malformed("snapshot without state".into()))?;
                self.current = Some((revision, state.clone()));
                Ok(())
            }
            Some("patches") => {
                let base = number(change, "baseRevision")?;
                let (current_revision, state) =
                    self.current.as_mut().ok_or(MirrorError::NoSnapshot)?;
                if base != *current_revision {
                    return Err(MirrorError::RevisionGap {
                        expected: *current_revision,
                        got: base,
                    });
                }
                let list = change
                    .get("patches")
                    .and_then(Value::as_array)
                    .ok_or_else(|| MirrorError::Malformed("patches is not a list".into()))?;
                for patch in list {
                    apply_patch(state, patch)?;
                }
                *current_revision = revision;
                Ok(())
            }
            other => Err(MirrorError::Malformed(format!("change type {other:?}"))),
        }
    }
}

fn number(change: &Value, key: &str) -> Result<u64, MirrorError> {
    change
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| MirrorError::Malformed(format!("missing {key}")))
}

fn apply_patch(state: &mut Value, patch: &Value) -> Result<(), MirrorError> {
    let bad = |why: &str| MirrorError::BadPatch(format!("{why}: {patch}"));
    let op = patch
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| bad("no op"))?;
    let path = patch
        .get("path")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("no path"))?;
    let value = || patch.get("value").cloned().ok_or_else(|| bad("no value"));
    let Some((last, parents)) = path.split_last() else {
        return match op {
            "replace" => {
                *state = value()?;
                Ok(())
            }
            _ => Err(bad("empty path")),
        };
    };
    let mut parent = state;
    for key in parents {
        parent = match parent {
            Value::Object(map) => key.as_str().and_then(|k| map.get_mut(k)),
            Value::Array(list) => index(key).and_then(|i| list.get_mut(i)),
            _ => None,
        }
        .ok_or_else(|| bad("missing parent"))?;
    }
    match parent {
        Value::Object(map) => {
            let key = last.as_str().ok_or_else(|| bad("non-string key"))?;
            match op {
                "add" | "replace" => {
                    map.insert(key.to_owned(), value()?);
                }
                // Removing a key that is already gone leaves the same state
                // immer would have, so it is not worth a resync.
                "remove" => {
                    map.remove(key);
                }
                _ => return Err(bad("unknown op")),
            }
        }
        Value::Array(list) => {
            if last.as_str() == Some("length") && op == "replace" {
                let length = value()?
                    .as_u64()
                    .ok_or_else(|| bad("length is not a number"))?;
                let length = usize::try_from(length).map_err(|_| bad("length too large"))?;
                if length > list.len() {
                    return Err(bad("length grows the array"));
                }
                list.truncate(length);
                return Ok(());
            }
            let at = index(last).ok_or_else(|| bad("non-index key"))?;
            match op {
                "add" if at <= list.len() => list.insert(at, value()?),
                "replace" if at < list.len() => list[at] = value()?,
                "remove" if at < list.len() => {
                    list.remove(at);
                }
                _ => return Err(bad("index out of range or unknown op")),
            }
        }
        _ => return Err(bad("parent is not a container")),
    }
    Ok(())
}

/// Immer writes array indices as numbers, but a numeric string addresses
/// the same element, so both are accepted.
fn index(key: &Value) -> Option<usize> {
    match key {
        Value::Number(n) => n.as_u64().and_then(|n| usize::try_from(n).ok()),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot(revision: u64, state: Value) -> Value {
        json!({"type": "snapshot", "revision": revision, "conversationState": state})
    }

    fn patches(base: u64, revision: u64, patches: Value) -> Value {
        json!({"type": "patches", "baseRevision": base, "revision": revision, "patches": patches})
    }

    fn mirrored(state: Value) -> Mirror {
        let mut mirror = Mirror::default();
        mirror.apply(&snapshot(1, state)).unwrap();
        mirror
    }

    #[test]
    fn snapshot_replaces_whatever_was_there() {
        let mut mirror = mirrored(json!({"a": 1}));
        mirror.apply(&snapshot(7, json!({"b": 2}))).unwrap();
        assert_eq!(mirror.state(), Some(&json!({"b": 2})));
        assert_eq!(mirror.revision(), Some(7));
    }

    #[test]
    fn object_patches_add_replace_and_remove_keys() {
        let mut mirror = mirrored(json!({"status": {"type": "idle"}, "old": true}));
        mirror
            .apply(&patches(
                1,
                2,
                json!([
                    {"op": "replace", "path": ["status"], "value": {"type": "active"}},
                    {"op": "add", "path": ["title"], "value": "New"},
                    {"op": "remove", "path": ["old"]}
                ]),
            ))
            .unwrap();
        assert_eq!(
            mirror.state(),
            Some(&json!({"status": {"type": "active"}, "title": "New"}))
        );
        assert_eq!(mirror.revision(), Some(2));
    }

    #[test]
    fn array_patches_insert_set_remove_and_truncate() {
        let mut mirror = mirrored(json!({"items": ["a", "c", "d", "e"]}));
        mirror
            .apply(&patches(
                1,
                2,
                json!([
                    {"op": "add", "path": ["items", 1], "value": "b"},
                    {"op": "replace", "path": ["items", 0], "value": "A"},
                    {"op": "remove", "path": ["items", 4]},
                    {"op": "replace", "path": ["items", "length"], "value": 3}
                ]),
            ))
            .unwrap();
        assert_eq!(mirror.state(), Some(&json!({"items": ["A", "b", "c"]})));
    }

    #[test]
    fn patches_before_a_snapshot_are_refused() {
        let mut mirror = Mirror::default();
        assert_eq!(
            mirror.apply(&patches(0, 1, json!([]))),
            Err(MirrorError::NoSnapshot)
        );
    }

    #[test]
    fn a_revision_gap_empties_the_mirror() {
        let mut mirror = mirrored(json!({"a": 1}));
        assert_eq!(
            mirror.apply(&patches(4, 5, json!([]))),
            Err(MirrorError::RevisionGap {
                expected: 1,
                got: 4
            })
        );
        assert_eq!(mirror.state(), None);
    }

    #[test]
    fn a_patch_into_a_missing_parent_empties_the_mirror() {
        let mut mirror = mirrored(json!({"a": 1}));
        let result = mirror.apply(&patches(
            1,
            2,
            json!([{"op": "replace", "path": ["missing", "x"], "value": 1}]),
        ));
        assert!(matches!(result, Err(MirrorError::BadPatch(_))));
        assert_eq!(mirror.state(), None);
    }

    #[test]
    fn unknown_change_types_are_malformed() {
        let mut mirror = mirrored(json!({}));
        assert!(matches!(
            mirror.apply(&json!({"type": "rewind"})),
            Err(MirrorError::Malformed(_))
        ));
    }

    /// The fixture is a real recording of the desktop app running a turn
    /// that asked for approval and was declined. It includes the full
    /// snapshots the owner sent along the way, so each one checks that the
    /// patches before it rebuilt the exact same state.
    #[test]
    fn recorded_patches_rebuild_every_later_snapshot() {
        let recording = include_str!("../../fixtures/codex-approval-declined.jsonl");
        let mut mirror = Mirror::default();
        let mut snapshots_checked = 0;
        for line in recording.lines() {
            let frame: Value = serde_json::from_str(line).unwrap();
            let change = &frame["params"]["change"];
            if change["type"] == "snapshot" && mirror.state().is_some() {
                assert_eq!(mirror.state(), Some(&change["conversationState"]));
                snapshots_checked += 1;
            }
            mirror.apply(change).unwrap();
        }
        assert_eq!(snapshots_checked, 3);
    }
}
