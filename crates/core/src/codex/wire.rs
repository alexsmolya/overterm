//! Framing and message shapes for the Codex desktop app's local IPC bus.
//!
//! Every frame is a little-endian `u32` byte length followed by that many
//! bytes of JSON. The router in the desktop app assigns each client an id
//! on `initialize`, forwards requests to whichever client says it can
//! handle them, and fans broadcasts out to everyone else.

use serde_json::{Value, json};

/// The desktop app refuses frames above this, so a larger length can only
/// mean the stream is out of step.
pub const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// Version the desktop app stamps on `thread-stream-state-changed`. A
/// different number means the conversation shape may have changed under us.
pub const STREAM_STATE_VERSION: u64 = 11;

pub const STREAM_STATE_CHANGED: &str = "thread-stream-state-changed";
pub const CLIENT_STATUS_CHANGED: &str = "client-status-changed";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("frame length {0} is out of range")]
    BadLength(usize),
    #[error("frame is not JSON: {0}")]
    BadJson(String),
}

/// Requests oTerm sends, each with the version the desktop app checks it
/// against before handling it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Initialize,
    OwnerDiscovery,
    LoadHistory,
    StartTurn,
    SteerTurn,
    InterruptTurn,
    CommandApproval,
    FileApproval,
}

impl Method {
    pub fn name(self) -> &'static str {
        match self {
            Method::Initialize => "initialize",
            Method::OwnerDiscovery => "thread-owner-discovery",
            Method::LoadHistory => "thread-follower-load-complete-history",
            Method::StartTurn => "thread-follower-start-turn",
            Method::SteerTurn => "thread-follower-steer-turn",
            Method::InterruptTurn => "thread-follower-interrupt-turn",
            Method::CommandApproval => "thread-follower-command-approval-decision",
            Method::FileApproval => "thread-follower-file-approval-decision",
        }
    }

    /// Copied from the desktop app's own table. It rejects a request whose
    /// version does not match with `request-version-mismatch`.
    pub fn version(self) -> u64 {
        match self {
            Method::Initialize => 0,
            Method::OwnerDiscovery
            | Method::LoadHistory
            | Method::SteerTurn
            | Method::CommandApproval
            | Method::FileApproval => 1,
            Method::StartTurn => 2,
            Method::InterruptTurn => 4,
        }
    }
}

/// One decoded frame, sorted by what oTerm has to do with it.
#[derive(Debug, PartialEq)]
pub enum Incoming {
    Response {
        request_id: String,
        /// The client that answered. For a thread request this is the
        /// thread's owner, which is how its disappearance is noticed.
        handled_by: Option<String>,
        outcome: Result<Value, String>,
    },
    Broadcast {
        method: String,
        version: u64,
        params: Value,
    },
    /// The router asking whether this client can take a request. oTerm
    /// owns no threads, so the answer is always no.
    DiscoveryRequest { request_id: String },
    /// Anything else, including requests routed here by mistake.
    Other,
}

pub fn encode(message: &Value) -> Vec<u8> {
    let body = message.to_string().into_bytes();
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Reassembles frames from reads that split or join them arbitrarily.
#[derive(Default)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
}

impl FrameDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Value>, WireError> {
        self.buffer.extend_from_slice(bytes);
        let mut messages = Vec::new();
        let mut start = 0;
        while self.buffer.len() - start >= 4 {
            let prefix: [u8; 4] = self.buffer[start..start + 4]
                .try_into()
                .expect("four bytes");
            let length = u32::from_le_bytes(prefix) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(WireError::BadLength(length));
            }
            let end = start + 4 + length;
            if self.buffer.len() < end {
                break;
            }
            let message = serde_json::from_slice(&self.buffer[start + 4..end])
                .map_err(|e| WireError::BadJson(e.to_string()))?;
            messages.push(message);
            start = end;
        }
        self.buffer.drain(..start);
        Ok(messages)
    }
}

pub fn classify(message: Value) -> Incoming {
    let text = |key: &str| message.get(key).and_then(Value::as_str).map(str::to_owned);
    match text("type").as_deref() {
        Some("response") => {
            let Some(request_id) = text("requestId") else {
                return Incoming::Other;
            };
            let outcome = if text("resultType").as_deref() == Some("success") {
                Ok(message.get("result").cloned().unwrap_or(Value::Null))
            } else {
                Err(text("error").unwrap_or_else(|| "unknown error".into()))
            };
            Incoming::Response {
                request_id,
                handled_by: text("handledByClientId"),
                outcome,
            }
        }
        Some("broadcast") => match text("method") {
            Some(method) => Incoming::Broadcast {
                method,
                version: message.get("version").and_then(Value::as_u64).unwrap_or(0),
                params: message.get("params").cloned().unwrap_or(Value::Null),
            },
            None => Incoming::Other,
        },
        Some("client-discovery-request") => match text("requestId") {
            Some(request_id) => Incoming::DiscoveryRequest { request_id },
            None => Incoming::Other,
        },
        _ => Incoming::Other,
    }
}

pub fn request(
    request_id: &str,
    source_client_id: Option<&str>,
    method: Method,
    params: Value,
) -> Value {
    let mut message = json!({
        "type": "request",
        "requestId": request_id,
        "method": method.name(),
        "version": method.version(),
        "params": params,
    });
    if let Some(source) = source_client_id {
        message["sourceClientId"] = json!(source);
    }
    message
}

/// Tells the owner to start or stop sending this client the conversation.
pub fn following_changed(source_client_id: &str, conversation_id: &str, following: bool) -> Value {
    json!({
        "type": "broadcast",
        "method": "thread-stream-following-changed",
        "sourceClientId": source_client_id,
        "version": 1,
        "params": {"conversationId": conversation_id, "hostId": "local", "following": following},
    })
}

pub fn discovery_refusal(request_id: &str) -> Value {
    json!({
        "type": "client-discovery-response",
        "requestId": request_id,
        "response": {"canHandle": false},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(values: &[Value]) -> Vec<u8> {
        values.iter().flat_map(encode).collect()
    }

    #[test]
    fn encode_prefixes_the_json_with_its_little_endian_length() {
        let bytes = encode(&json!({"a": 1}));
        assert_eq!(&bytes[..4], &7u32.to_le_bytes());
        assert_eq!(&bytes[4..], br#"{"a":1}"#);
    }

    #[test]
    fn decoder_reassembles_split_and_joined_frames() {
        let bytes = frames(&[json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]);
        let mut decoder = FrameDecoder::default();
        let mut decoded = Vec::new();
        // Split mid-prefix and mid-body, and join the last two frames.
        for chunk in [&bytes[..2], &bytes[2..9], &bytes[9..]] {
            decoded.extend(decoder.push(chunk).unwrap());
        }
        assert_eq!(
            decoded,
            vec![json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]
        );
    }

    #[test]
    fn decoder_rejects_zero_and_oversized_lengths() {
        let mut decoder = FrameDecoder::default();
        assert_eq!(
            decoder.push(&0u32.to_le_bytes()),
            Err(WireError::BadLength(0))
        );
        let mut decoder = FrameDecoder::default();
        let too_big = (MAX_FRAME_BYTES as u32) + 1;
        assert_eq!(
            decoder.push(&too_big.to_le_bytes()),
            Err(WireError::BadLength(too_big as usize))
        );
    }

    #[test]
    fn decoder_rejects_a_body_that_is_not_json() {
        let mut bytes = 3u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(b"{no");
        assert!(matches!(
            FrameDecoder::default().push(&bytes),
            Err(WireError::BadJson(_))
        ));
    }

    #[test]
    fn classify_splits_responses_by_result_type() {
        let ok = classify(json!({
            "type": "response", "requestId": "r1", "resultType": "success",
            "method": "initialize", "handledByClientId": "owner", "result": {"clientId": "c1"}
        }));
        assert_eq!(
            ok,
            Incoming::Response {
                request_id: "r1".into(),
                handled_by: Some("owner".into()),
                outcome: Ok(json!({"clientId": "c1"})),
            }
        );
        let failed = classify(json!({
            "type": "response", "requestId": "r2", "resultType": "error", "error": "no-client-found"
        }));
        assert_eq!(
            failed,
            Incoming::Response {
                request_id: "r2".into(),
                handled_by: None,
                outcome: Err("no-client-found".into()),
            }
        );
    }

    #[test]
    fn classify_reads_broadcasts_and_discovery_requests() {
        let broadcast = classify(json!({
            "type": "broadcast", "method": STREAM_STATE_CHANGED, "version": 11,
            "sourceClientId": "owner", "params": {"conversationId": "t1"}
        }));
        assert_eq!(
            broadcast,
            Incoming::Broadcast {
                method: STREAM_STATE_CHANGED.into(),
                version: 11,
                params: json!({"conversationId": "t1"}),
            }
        );
        let discovery = classify(json!({
            "type": "client-discovery-request", "requestId": "d1",
            "request": {"method": "ide-context"}
        }));
        assert_eq!(
            discovery,
            Incoming::DiscoveryRequest {
                request_id: "d1".into()
            }
        );
        assert_eq!(
            classify(json!({"type": "request", "requestId": "x"})),
            Incoming::Other
        );
    }

    #[test]
    fn requests_carry_the_version_the_desktop_app_checks() {
        let start = request(
            "r1",
            Some("c1"),
            Method::StartTurn,
            json!({"conversationId": "t1"}),
        );
        assert_eq!(start["type"], "request");
        assert_eq!(start["requestId"], "r1");
        assert_eq!(start["sourceClientId"], "c1");
        assert_eq!(start["method"], "thread-follower-start-turn");
        assert_eq!(start["version"], 2);
        assert_eq!(start["params"], json!({"conversationId": "t1"}));
        // Interrupt is 4 only with an expectedTurnId, which oTerm always sends.
        assert_eq!(Method::InterruptTurn.version(), 4);
        let init = request(
            "r0",
            None,
            Method::Initialize,
            json!({"clientType": "overterm"}),
        );
        assert_eq!(init["version"], 0);
        assert!(init.get("sourceClientId").is_none());
    }

    #[test]
    fn following_changed_and_discovery_refusal_have_the_router_shape() {
        assert_eq!(
            following_changed("c1", "t1", true),
            json!({
                "type": "broadcast", "method": "thread-stream-following-changed",
                "sourceClientId": "c1", "version": 1,
                "params": {"conversationId": "t1", "hostId": "local", "following": true}
            })
        );
        assert_eq!(
            discovery_refusal("d1"),
            json!({"type": "client-discovery-response", "requestId": "d1", "response": {"canHandle": false}})
        );
    }
}
