//! Read-only client for a separately owned `codex app-server --stdio` child.
//!
//! This deliberately has no generic "send RPC" command. The allowlist below is
//! the complete surface exposed to the webview; write-capable Codex methods
//! cannot be emitted by this module.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use overterm_core::{AgentState, Signal, StateChange};
use serde::Serialize;
use serde_json::{Value, json};
use tauri::{AppHandle, Emitter, State};

use crate::choreograph::Choreographer;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_GRACE: Duration = Duration::from_millis(750);
const THREAD_LIMIT: u64 = 20;
const STDERR_BUFFER: usize = 1024;
const CODEX_SESSION_ID: &str = "codex-readonly";
const EVENT_CODEX_STATUS: &str = "overterm://codex-status";

type PendingResponses = Arc<Mutex<HashMap<u64, Sender<Result<Value, RequestFailure>>>>>;

/// There must never be a write method in this list. Keep it public to make
/// that privacy boundary mechanically testable.
pub const READ_ONLY_METHODS: &[&str] = &[
    "initialize",
    "thread/list",
    "thread/read",
    "thread/resume",
    "thread/unsubscribe",
];

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSummary {
    pub id: String,
    pub title: String,
    pub workspace: Option<String>,
    pub updated_at: Option<u64>,
    pub status: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexStatus {
    pub available: bool,
    pub connected: bool,
    pub subscribed_thread_id: Option<String>,
    pub thread_status: String,
    pub detail: String,
    pub cause: String,
    pub notifications: Vec<String>,
}

impl Default for CodexStatus {
    fn default() -> Self {
        Self {
            available: false,
            connected: false,
            subscribed_thread_id: None,
            thread_status: "disconnected".into(),
            detail: "Codex is off".into(),
            cause: "disconnected".into(),
            notifications: Vec::new(),
        }
    }
}

#[derive(Debug)]
enum Incoming {
    Response {
        id: u64,
        result: Option<Value>,
        error: Option<Value>,
    },
    Notification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: u64,
        method: String,
        params: Value,
    },
    Malformed,
}

/// JSONL framing is independent from process I/O so fragmented and coalesced
/// reads can be tested without a real Codex installation.
#[cfg(test)]
#[derive(Default)]
struct FrameDecoder {
    pending: String,
}

#[cfg(test)]
impl FrameDecoder {
    fn push(&mut self, chunk: &str) -> Vec<Incoming> {
        self.pending.push_str(chunk);
        let mut messages = Vec::new();
        while let Some(end) = self.pending.find('\n') {
            let line = self.pending[..end].to_owned();
            self.pending.drain(..=end);
            if line.is_empty() {
                continue;
            }
            messages.push(match serde_json::from_str::<Value>(&line) {
                Ok(value) => classify(value),
                Err(_) => Incoming::Malformed,
            });
        }
        messages
    }
}

fn drain_stderr(stderr: ChildStderr) {
    let mut reader = BufReader::new(stderr);
    // stderr is intentionally consumed so a noisy child cannot block. Its
    // contents may contain private tool diagnostics, so a fixed-size buffer
    // discards them continuously instead of retaining or logging them.
    let mut discarded = [0_u8; STDERR_BUFFER];
    while let Ok(read) = reader.read(&mut discarded) {
        if read == 0 {
            break;
        }
    }
}

fn classify(value: Value) -> Incoming {
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);
    match (value.get("id").and_then(Value::as_u64), method) {
        (Some(id), None) => Incoming::Response {
            id,
            result: value.get("result").cloned(),
            error: value.get("error").cloned(),
        },
        (Some(id), Some(method)) => Incoming::ServerRequest {
            id,
            method: bounded_method(&method),
            params: value.get("params").cloned().unwrap_or(Value::Null),
        },
        (None, Some(method)) => Incoming::Notification {
            method: bounded_method(&method),
            params: value.get("params").cloned().unwrap_or(Value::Null),
        },
        _ => Incoming::Malformed,
    }
}

fn bounded_method(method: &str) -> String {
    method
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-'))
        .take(80)
        .collect()
}

fn pump_stdout(
    stdout: ChildStdout,
    pending: PendingResponses,
    orphaned: Arc<Mutex<HashMap<u64, Result<Value, RequestFailure>>>>,
    live: Arc<Mutex<LiveState>>,
    observer: Arc<Mutex<Option<LiveObserver>>>,
    child: Arc<Mutex<Option<Child>>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
) {
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let item = match line.and_then(|line| {
            serde_json::from_str::<Value>(&line)
                .map(classify)
                .map_err(std::io::Error::other)
        }) {
            Ok(item) => item,
            Err(_) => Incoming::Malformed,
        };
        match item {
            Incoming::Response { id, result, error } => {
                let reply = match (result, error) {
                    (Some(result), None) => Ok(result),
                    (_, Some(_)) => Err(RequestFailure::Rejected(
                        "Codex integration request was rejected".into(),
                    )),
                    _ => Err(RequestFailure::Ambiguous(
                        "Codex integration incompatible response".into(),
                    )),
                };
                if let Some(tx) = pending.lock().unwrap().remove(&id) {
                    let _ = tx.send(reply);
                } else {
                    orphaned.lock().unwrap().insert(id, reply);
                }
            }
            Incoming::Notification { method, params } => {
                live.lock().unwrap().observe_notification(&method, &params);
                notify_observer(&live, &observer);
            }
            Incoming::ServerRequest { id, method, params } => {
                live.lock().unwrap().observe_request(id, &method, &params);
                notify_observer(&live, &observer);
            }
            Incoming::Malformed => {
                live.lock().unwrap().detach("unavailable");
                notify_observer(&live, &observer);
                let mut pending = pending.lock().unwrap();
                for (_, tx) in pending.drain() {
                    let _ = tx.send(Err(RequestFailure::Ambiguous(
                        "Codex integration incompatible: malformed protocol frame".into(),
                    )));
                }
                stdin.lock().unwrap().take();
                if let Some(mut child) = child.lock().unwrap().take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                return;
            }
        }
    }
    live.lock().unwrap().detach("unavailable");
    notify_observer(&live, &observer);
    stdin.lock().unwrap().take();
    if let Some(mut child) = child.lock().unwrap().take() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let mut pending = pending.lock().unwrap();
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(RequestFailure::Ambiguous(
            "Codex integration connection closed".into(),
        )));
    }
}

fn notify_observer(live: &Arc<Mutex<LiveState>>, observer: &Arc<Mutex<Option<LiveObserver>>>) {
    let snapshot = live.lock().unwrap().snapshot();
    let observer = observer.lock().unwrap().clone();
    if let Some(observer) = observer {
        observer(snapshot);
    }
}

struct Client {
    child: Arc<Mutex<Option<Child>>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    next_id: u64,
    pending: PendingResponses,
    orphaned: Arc<Mutex<HashMap<u64, Result<Value, RequestFailure>>>>,
    requests: VecDeque<String>,
    subscribed: Option<String>,
    live: Arc<Mutex<LiveState>>,
    observer: Arc<Mutex<Option<LiveObserver>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RequestFailure {
    Rejected(String),
    Ambiguous(String),
}

/// The only Codex protocol state retained after a frame is read. In
/// particular this deliberately has no item, message, tool, command, path,
/// diff, or approval payload fields.
#[derive(Debug)]
struct LiveState {
    selected: Option<String>,
    state: AgentState,
    cause: String,
    active_turn: Option<String>,
    pending_requests: HashSet<String>,
    connected: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveSnapshot {
    selected_thread_id: Option<String>,
    state: AgentState,
    cause: String,
    connected: bool,
}

type LiveObserver = Arc<dyn Fn(LiveSnapshot) + Send + Sync>;

impl Default for LiveState {
    fn default() -> Self {
        Self {
            selected: None,
            state: AgentState::Idle,
            cause: "connected".into(),
            active_turn: None,
            pending_requests: HashSet::new(),
            connected: true,
        }
    }
}

impl LiveState {
    fn snapshot(&self) -> LiveSnapshot {
        LiveSnapshot {
            selected_thread_id: self.selected.clone(),
            state: self.state,
            cause: self.cause.clone(),
            connected: self.connected,
        }
    }
    fn select(&mut self, thread_id: &str) {
        self.selected = Some(thread_id.to_string());
        self.state = AgentState::Idle;
        self.cause = "connected".into();
        self.active_turn = None;
        self.pending_requests.clear();
        self.connected = true;
    }

    fn detach(&mut self, cause: &str) {
        self.selected = None;
        self.active_turn = None;
        self.pending_requests.clear();
        self.state = AgentState::Idle;
        self.cause = cause.into();
        self.connected = false;
    }

    fn selected(&self, params: &Value) -> bool {
        self.selected.as_deref() == params.get("threadId").and_then(Value::as_str)
    }

    fn set_state(&mut self, state: AgentState, cause: &str) {
        self.state = state;
        self.cause = cause.into();
    }

    fn observe_notification(&mut self, method: &str, params: &Value) {
        if !self.selected(params) {
            return;
        }
        match method {
            "turn/started" => {
                self.active_turn = params
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                if self.pending_requests.is_empty() {
                    self.set_state(AgentState::Busy, "active");
                }
            }
            "turn/completed" => match params
                .get("turn")
                .and_then(|turn| turn.get("status"))
                .and_then(Value::as_str)
            {
                Some("completed") => {
                    self.active_turn = None;
                    self.pending_requests.clear();
                    self.set_state(AgentState::Done, "completed");
                }
                Some("interrupted") => {
                    self.active_turn = None;
                    self.pending_requests.clear();
                    self.set_state(AgentState::Done, "interrupted");
                }
                Some("failed") => {
                    self.active_turn = None;
                    self.pending_requests.clear();
                    self.set_state(AgentState::Done, "failed");
                }
                _ => {}
            },
            "thread/status/changed" => {
                let status = params.get("status");
                let active = status
                    .and_then(|value| value.get("type"))
                    .and_then(Value::as_str)
                    == Some("active");
                let waiting = status
                    .and_then(|value| value.get("activeFlags"))
                    .and_then(Value::as_array)
                    .is_some_and(|flags| {
                        flags.iter().any(|flag| {
                            matches!(
                                flag.as_str(),
                                Some("waitingOnApproval" | "waitingOnUserInput")
                            )
                        })
                    });
                if waiting || !self.pending_requests.is_empty() {
                    self.set_state(AgentState::NeedsInput, "needs input");
                } else if active {
                    self.set_state(AgentState::Busy, "active");
                } else if status
                    .and_then(|value| value.get("type"))
                    .and_then(Value::as_str)
                    == Some("idle")
                {
                    // An attached historical idle thread is not a completed
                    // turn, so it must not create a fresh done cue.
                    self.set_state(AgentState::Idle, "idle");
                }
            }
            "serverRequest/resolved" => {
                let Some(id) = params.get("requestId") else {
                    return;
                };
                self.pending_requests.remove(&request_id(id));
                if self.pending_requests.is_empty() && self.active_turn.is_some() {
                    self.set_state(AgentState::Busy, "active");
                }
            }
            "thread/closed" => self.detach("unavailable"),
            _ => {}
        }
    }

    fn observe_request(&mut self, id: u64, method: &str, params: &Value) {
        if !is_user_decision(method) || !self.selected(params) {
            return;
        }
        self.pending_requests.insert(id.to_string());
        self.set_state(AgentState::NeedsInput, "needs input");
    }
}

fn request_id(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn is_user_decision(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "item/tool/requestUserInput"
            | "applyPatchApproval"
            | "execCommandApproval"
    )
}

impl RequestFailure {
    fn message(&self) -> &str {
        match self {
            Self::Rejected(message) | Self::Ambiguous(message) => message,
        }
    }
}

impl Client {
    fn publish_live(&self) {
        notify_observer(&self.live, &self.observer);
    }

    fn set_observer(&mut self, app: AppHandle, choreo: Choreographer) {
        let previous = Arc::new(Mutex::new(AgentState::Idle));
        *self.observer.lock().unwrap() = Some(Arc::new(move |snapshot| {
            let _ = app.emit(EVENT_CODEX_STATUS, snapshot.clone());
            if snapshot.selected_thread_id.is_none() || !snapshot.connected {
                choreo.remove_session(CODEX_SESSION_ID);
                return;
            }
            let mut previous = previous.lock().unwrap();
            if *previous == snapshot.state {
                return;
            }
            if snapshot.state == AgentState::Busy {
                // A resumed Desktop turn was explicitly started by the user,
                // even though its input did not originate in oTerm.
                choreo.on_submit(&app, CODEX_SESSION_ID);
            }
            let cause = match snapshot.state {
                AgentState::Busy => Signal::HookSubmit,
                AgentState::NeedsInput => Signal::HookNotification,
                AgentState::Done => Signal::HookStop,
                AgentState::Idle => Signal::FullScreenExited,
            };
            choreo.on_state_change(
                &app,
                CODEX_SESSION_ID,
                &StateChange {
                    from: *previous,
                    to: snapshot.state,
                    cause,
                },
            );
            *previous = snapshot.state;
        }));
    }

    fn start() -> Result<Self, String> {
        Self::start_command("codex", ["app-server", "--stdio"])
    }

    fn start_command<const N: usize>(program: &str, args: [&str; N]) -> Result<Self, String> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    String::from("Codex integration unavailable: `codex` was not found")
                } else {
                    String::from("Codex integration unavailable: could not start app-server")
                }
            })?;
        // A partial pipe setup is still an owned child. Reap it before
        // returning so even a platform-level pipe failure cannot leak one.
        let Some(stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Codex integration unavailable: stdin".into());
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Codex integration unavailable: stdout".into());
        };
        let Some(stderr) = child.stderr.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Codex integration unavailable: stderr".into());
        };
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let orphaned = Arc::new(Mutex::new(HashMap::new()));
        let live = Arc::new(Mutex::new(LiveState::default()));
        let observer = Arc::new(Mutex::new(None));
        let child = Arc::new(Mutex::new(Some(child)));
        let stdin = Arc::new(Mutex::new(Some(stdin)));
        thread::spawn({
            let pending = pending.clone();
            let orphaned = orphaned.clone();
            let live = live.clone();
            let observer = observer.clone();
            let child = child.clone();
            let stdin = stdin.clone();
            move || pump_stdout(stdout, pending, orphaned, live, observer, child, stdin)
        });
        thread::spawn(move || drain_stderr(stderr));
        let mut client = Self {
            child,
            stdin,
            next_id: 1,
            pending,
            orphaned,
            requests: VecDeque::new(),
            subscribed: None,
            live,
            observer,
        };
        let init = match client.request("initialize", json!({"clientInfo":{"name":"overterm","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":false,"optOutNotificationMethods":["item/started","item/completed","item/agentMessage/delta","item/reasoning/textDelta","item/commandExecution/outputDelta","turn/diff/updated"]}})) {
            Ok(response) => response,
            Err(error) => {
                client.shutdown();
                return Err(error.message().to_string());
            }
        };
        if init.get("platformOs").and_then(Value::as_str).is_none() {
            client.shutdown();
            return Err("Codex integration incompatible: invalid initialize response".into());
        }
        if let Err(error) = client.notify("initialized", json!({})) {
            client.shutdown();
            return Err(error.message().to_string());
        }
        Ok(client)
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<(), RequestFailure> {
        // `initialized` is the only notification this client may emit.
        if method != "initialized" {
            return Err(RequestFailure::Rejected(
                "Codex integration rejected a non-read-only notification".into(),
            ));
        }
        self.write(json!({"jsonrpc":"2.0","method":method,"params":params}))
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value, RequestFailure> {
        if !READ_ONLY_METHODS.contains(&method) {
            return Err(RequestFailure::Rejected(
                "Codex integration rejected a write-capable method".into(),
            ));
        }
        if self.requests.len() == 16 {
            self.requests.pop_front();
        }
        self.requests.push_back(method.to_string());
        let id = self.next_id;
        self.next_id += 1;
        let (tx, rx) = mpsc::channel();
        if let Some(reply) = self.orphaned.lock().unwrap().remove(&id) {
            return reply;
        }
        self.pending.lock().unwrap().insert(id, tx);
        if let Err(error) =
            self.write(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
        {
            self.pending.lock().unwrap().remove(&id);
            return Err(error);
        }
        self.await_response(id, rx)
    }

    fn write(&mut self, value: Value) -> Result<(), RequestFailure> {
        let mut stdin = self.stdin.lock().unwrap();
        let Some(stdin) = stdin.as_mut() else {
            return Err(RequestFailure::Ambiguous(
                "Codex integration connection closed".into(),
            ));
        };
        serde_json::to_writer(&mut *stdin, &value)
            .map_err(|_| RequestFailure::Ambiguous("Codex integration protocol error".into()))?;
        stdin
            .write_all(b"\n")
            .and_then(|_| stdin.flush())
            .map_err(|_| RequestFailure::Ambiguous("Codex integration connection closed".into()))
    }

    fn await_response(
        &mut self,
        wanted: u64,
        rx: mpsc::Receiver<Result<Value, RequestFailure>>,
    ) -> Result<Value, RequestFailure> {
        match rx.recv_timeout(REQUEST_TIMEOUT) {
            Ok(response) => response,
            Err(RecvTimeoutError::Timeout) => {
                self.pending.lock().unwrap().remove(&wanted);
                Err(RequestFailure::Ambiguous(
                    "Codex integration request timed out".into(),
                ))
            }
            Err(RecvTimeoutError::Disconnected) => Err(RequestFailure::Ambiguous(
                "Codex integration connection closed".into(),
            )),
        }
    }

    fn list(&mut self) -> Result<Vec<ThreadSummary>, RequestFailure> {
        let value = self.request(
            "thread/list",
            json!({"limit":THREAD_LIMIT,"archived":false,"useStateDbOnly":true}),
        )?;
        Ok(value
            .get("data")
            .and_then(Value::as_array)
            .unwrap_or(&Vec::new())
            .iter()
            .take(THREAD_LIMIT as usize)
            .filter_map(thread_summary)
            .collect())
    }

    fn attach(&mut self, id: &str) -> Result<(), RequestFailure> {
        if self.subscribed.as_deref() == Some(id) {
            return Ok(());
        }
        // Release the current subscription first. A failed unsubscribe leaves
        // the old ID recorded and aborts the replacement, so the client never
        // claims a different single-subscription state than the server knows.
        if let Some(current) = self.subscribed.clone() {
            self.request("thread/unsubscribe", json!({"threadId":current}))?;
            self.subscribed = None;
            self.live.lock().unwrap().detach("connected");
            self.publish_live();
        }
        self.request("thread/read", json!({"threadId":id,"includeTurns":false}))?;
        self.live.lock().unwrap().select(id);
        if let Err(error) =
            self.request("thread/resume", json!({"threadId":id,"excludeTurns":true}))
        {
            self.live.lock().unwrap().detach("unavailable");
            self.publish_live();
            return Err(error);
        }
        self.subscribed = Some(id.to_string());
        Ok(())
    }

    fn detach(&mut self) -> Result<(), RequestFailure> {
        if let Some(id) = self.subscribed.clone() {
            self.request("thread/unsubscribe", json!({"threadId":id}))?;
            self.subscribed = None;
            self.live.lock().unwrap().detach("disconnected");
            self.publish_live();
        }
        Ok(())
    }

    fn shutdown(mut self) {
        let _ = self.detach();
        self.close_owned();
    }

    fn close_owned(&mut self) {
        self.stdin.lock().unwrap().take();
        let Some(mut child) = self.child.lock().unwrap().take() else {
            return;
        };
        let deadline = Instant::now() + EXIT_GRACE;
        while Instant::now() < deadline {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => thread::sleep(Duration::from_millis(25)),
            }
        }
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Close the transport without attempting another RPC. This is required
    /// when a request outcome is ambiguous: the server may already have
    /// applied it, so connection closure is the only safe subscription reset.
    fn invalidate(mut self) {
        self.live.lock().unwrap().detach("unavailable");
        self.publish_live();
        self.close_owned();
    }
}

fn thread_summary(value: &Value) -> Option<ThreadSummary> {
    let id = value.get("id")?.as_str()?.to_string();
    let title = value
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Untitled thread")
        .chars()
        .take(120)
        .collect();
    let workspace = value
        .get("cwd")
        .and_then(Value::as_str)
        .and_then(|p| p.rsplit('/').next())
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(80).collect());
    let status = value
        .get("status")
        .and_then(|v| v.get("type"))
        .and_then(Value::as_str)
        .map(bounded_method)
        .unwrap_or_else(|| "unknown".into());
    Some(ThreadSummary {
        id,
        title,
        workspace,
        updated_at: value.get("updatedAt").and_then(Value::as_u64),
        status,
    })
}

#[derive(Default)]
pub struct CodexSessions(Arc<Mutex<Option<Client>>>);

impl Drop for CodexSessions {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.0.lock()
            && let Some(client) = guard.take()
        {
            client.shutdown();
        }
    }
}

fn with_client<T>(
    sessions: &CodexSessions,
    f: impl FnOnce(&mut Client) -> Result<T, RequestFailure>,
) -> Result<T, String> {
    let mut guard = sessions
        .0
        .lock()
        .map_err(|_| "Codex integration unavailable".to_string())?;
    if guard
        .as_ref()
        .is_some_and(|client| !client.live.lock().unwrap().connected)
        && let Some(client) = guard.take()
    {
        client.invalidate();
    }
    if guard.is_none() {
        *guard = Some(Client::start()?);
    }
    match f(guard.as_mut().expect("set above")) {
        Ok(value) => Ok(value),
        Err(error) => {
            if matches!(error, RequestFailure::Ambiguous(_))
                && let Some(client) = guard.take()
            {
                client.invalidate();
            }
            Err(error.message().to_string())
        }
    }
}

#[tauri::command]
pub fn codex_list_threads(
    sessions: State<'_, CodexSessions>,
) -> Result<Vec<ThreadSummary>, String> {
    with_client(&sessions, Client::list)
}

#[tauri::command]
pub fn codex_attach_thread(
    thread_id: String,
    app: AppHandle,
    sessions: State<'_, CodexSessions>,
    choreo: State<'_, Choreographer>,
) -> Result<(), String> {
    let live = with_client(&sessions, |client| {
        client.set_observer(app.clone(), choreo.inner().clone());
        client.attach(&thread_id)?;
        Ok(client.live.clone())
    })?;
    let live_for_check = live.clone();
    choreo.add_session(
        &app,
        CODEX_SESSION_ID,
        Arc::new(move || live_for_check.lock().unwrap().state == AgentState::Busy),
    );
    let snapshot = live.lock().unwrap().snapshot();
    if snapshot.state != AgentState::Idle {
        if snapshot.state == AgentState::Busy {
            choreo.on_submit(&app, CODEX_SESSION_ID);
        }
        let cause = match snapshot.state {
            AgentState::Busy => Signal::HookSubmit,
            AgentState::NeedsInput => Signal::HookNotification,
            AgentState::Done => Signal::HookStop,
            AgentState::Idle => Signal::FullScreenExited,
        };
        choreo.on_state_change(
            &app,
            CODEX_SESSION_ID,
            &StateChange {
                from: AgentState::Idle,
                to: snapshot.state,
                cause,
            },
        );
    }
    Ok(())
}

#[tauri::command]
pub fn codex_detach(
    sessions: State<'_, CodexSessions>,
    choreo: State<'_, Choreographer>,
) -> Result<(), String> {
    let mut guard = sessions
        .0
        .lock()
        .map_err(|_| "Codex integration unavailable".to_string())?;
    if let Some(client) = guard.take() {
        client.shutdown();
    }
    choreo.remove_session(CODEX_SESSION_ID);
    Ok(())
}

#[tauri::command]
pub fn codex_status(sessions: State<'_, CodexSessions>) -> CodexStatus {
    let Ok(guard) = sessions.0.lock() else {
        return CodexStatus::default();
    };
    guard
        .as_ref()
        .map(|client| {
            let live = client.live.lock().unwrap();
            CodexStatus {
                available: true,
                connected: live.connected,
                subscribed_thread_id: client
                    .subscribed
                    .clone()
                    .filter(|_| live.selected.is_some()),
                thread_status: match live.state {
                    AgentState::Idle => "idle",
                    AgentState::Busy => "busy",
                    AgentState::NeedsInput => "needsInput",
                    AgentState::Done => "done",
                }
                .into(),
                detail: if live.selected.is_some() {
                    format!("Read-only attachment {}", live.cause)
                } else if live.connected {
                    "Connected; choose a thread".into()
                } else {
                    "Codex attachment unavailable".into()
                },
                cause: live.cause.clone(),
                notifications: Vec::new(),
            }
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn write_methods_are_structurally_absent() {
        for method in READ_ONLY_METHODS {
            assert!(
                !method.starts_with("turn/")
                    && !matches!(
                        *method,
                        "thread/archive" | "thread/delete" | "thread/rollback" | "thread/fork"
                    )
            );
        }
    }
    #[test]
    fn classification_distinguishes_messages() {
        assert!(matches!(
            classify(json!({"id":7,"result":{}})),
            Incoming::Response { id: 7, .. }
        ));
        assert!(matches!(
            classify(json!({"method":"thread/status/changed","params":{}})),
            Incoming::Notification { .. }
        ));
        assert!(matches!(
            classify(json!({"id":9,"method":"approval/request","params":{}})),
            Incoming::ServerRequest { .. }
        ));
    }
    #[test]
    fn thread_metadata_drops_preview_and_home_path() {
        let t=thread_summary(&json!({"id":"fixture-thread","name":"Fixture","preview":"do not retain","cwd":"/private/fixture/project","updatedAt":1,"status":{"type":"idle"}})).unwrap();
        assert_eq!(t.workspace.as_deref(), Some("project"));
        assert_eq!(t.title, "Fixture");
        assert_eq!(t.status, "idle");
    }
    #[test]
    fn malformed_frame_is_safe() {
        assert!(serde_json::from_str::<Value>("{").err().is_some());
    }

    #[test]
    fn framing_handles_fragmented_and_coalesced_jsonl() {
        let mut decoder = FrameDecoder::default();
        assert!(decoder.push("{\"id\":1,").is_empty());
        let first =
            decoder.push("\"result\":{}}\n{\"method\":\"thread/status/changed\",\"params\":{}}\n");
        assert!(matches!(first[0], Incoming::Response { id: 1, .. }));
        assert!(matches!(first[1], Incoming::Notification { .. }));
    }

    #[test]
    fn unknown_notifications_and_server_requests_are_never_reinterpreted() {
        let unknown = classify(json!({"method":"future/event","params":{"secret":"ignored"}}));
        assert!(
            matches!(unknown, Incoming::Notification { method, .. } if method == "future/event")
        );
        let request =
            classify(json!({"id":2,"method":"approval/request","params":{"secret":"ignored"}}));
        assert!(
            matches!(request, Incoming::ServerRequest { method, .. } if method == "approval/request")
        );
    }

    #[test]
    fn bounded_method_never_keeps_untrusted_payload_text() {
        assert_eq!(bounded_method("bad method : payload"), "badmethodpayload");
    }

    #[cfg(unix)]
    #[test]
    fn fake_server_exercises_read_only_lifecycle_without_a_real_codex_home() {
        // The fixture neither reads HOME nor reaches a network. Each response is
        // deliberately tiny and contains metadata only; the notification arrives
        // before initialize's response to prove requests do not block the stream.
        let script = concat!(
            "printf '%s\\n' '",
            "{\"method\":\"thread/status/changed\",\"params\":{\"status\":{\"type\":\"idle\"}}}",
            "' '{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; ",
            "read _; read _; printf '%s\\n' '{\"id\":2,\"result\":{\"data\":[{\"id\":\"fixture\",\"name\":\"Fixture\",\"cwd\":\"/fixture/project\",\"status\":{\"type\":\"idle\"}}]}}'; ",
            "read _; printf '%s\\n' '{\"id\":3,\"result\":{\"thread\":{\"id\":\"fixture\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":4,\"result\":{\"thread\":{\"id\":\"fixture\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":5,\"result\":{\"status\":\"unsubscribed\"}}'; sleep 1"
        );
        let mut client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        let threads = client.list().expect("bounded list");
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].workspace.as_deref(), Some("project"));
        client.attach("fixture").expect("metadata-only resume");
        assert_eq!(client.subscribed.as_deref(), Some("fixture"));
        assert_eq!(client.live.lock().unwrap().state, AgentState::Idle);
        client.detach().expect("unsubscribe");
        assert!(client.subscribed.is_none());
        client.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn switching_threads_balances_subscriptions_and_keeps_failures_fail_closed() {
        // The fixture answers the exact A -> unsubscribe A -> B ->
        // unsubscribe B sequence; the request log below proves those
        // transitions were emitted in that order.
        let script = concat!(
            "printf '%s\\n' '{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; ",
            "read _; read _; printf '%s\\n' '{\"id\":2,\"result\":{\"thread\":{\"id\":\"A\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":3,\"result\":{\"thread\":{\"id\":\"A\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":4,\"result\":{}}'; ",
            "read _; printf '%s\\n' '{\"id\":5,\"result\":{\"thread\":{\"id\":\"B\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":6,\"result\":{\"thread\":{\"id\":\"B\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":7,\"result\":{}}'; sleep 1"
        );
        let mut client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        client.attach("A").expect("attach A");
        assert_eq!(client.subscribed.as_deref(), Some("A"));
        client.attach("B").expect("switch A to B");
        assert_eq!(client.subscribed.as_deref(), Some("B"));
        client.detach().expect("detach B");
        assert!(client.subscribed.is_none());
        assert_eq!(
            client.requests.iter().collect::<Vec<_>>(),
            [
                &"initialize".to_string(),
                &"thread/read".to_string(),
                &"thread/resume".to_string(),
                &"thread/unsubscribe".to_string(),
                &"thread/read".to_string(),
                &"thread/resume".to_string(),
                &"thread/unsubscribe".to_string(),
            ]
        );
        client.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn failed_unsubscribe_keeps_the_old_subscription_recorded() {
        let script = concat!(
            "printf '%s\\n' '{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; ",
            "read _; read _; printf '%s\\n' '{\"id\":2,\"result\":{}}'; ",
            "read _; printf '%s\\n' '{\"id\":3,\"result\":{}}'; ",
            "read _; printf '%s\\n' '{\"id\":4,\"error\":{\"code\":-32000}}'; sleep 10"
        );
        let mut client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        client.attach("A").expect("attach A");
        assert!(client.attach("B").is_err());
        assert_eq!(client.subscribed.as_deref(), Some("A"));
        client.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn failed_replacement_resume_leaves_no_subscription_claimed() {
        let script = concat!(
            "printf '%s\\n' '{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; ",
            "read _; read _; printf '%s\\n' '{\"id\":2,\"result\":{}}'; ",
            "read _; printf '%s\\n' '{\"id\":3,\"result\":{}}'; ",
            "read _; printf '%s\\n' '{\"id\":4,\"result\":{\"thread\":{\"id\":\"B\"}}}'; ",
            "read _; printf '%s\\n' '{\"id\":5,\"error\":{\"code\":-32000}}'; sleep 10"
        );
        let mut client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        client.attach("A").expect("attach A");
        assert!(client.attach("B").is_err());
        assert!(client.subscribed.is_none());
        client.shutdown();
    }

    #[test]
    fn live_state_scopes_and_sanitizes_selected_thread_events() {
        let mut live = LiveState::default();
        live.select("A");
        live.observe_notification(
            "turn/started",
            &json!({"threadId":"B","turn":{"id":"other"}}),
        );
        assert_eq!(live.state, AgentState::Idle);
        live.observe_notification(
            "turn/started",
            &json!({"threadId":"A","turn":{"id":"turn-a"}}),
        );
        assert_eq!(live.state, AgentState::Busy);
        live.observe_notification(
            "thread/status/changed",
            &json!({"threadId":"A","status":{"type":"active","activeFlags":["waitingOnApproval"]}}),
        );
        assert_eq!(live.state, AgentState::NeedsInput);
        live.observe_notification("turn/completed", &json!({"threadId":"B","turn":{"id":"other","status":"failed","error":{"message":"secret"}}}));
        assert_eq!(live.state, AgentState::NeedsInput);
        for status in ["completed", "interrupted", "failed"] {
            live.observe_notification("turn/completed", &json!({"threadId":"A","turn":{"id":"turn-a","status":status,"error":{"message":"secret"}}}));
            assert_eq!(live.state, AgentState::Done);
            assert_eq!(live.cause, status);
            live.select("A");
        }
        let snapshot = live.snapshot();
        let rendered = serde_json::to_string(&snapshot).unwrap();
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("command"));
    }

    #[test]
    fn pending_user_decisions_require_matching_resolution() {
        let mut live = LiveState::default();
        live.select("A");
        let sensitive = json!({"threadId":"A","turnId":"turn-a","command":"secret","cwd":"/private/path","body":"hidden"});
        live.observe_request(7, "item/commandExecution/requestApproval", &sensitive);
        live.observe_request(8, "item/tool/requestUserInput", &sensitive);
        assert_eq!(live.state, AgentState::NeedsInput);
        assert_eq!(live.pending_requests.len(), 2);
        live.active_turn = Some("turn-a".into());
        live.observe_notification(
            "serverRequest/resolved",
            &json!({"threadId":"A","requestId":7}),
        );
        assert_eq!(live.state, AgentState::NeedsInput);
        live.observe_notification(
            "serverRequest/resolved",
            &json!({"threadId":"B","requestId":8}),
        );
        assert_eq!(live.state, AgentState::NeedsInput);
        live.observe_notification(
            "serverRequest/resolved",
            &json!({"threadId":"A","requestId":8}),
        );
        assert_eq!(live.state, AgentState::Busy);
        assert!(live.pending_requests.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn pump_observes_notification_while_no_request_is_pending() {
        let script = concat!(
            "printf '%s\\n' '{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; ",
            "read _; read _; printf '%s\\n' '{\"id\":2,\"result\":{}}'; ",
            "read _; printf '%s\\n' '{\"id\":3,\"result\":{}}'; ",
            "sleep 0.1; printf '%s\\n' '{\"method\":\"turn/started\",\"params\":{\"threadId\":\"A\",\"turn\":{\"id\":\"turn-a\"}}}'; sleep 10"
        );
        let mut client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        client.attach("A").expect("attach");
        thread::sleep(Duration::from_millis(250));
        assert_eq!(client.live.lock().unwrap().state, AgentState::Busy);
        client.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn ambiguous_resume_timeout_invalidates_session_and_reaps_child() {
        let marker = std::env::temp_dir().join(format!(
            "overterm-codex-ambiguous-resume-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&marker);
        let script = format!(
            // Gate the initialization response on consuming the request so the
            // fixture cannot race Client::start_command with an unsolicited
            // response before the request/pump handshake is established.
            "read _; printf '%s\\n' '{{\"id\":1,\"result\":{{\"platformOs\":\"linux\"}}}}'; \\
             read _; read _; printf '%s\\n' '{{\"id\":2,\"result\":{{\"thread\":{{\"id\":\"A\"}}}}}}'; \\
             read _; printf '%s\\n' '{{\"id\":3,\"result\":{{\"thread\":{{\"id\":\"A\"}}}}}}'; \\
             read _; printf '%s\\n' '{{\"id\":4,\"result\":{{}}}}'; \\
             read _; printf '%s\\n' '{{\"id\":5,\"result\":{{\"thread\":{{\"id\":\"B\"}}}}}}'; \\
             read _; echo $$ > '{}'; sleep 30",
            marker.display()
        );
        let client = Client::start_command("/bin/sh", ["-c", &script]).expect("handshake");
        let child_pid = client.child.lock().unwrap().as_ref().unwrap().id();
        let sessions = CodexSessions(Arc::new(Mutex::new(Some(client))));
        with_client(&sessions, |client| client.attach("A")).expect("attach A");
        let started = Instant::now();
        let error = with_client(&sessions, |client| client.attach("B"))
            .expect_err("withheld resume response should time out");
        assert_eq!(error, "Codex integration request timed out");
        assert!(started.elapsed() >= REQUEST_TIMEOUT);
        assert!(marker.exists(), "fixture must consume thread/resume(B)");
        assert!(
            sessions.0.lock().unwrap().is_none(),
            "ambiguous outcome must not leave a client with no recorded subscription"
        );
        assert!(
            !Command::new("kill")
                .args(["-0", &child_pid.to_string()])
                .stderr(Stdio::null())
                .status()
                .expect("check child liveness")
                .success(),
            "owned app-server child must be reaped"
        );
        let _ = std::fs::remove_file(marker);
    }

    #[test]
    fn missing_executable_is_non_fatal() {
        let error = match Client::start_command("overterm-codex-does-not-exist", []) {
            Ok(client) => {
                client.shutdown();
                panic!("unexpectedly started a missing executable");
            }
            Err(error) => error,
        };
        assert_eq!(
            error,
            "Codex integration unavailable: `codex` was not found"
        );
    }

    #[cfg(unix)]
    #[test]
    fn invalid_initialize_response_is_rejected_and_owned_child_is_reaped() {
        let started = Instant::now();
        let error = match Client::start_command(
            "/bin/sh",
            ["-c", "printf '%s\\n' '{\"id\":1,\"result\":{}}'; sleep 10"],
        ) {
            Ok(client) => {
                client.shutdown();
                panic!("accepted incompatible initialize response");
            }
            Err(error) => error,
        };
        assert_eq!(
            error,
            "Codex integration incompatible: invalid initialize response"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "owned child should have been force-reaped after the grace period"
        );
    }

    #[cfg(unix)]
    #[test]
    fn malformed_json_is_a_controlled_incompatibility() {
        let error =
            match Client::start_command("/bin/sh", ["-c", "printf '%s\\n' '{not-json}'; sleep 10"])
            {
                Ok(client) => {
                    client.shutdown();
                    panic!("accepted malformed protocol frame");
                }
                Err(error) => error,
            };
        assert_eq!(
            error,
            "Codex integration incompatible: malformed protocol frame"
        );
    }

    #[cfg(unix)]
    #[test]
    fn eof_and_timeout_are_controlled_errors() {
        let eof = match Client::start_command("/bin/sh", ["-c", "read _; exit 7"]) {
            Ok(client) => {
                client.shutdown();
                panic!("server exited before initialize");
            }
            Err(error) => error,
        };
        assert_eq!(eof, "Codex integration connection closed");

        let script = concat!(
            "printf '%s\\n' '{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; ",
            "read _; read _; sleep 10"
        );
        let mut client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        let started = Instant::now();
        assert_eq!(
            client.list().unwrap_err(),
            RequestFailure::Ambiguous("Codex integration request timed out".into())
        );
        assert!(started.elapsed() >= REQUEST_TIMEOUT);
        client.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn out_of_order_response_is_retained_while_notifications_interleave() {
        let script = concat!(
            "printf '%s\\n' '{\"id\":99,\"result\":{}}' ",
            "'{\"method\":\"future/event\",\"params\":{}}' ",
            "'{\"id\":1,\"result\":{\"platformOs\":\"linux\"}}'; read _; sleep 10"
        );
        let client = Client::start_command("/bin/sh", ["-c", script]).expect("handshake");
        assert!(client.orphaned.lock().unwrap().contains_key(&99));
        client.shutdown();
    }
}
