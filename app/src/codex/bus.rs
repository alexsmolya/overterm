//! A client on the Codex desktop app's IPC bus.
//!
//! One reader thread owns the socket's read side. It hands responses back
//! to whichever call is waiting, refuses every request the router offers
//! (oTerm owns no threads), and passes stream changes to the sink of the
//! conversation they belong to. Sinks run on that thread, so they must not
//! make a request and wait for it: the answer would arrive on the thread
//! that is busy waiting.

use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use overterm_core::codex::wire::{
    self, CLIENT_STATUS_CHANGED, FrameDecoder, Incoming, Method, STREAM_STATE_CHANGED,
    STREAM_STATE_VERSION,
};
use serde_json::{Value, json};

/// The router gives up on a forwarded request after ten seconds, so
/// waiting longer than that only delays the same answer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum StreamEvent {
    /// The `change` object of a stream broadcast: a snapshot or patches.
    Change(Value),
    /// The desktop app speaks a newer version of the stream than this
    /// build understands.
    Incompatible,
    /// The client that owned the thread left the bus, as happens when the
    /// desktop app reloads a window. The thread may come back under a new
    /// owner.
    OwnerGone,
    /// The bus itself is gone: the desktop app quit.
    Disconnected,
}

pub type StreamSink = Arc<dyn Fn(StreamEvent) + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BusError {
    /// No bus to connect to, which means the desktop app is not running.
    NotRunning,
    /// Nobody on the bus has this thread open.
    NoOwner,
    /// The desktop app rejected a request's version.
    Incompatible,
    Refused(String),
    TimedOut,
    Closed,
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BusError::NotRunning => write!(f, "The Codex desktop app is not running."),
            BusError::NoOwner => write!(f, "The Codex desktop app does not have this thread open."),
            BusError::Incompatible => write!(
                f,
                "This version of the Codex desktop app is not supported yet."
            ),
            BusError::Refused(why) => write!(f, "Codex refused the request: {why}"),
            BusError::TimedOut => write!(f, "Codex did not answer in time."),
            BusError::Closed => write!(f, "The Codex desktop app closed the connection."),
        }
    }
}

/// Where the desktop app listens. It honours `CODEX_HOME` like the CLI.
pub fn socket_path(codex_home: Option<PathBuf>, home: Option<PathBuf>) -> Option<PathBuf> {
    codex_home
        .or_else(|| home.map(|home| home.join(".codex")))
        .map(|dir| dir.join("ipc").join("ipc.sock"))
}

pub struct Bus {
    shared: Arc<Shared>,
    client_id: OnceLock<String>,
}

struct Shared {
    writer: Mutex<UnixStream>,
    pending: Mutex<HashMap<String, Sender<Reply>>>,
    streams: Mutex<HashMap<String, StreamSink>>,
    /// Which client answered each followed thread's history request.
    owners: Mutex<HashMap<String, String>>,
    alive: AtomicBool,
}

type Reply = (Option<String>, Result<Value, String>);

impl Bus {
    pub fn connect(path: &Path) -> Result<Bus, BusError> {
        // A socket file left behind by a crashed app refuses connections,
        // which means the same thing to the user as no file at all.
        let stream = UnixStream::connect(path).map_err(|_| BusError::NotRunning)?;
        Bus::over(stream)
    }

    pub fn over(stream: UnixStream) -> Result<Bus, BusError> {
        let reader = stream.try_clone().map_err(|_| BusError::Closed)?;
        let shared = Arc::new(Shared {
            writer: Mutex::new(stream),
            pending: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            owners: Mutex::new(HashMap::new()),
            alive: AtomicBool::new(true),
        });
        std::thread::spawn({
            let shared = shared.clone();
            move || read_loop(reader, &shared)
        });
        let bus = Bus {
            shared,
            client_id: OnceLock::new(),
        };
        let registered = bus.request(Method::Initialize, json!({"clientType": "overterm"}))?;
        let client_id = registered
            .get("clientId")
            .and_then(Value::as_str)
            .ok_or_else(|| BusError::Refused("registration gave no client id".into()))?;
        let _ = bus.client_id.set(client_id.to_owned());
        Ok(bus)
    }

    pub fn is_alive(&self) -> bool {
        self.shared.alive.load(Ordering::SeqCst)
    }

    pub fn request(&self, method: Method, params: Value) -> Result<Value, BusError> {
        self.request_with_owner(method, params)
            .map(|(_, value)| value)
    }

    fn request_with_owner(
        &self,
        method: Method,
        params: Value,
    ) -> Result<(Option<String>, Value), BusError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let (reply, answer) = mpsc::channel();
        self.shared
            .pending
            .lock()
            .unwrap()
            .insert(request_id.clone(), reply);
        // Checked after registering, so a reader that is shutting down
        // either sees this request and drops it, or this sees it gone.
        let message = wire::request(
            &request_id,
            self.client_id.get().map(String::as_str),
            method,
            params,
        );
        if !self.is_alive() || self.shared.write(&message).is_err() {
            self.shared.pending.lock().unwrap().remove(&request_id);
            return Err(BusError::Closed);
        }
        match answer.recv_timeout(REQUEST_TIMEOUT) {
            Ok((handled_by, Ok(value))) => Ok((handled_by, value)),
            Ok((_, Err(error))) => Err(if error.starts_with("no-client-found") {
                BusError::NoOwner
            } else if error.contains("request-version-mismatch") {
                BusError::Incompatible
            } else {
                BusError::Refused(error)
            }),
            Err(RecvTimeoutError::Timeout) => {
                self.shared.pending.lock().unwrap().remove(&request_id);
                Err(BusError::TimedOut)
            }
            Err(RecvTimeoutError::Disconnected) => Err(BusError::Closed),
        }
    }

    /// Start receiving a thread. The owner answers the history request by
    /// sending a full snapshot, which reaches `sink` before this returns.
    pub fn follow(&self, conversation_id: &str, sink: StreamSink) -> Result<(), BusError> {
        let client_id = self.client_id.get().ok_or(BusError::Closed)?;
        self.shared
            .streams
            .lock()
            .unwrap()
            .insert(conversation_id.to_owned(), sink);
        let followed = self
            .shared
            .write(&wire::following_changed(client_id, conversation_id, true))
            .map_err(|_| BusError::Closed)
            .and_then(|()| {
                self.request_with_owner(
                    Method::LoadHistory,
                    json!({"conversationId": conversation_id}),
                )
            });
        match followed {
            Ok((owner, _)) => {
                if let Some(owner) = owner {
                    self.shared
                        .owners
                        .lock()
                        .unwrap()
                        .insert(conversation_id.to_owned(), owner);
                }
                Ok(())
            }
            Err(error) => {
                self.unfollow(conversation_id);
                Err(error)
            }
        }
    }

    /// Follow a thread again with the sink it already has, after its owner
    /// left and a new one may have taken it.
    pub fn refollow(&self, conversation_id: &str) -> Result<(), BusError> {
        let sink = self.shared.sink(conversation_id).ok_or(BusError::Closed)?;
        self.follow(conversation_id, sink)
    }

    pub fn unfollow(&self, conversation_id: &str) {
        self.shared.streams.lock().unwrap().remove(conversation_id);
        self.shared.owners.lock().unwrap().remove(conversation_id);
        if let Some(client_id) = self.client_id.get() {
            let _ = self
                .shared
                .write(&wire::following_changed(client_id, conversation_id, false));
        }
    }
}

impl Shared {
    fn write(&self, message: &Value) -> std::io::Result<()> {
        self.writer
            .lock()
            .unwrap()
            .write_all(&wire::encode(message))
    }

    fn sink(&self, conversation_id: &str) -> Option<StreamSink> {
        self.streams.lock().unwrap().get(conversation_id).cloned()
    }

    fn dispatch(&self, message: Value) {
        match wire::classify(message) {
            Incoming::Response {
                request_id,
                handled_by,
                outcome,
            } => {
                if let Some(waiting) = self.pending.lock().unwrap().remove(&request_id) {
                    let _ = waiting.send((handled_by, outcome));
                }
            }
            Incoming::DiscoveryRequest { request_id } => {
                let _ = self.write(&wire::discovery_refusal(&request_id));
            }
            Incoming::Broadcast {
                method,
                version,
                params,
            } if method == STREAM_STATE_CHANGED => {
                let Some(conversation_id) = params.get("conversationId").and_then(Value::as_str)
                else {
                    return;
                };
                // Cloned out so the sink runs without the table locked.
                let Some(sink) = self.sink(conversation_id) else {
                    return;
                };
                if version == STREAM_STATE_VERSION {
                    sink(StreamEvent::Change(
                        params.get("change").cloned().unwrap_or(Value::Null),
                    ));
                } else {
                    sink(StreamEvent::Incompatible);
                }
            }
            Incoming::Broadcast { method, params, .. } if method == CLIENT_STATUS_CHANGED => {
                if params.get("status").and_then(Value::as_str) != Some("disconnected") {
                    return;
                }
                let Some(client) = params.get("clientId").and_then(Value::as_str) else {
                    return;
                };
                let orphaned: Vec<String> = {
                    let mut owners = self.owners.lock().unwrap();
                    let gone: Vec<String> = owners
                        .iter()
                        .filter(|(_, owner)| owner.as_str() == client)
                        .map(|(conversation, _)| conversation.clone())
                        .collect();
                    for conversation in &gone {
                        owners.remove(conversation);
                    }
                    gone
                };
                for conversation in orphaned {
                    if let Some(sink) = self.sink(&conversation) {
                        sink(StreamEvent::OwnerGone);
                    }
                }
            }
            _ => {}
        }
    }
}

fn read_loop(mut stream: UnixStream, shared: &Shared) {
    let mut decoder = FrameDecoder::default();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        // A frame that does not decode means the two ends disagree about
        // where frames start, and nothing after it can be trusted.
        let Ok(messages) = decoder.push(&buffer[..read]) else {
            break;
        };
        for message in messages {
            shared.dispatch(message);
        }
    }
    shared.alive.store(false, Ordering::SeqCst);
    shared.pending.lock().unwrap().clear();
    let sinks: Vec<StreamSink> = shared
        .streams
        .lock()
        .unwrap()
        .drain()
        .map(|(_, sink)| sink)
        .collect();
    for sink in sinks {
        sink(StreamEvent::Disconnected);
    }
}

impl Drop for Bus {
    fn drop(&mut self) {
        let _ = self.shared.writer.lock().unwrap().shutdown(Shutdown::Both);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::Receiver;
    use std::thread;

    /// The far end of the socket, playing the desktop app's router.
    struct Router {
        stream: UnixStream,
        decoder: FrameDecoder,
        queued: Vec<Value>,
    }

    impl Router {
        fn read(&mut self) -> Value {
            let mut buffer = [0u8; 4096];
            while self.queued.is_empty() {
                let read = self.stream.read(&mut buffer).expect("router read");
                assert!(read > 0, "client hung up");
                self.queued = self.decoder.push(&buffer[..read]).unwrap();
            }
            self.queued.remove(0)
        }

        fn send(&mut self, message: Value) {
            self.stream.write_all(&wire::encode(&message)).unwrap();
        }

        fn answer(&mut self, request: &Value, handled_by: &str, result: Result<Value, &str>) {
            let mut reply = json!({
                "type": "response", "requestId": request["requestId"],
                "method": request["method"], "handledByClientId": handled_by,
            });
            match result {
                Ok(value) => {
                    reply["resultType"] = json!("success");
                    reply["result"] = value;
                }
                Err(error) => {
                    reply["resultType"] = json!("error");
                    reply["error"] = json!(error);
                }
            }
            self.send(reply);
        }

        /// Accept the handshake the way the desktop router does.
        fn register(&mut self) {
            let init = self.read();
            assert_eq!(init["method"], "initialize");
            assert_eq!(init["params"], json!({"clientType": "overterm"}));
            self.answer(
                &init,
                "overterm-client",
                Ok(json!({"clientId": "overterm-client"})),
            );
        }

        fn stream_change(&mut self, conversation: &str, version: u64, change: Value) {
            self.send(json!({
                "type": "broadcast", "method": STREAM_STATE_CHANGED, "version": version,
                "sourceClientId": "owner", "params": {"conversationId": conversation, "hostId": "local", "change": change}
            }));
        }
    }

    fn connected() -> (Bus, Router) {
        let (client, server) = UnixStream::pair().unwrap();
        let mut router = Router {
            stream: server,
            decoder: FrameDecoder::default(),
            queued: Vec::new(),
        };
        let registering = thread::spawn(move || {
            router.register();
            router
        });
        let bus = Bus::over(client).expect("handshake");
        (bus, registering.join().unwrap())
    }

    fn recording_sink() -> (StreamSink, Receiver<String>) {
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let sink: StreamSink = Arc::new(move |event| {
            let label = match event {
                StreamEvent::Change(change) => format!("change r{}", change["revision"]),
                StreamEvent::Incompatible => "incompatible".into(),
                StreamEvent::OwnerGone => "owner gone".into(),
                StreamEvent::Disconnected => "disconnected".into(),
            };
            let _ = tx.lock().unwrap().send(label);
        });
        (sink, rx)
    }

    fn next(events: &Receiver<String>) -> String {
        events
            .recv_timeout(Duration::from_secs(2))
            .expect("an event")
    }

    #[test]
    fn socket_path_prefers_codex_home() {
        assert_eq!(
            socket_path(Some("/custom".into()), Some("/Users/someone".into())),
            Some(PathBuf::from("/custom/ipc/ipc.sock"))
        );
        assert_eq!(
            socket_path(None, Some("/Users/someone".into())),
            Some(PathBuf::from("/Users/someone/.codex/ipc/ipc.sock"))
        );
        assert_eq!(socket_path(None, None), None);
    }

    #[test]
    fn a_missing_socket_means_the_desktop_app_is_not_running() {
        let path = std::env::temp_dir().join("overterm-no-such-codex.sock");
        assert_eq!(Bus::connect(&path).err(), Some(BusError::NotRunning));
    }

    #[test]
    fn requests_carry_the_assigned_client_id_and_get_their_answer() {
        let (bus, mut router) = connected();
        let serving = thread::spawn(move || {
            let request = router.read();
            assert_eq!(request["sourceClientId"], "overterm-client");
            assert_eq!(request["method"], "thread-owner-discovery");
            assert_eq!(request["version"], 1);
            router.answer(
                &request,
                "owner",
                Ok(json!({"supportsUntrustedAppInput": true})),
            );
            router
        });
        let reply = bus
            .request(
                Method::OwnerDiscovery,
                json!({"conversationId": "t1", "hostId": "local"}),
            )
            .unwrap();
        assert_eq!(reply, json!({"supportsUntrustedAppInput": true}));
        serving.join().unwrap();
    }

    #[test]
    fn router_errors_become_typed_errors() {
        let (bus, mut router) = connected();
        let serving = thread::spawn(move || {
            for error in [
                "no-client-found",
                "request-version-mismatch",
                "something-else",
            ] {
                let request = router.read();
                router.answer(&request, "", Err(error));
            }
        });
        let ask = || bus.request(Method::OwnerDiscovery, json!({}));
        assert_eq!(ask(), Err(BusError::NoOwner));
        assert_eq!(ask(), Err(BusError::Incompatible));
        assert_eq!(ask(), Err(BusError::Refused("something-else".into())));
        serving.join().unwrap();
    }

    #[test]
    fn discovery_requests_are_always_refused() {
        let (_bus, mut router) = connected();
        router.send(json!({
            "type": "client-discovery-request", "requestId": "d1",
            "request": {"type": "request", "method": "ide-context"}
        }));
        assert_eq!(
            router.read(),
            json!({"type": "client-discovery-response", "requestId": "d1", "response": {"canHandle": false}})
        );
    }

    #[test]
    fn follow_announces_itself_then_receives_the_snapshot() {
        let (bus, mut router) = connected();
        let (sink, events) = recording_sink();
        let serving = thread::spawn(move || {
            let following = router.read();
            assert_eq!(following["method"], "thread-stream-following-changed");
            assert_eq!(following["params"]["following"], true);
            let history = router.read();
            assert_eq!(history["method"], "thread-follower-load-complete-history");
            assert_eq!(history["params"], json!({"conversationId": "t1"}));
            // The owner sends the snapshot before answering, as the real
            // one does.
            router.stream_change(
                "t1",
                STREAM_STATE_VERSION,
                json!({"type": "snapshot", "revision": 1}),
            );
            router.stream_change(
                "other",
                STREAM_STATE_VERSION,
                json!({"type": "snapshot", "revision": 9}),
            );
            router.answer(&history, "owner", Ok(json!({"revision": 1})));
            router.stream_change(
                "t1",
                STREAM_STATE_VERSION + 1,
                json!({"type": "patches", "revision": 2}),
            );
            router
        });
        bus.follow("t1", sink).unwrap();
        assert_eq!(next(&events), "change r1");
        let mut router = serving.join().unwrap();
        assert_eq!(next(&events), "incompatible");

        bus.unfollow("t1");
        let unfollowing = router.read();
        assert_eq!(unfollowing["params"]["following"], false);
        router.stream_change(
            "t1",
            STREAM_STATE_VERSION,
            json!({"type": "snapshot", "revision": 3}),
        );
        assert!(events.recv_timeout(Duration::from_millis(200)).is_err());
    }

    #[test]
    fn a_follow_the_owner_refuses_leaves_nothing_registered() {
        let (bus, mut router) = connected();
        let (sink, events) = recording_sink();
        let serving = thread::spawn(move || {
            router.read(); // following
            let history = router.read();
            router.answer(&history, "", Err("no-client-found"));
            let unfollowing = router.read();
            assert_eq!(unfollowing["params"]["following"], false);
            router.stream_change(
                "t1",
                STREAM_STATE_VERSION,
                json!({"type": "snapshot", "revision": 1}),
            );
            router
        });
        assert_eq!(bus.follow("t1", sink), Err(BusError::NoOwner));
        serving.join().unwrap();
        assert!(events.recv_timeout(Duration::from_millis(200)).is_err());
    }

    #[test]
    fn the_owner_leaving_is_reported_to_its_threads_only() {
        let (bus, mut router) = connected();
        let (sink, events) = recording_sink();
        let serving = thread::spawn(move || {
            router.read();
            let history = router.read();
            router.answer(&history, "owner", Ok(json!({"revision": 1})));
            router
        });
        bus.follow("t1", sink).unwrap();
        let mut router = serving.join().unwrap();
        let status = |client: &str| {
            json!({
                "type": "broadcast", "method": CLIENT_STATUS_CHANGED, "version": 0,
                "params": {"clientId": client, "clientType": "electron", "status": "disconnected"}
            })
        };
        router.send(status("someone-else"));
        router.send(status("owner"));
        assert_eq!(next(&events), "owner gone");
    }

    #[test]
    fn refollow_announces_again_to_the_new_owner_with_the_same_sink() {
        let (bus, mut router) = connected();
        let (sink, events) = recording_sink();
        let serving = thread::spawn(move || {
            for owner in ["first-owner", "second-owner"] {
                assert_eq!(router.read()["params"]["following"], true);
                let history = router.read();
                router.stream_change(
                    "t1",
                    STREAM_STATE_VERSION,
                    json!({"type": "snapshot", "revision": 1}),
                );
                router.answer(&history, owner, Ok(json!({})));
            }
            router
        });
        bus.follow("t1", sink).unwrap();
        assert_eq!(next(&events), "change r1");
        bus.refollow("t1").unwrap();
        assert_eq!(next(&events), "change r1");
        serving.join().unwrap();
        assert_eq!(bus.refollow("never-followed"), Err(BusError::Closed));
    }

    #[test]
    fn the_router_hanging_up_disconnects_everything() {
        let (bus, mut router) = connected();
        let (sink, events) = recording_sink();
        let serving = thread::spawn(move || {
            router.read();
            let history = router.read();
            router.answer(&history, "owner", Ok(json!({})));
            router
        });
        bus.follow("t1", sink).unwrap();
        drop(serving.join().unwrap());
        assert_eq!(next(&events), "disconnected");
        assert!(!bus.is_alive());
        let started = std::time::Instant::now();
        assert_eq!(
            bus.request(Method::OwnerDiscovery, json!({})),
            Err(BusError::Closed)
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
