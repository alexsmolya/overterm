//! Following a conversation owned by the Codex desktop app.
//!
//! The desktop app runs the thread and publishes it on a local bus: a full
//! snapshot when a follower joins, then patches for every change. This
//! module holds the parts that do not touch a socket or a window: the
//! frame format, the mirrored conversation, and what that conversation
//! means for an agent's state.

pub mod mirror;
pub mod view;
pub mod wire;
