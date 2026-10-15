//! Shared session contract, independent of venue transport and protobuf.
pub mod backend;
pub mod dto;
mod env;
pub mod error;
pub mod events;
#[cfg(feature = "testing")]
pub mod fake;
pub mod history;
pub mod plants;
pub mod rpc_table;
pub mod streams;

pub use backend::{AccountTriple, ModifyOrder, PlaceBracketOrder, PlaceOrder, Session};
pub use dto::*;
pub use env::{Env, ParseEnvError};
pub use error::{Error, Result};
pub use events::{SessionEvent, SESSION_EVENT_CAPACITY};
pub use plants::PlantSet;
pub use streams::{Stream, StreamHub};

pub mod fanout;
pub mod gates;
pub mod host;
pub mod intents;
pub mod local;
pub mod pump;
pub use backend::Venue;
pub use fanout::SessionId;
pub use gates::{Gates, ParentGates};
pub use host::{HistoryPolicy, SessionHost, VenueHandle};
pub use local::{delivery::EventReceiver, LocalSession};
pub use pump::Timing;

pub mod flock;
pub mod registry;
pub use flock::SessionLock;
pub use registry::{HostFingerprint, HostOptions, HostRegistry};
