//! Shared session contract, independent of venue transport and protobuf.
pub mod backend;
pub mod dto;
mod env;
pub mod error;
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
pub use plants::PlantSet;
pub use streams::{Stream, StreamHub};
