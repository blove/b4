//! Portable contracts for the first Rust execution foundation.
//!
//! These types model a proposed commit boundary and selected Agent Protocol
//! payloads. They do not supply an HTTP adapter, storage, grant authority, or
//! an executed compatibility claim against the TypeScript runtime.

mod capabilities;
mod capabilities_v2;
mod capabilities_v3;
mod capabilities_v4;
mod capabilities_v5;
mod capabilities_v6;
mod capabilities_v7;
mod capabilities_v8;
mod ids;
mod model;
pub mod permissions;
pub mod wire;

pub use capabilities::*;
pub use capabilities_v2::*;
pub use capabilities_v3::*;
pub use capabilities_v4::*;
pub use capabilities_v5::*;
pub use capabilities_v6::*;
pub use capabilities_v7::*;
pub use capabilities_v8::*;
pub use ids::*;
pub use model::*;
pub use wire::*;
