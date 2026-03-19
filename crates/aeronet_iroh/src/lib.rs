#![cfg_attr(docsrs_aeronet, feature(doc_cfg))]

//! iroh IO layer for `aeronet`.

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "discovery")]
pub mod discovery;
mod runtime;
#[cfg(feature = "server")]
pub mod server;
pub mod session;

pub use iroh;
pub use runtime::IrohRuntime;

#[cfg(feature = "discovery")]
pub use discovery::{
    AnnounceRoom, DiscoveredRooms, DiscoveryConfig, GossipHandle, IrohDiscoveryPlugin, RoomAnnouncement,
    RoomDiscovered, RoomInfo, RoomLost, SearchRooms,
};
