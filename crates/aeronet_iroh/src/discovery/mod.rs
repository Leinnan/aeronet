//! Room discovery using [`iroh_gossip`].
//!
//! Hosts announce themselves on a gossip topic derived from
//! `(game, version)`, and other peers search that topic for active rooms
//! matching their game and version.
//!
//! # Setup
//!
//! 1. Add [`IrohPlugin`](crate::IrohPlugin), which includes
//!    [`IrohDiscoveryPlugin`] when this feature is enabled.
//! 2. Open an endpoint with
//!    [`IrohEndpoint::open_with_discovery`](crate::endpoint::IrohEndpoint::open_with_discovery),
//!    making sure its builder accepts
//!    [`GOSSIP_ALPN`](iroh_gossip::net::GOSSIP_ALPN).
//! 3. Once the endpoint has a [`GossipHandle`] (observe `On<Add,
//!    GossipHandle>`), spawn an entity with [`AnnounceRoom`] to host a room, or
//!    [`SearchRooms`] to look for rooms.
//!
//! # How it works
//!
//! The gossip topic ID is derived as
//! `blake3("aeronet/discovery/{game}/{version}")`.
//!
//! Hosts broadcast a [`RoomAnnouncement`] every [`ANNOUNCE_INTERVAL`] over the
//! topic. Searchers listen for these broadcasts and maintain
//! [`DiscoveredRooms`]. Rooms that have not been heard from for
//! [`ROOM_TIMEOUT`] are removed, and [`RoomLost`] is triggered.
//!
//! The endpoint's accept loop forwards incoming
//! [`GOSSIP_ALPN`](iroh_gossip::net::GOSSIP_ALPN) connections to the gossip
//! actor, so the same endpoint can host game sessions and take part in
//! discovery at the same time.

pub mod announce;
pub mod search;

pub use {announce::AnnounceRoom, search::SearchRooms};
use {
    bevy_app::prelude::*,
    bevy_ecs::prelude::*,
    bevy_platform::time::Instant,
    core::time::Duration,
    iroh::EndpointId,
    iroh_gossip::{net::Gossip, proto::TopicId},
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};

/// How often a host broadcasts its [`RoomAnnouncement`].
pub const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(10);

/// How long without a heartbeat before a room is considered gone.
pub const ROOM_TIMEOUT: Duration = Duration::from_secs(30);

/// Derives the gossip [`TopicId`] for the given game name and version string.
///
/// The topic is `blake3("aeronet/discovery/{game}/{version}")`.
#[must_use]
pub fn topic_id(game: &str, version: &str) -> TopicId {
    let input = format!("aeronet/discovery/{game}/{version}");
    TopicId::from_bytes(*blake3::hash(input.as_bytes()).as_bytes())
}

/// Allows using [`AnnounceRoom`] and [`SearchRooms`].
pub struct IrohDiscoveryPlugin;

impl Plugin for IrohDiscoveryPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<DiscoveredRooms>().add_systems(
            PreUpdate,
            (search::poll_searching, cull_stale_rooms).chain(),
        );
    }
}

/// Handle to the gossip actor of an
/// [`IrohEndpoint`](crate::endpoint::IrohEndpoint).
///
/// This is inserted on the endpoint entity when it was opened with
/// [`IrohEndpoint::open_with_discovery`](crate::endpoint::IrohEndpoint::open_with_discovery).
#[derive(Debug, Clone, Component)]
pub struct GossipHandle(Gossip);

impl GossipHandle {
    pub(crate) const fn new(gossip: Gossip) -> Self {
        Self(gossip)
    }

    /// Returns the underlying gossip actor.
    #[must_use]
    pub const fn gossip(&self) -> &Gossip {
        &self.0
    }
}

/// All currently known discoverable rooms, keyed by their host's
/// [`EndpointId`].
#[derive(Debug, Default, Resource)]
pub struct DiscoveredRooms {
    /// Map from host [`EndpointId`] to [`RoomInfo`].
    pub rooms: HashMap<EndpointId, RoomInfo>,
}

impl DiscoveredRooms {
    /// Iterates over all known rooms.
    pub fn iter(&self) -> impl Iterator<Item = (&EndpointId, &RoomInfo)> {
        self.rooms.iter()
    }
}

/// Configuration for a discovery topic (game name and version).
///
/// All peers using the same `game` and `version` share one gossip topic.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Game name, e.g. `"my_game"`.
    pub game: String,
    /// Game version string, e.g. `"1.0.0"`.
    pub version: String,
    /// Known endpoint IDs to bootstrap into the gossip swarm.
    ///
    /// The endpoint's address lookup services must be able to resolve these
    /// IDs, or their addresses must already be known to the endpoint.
    pub bootstrap_nodes: Vec<EndpointId>,
}

/// Metadata broadcast by a host every [`ANNOUNCE_INTERVAL`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoomAnnouncement {
    /// Human-readable room name.
    pub room_name: String,
    /// Current number of connected players.
    pub player_count: u32,
    /// Maximum number of players the room accepts.
    pub max_players: u32,
    /// Whether a password is required to join.
    pub password_protected: bool,
    /// Arbitrary key-value metadata (map name, game mode, region, ...).
    pub metadata: HashMap<String, String>,
}

/// Room discovered via gossip, combining announcement data with discovery
/// metadata.
#[derive(Debug, Clone)]
pub struct RoomInfo {
    /// Endpoint ID of the room host.
    ///
    /// Pass this to
    /// [`IrohEndpoint::connect`](crate::endpoint::IrohEndpoint::connect) to
    /// join the room.
    pub node_id: EndpointId,
    /// Latest announcement broadcast by the host.
    pub announcement: RoomAnnouncement,
    /// When the last heartbeat was received.
    pub last_seen: Instant,
}

/// Triggered when a room is first discovered, or when its announcement is
/// received again (e.g. on a player count update).
#[derive(Debug, Clone, Event)]
pub struct RoomDiscovered {
    /// Host's endpoint ID.
    pub node_id: EndpointId,
    /// Room info at the time of discovery or update.
    pub info: RoomInfo,
}

/// Triggered when a room has not sent a heartbeat for [`ROOM_TIMEOUT`] and is
/// removed from [`DiscoveredRooms`].
#[derive(Debug, Clone, Event)]
pub struct RoomLost {
    /// Host's endpoint ID.
    pub node_id: EndpointId,
}

fn cull_stale_rooms(mut commands: Commands, mut rooms: ResMut<DiscoveredRooms>) {
    let now = Instant::now();
    rooms.rooms.retain(|&node_id, info| {
        let alive = now.duration_since(info.last_seen) <= ROOM_TIMEOUT;
        if !alive {
            commands.trigger(RoomLost { node_id });
        }
        alive
    });
}
