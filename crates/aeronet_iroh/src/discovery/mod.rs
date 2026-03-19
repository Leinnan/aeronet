//! Server discoverability using iroh-gossip.
//!
//! This module allows game servers to announce themselves on a gossip topic
//! derived from `(game_name, game_version)`, and allows clients to search for
//! active servers matching their game and version.
//!
//! # Setup
//!
//! 1. Build an [`iroh::Endpoint`] (with optional pkarr/DHT discovery for
//!    bootstrapless operation).
//! 2. Create a [`iroh_gossip::net::Gossip`] from that endpoint and wrap it in
//!    a [`GossipHandle`] resource.
//! 3. Add [`IrohDiscoveryPlugin`] to your Bevy app.
//! 4. Use [`AnnounceRoom`] (on a server) or [`SearchRooms`] (on a client) as
//!    entity commands.
//!
//! # How it works
//!
//! The gossip topic ID is derived as:
//! `blake3("aeronet/discovery/{game}/{version}")`.
//!
//! Servers periodically broadcast a [`RoomAnnouncement`] (every
//! [`ANNOUNCE_INTERVAL`]) over the topic. Clients listen for these broadcasts
//! and maintain [`DiscoveredRooms`]. Rooms that have not been heard from for
//! [`ROOM_TIMEOUT`] are removed and a [`RoomLost`] event is fired.
//!
//! When an [`IrohServer`](crate::server::IrohServer) is opened and a
//! [`GossipHandle`] resource is present, incoming connections with the
//! `GOSSIP_ALPN` are automatically forwarded to the gossip actor. For
//! client-only discovery (no `IrohServer`), [`SearchRooms`] runs its own
//! gossip accept loop on the endpoint.

pub mod announce;
pub mod search;

pub use announce::AnnounceRoom;
pub use search::SearchRooms;

use {
    bevy_app::prelude::*,
    bevy_ecs::prelude::*,
    bevy_platform::time::Instant,
    core::time::Duration,
    iroh::{Endpoint, EndpointId},
    iroh_gossip::net::Gossip,
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};



/// How often a server broadcasts its [`RoomAnnouncement`].
pub const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(10);

/// How long without a heartbeat before a room is considered gone.
pub const ROOM_TIMEOUT: Duration = Duration::from_secs(30);

/// Derive the gossip [`TopicId`](iroh_gossip::proto::TopicId) for the given
/// game name and version string.
///
/// The topic is `blake3("aeronet/discovery/{game}/{version}")`.
#[must_use]
pub fn topic_id(game: &str, version: &str) -> iroh_gossip::proto::TopicId {
    let input = format!("aeronet/discovery/{game}/{version}");
    iroh_gossip::proto::TopicId::from_bytes(*blake3::hash(input.as_bytes()).as_bytes())
}

// ── Plugin ───────────────────────────────────────────────────────────────────

/// Bevy plugin that enables iroh-gossip server discovery.
///
/// Insert a [`GossipHandle`] resource before spawning [`AnnounceRoom`] or
/// [`SearchRooms`] entity commands.
pub struct IrohDiscoveryPlugin;

impl Plugin for IrohDiscoveryPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<DiscoveredRooms>().add_systems(
            PreUpdate,
            (
                search::poll_searching,
                announce::poll_announcing,
                cull_stale_rooms,
            ),
        );
    }
}

// ── Resources ────────────────────────────────────────────────────────────────

/// Handle to the iroh-gossip instance used for discovery.
///
/// Insert this as a Bevy resource before using [`AnnounceRoom`] or
/// [`SearchRooms`]. Build the [`Gossip`] from the same [`Endpoint`] you use
/// for your game server or client:
///
/// ```rust,ignore
/// let gossip = iroh_gossip::net::Gossip::builder().spawn(&endpoint);
/// app.insert_resource(GossipHandle::new(gossip, endpoint));
/// ```
#[derive(Resource, Clone)]
pub struct GossipHandle {
    /// The gossip actor.
    pub gossip: Gossip,
    /// The iroh endpoint the gossip actor was built from.
    ///
    /// Used by [`SearchRooms`] to run the gossip accept loop when there is no
    /// [`IrohServer`](crate::server::IrohServer) managing the accept loop.
    pub endpoint: Endpoint,
}

impl GossipHandle {
    /// Create a new handle from an already-built [`Gossip`] and its
    /// associated [`Endpoint`].
    #[must_use]
    pub const fn new(gossip: Gossip, endpoint: Endpoint) -> Self {
        Self { gossip, endpoint }
    }
}

/// All currently known discoverable rooms, keyed by their host's
/// [`EndpointId`].
#[derive(Resource, Default)]
pub struct DiscoveredRooms {
    /// Map from host [`EndpointId`] to [`RoomInfo`].
    pub rooms: HashMap<EndpointId, RoomInfo>,
}

impl DiscoveredRooms {
    /// Iterate over all known rooms.
    pub fn iter(&self) -> impl Iterator<Item = (&EndpointId, &RoomInfo)> {
        self.rooms.iter()
    }
}

// ── Types ─────────────────────────────────────────────────────────────────────

/// Configuration for a discovery topic (game name + version).
///
/// All peers using the same `game` and `version` share one gossip topic.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Game name, e.g. `"my_game"`.
    pub game: String,
    /// Game version string, e.g. `"1.0.0"`.
    pub version: String,
    /// Known iroh endpoint IDs to bootstrap into the gossip network.
    ///
    /// May be empty if the iroh endpoint was built with DHT/pkarr discovery
    /// (`address-lookup-pkarr-dht` feature), in which case peers can be
    /// located via the DHT.
    pub bootstrap_nodes: Vec<EndpointId>,
}

/// Metadata broadcast by a server every [`ANNOUNCE_INTERVAL`].
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
    /// Arbitrary key-value metadata (map name, game mode, region, …).
    pub metadata: HashMap<String, String>,
}

/// A room discovered via gossip, combining announcement data with discovery
/// metadata.
#[derive(Debug, Clone)]
pub struct RoomInfo {
    /// iroh endpoint ID of the room host. Pass this to
    /// [`IrohClient::connect`](crate::client::IrohClient::connect) to join.
    pub node_id: EndpointId,
    /// The latest announcement broadcast by the server.
    pub announcement: RoomAnnouncement,
    /// Timestamp of the last received heartbeat.
    pub last_seen: Instant,
}

// ── Events ────────────────────────────────────────────────────────────────────

/// Fired when a room is first discovered **or** when its announcement data
/// changes (e.g. player count update).
///
/// Observe this event with `app.add_observer(|trigger: On<RoomDiscovered>| { … })`.
#[derive(Event, Debug, Clone)]
pub struct RoomDiscovered {
    /// The host's iroh endpoint ID.
    pub node_id: EndpointId,
    /// Full room info at the time of discovery/update.
    pub info: RoomInfo,
}

/// Fired when a room has not sent a heartbeat for [`ROOM_TIMEOUT`] and is
/// removed from [`DiscoveredRooms`].
///
/// Observe this event with `app.add_observer(|trigger: On<RoomLost>| { … })`.
#[derive(Event, Debug, Clone)]
pub struct RoomLost {
    /// The host's iroh endpoint ID.
    pub node_id: EndpointId,
}

// ── Systems ───────────────────────────────────────────────────────────────────

fn cull_stale_rooms(mut commands: Commands, mut rooms: ResMut<DiscoveredRooms>) {
    let now = Instant::now();
    let mut lost = Vec::new();
    rooms.rooms.retain(|node_id, info| {
        if now.duration_since(info.last_seen) > ROOM_TIMEOUT {
            lost.push(*node_id);
            false
        } else {
            true
        }
    });
    for node_id in lost {
        commands.trigger(RoomLost { node_id });
    }
}
