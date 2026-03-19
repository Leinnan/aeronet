//! Client-side room search via iroh-gossip.

use {
    super::{
        DiscoveredRooms, DiscoveryConfig, GossipHandle, RoomAnnouncement, RoomDiscovered, RoomInfo,
        topic_id,
    },
    crate::IrohRuntime,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bevy_platform::time::Instant,
    futures::{FutureExt, StreamExt, channel::oneshot},
    iroh::EndpointId,
    iroh_gossip::{api::Event, net::GOSSIP_ALPN},
    tokio::sync::mpsc,
    tracing::{debug, trace, warn},
};



// ── Public API ────────────────────────────────────────────────────────────────

/// Entity command that starts listening for room announcements on the gossip
/// network.
///
/// Discovered rooms are stored in the [`DiscoveredRooms`] resource and
/// [`RoomDiscovered`] / [`RoomLost`](super::RoomLost) events are fired.
///
/// Requires a [`GossipHandle`] resource to be present.
///
/// When there is **no** [`IrohServer`](crate::server::IrohServer) managing the
/// endpoint's accept loop, `SearchRooms` runs its own gossip accept loop so
/// that other gossip peers can reach this node.
///
/// # Usage
///
/// ```rust,ignore
/// commands.spawn_empty().queue(SearchRooms::start(
///     DiscoveryConfig {
///         game: "my_game".into(),
///         version: "1.0.0".into(),
///         bootstrap_nodes: vec![known_node_id],
///     },
/// ));
/// ```
pub struct SearchRooms;

impl SearchRooms {
    /// Build an entity command that starts listening for rooms.
    ///
    /// Set `run_accept_loop` to `true` when there is no [`IrohServer`] on the
    /// same endpoint (i.e. pure-client mode). This lets other gossip nodes
    /// initiate connections to us, which is required for full gossip mesh
    /// participation.
    ///
    /// [`IrohServer`]: crate::server::IrohServer
    #[must_use]
    pub fn start(config: DiscoveryConfig, run_accept_loop: bool) -> impl EntityCommand {
        move |entity: EntityWorldMut| start_search(entity, config, run_accept_loop)
    }
}

// ── Internal component ────────────────────────────────────────────────────────

/// Internal component placed on the searching entity.
#[derive(Component)]
pub(super) struct Searching {
    /// Receives `(node_id, announcement)` pairs from the background task.
    rx_rooms: mpsc::UnboundedReceiver<(EndpointId, RoomAnnouncement)>,
    /// Signal the background task to stop.
    tx_stop: Option<oneshot::Sender<()>>,
}

impl Drop for Searching {
    fn drop(&mut self) {
        if let Some(tx) = self.tx_stop.take() {
            let _ = tx.send(());
        }
    }
}

// ── EntityCommand implementation ──────────────────────────────────────────────

fn start_search(
    mut entity: EntityWorldMut,
    config: DiscoveryConfig,
    run_accept_loop: bool,
) {
    let Some(gossip_handle) = entity.world().get_resource::<GossipHandle>().cloned() else {
        warn!(
            "SearchRooms: no GossipHandle resource found — insert GossipHandle before spawning"
        );
        return;
    };
    let runtime = entity.world().resource::<IrohRuntime>().clone();

    let (tx_rooms, rx_rooms) = mpsc::unbounded_channel::<(EndpointId, RoomAnnouncement)>();
    let (tx_stop, rx_stop) = oneshot::channel::<()>();

    let entity_id = entity.id();
    runtime.spawn_on_self(async move {
        debug!(%entity_id, game = %config.game, version = %config.version, "Starting room search");

        // Optionally run a gossip accept loop so remote peers can connect to us.
        if run_accept_loop {
            let endpoint = gossip_handle.endpoint.clone();
            let gossip = gossip_handle.gossip.clone();
            IrohRuntime::spawn(async move {
                debug!("SearchRooms: starting gossip accept loop");
                gossip_accept_loop(endpoint, gossip).await;
            });
        }

        search_loop(gossip_handle, config, tx_rooms, rx_stop).await;
        debug!(%entity_id, "Room search stopped");
    });

    entity.insert(Searching {
        rx_rooms,
        tx_stop: Some(tx_stop),
    });
}

// ── Background tasks ──────────────────────────────────────────────────────────

/// Accept incoming gossip connections on the endpoint and forward them to the
/// gossip actor. Only used when `run_accept_loop = true` (no `IrohServer`).
async fn gossip_accept_loop(endpoint: iroh::Endpoint, gossip: iroh_gossip::net::Gossip) {
    loop {
        let Some(incoming) = endpoint.accept().await else {
            debug!("SearchRooms: endpoint closed, stopping gossip accept loop");
            break;
        };

        let gossip = gossip.clone();
        IrohRuntime::spawn(async move {
            // Accept the connection to get ALPN.
            let connecting = match incoming.accept() {
                Ok(c) => c,
                Err(err) => {
                    debug!("SearchRooms: failed to accept incoming: {err:?}");
                    return;
                }
            };
            let conn = match connecting.await {
                Ok(c) => c,
                Err(err) => {
                    debug!("SearchRooms: handshake failed: {err:?}");
                    return;
                }
            };

            if conn.alpn() == GOSSIP_ALPN
                && let Err(err) = gossip.handle_connection(conn).await
            {
                debug!("SearchRooms: gossip handle_connection error: {err:?}");
            }
            // Silently drop non-gossip connections (shouldn't happen in pure-client mode).
        });
    }
}

/// Subscribe to the gossip topic and forward received announcements to the
/// Bevy frontend via an mpsc channel.
async fn search_loop(
    handle: GossipHandle,
    config: DiscoveryConfig,
    tx_rooms: mpsc::UnboundedSender<(EndpointId, RoomAnnouncement)>,
    rx_stop: oneshot::Receiver<()>,
) {
    let topic = topic_id(&config.game, &config.version);
    let bootstrap: Vec<_> = config.bootstrap_nodes.into_iter().collect();

    let mut gossip_topic = match handle.gossip.subscribe(topic, bootstrap).await {
        Ok(t) => t,
        Err(err) => {
            warn!("SearchRooms: failed to subscribe to gossip topic: {err:?}");
            return;
        }
    };

    let mut rx_stop = rx_stop.fuse();
    loop {
        futures::select! {
            event = gossip_topic.next().fuse() => {
                let Some(event) = event else {
                    debug!("SearchRooms: gossip topic stream ended");
                    return;
                };
                match event {
                    Ok(Event::Received(msg)) => {
                        match postcard::from_bytes::<RoomAnnouncement>(&msg.content) {
                            Ok(announcement) => {
                                let node_id = msg.delivered_from;
                                trace!(%node_id, room = %announcement.room_name, "Received room announcement");
                                if tx_rooms.send((node_id, announcement)).is_err() {
                                    debug!("SearchRooms: frontend dropped, stopping");
                                    return;
                                }
                            }
                            Err(err) => {
                                debug!("SearchRooms: failed to deserialize announcement: {err:?}");
                            }
                        }
                    }
                    Ok(Event::NeighborUp(node_id)) => {
                        debug!(%node_id, "SearchRooms: gossip neighbor up");
                    }
                    Ok(Event::NeighborDown(node_id)) => {
                        debug!(%node_id, "SearchRooms: gossip neighbor down");
                    }
                    Ok(_) => {}
                    Err(err) => {
                        debug!("SearchRooms: gossip event error: {err:?}");
                    }
                }
            }
            _ = rx_stop => {
                debug!("SearchRooms: received stop signal");
                return;
            }
        }
    }
}

// ── System ────────────────────────────────────────────────────────────────────

/// Poll the searching entities and update [`DiscoveredRooms`], triggering
/// [`RoomDiscovered`] events for new or updated rooms.
pub(super) fn poll_searching(
    mut commands: Commands,
    mut searchers: Query<&mut Searching>,
    mut rooms: ResMut<DiscoveredRooms>,
) {
    for mut searcher in &mut searchers {
        while let Ok((node_id, announcement)) = searcher.rx_rooms.try_recv() {
            let now = Instant::now();
            let info = RoomInfo {
                node_id,
                announcement,
                last_seen: now,
            };
            rooms.rooms.insert(node_id, info.clone());
            commands.trigger(RoomDiscovered { node_id, info });
        }
    }
}
