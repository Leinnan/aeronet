//! Server-side room announcement via iroh-gossip.

use {
    super::{DiscoveryConfig, GossipHandle, RoomAnnouncement, topic_id, ANNOUNCE_INTERVAL},
    crate::IrohRuntime,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bytes::Bytes,
    futures::{FutureExt, channel::oneshot},
    tracing::{debug, warn},
};

// ── Public API ────────────────────────────────────────────────────────────────

/// Entity command that starts broadcasting a room announcement on the gossip
/// network.
///
/// Requires a [`GossipHandle`] resource to be present in the world.
///
/// # Usage
///
/// ```rust,ignore
/// commands.spawn_empty().queue(AnnounceRoom::start(
///     DiscoveryConfig {
///         game: "my_game".into(),
///         version: "1.0.0".into(),
///         bootstrap_nodes: vec![],
///     },
///     RoomAnnouncement {
///         room_name: "Epic Server".into(),
///         max_players: 8,
///         ..default()
///     },
/// ));
/// ```
///
/// To stop announcing, despawn the entity.
pub struct AnnounceRoom;

impl AnnounceRoom {
    /// Build an entity command that starts a room announcement.
    #[must_use]
    pub fn start(config: DiscoveryConfig, announcement: RoomAnnouncement) -> impl EntityCommand {
        move |entity: EntityWorldMut| start_announce(entity, config, announcement)
    }
}

// ── Internal component ────────────────────────────────────────────────────────

/// Internal component placed on the entity that is announcing a room.
/// Dropping this component stops the background broadcast task.
#[derive(Component)]
pub(super) struct Announcing {
    tx_stop: Option<oneshot::Sender<()>>,
}

impl Drop for Announcing {
    fn drop(&mut self) {
        if let Some(tx) = self.tx_stop.take() {
            // Signal the backend loop to stop.
            let _ = tx.send(());
        }
    }
}

// ── EntityCommand implementation ──────────────────────────────────────────────

fn start_announce(
    mut entity: EntityWorldMut,
    config: DiscoveryConfig,
    announcement: RoomAnnouncement,
) {
    let Some(gossip_handle) = entity.world().get_resource::<GossipHandle>().cloned() else {
        warn!(
            "AnnounceRoom: no GossipHandle resource found — insert GossipHandle before spawning"
        );
        return;
    };
    let runtime = entity.world().resource::<IrohRuntime>().clone();

    let (tx_stop, rx_stop) = oneshot::channel::<()>();

    let entity_id = entity.id();
    runtime.spawn_on_self(
        async move {
            debug!(%entity_id, game = %config.game, version = %config.version, "Starting room announcement");
            announce_loop(gossip_handle, config, announcement, rx_stop).await;
            debug!(%entity_id, "Room announcement stopped");
        },
    );

    entity.insert(Announcing {
        tx_stop: Some(tx_stop),
    });
}

// ── Background task ───────────────────────────────────────────────────────────

async fn announce_loop(
    handle: GossipHandle,
    config: DiscoveryConfig,
    announcement: RoomAnnouncement,
    rx_stop: oneshot::Receiver<()>,
) {
    let topic = topic_id(&config.game, &config.version);
    let bootstrap: Vec<_> = config.bootstrap_nodes.into_iter().collect();

    let mut gossip_topic = match handle.gossip.subscribe(topic, bootstrap).await {
        Ok(t) => t,
        Err(err) => {
            warn!("AnnounceRoom: failed to subscribe to gossip topic: {err:?}");
            return;
        }
    };

    let serialized = match postcard::to_allocvec(&announcement) {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) => {
            warn!("AnnounceRoom: failed to serialize RoomAnnouncement: {err:?}");
            return;
        }
    };

    let mut rx_stop = rx_stop.fuse();
    loop {
        // Broadcast immediately, then wait for the interval.
        match gossip_topic.broadcast(serialized.clone()).await {
            Ok(()) => {}
            Err(err) => {
                debug!("AnnounceRoom: broadcast failed (topic may have no neighbors yet): {err:?}");
            }
        }

        futures::select! {
            () = IrohRuntime::sleep(ANNOUNCE_INTERVAL).fuse() => {},
            _ = rx_stop => {
                debug!("AnnounceRoom: received stop signal");
                return;
            }
        }
    }
}

// ── System ────────────────────────────────────────────────────────────────────

/// System that keeps announcing entities alive. Currently a no-op placeholder;
/// cleanup is driven by the `Drop` impl on [`Announcing`].
#[allow(clippy::missing_const_for_fn)]
pub(super) fn poll_announcing(
    _commands: Commands,
    _announcers: Query<Entity, With<Announcing>>,
) {
    // Intentionally empty: the Announcing component's Drop impl handles teardown.
}
