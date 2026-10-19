//! See [`AnnounceRoom`].

use {
    super::{ANNOUNCE_INTERVAL, DiscoveryConfig, GossipHandle, RoomAnnouncement, topic_id},
    crate::IrohRuntime,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bytes::Bytes,
    futures::{FutureExt, StreamExt, channel::oneshot},
    iroh_gossip::api::Event,
    tracing::{Instrument, debug, debug_span, warn},
};

/// Broadcasts a [`RoomAnnouncement`] on the gossip swarm of an endpoint.
///
/// The announcing entity becomes a child of the endpoint entity. To stop
/// announcing, despawn the entity.
///
/// # Examples
///
/// ```no_run
/// use {
///     aeronet_iroh::discovery::{AnnounceRoom, DiscoveryConfig, GossipHandle, RoomAnnouncement},
///     bevy_ecs::prelude::*,
/// };
///
/// fn on_gossip_ready(trigger: On<Add, GossipHandle>, mut commands: Commands) {
///     commands.spawn_empty().queue(AnnounceRoom::start(
///         trigger.event_target(),
///         DiscoveryConfig {
///             game: "my_game".into(),
///             version: "1.0.0".into(),
///             bootstrap_nodes: vec![],
///         },
///         RoomAnnouncement {
///             room_name: "Epic Server".into(),
///             max_players: 8,
///             ..Default::default()
///         },
///     ));
/// }
/// ```
#[derive(Debug, Component)]
pub struct AnnounceRoom {
    _tx_stop: oneshot::Sender<()>,
}

impl AnnounceRoom {
    /// Creates an [`EntityCommand`] to start announcing a room on the gossip
    /// swarm of `endpoint`.
    ///
    /// `endpoint` must have a [`GossipHandle`].
    #[must_use]
    pub fn start(
        endpoint: Entity,
        config: DiscoveryConfig,
        announcement: RoomAnnouncement,
    ) -> impl EntityCommand {
        move |entity: EntityWorldMut| start(entity, endpoint, config, announcement)
    }
}

fn start(
    mut entity: EntityWorldMut,
    endpoint: Entity,
    config: DiscoveryConfig,
    announcement: RoomAnnouncement,
) {
    let Some(gossip) = entity.world().get::<GossipHandle>(endpoint).cloned() else {
        warn!("Endpoint {endpoint} has no `GossipHandle`; cannot announce room");
        return;
    };
    let runtime = entity.world().resource::<IrohRuntime>().clone();

    let (tx_stop, rx_stop) = oneshot::channel::<()>();
    runtime.spawn_on_self(
        async move {
            debug!(game = %config.game, version = %config.version, "Starting room announcement");
            announce_loop(gossip, config, announcement, rx_stop).await;
            debug!("Room announcement stopped");
        }
        .instrument(debug_span!("announce", entity = %entity.id())),
    );

    entity.insert((ChildOf(endpoint), AnnounceRoom { _tx_stop: tx_stop }));
}

async fn announce_loop(
    gossip: GossipHandle,
    config: DiscoveryConfig,
    announcement: RoomAnnouncement,
    rx_stop: oneshot::Receiver<()>,
) {
    let payload = match postcard::to_allocvec(&announcement) {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) => {
            warn!("Failed to serialize room announcement: {err:?}");
            return;
        }
    };

    let topic = topic_id(&config.game, &config.version);
    let (tx_topic, mut rx_topic) = match gossip
        .gossip()
        .subscribe(topic, config.bootstrap_nodes)
        .await
    {
        Ok(topic) => topic.split(),
        Err(err) => {
            warn!("Failed to subscribe to gossip topic: {err:?}");
            return;
        }
    };

    // the sender is dropped (cancelling `rx_stop`) when the frontend goes away
    let mut rx_stop = rx_stop.fuse();
    loop {
        if let Err(err) = tx_topic.broadcast(payload.clone()).await {
            debug!("Failed to broadcast room announcement: {err:?}");
        }

        // re-announce as soon as a new neighbor joins, so that new searchers
        // don't have to wait for the next interval
        let mut sleep = IrohRuntime::sleep(ANNOUNCE_INTERVAL).boxed().fuse();
        loop {
            futures::select! {
                () = sleep => break,
                event = rx_topic.next().fuse() => match event {
                    Some(Ok(Event::NeighborUp(node_id))) => {
                        debug!(%node_id, "Gossip neighbor up");
                        break;
                    }
                    Some(_) => {}
                    None => {
                        debug!("Gossip topic stream ended");
                        return;
                    }
                },
                _ = rx_stop => return,
            }
        }
    }
}
