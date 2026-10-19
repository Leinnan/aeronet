//! See [`SearchRooms`].

use {
    super::{
        DiscoveredRooms, DiscoveryConfig, GossipHandle, RoomAnnouncement, RoomDiscovered, RoomInfo,
        topic_id,
    },
    crate::IrohRuntime,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bevy_platform::time::Instant,
    futures::{
        FutureExt, StreamExt,
        channel::{mpsc, oneshot},
    },
    iroh::EndpointId,
    iroh_gossip::api::Event,
    tracing::{Instrument, debug, debug_span, trace, warn},
};

/// Listens for [`RoomAnnouncement`]s on the gossip swarm of an endpoint.
///
/// Discovered rooms are stored in [`DiscoveredRooms`], and [`RoomDiscovered`]
/// and [`RoomLost`](super::RoomLost) are triggered.
///
/// The searching entity becomes a child of the endpoint entity. To stop
/// searching, despawn the entity.
///
/// # Examples
///
/// ```no_run
/// use {
///     aeronet_iroh::discovery::{DiscoveryConfig, GossipHandle, SearchRooms},
///     bevy_ecs::prelude::*,
///     iroh::EndpointId,
/// };
///
/// fn on_gossip_ready(
///     trigger: On<Add, GossipHandle>,
///     mut commands: Commands,
///     host: Res<KnownHost>,
/// ) {
///     commands.spawn_empty().queue(SearchRooms::start(
///         trigger.event_target(),
///         DiscoveryConfig {
///             game: "my_game".into(),
///             version: "1.0.0".into(),
///             bootstrap_nodes: vec![host.0],
///         },
///     ));
/// }
///
/// #[derive(Resource)]
/// struct KnownHost(EndpointId);
/// ```
#[derive(Debug, Component)]
pub struct SearchRooms {
    rx_rooms: mpsc::UnboundedReceiver<(EndpointId, RoomAnnouncement)>,
    _tx_stop: oneshot::Sender<()>,
}

impl SearchRooms {
    /// Creates an [`EntityCommand`] to start searching for rooms on the gossip
    /// swarm of `endpoint`.
    ///
    /// `endpoint` must have a [`GossipHandle`].
    #[must_use]
    pub fn start(endpoint: Entity, config: DiscoveryConfig) -> impl EntityCommand {
        move |entity: EntityWorldMut| start(entity, endpoint, config)
    }
}

fn start(mut entity: EntityWorldMut, endpoint: Entity, config: DiscoveryConfig) {
    let Some(gossip) = entity.world().get::<GossipHandle>(endpoint).cloned() else {
        warn!("Endpoint {endpoint} has no `GossipHandle`; cannot search for rooms");
        return;
    };
    let runtime = entity.world().resource::<IrohRuntime>().clone();

    let (tx_rooms, rx_rooms) = mpsc::unbounded();
    let (tx_stop, rx_stop) = oneshot::channel::<()>();
    runtime.spawn_on_self(
        async move {
            debug!(game = %config.game, version = %config.version, "Starting room search");
            search_loop(gossip, config, tx_rooms, rx_stop).await;
            debug!("Room search stopped");
        }
        .instrument(debug_span!("search", entity = %entity.id())),
    );

    entity.insert((
        ChildOf(endpoint),
        SearchRooms {
            rx_rooms,
            _tx_stop: tx_stop,
        },
    ));
}

async fn search_loop(
    gossip: GossipHandle,
    config: DiscoveryConfig,
    tx_rooms: mpsc::UnboundedSender<(EndpointId, RoomAnnouncement)>,
    rx_stop: oneshot::Receiver<()>,
) {
    let topic = topic_id(&config.game, &config.version);
    let mut gossip_topic = match gossip
        .gossip()
        .subscribe(topic, config.bootstrap_nodes)
        .await
    {
        Ok(topic) => topic,
        Err(err) => {
            warn!("Failed to subscribe to gossip topic: {err:?}");
            return;
        }
    };

    let mut rx_stop = rx_stop.fuse();
    loop {
        let event = futures::select! {
            event = gossip_topic.next().fuse() => event,
            _ = rx_stop => return,
        };
        let Some(event) = event else {
            debug!("Gossip topic stream ended");
            return;
        };
        match event {
            Ok(Event::Received(msg)) => {
                let announcement = match postcard::from_bytes::<RoomAnnouncement>(&msg.content) {
                    Ok(announcement) => announcement,
                    Err(err) => {
                        debug!("Failed to deserialize room announcement: {err:?}");
                        continue;
                    }
                };
                let node_id = msg.delivered_from;
                trace!(%node_id, room = %announcement.room_name, "Received room announcement");
                if tx_rooms.unbounded_send((node_id, announcement)).is_err() {
                    return;
                }
            }
            Ok(Event::NeighborUp(node_id)) => debug!(%node_id, "Gossip neighbor up"),
            Ok(Event::NeighborDown(node_id)) => debug!(%node_id, "Gossip neighbor down"),
            Ok(_) => {}
            Err(err) => debug!("Gossip event error: {err:?}"),
        }
    }
}

pub(super) fn poll_searching(
    mut commands: Commands,
    mut searchers: Query<&mut SearchRooms>,
    mut rooms: ResMut<DiscoveredRooms>,
) {
    for mut searcher in &mut searchers {
        while let Ok((node_id, announcement)) = searcher.rx_rooms.try_recv() {
            let info = RoomInfo {
                node_id,
                announcement,
                last_seen: Instant::now(),
            };
            rooms.rooms.insert(node_id, info.clone());
            commands.trigger(RoomDiscovered { node_id, info });
        }
    }
}
