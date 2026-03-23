//! Client support for iroh-backed aeronet sessions.

mod backend;

use {
    crate::{
        runtime::IrohRuntime,
        session::{
            self, IrohIo, IrohSessionPlugin, MIN_MTU, RemoteEndpointId, SessionError,
            SessionEstablished, SessionMeta,
        },
    },
    aeronet_io::{
        IoSystems, Session, SessionEndpoint,
        connection::{DisconnectReason, Disconnected, PeerAddr},
        packet::{PacketRtt, RecvPacket},
    },
    bevy_app::prelude::*,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{mem, net::SocketAddr, time::Duration},
    derive_more::{Display, Error},
    futures::channel::{mpsc, oneshot},
    iroh::{Endpoint, EndpointAddr, EndpointId},
    tracing::{Instrument, debug, debug_span},
};

/// Allows using [`IrohClient`].
pub struct IrohClientPlugin;

impl Plugin for IrohClientPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<IrohSessionPlugin>() {
            app.add_plugins(IrohSessionPlugin);
        }

        app.add_systems(
            PreUpdate,
            (poll_connecting, poll_connected)
                .in_set(IoSystems::Poll)
                .before(session::poll),
        );
    }
}

/// Dedicated client session implementation using an iroh endpoint.
#[derive(Debug, Component)]
#[require(SessionEndpoint)]
pub struct IrohClient(());

impl IrohClient {
    /// Connects the entity to a remote iroh endpoint.
    #[must_use]
    pub fn connect(
        endpoint: Endpoint,
        addr: impl Into<EndpointAddr>,
        alpn: impl Into<Vec<u8>>,
    ) -> impl EntityCommand {
        let addr = addr.into();
        let alpn = alpn.into();
        move |entity: EntityWorldMut| connect(entity, endpoint, addr.clone(), alpn.clone())
    }
}

fn connect(mut entity: EntityWorldMut, endpoint: Endpoint, addr: EndpointAddr, alpn: Vec<u8>) {
    let runtime = entity.world().resource::<IrohRuntime>().clone();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel::<DisconnectReason>();
    let (tx_next, rx_next) = oneshot::channel::<ToConnected>();
    runtime.spawn_on_self(
        async move {
            let Err(reason) = backend::start(endpoint, addr, alpn, tx_next).await;
            debug!("Client disconnected: {reason:?}");
            _ = tx_dc_reason.send(reason);
        }
        .instrument(debug_span!("client", entity = %entity.id())),
    );

    entity.insert((
        IrohClient(()),
        Connecting {
            rx_dc_reason,
            rx_next,
        },
    ));
}

/// Client-specific error.
#[derive(Debug, Display, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Failed to connect to the remote endpoint.
    #[display("failed to connect")]
    Connect(iroh::endpoint::ConnectError),
}

#[derive(Debug, Component)]
struct Connecting {
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
    rx_next: oneshot::Receiver<ToConnected>,
}

#[derive(Debug, Component)]
struct Connected {
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
}

#[derive(Debug)]
struct ToConnected {
    initial_peer_addr: Option<SocketAddr>,
    initial_rtt: Option<Duration>,
    remote_id: EndpointId,
    initial_mtu: usize,
    rx_meta: mpsc::Receiver<SessionMeta>,
    rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    tx_user_dc: oneshot::Sender<String>,
}

fn poll_connecting(
    mut commands: Commands,
    mut clients: Query<(Entity, &mut Connecting), With<IrohClient>>,
) {
    for (entity, mut client) in &mut clients {
        if try_disconnect(&mut commands, entity, &mut client.rx_dc_reason) {
            continue;
        }

        let Ok(Some(next)) = client.rx_next.try_recv() else {
            continue;
        };

        let mut session = Session::new(Instant::now(), MIN_MTU);
        if let Err(err) = session.set_mtu(next.initial_mtu) {
            commands.trigger(Disconnected {
                entity,
                reason: DisconnectReason::by_error(SessionError::MtuTooSmall(err)),
            });
            continue;
        }

        let (_, dummy) = oneshot::channel();
        let rx_dc_reason = mem::replace(&mut client.rx_dc_reason, dummy);
        let mut entity_commands = commands.entity(entity);
        entity_commands.remove::<Connecting>().insert((
            IrohIo {
                rx_meta: next.rx_meta,
                rx_packet_b2f: next.rx_packet_b2f,
                tx_packet_f2b: next.tx_packet_f2b,
                tx_user_dc: Some(next.tx_user_dc),
            },
            Connected { rx_dc_reason },
            RemoteEndpointId(next.remote_id),
            session,
        ));
        if let Some(peer_addr) = next.initial_peer_addr {
            entity_commands.insert(PeerAddr(peer_addr));
        }
        if let Some(rtt) = next.initial_rtt {
            entity_commands.insert(PacketRtt(rtt));
        }

        // Notify integration layers that this session is fully established.
        commands.trigger(SessionEstablished {
            entity,
            remote_id: next.remote_id,
            peer_addr: next.initial_peer_addr,
            initial_rtt: next.initial_rtt,
            mtu: next.initial_mtu,
        });
    }
}

fn poll_connected(
    mut commands: Commands,
    mut clients: Query<(Entity, &mut Connected), With<IrohClient>>,
) {
    for (entity, mut client) in &mut clients {
        try_disconnect(&mut commands, entity, &mut client.rx_dc_reason);
    }
}

fn try_disconnect(
    commands: &mut Commands,
    entity: Entity,
    recv_dc: &mut oneshot::Receiver<DisconnectReason>,
) -> bool {
    let dc_reason = match recv_dc.try_recv() {
        Ok(None) => None,
        Ok(Some(disconnected)) => Some(disconnected),
        Err(_) => Some(SessionError::BackendClosed.into()),
    };
    dc_reason.is_some_and(|reason| {
        commands.trigger(Disconnected { entity, reason });
        true
    })
}
