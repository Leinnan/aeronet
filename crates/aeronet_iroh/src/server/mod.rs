//! Server support for iroh-backed aeronet sessions.

mod backend;
pub mod config;

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
        connection::{DisconnectReason, Disconnected, LocalAddr, PeerAddr},
        packet::{PacketRtt, RecvPacket},
        server::{CloseReason, Closed, Server, ServerEndpoint},
    },
    bevy_app::prelude::*,
    bevy_ecs::{prelude::*, system::EntityCommand},
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{mem, net::SocketAddr, time::Duration},
    derive_more::{Display, Error},
    futures::channel::{mpsc, oneshot},
    iroh::{Endpoint, EndpointId},
    tracing::{Instrument, debug, debug_span},
};

/// Allows using [`IrohServer`].
pub struct IrohServerPlugin;

impl Plugin for IrohServerPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<IrohSessionPlugin>() {
            app.add_plugins(IrohSessionPlugin);
        }
        app.init_resource::<IrohRuntime>();
        app.add_systems(
            PreUpdate,
            (
                poll_opening,
                poll_opened,
                poll_connecting,
                poll_connected,
                config::handle_connected_to_relay,
            )
                .in_set(IoSystems::Poll)
                .before(session::poll),
        );
    }
}

/// iroh server endpoint component.
#[derive(Debug, Component)]
#[require(ServerEndpoint)]
pub struct IrohServer(());

/// Child component for clients accepted by an [`IrohServer`].
#[derive(Debug, Component)]
#[require(SessionEndpoint)]
pub struct IrohServerClient(());

impl IrohServer {
    /// Opens a listening server on an already-bound iroh endpoint.
    #[must_use]
    pub fn open(endpoint: Endpoint) -> impl EntityCommand {
        move |entity: EntityWorldMut| open(entity, endpoint.clone())
    }
}

fn open(mut entity: EntityWorldMut, endpoint: Endpoint) {
    let runtime = entity.world().resource::<IrohRuntime>().clone();

    // When the discovery feature is active, pick up the GossipHandle resource
    // (if present) so gossip connections can be routed in the accept loop.
    #[cfg(feature = "discovery")]
    let gossip = entity
        .world()
        .get_resource::<crate::discovery::GossipHandle>()
        .map(|h| h.gossip.clone());

    let (tx_close_reason, rx_close_reason) = oneshot::channel::<CloseReason>();
    let (tx_next, rx_next) = oneshot::channel::<ToOpen>();
    runtime.spawn_on_self(
        async move {
            let Err(reason) = backend::start(
                endpoint,
                tx_next,
                #[cfg(feature = "discovery")]
                gossip,
            )
            .await;
            debug!("Server closed: {reason:?}");
            _ = tx_close_reason.send(reason);
        }
        .instrument(debug_span!("server", entity = %entity.id())),
    );

    entity.insert((
        IrohServer(()),
        Opening {
            rx_close_reason,
            rx_next,
        },
    ));
}

/// Server response for a pending incoming connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionResponse {
    /// Accept the incoming connection.
    Accepted,
    /// Reject the incoming connection.
    Rejected,
}

/// Triggered when a new client attempts to connect to an [`IrohServer`].
#[derive(Debug, EntityEvent)]
pub struct SessionRequest {
    /// The client entity being prepared.
    pub entity: Entity,
    /// Peer iroh endpoint id.
    pub remote_id: EndpointId,
    /// Negotiated ALPN.
    pub alpn: Vec<u8>,
    /// Current selected peer socket address, if any.
    pub remote_addr: Option<SocketAddr>,
    tx_session_response: Option<oneshot::Sender<SessionResponse>>,
}

impl SessionRequest {
    /// Responds to the pending request.
    pub fn respond(&mut self, response: SessionResponse) {
        let send_session_response = self
            .tx_session_response
            .take()
            .expect("already responded to this request");
        _ = send_session_response.send(response);
    }
}

impl Drop for SessionRequest {
    fn drop(&mut self) {
        #[rustfmt::skip]
        assert!(
            self.tx_session_response.is_none(),
            "dropped a `SessionRequest` without sending a response; respond with `SessionRequest::respond`\n\nrequest info: {self:#?}"
        );
    }
}

/// Server-specific error.
#[derive(Debug, Display, Error)]
#[non_exhaustive]
pub enum ServerError {
    /// The incoming connection failed before the handshake completed.
    #[display("failed to accept incoming connection")]
    Accept(iroh::endpoint::ConnectingError),
    /// The connection was rejected by the app.
    #[display("user rejected session request")]
    Rejected,
}

#[derive(Debug, Component)]
struct Opening {
    rx_close_reason: oneshot::Receiver<CloseReason>,
    rx_next: oneshot::Receiver<ToOpen>,
}

#[derive(Debug, Component)]
struct Opened {
    rx_close_reason: oneshot::Receiver<CloseReason>,
    rx_connecting: mpsc::Receiver<ToConnecting>,
    _tx_dropped: mpsc::Sender<()>,
}

#[derive(Debug, Component)]
struct Connecting {
    remote_id: EndpointId,
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
    rx_next: oneshot::Receiver<ToConnected>,
}

#[derive(Debug, Component)]
struct Connected {
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
}

#[derive(Debug)]
struct ToOpen {
    local_addr: Option<SocketAddr>,
    rx_connecting: mpsc::Receiver<ToConnecting>,
    tx_dropped: mpsc::Sender<()>,
}

#[derive(Debug)]
struct ToConnecting {
    remote_id: EndpointId,
    alpn: Vec<u8>,
    remote_addr: Option<SocketAddr>,
    tx_session_entity: oneshot::Sender<Entity>,
    tx_session_response: oneshot::Sender<SessionResponse>,
    rx_dc_reason: oneshot::Receiver<DisconnectReason>,
    rx_next: oneshot::Receiver<ToConnected>,
}

#[derive(Debug)]
struct ToConnected {
    initial_peer_addr: Option<SocketAddr>,
    initial_rtt: Option<Duration>,
    initial_mtu: usize,
    rx_meta: mpsc::Receiver<SessionMeta>,
    rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    tx_user_dc: oneshot::Sender<String>,
}

fn poll_opening(
    mut commands: Commands,
    mut servers: Query<(Entity, &mut Opening), With<IrohServer>>,
) {
    for (entity, mut server) in &mut servers {
        if try_close(&mut commands, entity, &mut server.rx_close_reason) {
            continue;
        }

        let Ok(Some(next)) = server.rx_next.try_recv() else {
            continue;
        };
        let (_, dummy) = oneshot::channel();
        let rx_close_reason = mem::replace(&mut server.rx_close_reason, dummy);
        let mut entity_commands = commands.entity(entity);
        entity_commands.remove::<Opening>().insert((
            Server::new(Instant::now()),
            Opened {
                rx_close_reason,
                rx_connecting: next.rx_connecting,
                _tx_dropped: next.tx_dropped,
            },
        ));
        if let Some(local_addr) = next.local_addr {
            entity_commands.insert(LocalAddr(local_addr));
        }
    }
}

fn poll_opened(
    mut commands: Commands,
    mut servers: Query<(Entity, &mut Opened), With<IrohServer>>,
) {
    for (entity, mut server) in &mut servers {
        if try_close(&mut commands, entity, &mut server.rx_close_reason) {
            continue;
        }

        while let Ok(Some(connecting)) = server.rx_connecting.try_next() {
            let client = commands
                .spawn((
                    ChildOf(entity),
                    IrohServerClient(()),
                    // Store remote_id in Connecting so poll_connecting can use it
                    // when firing SessionEstablished after the handshake completes.
                    Connecting {
                        remote_id: connecting.remote_id,
                        rx_dc_reason: connecting.rx_dc_reason,
                        rx_next: connecting.rx_next,
                    },
                    // Insert RemoteEndpointId immediately — available before the
                    // session is accepted, so SessionRequest observers can read it.
                    RemoteEndpointId(connecting.remote_id),
                ))
                .id();
            _ = connecting.tx_session_entity.send(client);

            commands.trigger(SessionRequest {
                entity: client,
                remote_id: connecting.remote_id,
                alpn: connecting.alpn,
                remote_addr: connecting.remote_addr,
                tx_session_response: Some(connecting.tx_session_response),
            });
        }
    }
}

fn try_close(
    commands: &mut Commands,
    entity: Entity,
    rx_close_reason: &mut oneshot::Receiver<CloseReason>,
) -> bool {
    let close_reason = match rx_close_reason.try_recv() {
        Ok(None) => None,
        Ok(Some(closed)) => Some(closed),
        Err(_) => Some(SessionError::BackendClosed.into()),
    };
    close_reason.is_some_and(|reason| {
        commands.trigger(Closed { entity, reason });
        true
    })
}

fn poll_connecting(
    mut commands: Commands,
    mut clients: Query<(Entity, &mut Connecting), With<IrohServerClient>>,
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

        let remote_id = client.remote_id;
        let (_, dummy) = oneshot::channel();
        let rx_dc = mem::replace(&mut client.rx_dc_reason, dummy);
        let mut entity_commands = commands.entity(entity);
        // RemoteEndpointId was already inserted in poll_opened; re-inserting here
        // is a no-op but makes the component lifecycle explicit.
        entity_commands.remove::<Connecting>().insert((
            IrohIo {
                rx_meta: next.rx_meta,
                rx_packet_b2f: next.rx_packet_b2f,
                tx_packet_f2b: next.tx_packet_f2b,
                tx_user_dc: Some(next.tx_user_dc),
            },
            Connected {
                rx_dc_reason: rx_dc,
            },
            session,
        ));
        if let Some(peer_addr) = next.initial_peer_addr {
            entity_commands.insert(PeerAddr(peer_addr));
        }
        if let Some(rtt) = next.initial_rtt {
            entity_commands.insert(PacketRtt(rtt));
        }

        // Notify integration layers that this server-side session is established.
        commands.trigger(SessionEstablished {
            entity,
            remote_id,
            peer_addr: next.initial_peer_addr,
            initial_rtt: next.initial_rtt,
            mtu: next.initial_mtu,
        });
    }
}

fn poll_connected(
    mut commands: Commands,
    mut clients: Query<(Entity, &mut Connected), With<IrohServerClient>>,
) {
    for (entity, mut client) in &mut clients {
        try_disconnect(&mut commands, entity, &mut client.rx_dc_reason);
    }
}

fn try_disconnect(
    commands: &mut Commands,
    entity: Entity,
    rx_dc: &mut oneshot::Receiver<DisconnectReason>,
) -> bool {
    let dc_reason = match rx_dc.try_recv() {
        Ok(None) => None,
        Ok(Some(disconnected)) => Some(disconnected),
        Err(_) => Some(SessionError::BackendClosed.into()),
    };
    dc_reason.is_some_and(|reason| {
        commands.trigger(Disconnected { entity, reason });
        true
    })
}
