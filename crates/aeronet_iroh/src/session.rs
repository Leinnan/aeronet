//! Shared iroh session implementation.

use {
    crate::runtime::IrohRuntime,
    aeronet_io::{
        AeronetIoPlugin, IoSystems, Session,
        connection::{
            DROP_DISCONNECT_REASON, Disconnect, DisconnectReason, Disconnected, PeerAddr,
        },
        packet::{IP_MTU, MtuTooSmall, PacketRtt, RecvPacket},
    },
    bevy_app::prelude::*,
    bevy_ecs::prelude::*,
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::{net::SocketAddr, num::Saturating, time::Duration},
    derive_more::{Display, Error},
    futures::{
        FutureExt, SinkExt, StreamExt,
        channel::{mpsc, oneshot},
        never::Never,
    },
    iroh::{
        EndpointId, TransportAddr,
        endpoint::{Connection, ConnectionError, SendDatagramError, VarInt},
    },
    tracing::{trace, trace_span},
};

// ── Public session identity types ─────────────────────────────────────────────

/// The iroh [`EndpointId`] (public key) of the remote peer for this session.
///
/// Always inserted on every session entity (both client and server-side)
/// when the QUIC handshake completes. Unlike [`PeerAddr`], this is available
/// for both direct-connected and relay-only sessions.
///
/// Integration layers (e.g. `lightyear_iroh`) can use this to derive a stable
/// peer identity without querying for optional socket-address components.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Component)]
pub struct RemoteEndpointId(pub EndpointId);

/// Triggered on a session entity when the QUIC handshake completes and the
/// session is ready for datagram exchange.
///
/// Fires during `IoSystems::Poll` on the same frame as the transition
/// from `Connecting` → `Connected`. The `entity` field (and trigger target)
/// is the session entity — the same one that will receive [`Session`],
/// [`PeerAddr`], [`RemoteEndpointId`], etc. on the same frame.
///
/// This is the recommended hook point for integration layers instead of
/// `On<Add, Session>` with fragile optional-component queries, because it
/// carries all session metadata as fields regardless of whether a direct
/// socket address is available.
///
/// # Example
/// ```rust,ignore
/// app.add_observer(|trigger: On<SessionEstablished>, mut commands: Commands| {
///     let session_entity = trigger.entity;
///     println!("Session established with peer {}", trigger.event().remote_id);
/// });
/// ```
#[derive(Debug, Clone, EntityEvent)]
pub struct SessionEstablished {
    /// The session entity that was just established.
    pub entity: Entity,
    /// The remote peer's iroh endpoint identity (always available).
    pub remote_id: EndpointId,
    /// Direct socket address of the peer, if a direct path was selected.
    ///
    /// `None` for relay-only connections where no direct socket path exists.
    pub peer_addr: Option<SocketAddr>,
    /// Initial round-trip time estimate, if available.
    pub initial_rtt: Option<Duration>,
    /// Initial maximum datagram size for this connection.
    pub mtu: usize,
}

pub(crate) struct IrohSessionPlugin;

impl Plugin for IrohSessionPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<AeronetIoPlugin>() {
            app.add_plugins(AeronetIoPlugin);
        }

        app.init_resource::<IrohRuntime>()
            .add_systems(PreUpdate, poll.in_set(IoSystems::Poll))
            .add_systems(PostUpdate, flush.in_set(IoSystems::Flush))
            .add_observer(on_disconnect);
    }
}

/// Session IO component backed by an iroh QUIC connection.
#[derive(Debug, Component)]
#[require(Session::new(Instant::now(), MIN_MTU))]
pub struct IrohIo {
    pub(crate) rx_meta: mpsc::Receiver<SessionMeta>,
    pub(crate) rx_packet_b2f: mpsc::UnboundedReceiver<RecvPacket>,
    pub(crate) tx_packet_f2b: mpsc::UnboundedSender<Bytes>,
    pub(crate) tx_user_dc: Option<oneshot::Sender<String>>,
}

/// Minimum packet MTU supported by this IO layer.
pub const MIN_MTU: usize = IP_MTU;

/// Generic session-level error for iroh sessions.
#[derive(Debug, Display, Error)]
#[non_exhaustive]
pub enum SessionError {
    /// Frontend component was dropped.
    #[display("frontend closed")]
    FrontendClosed,
    /// Backend task exited unexpectedly.
    #[display("backend closed")]
    BackendClosed,
    /// The connection does not support datagrams.
    #[display("datagrams not supported")]
    DatagramsNotSupported,
    /// The connection MTU is too small.
    MtuTooSmall(MtuTooSmall),
    /// The connection was lost.
    #[display("connection lost")]
    Connection(ConnectionError),
}

impl Drop for IrohIo {
    fn drop(&mut self) {
        if let Some(tx_dc) = self.tx_user_dc.take() {
            _ = tx_dc.send(DROP_DISCONNECT_REASON.to_owned());
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionMeta {
    pub peer_addr: Option<SocketAddr>,
    pub packet_rtt: Option<Duration>,
    pub mtu: usize,
}

fn on_disconnect(trigger: On<Disconnect>, mut sessions: Query<&mut IrohIo>) {
    let target = trigger.event_target();
    let Ok(mut io) = sessions.get_mut(target) else {
        return;
    };

    if let Some(tx_dc) = io.tx_user_dc.take() {
        _ = tx_dc.send(trigger.reason.clone());
    }
}

pub(crate) fn poll(
    mut sessions: Query<(
        Entity,
        &mut Session,
        &mut IrohIo,
        Option<&mut PeerAddr>,
        Option<&mut PacketRtt>,
    )>,
    mut commands: Commands,
) {
    'sessions: for (entity, mut session, mut io, mut peer_addr, mut packet_rtt) in &mut sessions {
        let span = trace_span!("poll", %entity);
        let _span = span.enter();

        while let Ok(Some(meta)) = io.rx_meta.try_next() {
            if let Err(err) = session.set_mtu(meta.mtu) {
                commands.trigger(Disconnected {
                    entity,
                    reason: SessionError::MtuTooSmall(err).into(),
                });
                continue 'sessions;
            }

            if let (Some(addr), Some(peer_addr)) = (meta.peer_addr, &mut peer_addr) {
                peer_addr.0 = addr;
            }

            if let (Some(rtt), Some(packet_rtt)) = (meta.packet_rtt, &mut packet_rtt) {
                packet_rtt.0 = rtt;
            }
        }

        let mut num_packets = Saturating(0);
        let mut num_bytes = Saturating(0);
        while let Ok(Some(packet)) = io.rx_packet_b2f.try_next() {
            num_packets += 1;
            session.stats.packets_recv += 1;

            num_bytes += packet.payload.len();
            session.stats.bytes_recv += packet.payload.len();

            session.recv.push(packet);
        }

        if num_packets.0 > 0 {
            trace!(%num_packets, %num_bytes, "Received packets");
        }
    }
}

fn flush(mut sessions: Query<(Entity, &mut Session, &IrohIo)>) {
    for (entity, mut session, io) in &mut sessions {
        let span = trace_span!("flush", %entity);
        let _span = span.enter();

        let session = &mut *session;
        let mut num_packets = Saturating(0);
        let mut num_bytes = Saturating(0);
        for packet in session.send.drain(..) {
            num_packets += 1;
            session.stats.packets_sent += 1;

            num_bytes += packet.len();
            session.stats.bytes_sent += packet.len();

            _ = io.tx_packet_f2b.unbounded_send(packet);
        }

        if num_packets.0 > 0 {
            trace!(%num_packets, %num_bytes, "Flushed packets");
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionBackend {
    pub conn: Connection,
    pub tx_meta: mpsc::Sender<SessionMeta>,
    pub tx_packet_b2f: mpsc::UnboundedSender<RecvPacket>,
    pub rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
    pub rx_user_dc: oneshot::Receiver<String>,
}

impl SessionBackend {
    pub async fn start(self) -> DisconnectReason {
        let Self {
            conn,
            tx_meta,
            tx_packet_b2f,
            rx_packet_f2b,
            mut rx_user_dc,
        } = self;

        let (tx_err, mut rx_err) = mpsc::channel::<SessionError>(1);

        let (_tx_meta_closed, rx_meta_closed) = oneshot::channel();
        IrohRuntime::spawn({
            let conn = conn.clone();
            let mut tx_err = tx_err.clone();
            async move {
                let Err(err) = meta_loop(conn, rx_meta_closed, tx_meta).await;
                _ = tx_err.try_send(err);
            }
        });

        let (_tx_receiving_closed, rx_receiving_closed) = oneshot::channel();
        IrohRuntime::spawn({
            let conn = conn.clone();
            let mut tx_err = tx_err.clone();
            async move {
                let Err(err) = recv_loop(conn, rx_receiving_closed, tx_packet_b2f).await;
                _ = tx_err.try_send(err);
            }
        });

        let (_tx_sending_closed, rx_sending_closed) = oneshot::channel();
        IrohRuntime::spawn({
            let conn = conn.clone();
            let mut tx_err = tx_err.clone();
            async move {
                let Err(err) = send_loop(conn, rx_sending_closed, rx_packet_f2b).await;
                _ = tx_err.try_send(err);
            }
        });

        futures::select! {
            err = rx_err.next() => {
                let err = err.unwrap_or(SessionError::BackendClosed);
                get_disconnect_reason(err)
            }
            reason = rx_user_dc => {
                if let Ok(reason) = reason {
                    disconnect(conn, &reason).await;
                    DisconnectReason::by_user(reason)
                } else {
                    DisconnectReason::by_error(SessionError::FrontendClosed)
                }
            }
        }
    }
}

async fn meta_loop(
    conn: Connection,
    mut rx_closed: oneshot::Receiver<()>,
    mut tx_meta: mpsc::Sender<SessionMeta>,
) -> Result<Never, SessionError> {
    const META_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

    loop {
        futures::select! {
            () = IrohRuntime::sleep(META_UPDATE_INTERVAL).fuse() => {},
            _ = rx_closed => return Err(SessionError::FrontendClosed),
        };

        let (peer_addr, packet_rtt) = selected_path(&conn);
        let meta = SessionMeta {
            peer_addr,
            packet_rtt,
            mtu: conn
                .max_datagram_size()
                .ok_or(SessionError::DatagramsNotSupported)?,
        };
        match tx_meta.try_send(meta) {
            Ok(()) => {}
            Err(err) if err.is_full() => {}
            Err(_) => return Err(SessionError::FrontendClosed),
        }
    }
}

async fn recv_loop(
    conn: Connection,
    mut recv_closed: oneshot::Receiver<()>,
    mut tx_packet_b2f: mpsc::UnboundedSender<RecvPacket>,
) -> Result<Never, SessionError> {
    loop {
        let packet = futures::select! {
            x = conn.read_datagram().fuse() => x,
            _ = recv_closed => return Err(SessionError::FrontendClosed),
        }
        .map_err(SessionError::Connection)?;

        tx_packet_b2f
            .send(RecvPacket {
                recv_at: Instant::now(),
                payload: packet,
            })
            .await
            .map_err(|_| SessionError::BackendClosed)?;
    }
}

async fn send_loop(
    conn: Connection,
    mut rx_closed: oneshot::Receiver<()>,
    mut rx_packet_f2b: mpsc::UnboundedReceiver<Bytes>,
) -> Result<Never, SessionError> {
    loop {
        let packet = futures::select! {
            x = rx_packet_f2b.next() => x,
            _ = rx_closed => return Err(SessionError::FrontendClosed),
        }
        .ok_or(SessionError::FrontendClosed)?;

        let packet_len = packet.len();
        match conn.send_datagram(packet) {
            Ok(()) => {}
            Err(SendDatagramError::ConnectionLost(_)) => {}
            Err(SendDatagramError::TooLarge) => {
                let mtu = conn.max_datagram_size();
                tracing::debug!(
                    packet_len,
                    ?mtu,
                    "Attempted to send datagram larger than MTU"
                );
            }
            Err(SendDatagramError::UnsupportedByPeer | SendDatagramError::Disabled) => {
                return Err(SessionError::DatagramsNotSupported);
            }
        }
    }
}

fn get_disconnect_reason(err: SessionError) -> DisconnectReason {
    match err {
        SessionError::Connection(ConnectionError::ApplicationClosed(close)) => {
            DisconnectReason::by_peer(String::from_utf8_lossy(&close.reason).into_owned())
        }
        err => DisconnectReason::by_error(err),
    }
}

async fn disconnect(conn: Connection, reason: &str) {
    conn.close(VarInt::from_u32(0), reason.as_bytes());
    _ = conn.closed().await;
}

/// Socket address and RTT of the connection's currently selected path.
///
/// The address is `None` when the selected path goes through a relay.
pub(crate) fn selected_path(conn: &Connection) -> (Option<SocketAddr>, Option<Duration>) {
    let paths = conn.paths();
    let Some(path) = paths.iter().find(iroh::endpoint::Path::is_selected) else {
        return (None, None);
    };
    let addr = match path.remote_addr() {
        TransportAddr::Ip(addr) => Some(*addr),
        _ => None,
    };
    (addr, Some(path.rtt()))
}
