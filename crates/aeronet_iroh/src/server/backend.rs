use {
    super::{ServerError, SessionResponse, ToConnected, ToConnecting, ToOpen},
    crate::{
        IrohRuntime,
        session::{SessionBackend, SessionError, SessionMeta},
    },
    aeronet_io::{connection::DisconnectReason, packet::RecvPacket, server::CloseReason},
    bevy_ecs::prelude::Entity,
    bytes::Bytes,
    futures::{
        FutureExt, SinkExt, StreamExt,
        channel::{mpsc, oneshot},
        never::Never,
    },
    iroh::{Endpoint, Watcher},
    tracing::{Instrument, debug, debug_span},
};

pub async fn start(
    endpoint: Endpoint,
    tx_next: oneshot::Sender<ToOpen>,
    #[cfg(feature = "discovery")] gossip: Option<iroh_gossip::net::Gossip>,
) -> Result<Never, CloseReason> {
    debug!("Spawning backend task to open server");

    let (tx_connecting, rx_connecting) = mpsc::channel(1);
    let (tx_dropped, mut rx_dropped) = mpsc::channel::<()>(0);

    let local_addr = endpoint.bound_sockets().into_iter().next();
    let next = ToOpen {
        local_addr,
        rx_connecting,
        tx_dropped,
    };
    tx_next
        .send(next)
        .map_err(|_| SessionError::FrontendClosed)?;

    debug!("Starting server loop");
    loop {
        let connecting = futures::select! {
            x = endpoint.accept().fuse() => x,
            _ = rx_dropped.next() => {
                endpoint.close().await;
                return Err(CloseReason::ByError(SessionError::FrontendClosed.into()));
            }
        };

        let Some(connecting) = connecting else {
            endpoint.close().await;
            return Err(CloseReason::ByUser("endpoint closed".into()));
        };

        IrohRuntime::spawn({
            let tx_connecting = tx_connecting.clone();
            #[cfg(feature = "discovery")]
            let gossip = gossip.clone();
            async move {
                if let Err(err) = accept_session(
                    connecting,
                    tx_connecting,
                    #[cfg(feature = "discovery")]
                    gossip,
                )
                .await
                {
                    debug!("Failed to accept session: {err:?}");
                }
            }
        });
    }
}

async fn accept_session(
    incoming: iroh::endpoint::Incoming,
    mut tx_connecting: mpsc::Sender<ToConnecting>,
    #[cfg(feature = "discovery")] gossip: Option<iroh_gossip::net::Gossip>,
) -> Result<(), CloseReason> {
    let remote_addr = Some(incoming.remote_address());
    let conn = incoming.await.map_err(ServerError::Accept)?;

    // When the discovery feature is enabled and a gossip handle is present,
    // forward GOSSIP_ALPN connections to the gossip actor instead of treating
    // them as game sessions.
    #[cfg(feature = "discovery")]
    if conn.alpn() == iroh_gossip::net::GOSSIP_ALPN
        && let Some(gossip) = gossip
    {
        if let Err(err) = gossip.handle_connection(conn).await {
            debug!("Gossip handle_connection error: {err:?}");
        }
        return Ok(());
    }
    let alpn = conn.alpn().to_vec();
    let remote_id = conn.remote_id();
    let mut paths = conn.paths();
    let paths = paths.get();
    let selected_path = paths.iter().find(|path| path.is_selected()).cloned();

    let (tx_session_entity, rx_session_entity) = oneshot::channel::<Entity>();
    let (tx_session_response, rx_session_response) = oneshot::channel::<SessionResponse>();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel::<DisconnectReason>();
    let (tx_next, rx_next) = oneshot::channel::<ToConnected>();
    tx_connecting
        .send(ToConnecting {
            remote_id,
            alpn,
            remote_addr: selected_path
                .as_ref()
                .and_then(crate::session::path_socket_addr)
                .or(remote_addr),
            tx_session_entity,
            tx_session_response,
            rx_dc_reason,
            rx_next,
        })
        .await
        .map_err(|_| SessionError::FrontendClosed)?;
    let session = rx_session_entity
        .await
        .map_err(|_| SessionError::FrontendClosed)?;

    let Err(dc_reason) = handle_session(conn, rx_session_response, tx_next)
        .instrument(debug_span!("session", %session))
        .await;
    _ = tx_dc_reason.send(dc_reason);
    Ok(())
}

async fn handle_session(
    conn: iroh::endpoint::Connection,
    rx_session_response: oneshot::Receiver<SessionResponse>,
    tx_connected: oneshot::Sender<ToConnected>,
) -> Result<Never, DisconnectReason> {
    debug!("New session request");

    let session_response = rx_session_response
        .await
        .map_err(|_| SessionError::FrontendClosed)?;
    debug!("Frontend responded to this session request with {session_response:?}");

    if session_response == SessionResponse::Rejected {
        conn.close(iroh::endpoint::VarInt::from_u32(1), b"rejected");
        _ = conn.closed().await;
        return Err(ServerError::Rejected.into());
    }

    let mut paths = conn.paths();
    let paths = paths.get();
    let selected_path = paths.iter().find(|path| path.is_selected()).cloned();
    let (tx_meta, rx_meta) = mpsc::channel::<SessionMeta>(1);
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded::<RecvPacket>();
    let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded::<Bytes>();
    let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();
    let next = ToConnected {
        initial_peer_addr: selected_path
            .as_ref()
            .and_then(crate::session::path_socket_addr),
        initial_rtt: selected_path.as_ref().map(iroh::endpoint::PathInfo::rtt),
        initial_mtu: conn
            .max_datagram_size()
            .ok_or(SessionError::DatagramsNotSupported)?,
        rx_meta,
        rx_packet_b2f,
        tx_packet_f2b,
        tx_user_dc,
    };
    let backend = SessionBackend {
        conn,
        tx_meta,
        tx_packet_b2f,
        rx_packet_f2b,
        rx_user_dc,
    };
    tx_connected
        .send(next)
        .map_err(|_| SessionError::FrontendClosed)?;

    debug!("Starting session loop");
    Err(backend.start().await)
}
