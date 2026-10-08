use {
    super::{ClientError, ToConnected},
    crate::session::{SessionBackend, SessionError, SessionMeta},
    aeronet_io::{connection::DisconnectReason, packet::RecvPacket},
    bytes::Bytes,
    futures::{
        channel::{mpsc, oneshot},
        never::Never,
    },
    iroh::{Endpoint, EndpointAddr},
    tracing::debug,
};

pub async fn start(
    endpoint: Endpoint,
    addr: EndpointAddr,
    alpn: Vec<u8>,
    tx_next: oneshot::Sender<ToConnected>,
) -> Result<Never, DisconnectReason> {
    debug!(target = ?addr, "Spawning backend task to connect");
    let conn = endpoint
        .connect(addr, &alpn)
        .await
        .map_err(ClientError::Connect)?;
    debug!("Connected");

    let (initial_peer_addr, initial_rtt) = crate::session::selected_path(&conn);
    let (tx_meta, rx_meta) = mpsc::channel::<SessionMeta>(1);
    let (tx_packet_b2f, rx_packet_b2f) = mpsc::unbounded::<RecvPacket>();
    let (tx_packet_f2b, rx_packet_f2b) = mpsc::unbounded::<Bytes>();
    let (tx_user_dc, rx_user_dc) = oneshot::channel::<String>();
    let next = ToConnected {
        initial_peer_addr,
        initial_rtt,
        remote_id: conn.remote_id(),
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
    tx_next
        .send(next)
        .map_err(|_| SessionError::FrontendClosed)?;

    debug!("Starting session loop");
    Err(backend.start().await)
}
