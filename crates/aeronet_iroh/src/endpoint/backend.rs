use {
    super::{EndpointError, ToConnecting, ToOpen},
    crate::{IrohRuntime, session},
    aeronet_io::connection::DisconnectReason,
    futures::{
        FutureExt, SinkExt, StreamExt,
        channel::{mpsc, oneshot},
        never::Never,
    },
    iroh::endpoint::{Builder, Incoming},
    tracing::{Instrument, debug, debug_span},
};

#[cfg(all(feature = "discovery", not(target_family = "wasm")))]
type Gossip = iroh_gossip::net::Gossip;
/// Uninhabited stand-in when discovery is disabled.
#[cfg(not(all(feature = "discovery", not(target_family = "wasm"))))]
#[derive(Debug, Clone)]
enum Gossip {}

pub async fn start(
    builder: Builder,
    discovery: bool,
    tx_next: oneshot::Sender<ToOpen>,
) -> Result<Never, EndpointError> {
    debug!("Spawning backend task to open endpoint");

    let endpoint = builder.bind().await.map_err(EndpointError::Bind)?;
    debug!(id = %endpoint.id(), "Opened endpoint");

    let gossip = spawn_gossip(&endpoint, discovery);

    let (tx_connecting, rx_connecting) = mpsc::channel(1);
    let (tx_dropped, mut rx_dropped) = mpsc::channel::<()>(0);
    tx_next
        .send(ToOpen {
            endpoint: endpoint.clone(),
            #[cfg(all(feature = "discovery", not(target_family = "wasm")))]
            gossip: gossip.clone(),
            rx_connecting,
            tx_dropped,
        })
        .map_err(|_| EndpointError::FrontendClosed)?;

    debug!("Starting endpoint accept loop");
    loop {
        let incoming = futures::select! {
            incoming = endpoint.accept().fuse() => incoming,
            _ = rx_dropped.next() => {
                endpoint.close().await;
                return Err(EndpointError::FrontendClosed);
            }
        }
        .ok_or(EndpointError::Closed)?;

        IrohRuntime::spawn({
            let tx_connecting = tx_connecting.clone();
            let gossip = gossip.clone();
            async move {
                if let Err(err) = accept_session(incoming, tx_connecting, gossip).await {
                    debug!("Failed to accept session: {err:?}");
                }
            }
        });
    }
}

async fn accept_session(
    incoming: Incoming,
    mut tx_connecting: mpsc::Sender<ToConnecting>,
    gossip: Option<Gossip>,
) -> Result<(), DisconnectReason> {
    let conn = incoming.await.map_err(session::SessionError::Accept)?;

    // Gossip connections belong to the discovery swarm, not to the app.
    #[cfg(all(feature = "discovery", not(target_family = "wasm")))]
    if conn.alpn() == iroh_gossip::net::GOSSIP_ALPN
        && let Some(gossip) = gossip
    {
        if let Err(err) = gossip.handle_connection(conn).await {
            debug!("Gossip failed to handle connection: {err:?}");
        }
        return Ok(());
    }
    #[cfg(not(all(feature = "discovery", not(target_family = "wasm"))))]
    let _ = gossip;
    let peer_id = conn.remote_id();

    let (tx_response, rx_response) = oneshot::channel();
    let (tx_dc_reason, rx_dc_reason) = oneshot::channel();
    let (tx_next, rx_next) = oneshot::channel();
    tx_connecting
        .send(ToConnecting {
            peer_id,
            tx_response,
            rx_dc_reason,
            rx_next,
        })
        .await
        .map_err(|_| session::SessionError::FrontendClosed)?;

    let Err(reason) = session::start_incoming(conn, rx_response, tx_next)
        .instrument(debug_span!("session", %peer_id, side = "incoming"))
        .await;
    _ = tx_dc_reason.send(reason);
    Ok(())
}

#[cfg(all(feature = "discovery", not(target_family = "wasm")))]
fn spawn_gossip(endpoint: &iroh::Endpoint, discovery: bool) -> Option<Gossip> {
    discovery.then(|| {
        debug!("Spawning gossip actor for discovery");
        Gossip::builder().spawn(endpoint.clone())
    })
}

#[cfg(not(all(feature = "discovery", not(target_family = "wasm"))))]
const fn spawn_gossip(_: &iroh::Endpoint, _: bool) -> Option<Gossip> {
    None
}
