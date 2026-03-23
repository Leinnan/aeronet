//! Server configuration and establishing endpoint id

use std::str::FromStr;

use bevy_ecs::{
    component::Component,
    entity::Entity,
    system::{Commands, EntityCommand, Query},
    world::EntityWorldMut,
};
use bevy_reflect::Reflect;
use futures::channel::oneshot;
use iroh::{Endpoint, SecretKey, endpoint::BindError};
use tracing::{Instrument, debug_span, info};

use crate::{IrohRuntime, server::IrohServer};

/// Iroh server configuration
#[derive(Debug, Clone, Default, Reflect)]
#[reflect(Debug, Clone)]
pub struct ServerConfiguration {
    /// Optional secret key. If not set, a new secret key will be generated.
    pub secret_key: Option<String>,
    /// ALPNs protocol in use
    pub alpns: Vec<String>,
}

impl ServerConfiguration {
    /// Creates new configuration with given ALPN.
    pub fn new(alpn: impl ToString) -> Self {
        Self {
            secret_key: None,
            alpns: vec![alpn.to_string()],
        }
    }
}

impl Into<iroh::endpoint::Builder> for ServerConfiguration {
    fn into(self) -> iroh::endpoint::Builder {
        let mut endpoint_builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0).alpns(
            self.alpns
                .iter()
                .map(|alpn| alpn.as_bytes().to_vec())
                .collect(),
        );
        if let Some(secret) = &self.secret_key {
            if let Ok(secret_key) = SecretKey::from_str(secret) {
                endpoint_builder = endpoint_builder.secret_key(secret_key);
            }
        }
        endpoint_builder
    }
}

/// Relay Connecting component
#[derive(Debug, Component)]
pub struct RelayConnecting(oneshot::Receiver<Result<Endpoint, BindError>>);

/// Starts server connection
pub fn relay_connect(builder: impl Into<iroh::endpoint::Builder>) -> impl EntityCommand {
    let builder = builder.into();
    move |entity: EntityWorldMut| relay_connect_impl(entity, builder)
}

/// Starts server connection
fn relay_connect_impl(mut entity: EntityWorldMut, builder: iroh::endpoint::Builder) {
    let runtime = entity.world().resource::<IrohRuntime>().clone();

    let (tx_next, rx_next) = oneshot::channel::<Result<Endpoint, BindError>>();
    runtime.spawn_on_self(
        async move {
            let build = builder.bind().await;
            _ = tx_next.send(build);
        }
        .instrument(debug_span!("server", entity = %entity.id())),
    );
    entity.insert((IrohServer(()), RelayConnecting(rx_next)));
}

pub(super) fn handle_connected_to_relay(
    mut q: Query<(&mut RelayConnecting, Entity)>,
    mut commands: Commands,
) {
    for (mut server, entity) in q.iter_mut() {
        if let Ok(Some(message)) = server.0.try_recv() {
            match message {
                Ok(endpoint) => {
                    let endpoint_id = endpoint.id();
                    let endpoint_addr = endpoint.bound_sockets();
                    info!("server endpoint id: {endpoint_id}");
                    info!("server endpoint addr: {endpoint_addr:?}");
                    if let Ok(mut e_cmd) = commands.get_entity(entity) {
                        e_cmd
                            .remove::<RelayConnecting>()
                            .queue(IrohServer::open(endpoint));
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect to the server: {}", e);
                    if let Ok(mut e_cmd) = commands.get_entity(entity) {
                        e_cmd.remove::<RelayConnecting>();
                    }
                }
            }
        }
    }
}
