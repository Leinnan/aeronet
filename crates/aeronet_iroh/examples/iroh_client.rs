#![expect(missing_docs, reason = "example")]

use {
    aeronet_io::{Session, connection::Disconnected},
    aeronet_iroh::{
        client::{IrohClient, IrohClientPlugin},
        iroh,
    },
    bevy::{log::LogPlugin, prelude::*},
    bytes::Bytes,
};

const ALPN: &[u8] = b"aeronet/example/iroh";

fn main() -> AppExit {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let endpoint = runtime.block_on(async {
        iroh::Endpoint::builder()
            .bind()
            .await
            .expect("failed to bind client endpoint")
    });

    let target = std::env::args()
        .nth(1)
        .expect("pass the server endpoint id as the first argument")
        .parse::<iroh::EndpointId>()
        .expect("invalid endpoint id");

    App::new()
        .add_plugins((MinimalPlugins, LogPlugin::default(), IrohClientPlugin))
        .insert_resource(ClientEndpoint(endpoint))
        .insert_resource(Target(target))
        .add_systems(Startup, connect)
        .add_systems(Update, send_and_receive)
        .add_observer(on_connected)
        .add_observer(on_disconnected)
        .run()
}

#[derive(Resource, Deref, DerefMut)]
struct ClientEndpoint(iroh::Endpoint);

#[derive(Resource, Deref, DerefMut)]
struct Target(iroh::EndpointId);

#[derive(Resource, Default)]
struct SentOnce(bool);

fn connect(mut commands: Commands, endpoint: Res<ClientEndpoint>, target: Res<Target>) {
    commands.init_resource::<SentOnce>();
    commands.spawn_empty().queue(IrohClient::connect(
        (**endpoint).clone(),
        **target,
        ALPN.to_vec(),
    ));
}

fn on_connected(trigger: On<Add, Session>) {
    info!("{} connected", trigger.event_target());
}

fn on_disconnected(trigger: On<Disconnected>) {
    info!(
        "{} disconnected: {:?}",
        trigger.event_target(),
        trigger.reason
    );
}

fn send_and_receive(mut sent: ResMut<SentOnce>, mut sessions: Query<&mut Session>) {
    for mut session in &mut sessions {
        let session = &mut *session;
        if !sent.0 {
            session.send.push(Bytes::from_static(b"hello from client"));
            sent.0 = true;
        }

        for packet in session.recv.drain(..) {
            let msg =
                String::from_utf8(packet.payload.into()).unwrap_or_else(|_| "(not UTF-8)".into());
            info!("server replied: {msg}");
        }
    }
}
