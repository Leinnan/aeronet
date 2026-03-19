#![expect(missing_docs, reason = "example")]

use {
    aeronet_io::{
        Session,
        connection::{DisconnectReason, Disconnected, LocalAddr},
        server::Server,
    },
    aeronet_iroh::server::{IrohServerPlugin, SessionRequest, SessionResponse},
    bevy::{log::LogPlugin, prelude::*},
};

const ALPN: &str = "aeronet/example/iroh";

fn main() -> AppExit {
    App::new()
        .add_plugins((MinimalPlugins, LogPlugin::default(), IrohServerPlugin))
        .add_systems(Startup, open_server)
        .add_systems(Update, reply)
        .add_observer(on_opened)
        .add_observer(on_session_request)
        .add_observer(on_connected)
        .add_observer(on_disconnected)
        .run()
}

fn open_server(mut commands: Commands) {
    commands
        .spawn_empty()
        .queue(aeronet_iroh::server::config::relay_connect(
            aeronet_iroh::server::config::ServerConfiguration::new(ALPN),
        ));
}

fn on_opened(trigger: On<Add, Server>, servers: Query<Option<&LocalAddr>>) {
    let server = trigger.event_target();
    let local_addr = servers.get(server).ok().flatten().map(|addr| **addr);
    info!("{server} opened with local addr {local_addr:?}");
}

fn on_session_request(mut request: On<SessionRequest>) {
    info!(
        "incoming client {:?}, alpn={}, remote_addr={:?}",
        request.remote_id,
        String::from_utf8_lossy(&request.alpn),
        request.remote_addr,
    );
    request.respond(SessionResponse::Accepted);
}

fn on_connected(trigger: On<Add, Session>) {
    info!("{} connected", trigger.event_target());
}

fn on_disconnected(trigger: On<Disconnected>) {
    match &trigger.reason {
        DisconnectReason::ByUser(reason) => {
            info!("{} disconnected by user: {reason}", trigger.event_target())
        }
        DisconnectReason::ByPeer(reason) => {
            info!("{} disconnected by peer: {reason}", trigger.event_target())
        }
        DisconnectReason::ByError(err) => warn!(
            "{} disconnected due to error: {err:#}",
            trigger.event_target()
        ),
    }
}

fn reply(mut clients: Query<(Entity, &mut Session), With<ChildOf>>) {
    for (client, mut session) in &mut clients {
        let session = &mut *session;
        for packet in session.recv.drain(..) {
            let msg =
                String::from_utf8(packet.payload.into()).unwrap_or_else(|_| "(not UTF-8)".into());
            info!("{client} > {msg}");
            session.send.push(format!("You sent: {msg}").into());
        }
    }
}
