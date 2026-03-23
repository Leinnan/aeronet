#![expect(missing_docs, reason = "testing")]

use {
    aeronet_io::{Session, SessionEndpoint, packet::RecvPacket, server::Server},
    aeronet_iroh::{
        client::{IrohClient, IrohClientPlugin},
        iroh,
        server::{IrohServer, IrohServerPlugin, SessionRequest, SessionResponse},
    },
    bevy::prelude::*,
    bevy_platform::time::Instant,
    bytes::Bytes,
    core::time::Duration,
};

const ALPN: &[u8] = b"aeronet/test/iroh";
const PING: Bytes = Bytes::from_static(b"ping");
const PONG: Bytes = Bytes::from_static(b"pong");

#[derive(Resource)]
struct Finished(bool);

#[test]
fn connect_and_ping_pong() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server_endpoint = runtime.block_on(async {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .alpns(vec![ALPN.to_vec()])
            .clear_relay_transports()
            .bind()
            .await
            .unwrap()
    });
    let client_endpoint = runtime.block_on(async {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .clear_relay_transports()
            .bind()
            .await
            .unwrap()
    });
    let server_addr = server_endpoint.addr();

    let mut app = App::new();
    app.add_plugins((MinimalPlugins, IrohServerPlugin, IrohClientPlugin))
        .insert_resource(Finished(false))
        .add_observer(|mut request: On<SessionRequest>| {
            request.respond(SessionResponse::Accepted);
        })
        .add_observer(|trigger: On<Add, SessionEndpoint>| {
            let _ = trigger.event_target();
        })
        .add_observer(|trigger: On<Add, Server>| {
            let _ = trigger.event_target();
        })
        .add_observer(on_session_added)
        .add_systems(Update, (server_recv, client_recv));

    let world = app.world_mut();
    let server = world.spawn_empty().id();
    IrohServer::open(server_endpoint).apply(world.entity_mut(server));
    let client = world.spawn_empty().id();
    IrohClient::connect(client_endpoint, server_addr, ALPN.to_vec())
        .apply(world.entity_mut(client));

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        app.update();
        if app.world().resource::<Finished>().0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for iroh ping-pong");
}

fn on_session_added(
    trigger: On<Add, Session>,
    parents: Query<&ChildOf>,
    mut sessions: Query<&mut Session>,
) {
    let entity = trigger.event_target();
    if parents.get(entity).is_err() {
        let Ok(mut session) = sessions.get_mut(entity) else {
            return;
        };
        session.send.push(PING.clone());
    }
}

fn server_recv(mut sessions: Query<(&ChildOf, &mut Session)>) {
    for (_, mut session) in &mut sessions {
        let session = &mut *session;
        for RecvPacket { payload, .. } in session.recv.drain(..) {
            if payload == PING {
                session.send.push(PONG.clone());
            }
        }
    }
}

fn client_recv(
    mut sessions: Query<(&mut Session, Option<&ChildOf>)>,
    mut finished: ResMut<Finished>,
) {
    for (mut session, parent) in &mut sessions {
        if parent.is_some() {
            continue;
        }
        let session = &mut *session;
        for RecvPacket { payload, .. } in session.recv.drain(..) {
            if payload == PONG {
                finished.0 = true;
            }
        }
    }
}
