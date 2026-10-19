//! End-to-end tests for gossip room discovery.

#![cfg(all(feature = "discovery", not(target_family = "wasm")))]

use {
    aeronet_io::Session,
    aeronet_iroh::{
        IrohPlugin,
        discovery::{
            AnnounceRoom, DiscoveredRooms, DiscoveryConfig, RoomAnnouncement, SearchRooms,
        },
        endpoint::IrohEndpoint,
        session::{SessionRequest, SessionResponse},
    },
    bevy::{ecs::system::EntityCommand, prelude::*},
    core::time::Duration,
    iroh::{address_lookup::memory::MemoryLookup, endpoint::presets},
    iroh_gossip::net::GOSSIP_ALPN,
    std::{thread, time::Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);
const ALPN: &[u8] = b"aeronet-iroh/tests/0";
const ROOM_NAME: &str = "test room";

#[derive(Debug, Default, Resource)]
struct SessionRequests(usize);

#[test]
fn announce_and_discover_room() {
    let lookup = MemoryLookup::new();
    let mut app = test_app();
    let host = open_endpoint(&mut app, &lookup);
    let searcher = open_endpoint(&mut app, &lookup);
    wait_until(&mut app, |world| {
        world.get::<IrohEndpoint>(host).is_some() && world.get::<IrohEndpoint>(searcher).is_some()
    });

    let host_endpoint = app.world().get::<IrohEndpoint>(host).unwrap();
    let host_id = host_endpoint.id();
    lookup.add_endpoint_info(host_endpoint.addr());
    lookup.add_endpoint_info(app.world().get::<IrohEndpoint>(searcher).unwrap().addr());

    let announce = AnnounceRoom::start(
        host,
        config(Vec::new()),
        RoomAnnouncement {
            room_name: ROOM_NAME.to_owned(),
            max_players: 4,
            ..Default::default()
        },
    );
    let entity = app.world_mut().spawn_empty().id();
    announce.apply(app.world_mut().entity_mut(entity));

    let search = SearchRooms::start(searcher, config(vec![host_id]));
    let entity = app.world_mut().spawn_empty().id();
    search.apply(app.world_mut().entity_mut(entity));

    wait_until(&mut app, |world| {
        world
            .resource::<DiscoveredRooms>()
            .rooms
            .get(&host_id)
            .is_some_and(|room| room.announcement.room_name == ROOM_NAME)
    });

    // a discovery endpoint must still accept game sessions
    let target = app.world().get::<IrohEndpoint>(host).unwrap().addr();
    let connect = app
        .world()
        .get::<IrohEndpoint>(searcher)
        .unwrap()
        .connect(target, ALPN);
    let outgoing = app.world_mut().spawn_empty().id();
    connect.apply(app.world_mut().entity_mut(outgoing));

    wait_until(&mut app, |world| world.get::<Session>(outgoing).is_some());
    assert_eq!(app.world().resource::<SessionRequests>().0, 1);
}

fn config(bootstrap_nodes: Vec<iroh::EndpointId>) -> DiscoveryConfig {
    DiscoveryConfig {
        game: "aeronet-iroh-tests".to_owned(),
        version: "0".to_owned(),
        bootstrap_nodes,
    }
}

fn test_app() -> App {
    let mut app = App::new();
    app.add_plugins((MinimalPlugins, IrohPlugin))
        .init_resource::<SessionRequests>()
        .add_observer(
            |mut request: On<SessionRequest>, mut requests: ResMut<SessionRequests>| {
                requests.0 += 1;
                request.respond(SessionResponse::Accepted);
            },
        );
    app
}

fn open_endpoint(app: &mut App, lookup: &MemoryLookup) -> Entity {
    let entity = app.world_mut().spawn_empty().id();
    let builder = iroh::Endpoint::builder(presets::Minimal)
        .address_lookup(lookup.clone())
        .alpns(vec![ALPN.to_vec(), GOSSIP_ALPN.to_vec()]);
    IrohEndpoint::open_with_discovery(builder).apply(app.world_mut().entity_mut(entity));
    entity
}

fn wait_until(app: &mut App, mut condition: impl FnMut(&mut World) -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        app.update();
        if condition(app.world_mut()) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        thread::sleep(Duration::from_millis(5));
    }
}
