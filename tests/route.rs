//! Integration test: MapkyAppRoute lifecycle — create with waypoints,
//! verify bounding box in Neo4j, delete.

use anyhow::Result;
use futures::TryStreamExt;
use mapky_app_specs::{MapkyAppRoute, RouteActivityType, Waypoint};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use pubky_app_specs::PubkyAppUser;
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_route_lifecycle() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Hiker".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    let route = MapkyAppRoute {
        name: "Uetliberg trail".to_string(),
        description: Some("A walk from Zurich to Uetliberg".to_string()),
        activity: RouteActivityType::Hiking,
        difficulty: None,
        waypoints: vec![
            Waypoint {
                lat: 47.3769,
                lon: 8.5417,
                ele: Some(408.0),
                name: Some("Zurich HB".to_string()),
            },
            Waypoint {
                lat: 47.3494,
                lon: 8.4920,
                ele: Some(869.0),
                name: Some("Uetliberg".to_string()),
            },
        ],
        osm_ways: None,
        control_points: None,
        steps: None,
        distance_m: Some(5800.0),
        elevation_gain_m: Some(461.0),
        elevation_loss_m: None,
        estimated_duration_s: Some(5400),
        image_uri: None,
    };
    let route_id = route.create_id();
    let route_path: pubky::ResourcePath = MapkyAppRoute::create_path(&route_id).parse()?;
    test.put(&user_kp, &route_path, &route).await?;

    // Verify indexed with correct bbox.
    let compound_id = format!("{user_id}:{route_id}");
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_route",
                "MATCH (u:User {id: $user_id})-[:CREATED]->(r:MapkyAppRoute {id: $id})
                 RETURN r.name AS name, r.min_lat AS min_lat, r.max_lat AS max_lat,
                        r.waypoint_count AS waypoint_count, r.distance_m AS distance_m",
            )
            .param("user_id", user_id.as_str())
            .param("id", compound_id.as_str()),
        )
        .await?;

    let row = stream.try_next().await?;
    assert!(row.is_some(), "Route should exist in Neo4j");
    let row = row.unwrap();
    let name: String = row.get("name")?;
    let min_lat: f64 = row.get("min_lat")?;
    let max_lat: f64 = row.get("max_lat")?;
    let waypoint_count: i64 = row.get("waypoint_count")?;
    assert_eq!(name, "Uetliberg trail");
    assert!((min_lat - 47.3494).abs() < 0.001);
    assert!((max_lat - 47.3769).abs() < 0.001);
    assert_eq!(waypoint_count, 2);

    // Delete and verify.
    test.del(&user_kp, &route_path).await?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_route_deleted",
                "MATCH (r:MapkyAppRoute {id: $id}) RETURN count(r) AS cnt",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let cnt: i64 = stream.try_next().await?.unwrap().get("cnt")?;
    assert_eq!(cnt, 0);

    test.cleanup_user(&user_kp).await?;
    Ok(())
}
