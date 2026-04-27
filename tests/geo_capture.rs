//! Integration test: MapkyAppGeoCapture lifecycle — create, verify in Neo4j, delete.

use anyhow::Result;
use futures::TryStreamExt;
use mapky_app_specs::{GeoCaptureKind, MapkyAppGeoCapture};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use pubky_app_specs::PubkyAppUser;
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_geo_capture_lifecycle() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Photographer".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    let capture = MapkyAppGeoCapture {
        file_uri: format!("pubky://{user_id}/pub/mapky.app/files/photo001"),
        kind: GeoCaptureKind::Photo,
        lat: 46.9481,
        lon: 7.4474,
        ele: Some(540.0),
        heading: Some(90.0),
        pitch: Some(-10.0),
        fov: Some(75.0),
        caption: Some("View from Gurten".to_string()),
        sequence_uri: None,
        sequence_index: None,
        captured_at: Some(1_750_000_000_000_000), // mid-2025 (microseconds)
    };
    let capture_id = capture.create_id();
    let capture_path: pubky::ResourcePath =
        MapkyAppGeoCapture::create_path(&capture_id).parse()?;
    test.put(&user_kp, &capture_path, &capture).await?;

    // Verify indexed.
    let compound_id = format!("{user_id}:{capture_id}");
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_geo_capture",
                "MATCH (u:User {id: $user_id})-[:CAPTURED]->(g:MapkyAppGeoCapture {id: $id})
                 RETURN g.kind AS kind, g.ele AS ele, g.caption AS caption,
                        g.captured_at AS captured_at",
            )
            .param("user_id", user_id.as_str())
            .param("id", compound_id.as_str()),
        )
        .await?;

    let row = stream.try_next().await?;
    assert!(row.is_some(), "GeoCapture should exist in Neo4j");
    let row = row.unwrap();
    let kind: String = row.get("kind")?;
    let ele: f64 = row.get("ele")?;
    let captured_at: i64 = row.get("captured_at")?;
    assert_eq!(kind, "photo");
    assert!((ele - 540.0).abs() < 0.1);
    assert_eq!(captured_at, 1_750_000_000_000_000);

    // Delete and verify.
    test.del(&user_kp, &capture_path).await?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_geo_capture_deleted",
                "MATCH (g:MapkyAppGeoCapture {id: $id}) RETURN count(g) AS cnt",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let cnt: i64 = stream.try_next().await?.unwrap().get("cnt")?;
    assert_eq!(cnt, 0);

    test.cleanup_user(&user_kp).await?;
    Ok(())
}
