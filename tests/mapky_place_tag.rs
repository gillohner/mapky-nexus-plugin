//! Integration test: A `PubkyAppTag` stored under `/pub/mapky.app/tags/` with
//! an OSM URL as the `uri` field creates a `(User)-[:TAGGED]->(Place)` relationship
//! and increments `tag_count` on the Place node.

use anyhow::Result;
use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::{MapkyAppPost, MapkyAppPostKind};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use pubky_app_specs::{
    traits::{HasIdPath, HashId, TimestampId},
    PubkyAppTag, PubkyAppUser,
};
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_tag_on_osm_place() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "PlaceTagger".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // First create a post so the Place node exists (via Nominatim geocoding).
    let osm_url = "https://www.openstreetmap.org/node/1573053883";
    let post = MapkyAppPost::new(
        MapkyAppPostKind::Review,
        osm_url.to_string(),
        Some("Nice place".to_string()),
        Some(8),
        None,
        None,
    );
    let post_id = post.create_id();
    let post_path: pubky::ResourcePath = MapkyAppPost::create_path(&post_id).parse()?;
    test.put(&user_kp, &post_path, &post).await?;

    // Verify Place exists.
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_place_exists",
                "MATCH (p:Place {osm_canonical: 'node/1573053883'})
                 RETURN p.tag_count AS tag_count",
            ),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(row.is_some(), "Place should exist after post indexing");
    let initial_tag_count: i64 = row.unwrap().get("tag_count")?;

    // Write a PubkyAppTag at /pub/mapky.app/tags/{hash} targeting the OSM URL.
    let tag = PubkyAppTag {
        uri: osm_url.to_string(),
        label: "bitcoin-friendly".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_id = tag.create_id();
    // Store under mapky.app namespace — this gets routed to the plugin.
    let tag_path_str = format!("/pub/mapky.app/tags/{tag_id}");
    let tag_path: pubky::ResourcePath = tag_path_str.parse()?;
    test.put(&user_kp, &tag_path, &tag).await?;

    // Verify the TAGGED relationship exists.
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_place_tag",
                "MATCH (u:User {id: $user_id})-[t:TAGGED {label: 'bitcoin-friendly'}]->(p:Place {osm_canonical: 'node/1573053883'})
                 RETURN t.label AS label, p.tag_count AS tag_count",
            )
            .param("user_id", user_id.as_str()),
        )
        .await?;

    let tag_row = stream.try_next().await?;
    assert!(
        tag_row.is_some(),
        "TAGGED relationship should exist between User and Place"
    );
    let tag_row = tag_row.unwrap();
    let label: String = tag_row.get("label")?;
    let new_tag_count: i64 = tag_row.get("tag_count")?;
    assert_eq!(label, "bitcoin-friendly");
    assert_eq!(
        new_tag_count,
        initial_tag_count + 1,
        "tag_count should be incremented"
    );

    // Cleanup.
    test.del(&user_kp, &tag_path).await?;
    test.del(&user_kp, &post_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
