//! Integration test: A `PubkyAppTag` stored under `/pub/mapky.app/tags/` with
//! an OSM URL as the `uri` field is indexed as a universal Resource tag and
//! surfaced through the MapKy place tag endpoint.

use anyhow::Result;
use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use mapky_app_specs::MapkyAppReview;
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_common::models::resource::{normalize_uri, resource_id};
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{PubkyAppTag, PubkyAppUser};
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_tag_on_osm_place() -> Result<()> {
    let mut test = WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "PlaceTagger".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // First create a review so the Place node exists (via Nominatim geocoding).
    let osm_url = "https://www.openstreetmap.org/node/1573053883";
    let review = MapkyAppReview::new(osm_url.to_string(), 8, Some("Nice place".to_string()), None);
    let review_id = review.create_id();
    let review_path: pubky::ResourcePath = MapkyAppReview::create_path(&review_id).parse()?;
    test.put(&user_kp, &review_path, &review).await?;

    // Verify Place exists.
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(Query::new(
            "test_place_exists",
            "MATCH (p:Place {osm_canonical: 'node/1573053883'})
                 RETURN p.tag_count AS tag_count",
        ))
        .await?;
    let row = stream.try_next().await?;
    assert!(row.is_some(), "Place should exist after review indexing");
    let initial_tag_count: i64 = row.unwrap().get("tag_count")?;

    // Write a PubkyAppTag at /pub/mapky.app/tags/{hash} targeting the OSM URL.
    let tag = PubkyAppTag {
        uri: osm_url.to_string(),
        label: "bitcoin-friendly".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_id = tag.create_id();
    // Store under mapky.app namespace — Nexus core indexes this as a universal tag.
    let tag_path_str = format!("/pub/mapky.app/tags/{tag_id}");
    let tag_path: pubky::ResourcePath = tag_path_str.parse()?;
    test.put(&user_kp, &tag_path, &tag).await?;

    let (normalized, _) = normalize_uri(osm_url).map_err(anyhow::Error::msg)?;
    let resource_id = resource_id(&normalized);

    // Verify the TAGGED relationship exists on the universal Resource.
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_universal_place_tag",
                "MATCH (u:User {id: $user_id})-[t:TAGGED {label: 'bitcoin-friendly'}]->(r:Resource {id: $resource_id})
                 RETURN t.label AS label, r.uri AS uri",
            )
            .param("user_id", user_id.as_str())
            .param("resource_id", resource_id.as_str()),
        )
        .await?;

    let tag_row = stream.try_next().await?;
    assert!(
        tag_row.is_some(),
        "TAGGED relationship should exist between User and Resource"
    );
    let tag_row = tag_row.unwrap();
    let label: String = tag_row.get("label")?;
    let uri: String = tag_row.get("uri")?;
    assert_eq!(label, "bitcoin-friendly");
    assert_eq!(uri, osm_url);

    // The Place projection remains unchanged; tags are read through Resource.
    let mut stream = graph
        .execute(Query::new(
            "test_place_tag_count_unchanged",
            "MATCH (p:Place {osm_canonical: 'node/1573053883'})
             RETURN p.tag_count AS tag_count",
        ))
        .await?;
    let row = stream.try_next().await?.expect("Place should still exist");
    let new_tag_count: i64 = row.get("tag_count")?;
    assert_eq!(new_tag_count, initial_tag_count);

    // Cleanup.
    test.del(&user_kp, &tag_path).await?;
    test.del(&user_kp, &review_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
