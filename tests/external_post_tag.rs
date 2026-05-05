//! Integration test: a `PubkyAppTag` targeting a `MapkyAppReview` URI gets indexed
//! as a `TAGGED` relationship from the `User` node to the `:MapkyAppReview` node in Neo4j.

use anyhow::Result;
use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::MapkyAppReview;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use pubky_app_specs::traits::{HasIdPath as PubkyHasIdPath, HashId};
use pubky_app_specs::{PubkyAppTag, PubkyAppUser};
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_pubky_tag_on_mapky_review() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    // ── Step 1: Create a user ──────────────────────────────────────────────────
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("cross-domain tag test".to_string()),
        image: None,
        links: None,
        name: "CrossDomainTagger".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // ── Step 2: Write a MapkyAppReview to the homeserver ───────────────────────
    let review = MapkyAppReview::new(
        "https://www.openstreetmap.org/node/1573053883".to_string(),
        9,
        Some("Great Bitcoin bar!".to_string()),
        None,
    );
    let review_id = review.create_id();
    let review_path: pubky::ResourcePath = MapkyAppReview::create_path(&review_id).parse()?;
    test.put(&user_kp, &review_path, &review).await?;

    // ── Step 3: Verify the MapkyAppReview was indexed in Neo4j ─────────────────
    let compound_id = format!("{user_id}:{review_id}");
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_mapky_review",
                "MATCH (r:MapkyAppReview {id: $id}) RETURN r.id AS id",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(
        row.is_some(),
        "MapkyAppReview should exist in Neo4j after indexing"
    );

    // ── Step 4: Create a PubkyAppTag targeting the review ──────────────────────
    let review_uri = format!("pubky://{user_id}/pub/mapky.app/reviews/{review_id}");
    let tag = PubkyAppTag {
        uri: review_uri.clone(),
        label: "bitcoin-bar".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_id = tag.create_id();
    let tag_path: pubky::ResourcePath = PubkyAppTag::create_path(&tag_id).parse()?;
    test.put(&user_kp, &tag_path, &tag).await?;

    // ── Step 5: Verify the TAGGED relationship exists in Neo4j ─────────────────
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_cross_domain_tag",
                "MATCH (u:User {id: $user_id})-[t:TAGGED {label: $label}]->(r:MapkyAppReview {id: $compound_id})
                 RETURN t.label AS label",
            )
            .param("user_id", user_id.as_str())
            .param("label", "bitcoin-bar")
            .param("compound_id", compound_id.as_str()),
        )
        .await?;

    let tag_row = stream.try_next().await?;
    assert!(
        tag_row.is_some(),
        "TAGGED relationship should exist between User and MapkyAppReview"
    );
    let label: String = tag_row.unwrap().get("label")?;
    assert_eq!(label, "bitcoin-bar");

    // ── Cleanup ────────────────────────────────────────────────────────────────
    test.del(&user_kp, &tag_path).await?;
    test.del(&user_kp, &review_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
