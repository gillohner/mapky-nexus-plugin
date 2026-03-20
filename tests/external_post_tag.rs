//! Integration test: a `PubkyAppTag` targeting a `MapkyAppPost` URI gets indexed
//! as a `TAGGED` relationship from the `User` node to the `MapkyPost` node in Neo4j.

use anyhow::Result;
use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::{MapkyAppPost, MapkyAppPostKind, OsmElementType, OsmRef};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::{HomeserverIdPath, WatcherTest};
use pubky::Keypair;
use pubky_app_specs::{
    traits::{HasIdPath, HashId, TimestampId},
    PubkyAppTag, PubkyAppUser,
};
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_pubky_tag_on_mapky_post() -> Result<()> {
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

    // ── Step 2: Write a MapkyAppPost to the homeserver ─────────────────────────
    let mapky_post = MapkyAppPost::new(
        MapkyAppPostKind::Review,
        OsmRef::new(OsmElementType::Node, 1573053883),
        Some("Great Bitcoin bar!".to_string()),
        Some(9),
        None,
        None,
    );
    let post_id = mapky_post.create_id();
    let post_path: pubky::ResourcePath = MapkyAppPost::create_path(&post_id).parse()?;
    test.put(&user_kp, &post_path, &mapky_post).await?;

    // ── Step 3: Verify the MapkyAppPost was indexed in Neo4j ──────────────────
    let compound_id = format!("{user_id}:{post_id}");
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_mapky_post",
                "MATCH (p:MapkyAppPost {id: $id}) RETURN p.id AS id",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(row.is_some(), "MapkyAppPost should exist in Neo4j after indexing");

    // ── Step 4: Create a PubkyAppTag targeting the MapkyPost ─────────────────
    let mapky_post_uri = format!("pubky://{user_id}/pub/mapky.app/posts/{post_id}");
    let tag = PubkyAppTag {
        uri: mapky_post_uri.clone(),
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
                "MATCH (u:User {id: $user_id})-[t:TAGGED {label: $label}]->(p:MapkyAppPost {id: $compound_id})
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
        "TAGGED relationship should exist between User and MapkyAppPost"
    );
    let label: String = tag_row.unwrap().get("label")?;
    assert_eq!(label, "bitcoin-bar");

    // ── Cleanup ────────────────────────────────────────────────────────────────
    test.del(&user_kp, &tag_path).await?;
    test.del(&user_kp, &post_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
