//! Integration test: collection-kind mapky post lifecycle — create with multiple places,
//! verify CONTAINS edges, update (remove a place), verify stale edge cleanup, delete.

use anyhow::Result;
use futures::TryStreamExt;
use mapky_app_specs::traits::TimestampId;
use mapky_app_specs::{PubkyAppCollectionContent, PubkyAppPost, PubkyAppPostKind};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use pubky_app_specs::PubkyAppUser;
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_collection_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Curator".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Create a collection with 2 places.
    let envelope = PubkyAppCollectionContent {
        name: "Bitcoin spots".to_string(),
        description: Some("Places that accept Bitcoin".to_string()),
        cover_image: None,
        items: vec![
            "https://www.openstreetmap.org/node/1573053883".to_string(),
            "https://www.openstreetmap.org/node/3646146894".to_string(),
        ],
    };
    let collection = PubkyAppPost::new(
        serde_json::to_string(&envelope)?,
        PubkyAppPostKind::Collection,
        None,
        None,
        None,
    );
    let collection_id = collection.create_id();
    let collection_path: pubky::ResourcePath =
        format!("/pub/mapky.app/posts/{collection_id}").parse()?;
    test.put(&user_kp, &collection_path, &collection).await?;

    // Verify collection node + CONTAINS edges.
    let compound_id = format!("{user_id}:{collection_id}");
    let graph = get_neo4j_graph()?;

    let mut stream = graph
        .execute(
            Query::new(
                "test_check_collection",
                "MATCH (u:User {id: $user_id})-[:CREATED]->(c:MapkyAppCollection {id: $id})
                 OPTIONAL MATCH (c)-[:CONTAINS]->(p:Place)
                 RETURN c.name AS name, count(p) AS place_count",
            )
            .param("user_id", user_id.as_str())
            .param("id", compound_id.as_str()),
        )
        .await?;

    let row = stream.try_next().await?;
    assert!(row.is_some(), "Collection should exist in Neo4j");
    let row = row.unwrap();
    let name: String = row.get("name")?;
    let place_count: i64 = row.get("place_count")?;
    assert_eq!(name, "Bitcoin spots");
    assert_eq!(place_count, 2);

    // Update: remove one place.
    let updated_envelope = PubkyAppCollectionContent {
        name: "Bitcoin spots".to_string(),
        description: Some("Places that accept Bitcoin".to_string()),
        cover_image: None,
        items: vec!["https://www.openstreetmap.org/node/1573053883".to_string()],
    };
    let updated_collection = PubkyAppPost::new(
        serde_json::to_string(&updated_envelope)?,
        PubkyAppPostKind::Collection,
        None,
        None,
        None,
    );
    test.put(&user_kp, &collection_path, &updated_collection)
        .await?;

    // Verify stale edge was cleaned up.
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_collection_updated",
                "MATCH (c:MapkyAppCollection {id: $id})-[:CONTAINS]->(p:Place)
                 RETURN count(p) AS place_count",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let place_count: i64 = stream.try_next().await?.unwrap().get("place_count")?;
    assert_eq!(place_count, 1, "Stale CONTAINS edge should be removed");

    // Delete and verify.
    test.del(&user_kp, &collection_path).await?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_collection_deleted",
                "MATCH (c:MapkyAppCollection {id: $id}) RETURN count(c) AS cnt",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let cnt: i64 = stream.try_next().await?.unwrap().get("cnt")?;
    assert_eq!(cnt, 0);

    test.cleanup_user(&user_kp).await?;
    Ok(())
}
