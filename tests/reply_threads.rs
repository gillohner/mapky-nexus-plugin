//! Integration test: a `PubkyAppPost` stored at `/pub/mapky.app/posts/{id}` with
//! `parent` set to a `MapkyAppReview` URI gets indexed as a dual-labeled
//! `:Post:MapkyAppPost` node with a `[:REPLY_TO]` edge to the review.
//!
//! Also covers the cross-domain branch: when `parent` points at a non-MapKy
//! resource (e.g. `pubky.app/posts/`), the property is stored but no edge is
//! created — per the design decision that threads only chain within MapKy.

use anyhow::Result;
use futures::TryStreamExt;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use mapky_app_specs::{MapkyAppReview, PubkyAppPost, PubkyAppPostKind};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use pubky_app_specs::PubkyAppUser;
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_pubky_post_reply_to_review() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    // ── Step 1: Create a user ──────────────────────────────────────────────────
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Replier".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // ── Step 2: Author a review ────────────────────────────────────────────────
    let review = MapkyAppReview::new(
        "https://www.openstreetmap.org/node/1573053883".to_string(),
        7,
        Some("Decent coffee".to_string()),
        None,
    );
    let review_id = review.create_id();
    let review_path: pubky::ResourcePath = MapkyAppReview::create_path(&review_id).parse()?;
    test.put(&user_kp, &review_path, &review).await?;

    let review_compound = format!("{user_id}:{review_id}");

    // ── Step 3: Author a PubkyAppPost replying to the review, stored under
    //          /pub/mapky.app/posts/{id} ────────────────────────────────────────
    let review_uri = format!("pubky://{user_id}/pub/mapky.app/reviews/{review_id}");
    let reply = PubkyAppPost::new(
        "Agreed, the espresso is excellent".to_string(),
        PubkyAppPostKind::Short,
        Some(review_uri.clone()),
        None,
        None,
    );
    let reply_id = reply.create_id();
    let reply_path: pubky::ResourcePath =
        format!("/pub/mapky.app/posts/{reply_id}").parse()?;
    test.put(&user_kp, &reply_path, &reply).await?;

    let reply_compound = format!("{user_id}:{reply_id}");

    // ── Step 4: Verify the dual-labeled node exists ────────────────────────────
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_dual_label",
                "MATCH (p:Post:MapkyAppPost {id: $id})
                 RETURN p.id AS id, p.namespace AS namespace, p.parent_uri AS parent_uri",
            )
            .param("id", reply_compound.as_str()),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(row.is_some(), "Dual-labeled :Post:MapkyAppPost should exist");
    let row = row.unwrap();
    let namespace: String = row.get("namespace")?;
    let parent_uri: String = row.get("parent_uri")?;
    assert_eq!(namespace, "mapky.app");
    assert_eq!(parent_uri, review_uri);

    // ── Step 5: Verify the [:REPLY_TO] edge to the review ──────────────────────
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_reply_edge",
                "MATCH (reply:MapkyAppPost {id: $reply_id})-[:REPLY_TO]->(parent:MapkyAppReview {id: $parent_id})
                 RETURN parent.id AS parent_id",
            )
            .param("reply_id", reply_compound.as_str())
            .param("parent_id", review_compound.as_str()),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(
        row.is_some(),
        "Should have :REPLY_TO edge from MapkyAppPost to MapkyAppReview"
    );

    // ── Step 6: Cross-domain parent — author another post pointing at a
    //          /pub/pubky.app/posts/ URI. Edge should NOT be created, but the
    //          parent_uri property must persist. ──────────────────────────────
    let cross_domain_parent =
        format!("pubky://{user_id}/pub/pubky.app/posts/0034A0X7NJ52G");
    let cross_post = PubkyAppPost::new(
        "x-domain reply".to_string(),
        PubkyAppPostKind::Short,
        Some(cross_domain_parent.clone()),
        None,
        None,
    );
    let cross_id = cross_post.create_id();
    let cross_path: pubky::ResourcePath =
        format!("/pub/mapky.app/posts/{cross_id}").parse()?;
    test.put(&user_kp, &cross_path, &cross_post).await?;
    let cross_compound = format!("{user_id}:{cross_id}");

    // The node exists with parent_uri set
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_xdomain_node",
                "MATCH (p:MapkyAppPost {id: $id}) RETURN p.parent_uri AS parent_uri",
            )
            .param("id", cross_compound.as_str()),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(row.is_some(), "Cross-domain post should still index");
    let stored: String = row.unwrap().get("parent_uri")?;
    assert_eq!(stored, cross_domain_parent);

    // But no [:REPLY_TO] edge originates from it
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_no_xdomain_edge",
                "MATCH (p:MapkyAppPost {id: $id})-[:REPLY_TO]->(target)
                 RETURN target",
            )
            .param("id", cross_compound.as_str()),
        )
        .await?;
    let row = stream.try_next().await?;
    assert!(
        row.is_none(),
        "Cross-domain parent must NOT produce a :REPLY_TO edge"
    );

    // ── Cleanup ────────────────────────────────────────────────────────────────
    test.del(&user_kp, &cross_path).await?;
    test.del(&user_kp, &reply_path).await?;
    test.del(&user_kp, &review_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
