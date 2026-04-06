//! POST event handler — indexes a MapkyAppPost into Neo4j.

use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::MapkyAppPost;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::place::PlaceDetails;
use crate::models::post::PostDetails;
use crate::queries;

/// Handle a PUT event: ensure User + Place exist, create MapkyPost, update rating aggregate.
pub async fn sync_put(post: &MapkyAppPost, user_id: &str, post_id: &str) -> Result<(), DynError> {
    debug!("Indexing mapky post: {user_id}/{post_id}");

    let graph = get_neo4j_graph()?;
    let indexed_at = Utc::now().timestamp_millis();

    // 1. Ensure User node exists
    graph
        .run(queries::put::create_user(user_id, indexed_at))
        .await?;

    // 2. Ensure Place node exists — skip Nominatim if the Place is already indexed
    let osm_canonical = crate::models::place::osm_canonical_from_url(&post.place);

    let already_exists: bool = {
        let mut stream = graph
            .execute(queries::get::place_exists(&osm_canonical))
            .await?;
        stream
            .try_next()
            .await?
            .and_then(|row| row.get("exists").ok())
            .unwrap_or(false)
    };

    if !already_exists {
        let place = PlaceDetails::from_osm_url(&post.place).await;
        graph.run(queries::put::create_place(&place)).await?;
    }

    // 3. Create the MapkyAppPost node with AUTHORED + ABOUT edges
    // post_details.id is the compound key author_id:post_id
    let post_details = PostDetails::from_mapky_post(post, user_id, post_id);
    graph.run(queries::put::create_post(&post_details)).await?;

    // 4. Link to parent post if this is a reply (out-of-order safe — silently no-ops)
    if let Some(ref parent_uri) = post_details.parent_uri {
        if let Some(parent_compound_id) = extract_compound_id_from_uri(parent_uri) {
            graph
                .run(queries::put::link_reply(&post_details.id, &parent_compound_id))
                .await?;
        }
    }

    // 5. Update running average rating if this is a Review
    if let Some(rating) = post.rating {
        graph
            .run(queries::put::increment_place_rating(
                &post_details.osm_canonical,
                rating,
            ))
            .await?;
    }

    Ok(())
}

/// Extract a compound `author_id:post_id` key from a
/// `pubky://<author_id>/pub/mapky.app/posts/<post_id>` URI.
/// Returns `None` if the URI doesn't match the expected path structure.
fn extract_compound_id_from_uri(uri: &str) -> Option<String> {
    // Expected: pubky://<author_id>/pub/mapky.app/posts/<post_id>
    let path = uri.strip_prefix("pubky://")?;
    let mut parts = path.splitn(2, "/pub/mapky.app/posts/");
    let author_id = parts.next()?;
    let post_id = parts.next()?.trim_end_matches('/');
    if author_id.is_empty() || post_id.is_empty() {
        None
    } else {
        Some(format!("{author_id}:{post_id}"))
    }
}

/// Handle a DEL event: remove MapkyPost and roll back rating aggregate.
pub async fn del(user_id: &str, post_id: &str) -> Result<(), DynError> {
    debug!("Deleting mapky post: {user_id}/{post_id}");

    let compound_id = format!("{user_id}:{post_id}");
    let graph = get_neo4j_graph()?;

    let mut stream = graph
        .execute(queries::del::delete_post(user_id, &compound_id))
        .await?;

    // Roll back rating aggregate if the deleted post was a review
    if let Some(row) = stream.try_next().await? {
        let osm_canonical: Option<String> = row.get("osm_canonical").ok();
        let rating: Option<i64> = row.get("rating").ok();

        if let (Some(canonical), Some(r)) = (osm_canonical, rating) {
            if r > 0 {
                graph
                    .run(queries::put::decrement_place_rating(&canonical, r as u8))
                    .await?;
            }
        }
    }

    Ok(())
}
