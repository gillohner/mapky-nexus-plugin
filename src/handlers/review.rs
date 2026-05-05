//! REVIEW event handler — indexes a `MapkyAppReview` into Neo4j.
//!
//! Reviews are rating-mandatory and place-anchored; they are never replies.
//! Generic comments and threaded replies use `PubkyAppPost` blobs at
//! `/pub/mapky.app/posts/{id}` and are handled by `handlers::mapky_post`.

use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::MapkyAppReview;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::place::PlaceDetails;
use crate::models::review::ReviewDetails;
use crate::queries;

pub async fn sync_put(
    review: &MapkyAppReview,
    user_id: &str,
    review_id: &str,
) -> Result<(), DynError> {
    debug!("Indexing mapky review: {user_id}/{review_id}");

    let graph = get_neo4j_graph()?;
    let indexed_at = Utc::now().timestamp_millis();

    graph
        .run(queries::put::create_user(user_id, indexed_at))
        .await?;

    let osm_canonical = crate::models::place::osm_canonical_from_url(&review.place);

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
        let place = PlaceDetails::from_osm_url(&review.place).await;
        graph.run(queries::put::create_place(&place)).await?;
    }

    let review_details = ReviewDetails::from_mapky_review(review, user_id, review_id);
    graph.run(queries::put::create_review(&review_details)).await?;

    graph
        .run(queries::put::increment_place_rating(
            &review_details.osm_canonical,
            review_details.rating,
        ))
        .await?;

    Ok(())
}

pub async fn del(user_id: &str, review_id: &str) -> Result<(), DynError> {
    debug!("Deleting mapky review: {user_id}/{review_id}");

    let compound_id = format!("{user_id}:{review_id}");
    let graph = get_neo4j_graph()?;

    let mut stream = graph
        .execute(queries::del::delete_review(user_id, &compound_id))
        .await?;

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
