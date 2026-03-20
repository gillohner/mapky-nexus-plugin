//! Neo4j write queries for the mapky plugin.
//!
//! Uses `:MapkyAppPost` label to avoid collision with nexus's existing `:Post` label.
//! Post identity follows the same compound-key convention as nexus: `author_id:post_id`.

use crate::models::place::PlaceDetails;
use crate::models::post::PostDetails;
use nexus_common::db::graph::Query;

/// Create or update a User node (minimal — no profile data from post events).
pub fn create_user(user_id: &str, indexed_at: i64) -> Query {
    Query::new(
        "mapky_create_user",
        "MERGE (u:User {id: $id})
         ON CREATE SET u.indexed_at = $indexed_at",
    )
    .param("id", user_id)
    .param("indexed_at", indexed_at)
}

/// MERGE a Place node with spatial index point.
pub fn create_place(place: &PlaceDetails) -> Query {
    Query::new(
        "mapky_create_place",
        "MERGE (p:Place {osm_canonical: $osm_canonical})
         ON CREATE SET
             p.osm_type = $osm_type,
             p.osm_id = $osm_id,
             p.location = point({latitude: $lat, longitude: $lon}),
             p.lat = $lat,
             p.lon = $lon,
             p.geocoded = $geocoded,
             p.review_count = 0,
             p.avg_rating = 0.0,
             p.tag_count = 0,
             p.photo_count = 0,
             p.indexed_at = $indexed_at",
    )
    .param("osm_canonical", place.osm_canonical.clone())
    .param("osm_type", place.osm_type.clone())
    .param("osm_id", place.osm_id)
    .param("lat", place.lat)
    .param("lon", place.lon)
    .param("geocoded", place.geocoded)
    .param("indexed_at", place.indexed_at)
}

/// Create a MapkyAppPost node with AUTHORED and ABOUT relationships.
/// `post.id` is the compound key `author_id:post_id`.
pub fn create_post(post: &PostDetails) -> Query {
    Query::new(
        "mapky_create_post",
        "MATCH (author:User {id: $author_id})
         MATCH (place:Place {osm_canonical: $osm_canonical})
         MERGE (author)-[:AUTHORED]->(p:MapkyAppPost {id: $post_id})
         MERGE (p)-[:ABOUT]->(place)
         ON CREATE SET p.indexed_at = $indexed_at
         SET p.content = $content,
             p.rating = $rating,
             p.kind = $kind,
             p.parent_uri = $parent_uri,
             p.attachments = $attachments",
    )
    .param("author_id", post.author_id.clone())
    .param("post_id", post.id.clone())
    .param("osm_canonical", post.osm_canonical.clone())
    .param("content", post.content.clone().unwrap_or_default())
    .param("rating", post.rating.map(|r| r as i64))
    .param("kind", post.kind.clone())
    .param("parent_uri", post.parent_uri.clone())
    .param("attachments", post.attachments.clone())
    .param("indexed_at", post.indexed_at)
}

/// Create a REPLY_TO edge from a child post to its parent.
/// Uses MATCH for the parent — if the parent doesn't exist yet (out-of-order
/// delivery), the query silently does nothing.
/// `child_id` and `parent_id` are both compound keys (`author_id:post_id`).
pub fn link_reply(child_id: &str, parent_id: &str) -> Query {
    Query::new(
        "mapky_link_reply",
        "MATCH (child:MapkyAppPost {id: $child_id})
         MATCH (parent:MapkyAppPost {id: $parent_id})
         MERGE (child)-[:REPLY_TO]->(parent)",
    )
    .param("child_id", child_id)
    .param("parent_id", parent_id)
}

/// Increment the running average rating on a Place after a new review.
pub fn increment_place_rating(osm_canonical: &str, rating: u8) -> Query {
    Query::new(
        "mapky_increment_rating",
        "MATCH (p:Place {osm_canonical: $osm_canonical})
         SET p.avg_rating = (p.avg_rating * p.review_count + $rating) / (p.review_count + 1),
             p.review_count = p.review_count + 1",
    )
    .param("osm_canonical", osm_canonical)
    .param("rating", rating as i64)
}

/// Roll back the running average rating after a review is deleted.
pub fn decrement_place_rating(osm_canonical: &str, rating: u8) -> Query {
    Query::new(
        "mapky_decrement_rating",
        "MATCH (p:Place {osm_canonical: $osm_canonical})
         WITH p,
              CASE WHEN p.review_count > 1
                   THEN (p.avg_rating * p.review_count - $rating) / (p.review_count - 1)
                   ELSE 0.0 END AS new_avg
         SET p.review_count = CASE WHEN p.review_count > 0 THEN p.review_count - 1 ELSE 0 END,
             p.avg_rating = new_avg",
    )
    .param("osm_canonical", osm_canonical)
    .param("rating", rating as i64)
}
