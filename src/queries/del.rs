//! Neo4j delete queries for the mapky plugin.

use nexus_common::db::graph::Query;

/// Delete a MapkyPost and its relationships.
/// Returns `osm_canonical` and `rating` so the caller can roll back aggregates.
pub fn delete_post(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "mapky_delete_post",
        "MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:MapkyPost {id: $post_id})
         MATCH (p)-[:ABOUT]->(place:Place)
         WITH p, place.osm_canonical AS osm_canonical, p.rating AS rating
         DETACH DELETE p
         RETURN osm_canonical, rating",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}
