//! Neo4j delete queries for the mapky plugin.

use nexus_common::db::graph::Query;

/// Delete a MapkyAppPost and its relationships.
/// `compound_id` is the compound key `author_id:post_id`.
/// Returns `osm_canonical` and `rating` so the caller can roll back aggregates.
pub fn delete_post(author_id: &str, compound_id: &str) -> Query {
    Query::new(
        "mapky_delete_post",
        "MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:MapkyAppPost {id: $compound_id})
         MATCH (p)-[:ABOUT]->(place:Place)
         WITH p, place.osm_canonical AS osm_canonical, p.rating AS rating
         DETACH DELETE p
         RETURN osm_canonical, rating",
    )
    .param("author_id", author_id)
    .param("compound_id", compound_id)
}
