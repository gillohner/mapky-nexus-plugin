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

/// Delete a TAGGED relationship by its `id` property.
/// Returns the label of the target node (e.g. "Place", "MapkyAppPost") and
/// the Place `osm_canonical` if the target was a Place (for aggregate rollback).
pub fn delete_tag(tagger_user_id: &str, tag_id: &str) -> Query {
    Query::new(
        "mapky_delete_tag",
        "MATCH (u:User {id: $user_id})-[t:TAGGED {id: $tag_id}]->(target)
         WITH u, t, target, labels(target) AS lbls,
              CASE WHEN target:Place THEN target.osm_canonical ELSE null END AS osm_canonical
         DELETE t
         WITH target, osm_canonical, lbls
         // Decrement tag_count on Place targets
         FOREACH (_ IN CASE WHEN target:Place THEN [1] ELSE [] END |
             SET target.tag_count = CASE WHEN target.tag_count > 0
                 THEN target.tag_count - 1 ELSE 0 END
         )
         RETURN osm_canonical, lbls",
    )
    .param("user_id", tagger_user_id)
    .param("tag_id", tag_id)
}

/// Delete a MapkyAppIncident and its relationships.
pub fn delete_incident(author_id: &str, compound_id: &str) -> Query {
    Query::new(
        "mapky_delete_incident",
        "MATCH (u:User {id: $author_id})-[:REPORTED]->(i:MapkyAppIncident {id: $compound_id})
         DETACH DELETE i",
    )
    .param("author_id", author_id)
    .param("compound_id", compound_id)
}

/// Delete a MapkyAppGeoCapture and its relationships.
pub fn delete_geo_capture(author_id: &str, compound_id: &str) -> Query {
    Query::new(
        "mapky_delete_geo_capture",
        "MATCH (u:User {id: $author_id})-[:CAPTURED]->(g:MapkyAppGeoCapture {id: $compound_id})
         DETACH DELETE g",
    )
    .param("author_id", author_id)
    .param("compound_id", compound_id)
}

/// Delete a MapkyAppCollection and its relationships (CREATED + CONTAINS edges).
pub fn delete_collection(author_id: &str, compound_id: &str) -> Query {
    Query::new(
        "mapky_delete_collection",
        "MATCH (u:User {id: $author_id})-[:CREATED]->(c:MapkyAppCollection {id: $compound_id})
         DETACH DELETE c",
    )
    .param("author_id", author_id)
    .param("compound_id", compound_id)
}

/// Delete a MapkyAppRoute and its relationships.
pub fn delete_route(author_id: &str, compound_id: &str) -> Query {
    Query::new(
        "mapky_delete_route",
        "MATCH (u:User {id: $author_id})-[:CREATED]->(r:MapkyAppRoute {id: $compound_id})
         DETACH DELETE r",
    )
    .param("author_id", author_id)
    .param("compound_id", compound_id)
}

/// Delete a MapkyAppSequence and its relationships.
///
/// GeoCaptures that referenced the sequence via `sequence_uri` continue to exist
/// — their property simply becomes a dangling reference. The alternative (cascading
/// GeoCapture deletes) would be destructive; callers should delete the captures
/// separately if desired.
pub fn delete_sequence(author_id: &str, compound_id: &str) -> Query {
    Query::new(
        "mapky_delete_sequence",
        "MATCH (u:User {id: $author_id})-[:CAPTURED]->(s:MapkyAppSequence {id: $compound_id})
         DETACH DELETE s",
    )
    .param("author_id", author_id)
    .param("compound_id", compound_id)
}
