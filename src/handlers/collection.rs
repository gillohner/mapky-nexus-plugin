//! Collection event handler — indexes `MapkyAppCollection` into Neo4j with
//! `(User)-[:CREATED]->(MapkyAppCollection)-[:CONTAINS]->(Place)` edges.
//!
//! On update, stale CONTAINS edges (places removed from the collection)
//! are cleaned up.

use futures::TryStreamExt;
use mapky_app_specs::MapkyAppCollection;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::collection::CollectionDetails;
use crate::models::place::{osm_canonical_from_url, PlaceDetails};
use crate::queries;

pub async fn sync_put(
    collection: &MapkyAppCollection,
    user_id: &str,
    collection_id: &str,
) -> Result<(), DynError> {
    debug!("Indexing collection {user_id}:{collection_id}");

    let details = CollectionDetails::from_mapky_collection(collection, user_id, collection_id);
    let graph = get_neo4j_graph()?;

    // 1. Ensure user exists.
    graph
        .run(queries::put::create_user(user_id, details.indexed_at))
        .await?;

    // 2. Create the collection node + CREATED edge.
    graph.run(queries::put::create_collection(&details)).await?;

    // 3. Ensure each Place exists and create CONTAINS edges.
    let mut current_canonicals: Vec<String> = Vec::with_capacity(collection.items.len());

    for osm_url in &collection.items {
        let osm_canonical = osm_canonical_from_url(osm_url);

        // Check if Place exists, geocode if new.
        let exists: bool = graph
            .execute(queries::get::place_exists(&osm_canonical))
            .await?
            .try_next()
            .await?
            .and_then(|row| row.get("exists").ok())
            .unwrap_or(false);

        if !exists {
            let place = PlaceDetails::from_osm_url(osm_url).await;
            graph.run(queries::put::create_place(&place)).await?;
        }

        graph
            .run(queries::put::link_collection_place(
                &details.id,
                &osm_canonical,
            ))
            .await?;

        current_canonicals.push(osm_canonical);
    }

    // 4. Remove stale CONTAINS edges (places removed from collection on update).
    graph
        .run(queries::put::cleanup_collection_places(
            &details.id,
            &current_canonicals,
        ))
        .await?;

    Ok(())
}

pub async fn del(user_id: &str, collection_id: &str) -> Result<(), DynError> {
    debug!("Deleting collection {user_id}:{collection_id}");
    let compound_id = format!("{user_id}:{collection_id}");
    let graph = get_neo4j_graph()?;
    graph
        .run(queries::del::delete_collection(user_id, &compound_id))
        .await?;
    Ok(())
}
