//! Route event handler — indexes `MapkyAppRoute` into Neo4j with bounding box
//! metadata and `(User)-[:CREATED]->(MapkyAppRoute)`.
//!
//! Full waypoint data stays on the homeserver — only searchable metadata
//! (name, activity, distance, bbox) is stored in Neo4j.

use mapky_app_specs::MapkyAppRoute;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::route::RouteDetails;
use crate::queries;

pub async fn sync_put(
    route: &MapkyAppRoute,
    user_id: &str,
    route_id: &str,
) -> Result<(), DynError> {
    debug!("Indexing route {user_id}:{route_id}");

    let details = RouteDetails::from_mapky_route(route, user_id, route_id);
    let graph = get_neo4j_graph()?;

    graph
        .run(queries::put::create_user(user_id, details.indexed_at))
        .await?;

    graph.run(queries::put::create_route(&details)).await?;

    Ok(())
}

pub async fn del(user_id: &str, route_id: &str) -> Result<(), DynError> {
    debug!("Deleting route {user_id}:{route_id}");
    let compound_id = format!("{user_id}:{route_id}");
    let graph = get_neo4j_graph()?;
    graph
        .run(queries::del::delete_route(user_id, &compound_id))
        .await?;
    Ok(())
}
