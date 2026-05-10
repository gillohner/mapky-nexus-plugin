//! Incident event handler — indexes `MapkyAppIncident` into Neo4j as a
//! spatial point node with `(User)-[:REPORTED]->(MapkyAppIncident)`.

use mapky_app_specs::MapkyAppIncident;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::incident::IncidentDetails;
use crate::queries;

pub async fn sync_put(
    incident: &MapkyAppIncident,
    user_id: &str,
    incident_id: &str,
) -> Result<(), DynError> {
    debug!("Indexing incident {user_id}:{incident_id}");

    let details = IncidentDetails::from_mapky_incident(incident, user_id, incident_id);
    let graph = get_neo4j_graph()?;

    // 1. Ensure user exists.
    graph
        .run(queries::put::create_user(user_id, details.indexed_at))
        .await?;

    // 2. Create the incident node + REPORTED edge.
    graph.run(queries::put::create_incident(&details)).await?;

    Ok(())
}

pub async fn del(user_id: &str, incident_id: &str) -> Result<(), DynError> {
    debug!("Deleting incident {user_id}:{incident_id}");
    let compound_id = format!("{user_id}:{incident_id}");
    let graph = get_neo4j_graph()?;
    graph
        .run(queries::del::delete_incident(user_id, &compound_id))
        .await?;
    Ok(())
}
