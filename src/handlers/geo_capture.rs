//! GeoCapture event handler — indexes `MapkyAppGeoCapture` into Neo4j as a
//! spatial point node with `(User)-[:CAPTURED]->(MapkyAppGeoCapture)`.

use mapky_app_specs::MapkyAppGeoCapture;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::geo_capture::GeoCaptureDetails;
use crate::queries;

pub async fn sync_put(
    capture: &MapkyAppGeoCapture,
    user_id: &str,
    capture_id: &str,
) -> Result<(), DynError> {
    debug!("Indexing geo_capture {user_id}:{capture_id}");

    let details = GeoCaptureDetails::from_mapky_geo_capture(capture, user_id, capture_id);
    let graph = get_neo4j_graph()?;

    graph
        .run(queries::put::create_user(user_id, details.indexed_at))
        .await?;

    graph
        .run(queries::put::create_geo_capture(&details))
        .await?;

    Ok(())
}

pub async fn del(user_id: &str, capture_id: &str) -> Result<(), DynError> {
    debug!("Deleting geo_capture {user_id}:{capture_id}");
    let compound_id = format!("{user_id}:{capture_id}");
    let graph = get_neo4j_graph()?;
    graph
        .run(queries::del::delete_geo_capture(user_id, &compound_id))
        .await?;
    Ok(())
}
