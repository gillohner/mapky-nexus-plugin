//! Sequence event handler — indexes `MapkyAppSequence` into Neo4j as a
//! `:MapkyAppSequence` node with `(User)-[:CAPTURED]->(:MapkyAppSequence)`.

use mapky_app_specs::MapkyAppSequence;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use tracing::debug;

use crate::models::sequence::SequenceDetails;
use crate::queries;

pub async fn sync_put(
    sequence: &MapkyAppSequence,
    user_id: &str,
    sequence_id: &str,
) -> Result<(), DynError> {
    debug!("Indexing sequence {user_id}:{sequence_id}");

    let details = SequenceDetails::from_mapky_sequence(sequence, user_id, sequence_id);
    let graph = get_neo4j_graph()?;

    graph
        .run(queries::put::create_user(user_id, details.indexed_at))
        .await?;

    graph
        .run(queries::put::create_sequence(&details))
        .await?;

    Ok(())
}

pub async fn del(user_id: &str, sequence_id: &str) -> Result<(), DynError> {
    debug!("Deleting sequence {user_id}:{sequence_id}");
    let compound_id = format!("{user_id}:{sequence_id}");
    let graph = get_neo4j_graph()?;
    graph
        .run(queries::del::delete_sequence(user_id, &compound_id))
        .await?;
    Ok(())
}
