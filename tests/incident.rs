//! Integration test: MapkyAppIncident lifecycle — create, verify in Neo4j, delete.

use anyhow::Result;
use futures::TryStreamExt;
use mapky_app_specs::{IncidentSeverity, IncidentType, MapkyAppIncident};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use pubky_app_specs::PubkyAppUser;
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_incident_lifecycle() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    // Create user.
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "IncidentReporter".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Write a MapkyAppIncident.
    let incident = MapkyAppIncident {
        incident_type: IncidentType::Flooding,
        severity: IncidentSeverity::High,
        lat: 47.3769,
        lon: 8.5417,
        heading: Some(180.0),
        description: Some("Street flooded near Zurich HB".to_string()),
        attachments: None,
        expires_at: None,
    };
    let incident_id = incident.create_id();
    let incident_path: pubky::ResourcePath =
        MapkyAppIncident::create_path(&incident_id).parse()?;
    test.put(&user_kp, &incident_path, &incident).await?;

    // Verify indexed in Neo4j.
    let compound_id = format!("{user_id}:{incident_id}");
    let graph = get_neo4j_graph()?;
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_incident",
                "MATCH (u:User {id: $user_id})-[:REPORTED]->(i:MapkyAppIncident {id: $id})
                 RETURN i.lat AS lat, i.lon AS lon, i.incident_type AS incident_type",
            )
            .param("user_id", user_id.as_str())
            .param("id", compound_id.as_str()),
        )
        .await?;

    let row = stream.try_next().await?;
    assert!(row.is_some(), "Incident should exist in Neo4j");
    let row = row.unwrap();
    let lat: f64 = row.get("lat")?;
    let incident_type: String = row.get("incident_type")?;
    assert!((lat - 47.3769).abs() < 0.001);
    assert_eq!(incident_type, "Flooding");

    // Delete and verify gone.
    test.del(&user_kp, &incident_path).await?;

    let mut stream = graph
        .execute(
            Query::new(
                "test_check_incident_deleted",
                "MATCH (i:MapkyAppIncident {id: $id}) RETURN count(i) AS cnt",
            )
            .param("id", compound_id.as_str()),
        )
        .await?;
    let cnt: i64 = stream.try_next().await?.unwrap().get("cnt")?;
    assert_eq!(cnt, 0, "Incident should be deleted");

    test.cleanup_user(&user_kp).await?;
    Ok(())
}
