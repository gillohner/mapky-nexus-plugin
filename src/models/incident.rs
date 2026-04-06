//! Incident model — a crowdsourced traffic/hazard report stored as a Neo4j
//! `:MapkyAppIncident` node with a spatial point index.

use mapky_app_specs::MapkyAppIncident;
use serde::{Deserialize, Serialize};

use chrono::Utc;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IncidentDetails {
    pub id: String, // compound: "user_id:incident_id"
    pub author_id: String,
    pub incident_type: String,
    pub severity: String,
    pub lat: f64,
    pub lon: f64,
    pub heading: Option<f64>,
    pub description: Option<String>,
    pub attachments: Vec<String>,
    pub expires_at: Option<i64>,
    pub indexed_at: i64,
}

impl IncidentDetails {
    pub fn from_mapky_incident(
        incident: &MapkyAppIncident,
        user_id: &str,
        incident_id: &str,
    ) -> Self {
        Self {
            id: format!("{user_id}:{incident_id}"),
            author_id: user_id.to_string(),
            incident_type: serde_json::to_value(&incident.incident_type)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default(),
            severity: serde_json::to_value(&incident.severity)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default(),
            lat: incident.lat,
            lon: incident.lon,
            heading: incident.heading,
            description: incident.description.clone(),
            attachments: incident
                .attachments
                .as_ref()
                .cloned()
                .unwrap_or_default(),
            expires_at: incident.expires_at,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }
}
