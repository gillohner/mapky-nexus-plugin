//! Collection model — a curated list of OSM places stored as a Neo4j
//! `:MapkyAppCollection` node with `CONTAINS` edges to `:Place` nodes.

use serde::{Deserialize, Serialize};

use chrono::Utc;
use mapky_app_specs::MapkyAppCollection;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CollectionDetails {
    pub id: String, // compound: "user_id:collection_id"
    pub author_id: String,
    pub name: String,
    pub description: Option<String>,
    pub items: Vec<String>, // OSM URLs from the spec
    pub image_uri: Option<String>,
    pub indexed_at: i64,
}

impl CollectionDetails {
    pub fn from_mapky_collection(
        collection: &MapkyAppCollection,
        user_id: &str,
        collection_id: &str,
    ) -> Self {
        Self {
            id: format!("{user_id}:{collection_id}"),
            author_id: user_id.to_string(),
            name: collection.name.clone(),
            description: collection.description.clone(),
            items: collection.items.clone(),
            image_uri: collection.image_uri.clone(),
            indexed_at: Utc::now().timestamp_millis(),
        }
    }
}
