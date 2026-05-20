//! Collection model — a curated list encoded inside
//! `PubkyAppPost(kind = Collection).content` and indexed into Neo4j as
//! `:MapkyAppCollection` with `CONTAINS` edges to `:Place` nodes.

use serde::{Deserialize, Serialize};

use chrono::Utc;
use mapky_app_specs::{PubkyAppCollectionContent, PubkyAppPost};

#[derive(Debug, Deserialize)]
struct MapkyCollectionEnvelope {
    name: String,
    description: Option<String>,
    #[serde(default)]
    items: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CollectionDetails {
    pub id: String, // compound: "user_id:collection_id"
    pub author_id: String,
    pub name: String,
    pub description: Option<String>,
    pub items: Vec<String>, // OSM URLs from the spec
    pub indexed_at: i64,
}

impl CollectionDetails {
    pub fn from_collection_post(
        post: &PubkyAppPost,
        user_id: &str,
        collection_id: &str,
    ) -> Result<Self, String> {
        let envelope: MapkyCollectionEnvelope = serde_json::from_str(&post.content)
            .or_else(|_| {
                serde_json::from_str::<PubkyAppCollectionContent>(&post.content).map(|v| {
                    MapkyCollectionEnvelope {
                        name: v.name,
                        description: v.description,
                        items: v.items,
                    }
                })
            })
            .map_err(|e| format!("Invalid collection post content envelope: {e}"))?;

        let name = envelope.name.trim().to_string();

        Ok(Self {
            id: format!("{user_id}:{collection_id}"),
            author_id: user_id.to_string(),
            name,
            description: envelope.description,
            items: envelope.items,
            indexed_at: Utc::now().timestamp_millis(),
        })
    }
}
