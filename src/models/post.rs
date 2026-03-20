//! Post model — a review or comment about an OSM place.

use chrono::Utc;
use mapky_app_specs::{MapkyAppPost, MapkyAppPostKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PostDetails {
    pub id: String,
    pub author_id: String,
    pub osm_canonical: String,
    pub content: Option<String>,
    pub rating: Option<u8>,
    pub kind: String,
    pub parent_uri: Option<String>,
    pub attachments: Vec<String>,
    pub indexed_at: i64,
}

impl PostDetails {
    pub fn from_mapky_post(post: &MapkyAppPost, user_id: &str, post_id: &str) -> Self {
        Self {
            id: format!("{user_id}:{post_id}"),
            author_id: user_id.to_string(),
            osm_canonical: post.place.canonical(),
            content: post.content.clone(),
            rating: post.rating,
            kind: match post.kind {
                MapkyAppPostKind::Review => "review",
                MapkyAppPostKind::Post => "post",
            }
            .to_string(),
            parent_uri: post.parent.clone(),
            attachments: post.attachments.clone().unwrap_or_default(),
            indexed_at: Utc::now().timestamp_millis(),
        }
    }
}
