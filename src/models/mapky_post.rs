//! MapkyPost model — a `PubkyAppPost`-shaped comment stored under the MapKy
//! namespace at `/pub/mapky.app/posts/{id}`.
//!
//! These nodes carry the dual label `:Post:MapkyAppPost` in Neo4j: the `:Post`
//! label aligns with pubky-nexus core's social posts (so reply chains have the
//! same shape), while `:MapkyAppPost` scopes plugin-side queries to only the
//! mapky-namespaced posts.

use chrono::Utc;
use mapky_app_specs::PubkyAppPost;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct MapkyPostDetails {
    pub id: String,
    pub author_id: String,
    pub content: String,
    pub kind: String,
    pub parent_uri: Option<String>,
    pub embed_uri: Option<String>,
    pub embed_kind: Option<String>,
    pub attachments: Vec<String>,
    pub indexed_at: i64,
}

impl MapkyPostDetails {
    pub fn from_pubky_post(post: &PubkyAppPost, user_id: &str, post_id: &str) -> Self {
        let (embed_uri, embed_kind) = match &post.embed {
            Some(embed) => (Some(embed.uri.clone()), Some(embed.kind.to_string())),
            None => (None, None),
        };

        Self {
            id: format!("{user_id}:{post_id}"),
            author_id: user_id.to_string(),
            content: post.content.clone(),
            kind: post.kind.to_string(),
            parent_uri: post.parent.clone(),
            embed_uri,
            embed_kind,
            attachments: post.attachments.clone().unwrap_or_default(),
            indexed_at: Utc::now().timestamp_millis(),
        }
    }
}
