//! File event handler — indexes `PubkyAppFile` blobs stored under
//! `/pub/mapky.app/files/` using the same pipeline as pubky-nexus core,
//! so that frontends can use nexus to render image src URLs.
//!
//! Blobs (`/pub/mapky.app/blobs/`) are NOT indexed separately — they are
//! fetched on-demand when the corresponding file's `src` field is resolved.

use chrono::Utc;
use nexus_common::db::PubkyConnector;
use nexus_common::get_files_dir_pathbuf;
use nexus_common::media::{FileVariant, VariantController};
use nexus_common::models::file::FileDetails;
use nexus_common::models::traits::Collection;
use nexus_common::types::DynError;
use serde::Deserialize;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::debug;

/// Lightweight deserialization target — avoids depending on `pubky-app-specs`
/// types which may conflict with the version used by `nexus-common`.
#[derive(Deserialize)]
struct PubkyFile {
    name: String,
    created_at: i64,
    src: String,
    content_type: String,
    size: usize,
}

pub async fn sync_put(data: &[u8], uri: &str, user_id: &str) -> Result<(), DynError> {
    let file: PubkyFile =
        serde_json::from_slice(data).map_err(|e| format!("Failed to parse PubkyAppFile: {e}"))?;

    let file_id = uri
        .rsplit('/')
        .next()
        .ok_or_else(|| format!("Cannot extract file_id from URI: {uri}"))?;

    debug!("Indexing mapky file {user_id}/{file_id}");

    let files_path = get_files_dir_pathbuf();

    // Fetch the blob from the homeserver via the file's src URI.
    let pubky = PubkyConnector::get()?;
    let response = pubky
        .public_storage()
        .get(&file.src)
        .await
        .map_err(|e| format!("Failed to fetch blob from homeserver: {e}"))?;

    let blob_bytes = response
        .bytes()
        .await
        .map_err(|e| format!("Failed to read blob bytes: {e}"))?;

    // Store blob to disk at {files_path}/{user_id}/{file_id}/main
    let relative_path = Path::new(user_id).join(file_id);
    let full_path = files_path.join(&relative_path);

    if !fs::metadata(&full_path)
        .await
        .is_ok_and(|m| m.is_dir())
    {
        fs::create_dir_all(&full_path).await?;
    }
    let main_path = full_path.join(FileVariant::Main.to_string());
    let mut static_file = fs::File::create(main_path).await?;
    static_file.write_all(&blob_bytes).await?;

    // Generate variant URLs based on content type.
    let urls =
        VariantController::get_file_urls_by_content_type(&file.content_type, &relative_path);

    // Build FileDetails and persist to Neo4j + Redis.
    let file_details = FileDetails {
        id: file_id.to_string(),
        uri: uri.to_string(),
        owner_id: user_id.to_string(),
        indexed_at: Utc::now().timestamp_millis(),
        created_at: file.created_at,
        src: file.src,
        name: file.name,
        size: file.size as i64,
        content_type: file.content_type,
        urls,
        metadata: None,
    };

    file_details.put_to_graph().await?;

    let owner_id = file_details.owner_id.clone();
    let id = file_details.id.clone();
    FileDetails::put_to_index(&[&[owner_id.as_str(), id.as_str()]], vec![Some(file_details)])
        .await?;

    Ok(())
}

pub async fn del(uri: &str, user_id: &str) -> Result<(), DynError> {
    let file_id = uri
        .rsplit('/')
        .next()
        .ok_or_else(|| format!("Cannot extract file_id from URI: {uri}"))?;

    debug!("Deleting mapky file {user_id}/{file_id}");

    let result = FileDetails::get_by_ids(&[&[user_id, file_id]]).await?;

    if let Some(Some(file_details)) = result.first() {
        file_details.delete().await?;
    }

    let files_path = get_files_dir_pathbuf();
    let folder_path = Path::new(user_id).join(file_id);
    let full_path = files_path.join(folder_path);

    match tokio::fs::remove_dir_all(full_path.as_path()).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
