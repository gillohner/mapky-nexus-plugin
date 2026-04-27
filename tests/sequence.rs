//! Integration test: MapkyAppSequence lifecycle and cross-domain tagging.
//!
//! Covers:
//! - PUT a `MapkyAppSequence` → verify `:MapkyAppSequence` node + `:CAPTURED` edge
//! - PUT 3 `MapkyAppGeoCapture`s referencing the sequence → verify ordering query
//! - PUT a `PubkyAppTag` targeting the sequence → verify `:TAGGED` edge
//! - PUT a `PubkyAppTag` targeting a capture → verify retrieval query closes the gap
//! - DEL the sequence → verify node removed

use anyhow::Result;
use chrono::Utc;
use futures::TryStreamExt;
use mapky_app_specs::traits::{HasIdPath, TimestampId};
use mapky_app_specs::{GeoCaptureKind, MapkyAppGeoCapture, MapkyAppSequence};
use mapky_nexus_plugin::MapkyPlugin;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::testing::WatcherTest;
use pubky::Keypair;
use pubky_app_specs::traits::{HasIdPath as PubkyHasIdPath, HashId};
use pubky_app_specs::{PubkyAppTag, PubkyAppUser};
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_sequence_lifecycle_and_tagging() -> Result<()> {
    let mut test =
        WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin::new())]).await?;

    // ── Create user ─────────────────────────────────────────────────────────
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "SequenceTester".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // ── PUT a sequence ──────────────────────────────────────────────────────
    let t0: i64 = 1_750_000_000_000_000; // mid-2025, microseconds
    let mut sequence = MapkyAppSequence::new(GeoCaptureKind::Photo, t0, t0 + 3_000_000, 3);
    sequence.name = Some("Walk down Lambeth Rd".to_string());
    sequence.device = Some("iPhone 15 Pro".to_string());
    let sequence_id = sequence.create_id();
    let sequence_path: pubky::ResourcePath =
        MapkyAppSequence::create_path(&sequence_id).parse()?;
    test.put(&user_kp, &sequence_path, &sequence).await?;

    let compound_seq_id = format!("{user_id}:{sequence_id}");
    let graph = get_neo4j_graph()?;

    // Verify :MapkyAppSequence node exists with expected properties.
    let mut stream = graph
        .execute(
            Query::new(
                "test_check_sequence_node",
                "MATCH (u:User {id: $user_id})-[:CAPTURED]->(s:MapkyAppSequence {id: $id})
                 RETURN s.name AS name, s.device AS device,
                        s.captured_at_start AS t_start, s.captured_at_end AS t_end,
                        s.capture_count AS cnt, s.kind AS kind",
            )
            .param("user_id", user_id.as_str())
            .param("id", compound_seq_id.as_str()),
        )
        .await?;
    let row = stream.try_next().await?.expect("Sequence node should exist");
    let name: String = row.get("name")?;
    let device: String = row.get("device")?;
    let t_start: i64 = row.get("t_start")?;
    let t_end: i64 = row.get("t_end")?;
    let cnt: i64 = row.get("cnt")?;
    let kind: String = row.get("kind")?;
    assert_eq!(name, "Walk down Lambeth Rd");
    assert_eq!(device, "iPhone 15 Pro");
    assert_eq!(t_start, t0);
    assert_eq!(t_end, t0 + 3_000_000);
    assert_eq!(cnt, 3);
    assert_eq!(kind, "photo");

    // ── PUT 3 GeoCaptures referencing the sequence ─────────────────────────
    let sequence_uri = format!("pubky://{user_id}/pub/mapky.app/sequences/{sequence_id}");
    let mut capture_paths = Vec::new();
    for i in 0..3u32 {
        let capture = MapkyAppGeoCapture {
            file_uri: format!("pubky://{user_id}/pub/mapky.app/files/photo{i:03}"),
            kind: GeoCaptureKind::Photo,
            lat: 51.4935 + (i as f64) * 0.0001,
            lon: -0.1155,
            ele: None,
            heading: Some(90.0),
            pitch: None,
            fov: None,
            caption: Some(format!("frame {i}")),
            sequence_uri: Some(sequence_uri.clone()),
            sequence_index: Some(i),
            captured_at: Some(t0 + (i as i64) * 1_000_000),
        };
        let capture_id = capture.create_id();
        let capture_path: pubky::ResourcePath =
            MapkyAppGeoCapture::create_path(&capture_id).parse()?;
        test.put(&user_kp, &capture_path, &capture).await?;
        capture_paths.push(capture_path);
    }

    // ── Verify ordered fetch by sequence_uri ───────────────────────────────
    let mut stream = graph
        .execute(
            Query::new(
                "test_captures_in_sequence",
                "MATCH (u:User {id: $user_id})-[:CAPTURED]->(g:MapkyAppGeoCapture)
                 WHERE g.sequence_uri = $sequence_uri
                 RETURN g.sequence_index AS idx, g.caption AS caption
                 ORDER BY g.sequence_index ASC",
            )
            .param("user_id", user_id.as_str())
            .param("sequence_uri", sequence_uri.as_str()),
        )
        .await?;
    let mut indices = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let idx: i64 = row.get("idx")?;
        indices.push(idx);
    }
    assert_eq!(indices, vec![0, 1, 2]);

    // ── PUT a PubkyAppTag targeting the sequence ───────────────────────────
    let seq_tag = PubkyAppTag {
        uri: sequence_uri.clone(),
        label: "streetview".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let seq_tag_id = seq_tag.create_id();
    let seq_tag_path: pubky::ResourcePath = PubkyAppTag::create_path(&seq_tag_id).parse()?;
    test.put(&user_kp, &seq_tag_path, &seq_tag).await?;

    // Verify TAGGED edge on :MapkyAppSequence.
    let mut stream = graph
        .execute(
            Query::new(
                "test_sequence_tag_edge",
                "MATCH (u:User {id: $user_id})-[t:TAGGED {label: $label}]->(s:MapkyAppSequence {id: $id})
                 RETURN t.label AS label",
            )
            .param("user_id", user_id.as_str())
            .param("label", "streetview")
            .param("id", compound_seq_id.as_str()),
        )
        .await?;
    let tag_row = stream.try_next().await?;
    assert!(
        tag_row.is_some(),
        "TAGGED edge should exist on :MapkyAppSequence"
    );

    // ── PUT a PubkyAppTag targeting a GeoCapture (closes the retrieval gap) ─
    // Re-derive the first capture's compound id via the sequence_uri scan.
    let mut stream = graph
        .execute(
            Query::new(
                "test_first_capture_id",
                "MATCH (g:MapkyAppGeoCapture) WHERE g.sequence_uri = $sequence_uri AND g.sequence_index = 0
                 RETURN g.id AS id",
            )
            .param("sequence_uri", sequence_uri.as_str()),
        )
        .await?;
    let first_capture_compound_id: String = stream
        .try_next()
        .await?
        .expect("first capture should exist")
        .get("id")?;
    let first_capture_short = first_capture_compound_id
        .split_once(':')
        .map(|(_, s)| s.to_string())
        .unwrap();
    let capture_uri = format!(
        "pubky://{user_id}/pub/mapky.app/geo_captures/{first_capture_short}"
    );

    let cap_tag = PubkyAppTag {
        uri: capture_uri,
        label: "needs-review".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let cap_tag_id = cap_tag.create_id();
    let cap_tag_path: pubky::ResourcePath = PubkyAppTag::create_path(&cap_tag_id).parse()?;
    test.put(&user_kp, &cap_tag_path, &cap_tag).await?;

    // Exercise the retrieval query that powers `/geo_captures/.../tags`.
    let mut stream = graph
        .execute(
            Query::new(
                "test_geo_capture_tag_retrieval",
                "MATCH (g:MapkyAppGeoCapture {id: $id})
                 OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(g)
                 RETURN tag.label AS label, tagger.id AS tagger_id",
            )
            .param("id", first_capture_compound_id.as_str()),
        )
        .await?;
    let mut labels = Vec::new();
    while let Some(row) = stream.try_next().await? {
        if let Ok(label) = row.get::<String>("label") {
            labels.push(label);
        }
    }
    assert!(
        labels.iter().any(|l| l == "needs-review"),
        "geo_capture tag retrieval query should return the new tag, got {labels:?}"
    );

    // ── Delete the sequence ─────────────────────────────────────────────────
    test.del(&user_kp, &seq_tag_path).await?;
    test.del(&user_kp, &cap_tag_path).await?;
    for p in &capture_paths {
        test.del(&user_kp, p).await?;
    }
    test.del(&user_kp, &sequence_path).await?;

    let mut stream = graph
        .execute(
            Query::new(
                "test_sequence_deleted",
                "MATCH (s:MapkyAppSequence {id: $id}) RETURN count(s) AS cnt",
            )
            .param("id", compound_seq_id.as_str()),
        )
        .await?;
    let cnt: i64 = stream.try_next().await?.unwrap().get("cnt")?;
    assert_eq!(cnt, 0);

    test.cleanup_user(&user_kp).await?;
    Ok(())
}
