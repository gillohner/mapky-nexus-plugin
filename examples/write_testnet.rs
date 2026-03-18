//! Write test data to a pubky-docker testnet homeserver.
//!
//! Creates ephemeral users, signs them up on the testnet homeserver,
//! and PUTs MapkyAppPost blobs to `/pub/mapky.app/posts/` paths.
//! nexusd (running with testnet config) will pick these up and index
//! them into Neo4j via the MapkyPlugin.
//!
//! # Prerequisites
//!
//! 1. pubky-docker testnet running:
//!    ```sh
//!    cd /path/to/pubky-docker && docker compose up -d
//!    ```
//!
//! 2. nexusd running with testnet config:
//!    ```sh
//!    cargo run -p nexusd
//!    ```
//!
//! # Usage
//!
//! ```sh
//! cargo run -p mapky-nexus-plugin --example write_testnet
//!
//! # Wait ~10s for watcher poll + Nominatim lookups, then verify:
//! curl -s 'localhost:8080/v0/mapky/viewport?min_lat=-90&min_lon=-180&max_lat=90&max_lon=180&limit=100' | jq .
//! curl -s 'localhost:8080/v0/mapky/place/node/1573053883/posts' | jq .   # Hafenbar, Luzern
//! curl -s 'localhost:8080/v0/mapky/place/way/618456759/posts' | jq .     # Bitcoin Ekasi, Mossel Bay
//! curl -s 'localhost:8080/v0/mapky/place/node/3646146894/posts' | jq .   # Insider, Zürich
//! ```

use mapky_app_specs::traits::{HasIdPath, TimestampId};
use mapky_app_specs::{MapkyAppPost, MapkyAppPostKind, OsmElementType, OsmRef};
use pubky::{Keypair, PubkyHttpClient, PublicKey};

/// The homeserver public key from pubky-docker config.toml.
/// Must match the instance you're running.
const HOMESERVER_PK: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";

fn test_posts() -> Vec<(OsmRef, &'static str, Option<u8>)> {
    vec![
        // Luzern — Hafenbar zur Metzgerhalle (node/1573053883)
        (
            OsmRef::new(OsmElementType::Node, 1573053883),
            "Great Bitcoin bar in Luzern. Lightning payments work perfectly, friendly staff.",
            Some(9),
        ),
        (
            OsmRef::new(OsmElementType::Node, 1573053883),
            "Nice vibe and good beer selection. A bit loud on Friday nights but worth it.",
            Some(7),
        ),
        (
            OsmRef::new(OsmElementType::Node, 1573053883),
            "Does the kitchen serve food or just drinks?",
            None,
        ),
        // Mossel Bay — Bitcoin Ekasi Center (way/618456759)
        (
            OsmRef::new(OsmElementType::Way, 618456759),
            "Incredible community work. Teaching Bitcoin to kids in Mossel Bay — genuinely inspiring.",
            Some(10),
        ),
        (
            OsmRef::new(OsmElementType::Way, 618456759),
            "Visited during a trip along the Garden Route. The team here is doing amazing work.",
            Some(9),
        ),
        // Zürich — Insider restaurant (node/3646146894)
        (
            OsmRef::new(OsmElementType::Node, 3646146894),
            "Solid lunch spot. Great value, fast service, and the daily specials are always good.",
            Some(8),
        ),
        (
            OsmRef::new(OsmElementType::Node, 3646146894),
            "Are you open on Saturdays? The OSM hours say closed but the website says otherwise.",
            None,
        ),
    ]
}

/// Create a testnet SDK client and sign up a user.
/// Tolerates Pkarr DHT publish failures (common with isolated testnet DHT).
async fn signup_user(
    homeserver: &PublicKey,
    user_index: usize,
) -> Result<(String, pubky::PubkySession), Box<dyn std::error::Error + Send + Sync>> {
    let client = PubkyHttpClient::testnet()?;
    let pubky = pubky::Pubky::with_client(client);

    let keypair = Keypair::random();
    let pk = keypair.public_key().to_z32();
    let signer = pubky.signer(keypair);

    match signer.signup(homeserver, None).await {
        Ok(session) => {
            println!("  User {}: {pk}", user_index + 1);
            Ok((pk, session))
        }
        Err(e) => {
            let err_str = format!("{e}");
            // The signup HTTP request succeeds but Pkarr DHT publish fails
            // on isolated testnet. Try signin instead — the account exists.
            if err_str.contains("NoClosestNodes") || err_str.contains("Pkarr") {
                println!(
                    "  User {}: {pk} (signup ok, DHT publish skipped — isolated testnet)",
                    user_index + 1
                );
                let session = signer.signin().await?;
                Ok((pk, session))
            } else {
                Err(e.into())
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Connecting to pubky-docker testnet...\n");

    let homeserver = PublicKey::try_from(HOMESERVER_PK)?;

    let user_count = 2;
    let mut sessions = Vec::new();

    println!("── Creating {user_count} test users ──────────────────────");
    for i in 0..user_count {
        let (pk, session) = signup_user(&homeserver, i).await?;
        sessions.push((pk, session));
    }

    let posts = test_posts();

    println!(
        "\n── Writing {} posts to homeserver ────────────────",
        posts.len()
    );
    for (i, (place, content, rating)) in posts.iter().enumerate() {
        let (ref user_pk, ref session) = sessions[i % sessions.len()];

        let kind = if rating.is_some() {
            MapkyAppPostKind::Review
        } else {
            MapkyAppPostKind::Post
        };
        let post = MapkyAppPost::new(
            kind,
            place.clone(),
            Some(content.to_string()),
            *rating,
            None,
            None,
        );
        let post_id = post.create_id();
        let path = MapkyAppPost::create_path(&post_id);

        let body = serde_json::to_vec(&post)?;
        let response = session.storage().put(&path, body).await?;

        let status = response.status();
        let rating_str = rating
            .map(|r| format!("★ {r}/10"))
            .unwrap_or_else(|| "comment".to_string());

        println!("  [{status}] {:.12}… → {path}  ({rating_str})", user_pk);

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            eprintln!("    ERROR: {body}");
        }
    }

    println!("\n── Done ──────────────────────────────────────────");
    println!(
        "  {} users, {} posts written to homeserver",
        user_count,
        posts.len()
    );
    println!();
    println!("  nexusd will index these on its next watcher poll cycle (~5s).");
    println!("  Nominatim geocoding adds a few more seconds per new place.");
    println!();
    println!("  Verify with:");
    println!("  curl -s 'localhost:8080/v0/mapky/viewport?min_lat=-90&min_lon=-180&max_lat=90&max_lon=180&limit=100' | jq .");
    println!("  curl -s 'localhost:8080/v0/mapky/place/node/1573053883/posts' | jq .  # Hafenbar, Luzern");
    println!("  curl -s 'localhost:8080/v0/mapky/place/way/618456759/posts' | jq .    # Bitcoin Ekasi, Mossel Bay");
    println!("  curl -s 'localhost:8080/v0/mapky/place/node/3646146894/posts' | jq .  # Insider, Zürich");
    println!();
    println!("  User public keys (for events-stream debugging):");
    for (i, (pk, _)) in sessions.iter().enumerate() {
        println!("    User {}: {pk}", i + 1);
    }

    Ok(())
}
