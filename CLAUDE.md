# MapKy Nexus Plugin

Pubky-nexus plugin for geo-spatial MapKy indexing. Implements `NexusPlugin` trait to add place/post/review nodes to the shared Neo4j graph.

## Structure

```
src/
├── lib.rs              — NexusPlugin trait impl, event routing, schema setup
├── api/mod.rs          — 4 REST endpoints + OpenAPI docs
├── handlers/post.rs    — PUT/DEL orchestration for MapkyAppPost
├── models/
│   ├── place.rs        — PlaceDetails (Nominatim geocoding, spatial point)
│   ├── post.rs         — PostDetails (compound ID DTO)
│   └── tag.rs          — PostTagDetails (aggregated tag labels)
└── queries/
    ├── put.rs          — Neo4j MERGE queries (user, place, post, reply, rating)
    ├── get.rs          — Neo4j READ queries (viewport, place, posts, tags, existence)
    └── del.rs          — Neo4j DELETE query (post + rating rollback)
```

## Plugin Manifest

- **name**: `"mapky"` (Redis prefix, route mount at `/v0/mapky/`)
- **namespace**: `"/pub/mapky.app/"` (claims all events under this path)

## Dependencies

- `nexus-common` — `NexusPlugin` trait, `get_neo4j_graph()`, `get_redis_conn()`
- `mapky-app-specs` — `MapkyAppPost` and other model types
- `nexus-watcher` (dev) — testing harness with `WatcherTest::setup_with_plugins()`

## Event Handling

Currently handles `posts` resource type. Other types (`collections`, `incidents`, `geo_captures`, `routes`) are stubbed with debug logging. Tags on places use standard `PubkyAppTag` (universal tags) stored at `/pub/mapky.app/tags/`, indexed by pubky-nexus core as `Resource` nodes.

### PUT flow (posts)
1. Ensure `:User` exists (MERGE, reuses nexus core's User nodes)
2. Ensure `:Place` exists — checks first, geocodes via Nominatim if new (rate-limited 1 req/s)
3. Create/update `:MapkyAppPost` with `(User)-[:AUTHORED]->(MapkyAppPost)-[:ABOUT]->(Place)`
4. Link `[:REPLY_TO]` if parent_uri is set (MATCH, safe for out-of-order delivery)
5. Increment `:Place` rolling average rating if review

### DEL flow (posts)
1. Delete `:MapkyAppPost` (DETACH DELETE), returns place + rating
2. Decrement `:Place` rating aggregate if it was a review

## Neo4j Schema

**Constraints**: UNIQUE on `Place.osm_canonical`, UNIQUE on `MapkyAppPost.id`
**Indexes**: POINT INDEX on `Place.location` (spatial bbox queries)

**Node labels**: `:Place`, `:MapkyAppPost` (uses `:MapkyAppPost` to avoid collision with nexus core `:Post`)

## API Endpoints (mounted at `/v0/mapky/`)

| Method | Path | Description |
|---|---|---|
| GET | `/viewport?min_lat&min_lon&max_lat&max_lon&limit` | Spatial bbox query for places |
| GET | `/place/{osm_type}/{osm_id}` | Single place details |
| GET | `/place/{osm_type}/{osm_id}/posts?skip&limit&reviews_only` | Posts for a place |
| GET | `/posts/{author_id}/{post_id}/tags` | Tags on a mapky post |

## Cross-Domain Support

Implements `resolve_graph_node()` so pubky-app-specs Tags can reference MapkyAppPost resources:
- Resource type `"posts"` → checks Neo4j for `MapkyAppPost` by compound ID → returns `GraphNodeRef`
- Enables `(User)-[:TAGGED {label}]->(MapkyAppPost)` relationships created by nexus core

## Testing

```bash
cargo test                                    # unit tests
cargo test --test external_post_tag           # cross-domain tag integration test
```

Integration tests use `WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin)])` from nexus-watcher.

## Key Design Details

- **Compound ID**: `author_id:post_id` — ensures uniqueness in shared graph
- **Nominatim rate limiting**: Global `Mutex<Instant>`, 1 req/s. On failure stores `(0.0, 0.0)` with `geocoded: false`
- **Place existence check**: Before geocoding, checks `place_exists()` to avoid redundant API calls
- **Idempotent writes**: All Neo4j mutations use MERGE (required by plugin contract — event retries on failure)
- **Rating rollback**: DELETE returns the old rating so the handler can decrement the place aggregate

## Related Repos

- `../mapky-app-specs/` — source of truth for MapKy data models
- `../../pubky-nexus/` — host for this plugin; provides NexusPlugin trait and infrastructure
- `../../pubky-app-specs/` — base traits (Validatable, TimestampId, etc.)
