# MapKy Nexus Plugin

Pubky-nexus plugin for geo-spatial MapKy indexing. Implements `NexusPlugin` trait to add place/post/incident/geo-capture/sequence/collection/route nodes to the shared Neo4j graph.

## Structure

```
src/
├── lib.rs              — NexusPlugin trait impl, event routing, schema setup
├── api/mod.rs          — Axum router + handlers + OpenAPI docs
├── handlers/
│   ├── post.rs         — PUT/DEL for MapkyAppPost (rating roll-up)
│   ├── collection.rs   — PUT/DEL for MapkyAppCollection
│   ├── incident.rs     — PUT/DEL for MapkyAppIncident
│   ├── geo_capture.rs  — PUT/DEL for MapkyAppGeoCapture
│   ├── sequence.rs     — PUT/DEL for MapkyAppSequence
│   ├── route.rs        — PUT/DEL for MapkyAppRoute (metadata + bbox)
│   └── tag.rs          — Universal tag resolution (cross-domain)
├── models/             — DTOs (Place, Post, Tag, Collection, Incident,
│                          GeoCapture, Sequence, Route)
└── queries/
    ├── put.rs          — Neo4j MERGE queries
    ├── get.rs          — Neo4j READ queries (viewport, near-point, ...)
    └── del.rs          — Neo4j DELETE queries
```

## Plugin Manifest

- **name**: `"mapky"` (Redis prefix, route mount at `/v0/mapky/`)
- **namespace**: `"/pub/mapky.app/"` (claims all events under this path)

## Dependencies

- `nexus-common` — `NexusPlugin` trait, `get_neo4j_graph()`, `get_redis_conn()`
- `mapky-app-specs` — `MapkyAppPost`, `MapkyAppRoute`, `MapkyAppCollection`, `MapkyAppIncident`, `MapkyAppGeoCapture`, `MapkyAppSequence`
- `nexus-watcher` (dev) — testing harness with `WatcherTest::setup_with_plugins()`

## Event Handling

All MapkyAppObject variants are dispatched via `MapkyAppObject::from_path()`:

| Resource | Handler | Indexed |
|---|---|---|
| `posts/<id>` | `handlers::post` | `(User)-[:AUTHORED]->(MapkyAppPost)-[:ABOUT]->(Place)`; rating roll-up; `[:REPLY_TO]` for threaded replies |
| `collections/<id>` | `handlers::collection` | `(User)-[:CREATED]->(MapkyAppCollection)`; OSM URL items |
| `incidents/<id>` | `handlers::incident` | `(User)-[:REPORTED]->(MapkyAppIncident)`; spatial point |
| `geo_captures/<id>` | `handlers::geo_capture` | `(User)-[:AUTHORED]->(MapkyAppGeoCapture)`; spatial point + heading |
| `sequences/<id>` | `handlers::sequence` | Capture sequence (ordered list) |
| `routes/<id>` | `handlers::route` | `(User)-[:CREATED]->(MapkyAppRoute)`; bbox + start point + activity. Body (waypoints + polyline) stays on the homeserver — only searchable metadata is indexed. |
| `tags/<id>` | `handlers::tag` | Universal tag — resolved cross-domain via `resolve_graph_node()` |
| `files`, `blobs` | nexus core | Forwarded |

### PUT flow (posts, illustrative)
1. Ensure `:User` exists (MERGE, reuses nexus core's User nodes)
2. Ensure `:Place` exists — checks first, geocodes via Nominatim if new (rate-limited 1 req/s)
3. Create/update `:MapkyAppPost` with `(User)-[:AUTHORED]->(MapkyAppPost)-[:ABOUT]->(Place)`
4. Link `[:REPLY_TO]` if `parent_uri` is set (MATCH, safe for out-of-order delivery)
5. Increment `:Place` rolling average rating if review

### Routes — what's stored where

Routes are typically large (hundreds of waypoints, encoded polyline ~10s of KB). To keep Neo4j and the indexer responses small:

- **In Neo4j (`:MapkyAppRoute`)**: id, author_id, name, description, activity, distance_m, estimated_duration_s, elevation_gain/loss_m, waypoint_count, bbox (`min_lat/min_lon/max_lat/max_lon`), `start_point` (spatial), `indexed_at`. A POINT INDEX on `start_point` powers viewport queries.
- **On the homeserver**: full `MapkyAppRoute` JSON (waypoints + encoded polyline + costing metadata) at `/pub/mapky.app/routes/<id>`. The frontend fetches this directly via the Pubky SDK when rendering a route detail.

The viewport endpoint returns metadata + start point only — no polyline. This keeps the response small for "all routes in the city" queries; clicking into a route lazily fetches the body.

### DEL flow

Each handler reverses its PUT. Notably: post DEL returns the old rating so the place aggregate can be decremented; route DEL detaches the `:MapkyAppRoute` and any tag relationships.

## Neo4j Schema

**Constraints (UNIQUE id)**: `Place.osm_canonical`, `MapkyAppPost.id`, `MapkyAppCollection.id`, `MapkyAppIncident.id`, `MapkyAppGeoCapture.id`, `MapkyAppRoute.id`.

**Spatial POINT INDEXES**: `Place.location`, `MapkyAppIncident.location`, `MapkyAppGeoCapture.location`, `MapkyAppRoute.start_point`.

**Node labels**: `:Place`, `:MapkyAppPost`, `:MapkyAppCollection`, `:MapkyAppIncident`, `:MapkyAppGeoCapture`, `:MapkyAppSequence`, `:MapkyAppRoute`. The `MapkyApp*` prefix avoids collision with nexus core (`:Post`, etc.).

**Edges**: `[:AUTHORED]`, `[:ABOUT]`, `[:REPLY_TO]`, `[:CREATED]`, `[:REPORTED]`, `[:CAPTURED]`, plus `[:TAGGED {label}]` from cross-domain tags.

## API Endpoints (mounted at `/v0/mapky/`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/viewport` | Place dots in a bbox |
| `GET` | `/place/{osm_type}/{osm_id}` | Single place detail |
| `GET` | `/place/{osm_type}/{osm_id}/posts` | Posts for a place (paginated, optional `reviews_only`) |
| `GET` | `/place/{osm_type}/{osm_id}/tags` | Universal tags on a place |
| `GET` | `/place/{osm_type}/{osm_id}/routes` | Routes that pass near a place (bbox-contains) |
| `GET` | `/posts/{author_id}/{post_id}/tags` | Tags on a Mapky post |
| `GET` | `/posts/user/{user_id}` | A user's posts |
| `GET` | `/incidents/viewport` | Incidents in a bbox |
| `GET` | `/incidents/{author_id}/{incident_id}` | Incident detail |
| `GET` | `/incidents/user/{user_id}` | A user's incidents |
| `GET` | `/geo_captures/viewport` | Geo-captures in a bbox |
| `GET` | `/geo_captures/nearby` | Geo-captures near a point |
| `GET` | `/geo_captures/user/{user_id}` | A user's geo-captures |
| `GET` | `/sequences/user/{user_id}` | A user's capture sequences |
| `GET` | `/collections/user/{user_id}` | A user's collections |
| `GET` | `/routes/viewport` | Routes intersecting a bbox (metadata only) |
| `GET` | `/routes/{author_id}/{route_id}` | Route metadata |
| `GET` | `/routes/{author_id}/{route_id}/tags` | Tags on a route |
| `GET` | `/routes/user/{user_id}` | A user's routes |
| `GET` | `/search/tags?q=` | Tag search across places, collections, posts |

## Cross-Domain Tags

Tag handling for cross-domain resources flows through `resolve_graph_node()`:
- Universal `PubkyAppTag` blobs at `/pub/mapky.app/tags/<id>` are dispatched here by nexus core.
- The handler extracts the tagged URI, resolves it to a `GraphNodeRef` (matching `MapkyAppPost`, `MapkyAppRoute`, `MapkyAppCollection`, `MapkyAppGeoCapture`, …), and creates `(User)-[:TAGGED {label}]->(<node>)`.
- This is what makes route tag-search possible without dedicated route-tag endpoints — a route is just another taggable resource.

## Testing

```bash
cargo test                                    # unit tests
cargo test --test external_post_tag           # cross-domain tag integration test
```

Integration tests use `WatcherTest::setup_with_plugins(vec![Arc::new(MapkyPlugin)])` from nexus-watcher.

## Key Design Details

- **Compound IDs**: `author_id:resource_id` for any resource that's namespaced per user (posts, routes, collections, incidents, geo-captures, sequences) — ensures uniqueness in the shared graph.
- **Nominatim rate limiting**: Global `Mutex<Instant>`, 1 req/s. On failure stores `(0.0, 0.0)` with `geocoded: false`.
- **Place existence check**: Before geocoding, `place_exists()` short-circuits redundant API calls.
- **Idempotent writes**: All Neo4j mutations use MERGE (required by plugin contract — events retry on failure).
- **Rating rollback**: Post DEL returns the old rating so the handler can decrement the place aggregate.
- **Routes — metadata only in graph**: Polyline + waypoints are NOT in Neo4j. Saved on the homeserver, fetched directly by the frontend on detail view.

## Related Repos

- `../mapky-app-specs/` — source of truth for MapKy data models
- `../../pubky-nexus/` — host for this plugin; provides NexusPlugin trait and infrastructure
- `../../pubky-app-specs/` — base traits (Validatable, TimestampId, etc.)
