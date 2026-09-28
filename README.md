# Elasticsearch Geo Insights Agent

Chat with your Elasticsearch data in [Open WebUI](https://github.com/open-webui/open-webui). This is an Open WebUI
**Tool** plus a system prompt. Together they let any tool-calling LLM discover unfamiliar indices, work out how the
geographic data is stored, run analytics, and draw interactive maps in the chat.

```
 You ──► Open WebUI ──► LLM (native tool calling)
             │               │ tool calls
             │               ▼
             │     tools/elasticsearch_geo_tools.py ──► Elasticsearch REST API (7.13+ / 8.x / 9.x)
             └──── Leaflet map embeds ◄───────┘
```

## Built for data whose format you don't know

`detect_geo_fields` inspects mappings, value ranges, and sample documents, and reports what it finds:

| Stored as | Detected by | Usable in geo tools as |
|---|---|---|
| `geo_point` (objects, `"lat,lon"`, `[lon,lat]`, geohash, WKT) | mapping + sample | the field name |
| `geo_shape` (GeoJSON / WKT polygons, lines) | mapping | the field name |
| Separate latitude/longitude numbers (`lat`/`lon`, `latitude`, `lng`, `decimalLatitude`, `pickup_lat`…) | field names (snake and camelCase) + range check | `"lat_field,lon_field"` |
| `"lat,lon"` text/keyword strings | sample values | `"latlon_string:field"` |
| GeoJSON Points in a plain object mapping | `x.type` + `x.coordinates` fields | `"geojson_point:field"` |
| WKT or GeoJSON text, projected x/y/easting/northing | sample values / names | reported, with how to fix |
| Place names only (country, city, postcode…) | field names | grouped via `aggregate` |

For lat/lon pairs it also flags **swapped** coordinates, **projected or scaled** values (metres, degrees × 1e7), and
**0,0 placeholder** documents. Derived formats become a runtime `geo_point` computed at query time. That means
distance, box, polygon, heatmap, and centroid queries work **without reindexing**.

## Tools

| Area | Tools |
|---|---|
| Discovery | `cluster_overview`, `list_indices`, `describe_index`, `detect_geo_fields` |
| Insights | `profile_index` (auto report: time span, top categories, numeric stats, completeness, geo extent and hotspots), `field_stats` |
| Query | `search` (Lucene + JSON filters + time range), `get_document`, `aggregate` (up to 3-level group-by, 10 metrics, time series, per-group centroids), `query_dsl`, `run_esql`, `run_sql` |
| Geo | `geo_distance_search` (nearest first, with km), `geo_bbox_search`, `geo_shape_search` (GeoJSON/WKT; intersects/within/disjoint/contains), `geo_heatmap` (hotspots + concentration), `geo_distance_bands`, `map_documents`, `geocode`\* |
| Writes\*\* | `index_document`, `update_document`, `delete_document` (needs `confirm=true`) |

\* Off by default. It sends place names to Nominatim (or your own `GEOCODER_URL`).
\*\* Disabled while `READ_ONLY` is on (the default).

Geo tools render an interactive **Leaflet map** in the chat through Open WebUI's rich-UI embeds. The model is told a
map is showing, so it doesn't dump coordinates. Each call also shows a live status line ("Computing spatial density…").

## Quick start

1. **Add the tool.** In Open WebUI, go to *Workspace → Tools → +* and paste in
   [`tools/elasticsearch_geo_tools.py`](tools/elasticsearch_geo_tools.py). `requests` is already in Open WebUI.
2. **Connect it.** Click the tool's gear icon (Valves) and set `ES_URL` (or `ES_CLOUD_ID`) and `ES_API_KEY` (or
   `ES_USERNAME`/`ES_PASSWORD`). Environment variables with the same names work as a fallback.
3. **Create the agent.** Go to *Workspace → Models → +*, pick a base model with good tool calling, and set these:
   - **System prompt:** paste in [`docs/SYSTEM_PROMPT.md`](docs/SYSTEM_PROMPT.md)
   - **Tools:** tick *Elasticsearch Geo Insights Agent*
   - **Advanced Params → Function Calling: Native**
4. Ask: *"What data do you have, and what does the geographic data look like?"* See
   [docs/EXAMPLE_PROMPTS.md](docs/EXAMPLE_PROMPTS.md) for more.

Strong tool-callers work best (GPT-4.1/4o-class, Claude, Qwen 2.5/3 32B+, Llama 3.3 70B, Nemotron Super). Small local
models often pick the wrong tool or malform JSON arguments.

### Try it with demo data

```bash
cp .env.example .env        # set WEBUI_SECRET_KEY (and an LLM endpoint, or add one in the UI)
docker compose --profile dev up -d
python3 -m venv .venv && .venv/bin/pip install requests
.venv/bin/python scripts/load_sample_data.py
```

This loads four small indices, each storing location differently: `demo-quakes` (geo_point), `demo-stations`
(decimalLatitude/decimalLongitude floats, plus some 0,0 placeholders), `demo-places` ("lat,lon" strings), and
`demo-regions` (geo_shape polygons). Open WebUI runs at <http://localhost:3000>. Inside compose, the tool's default
`ES_URL` is `http://elasticsearch:9200`.

## Valves

| Valve | Default | Purpose |
|---|---|---|
| `ES_URL` / `ES_CLOUD_ID` | env, then `http://localhost:9200` | cluster address |
| `ES_API_KEY` or `ES_USERNAME` + `ES_PASSWORD` | – | auth (API key preferred) |
| `VERIFY_SSL`, `CA_CERT_PATH` | `true`, – | TLS |
| `DEFAULT_INDEX` | – | used when the model omits `index` |
| `INDEX_ALLOWLIST` | – (any non-system index) | e.g. `geo-*,places`; also applied to ES\|QL/SQL `FROM` |
| `ALLOW_SYSTEM_INDICES` | `false` | allow `.`-prefixed indices |
| `MAX_RESULTS`, `MAX_OUTPUT_CHARS` | 50, 24000 | protect the model's context window |
| `READ_ONLY` | `true` | disable write tools |
| `ENABLE_MAPS`, `MAX_MAP_POINTS` | `true`, 2000 | map embeds |
| `MAP_TILE_URL`, `MAP_ATTRIBUTION` | OpenStreetMap | point at an internal tile server when air-gapped |
| `ENABLE_GEOCODING`, `GEOCODER_URL` | `false`, Nominatim | place-name lookup |

## Safety

- Give the tool a **dedicated API key with a read-only role** limited to the indices it needs:
  `read` and `view_index_metadata` on those indices, plus `monitor` cluster privilege for `cluster_overview`. The
  allowlist and `READ_ONLY` are guardrails. Elasticsearch privileges are the real boundary. The allowlist check on
  ES|QL/SQL `FROM` clauses is best-effort.
- Valve credentials are stored in Open WebUI's database, where Open WebUI admins can see them.
- Maps load Leaflet from cdnjs and tiles from `MAP_TILE_URL` in the user's browser. Document fields shown in map
  popups are inserted as text, never HTML.
- LLMs make mistakes. Check the tool calls Open WebUI shows before trusting a number, and before approving any write.

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest
```

The tests use a fake Elasticsearch transport. They cover config and auth, the allowlist, geo-format detection and
diagnosis (valid, swapped, projected, 0,0), geo_field resolution, query/filter building, aggregation
building and flattening, distance-sort fallback, map embedding, error reporting, size caps, and write guards.
**Not yet exercised against a live cluster:** the Painless runtime script for derived coordinates, and the exact
response shapes across ES versions. Run `scripts/load_sample_data.py` on a dev cluster and try each geo format
before pointing the agent at production.

## Known limitations / ideas

- Derived coordinates (lat/lon pairs, strings, GeoJSON objects) are computed per query. On very large indices,
  reindex into a real `geo_point` with an ingest pipeline for speed.
- Runtime geo fields need Elasticsearch 7.13+. ES|QL needs 8.11+. OpenSearch works for basic search and aggregations,
  but not runtime fields or ES|QL.
- Projected coordinates (UTM, Web Mercator) are detected but not reprojected.
- One cluster per tool instance. For several clusters, duplicate the tool with different valves.
