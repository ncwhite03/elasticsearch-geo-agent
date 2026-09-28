"""
title: Elasticsearch Geo Insights Agent
author: ncwhite03
version: 0.1.0
license: MIT
description: Lets a tool-calling LLM discover, query, map, and analyze Elasticsearch data - including unknown schemas and whatever geo format the data uses (geo_point, geo_shape, lat/lon columns, "lat,lon" strings, GeoJSON objects).
requirements: requests
"""

import asyncio
import base64
import difflib
import fnmatch
import json
import math
import os
import re
import time
from typing import Any, Callable, Optional
from urllib.parse import quote

import requests
from pydantic import BaseModel, Field

GEO_TYPES = {"geo_point", "geo_shape"}
CARTESIAN_TYPES = {"point", "shape"}
NUMERIC_TYPES = {
    "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "unsigned_long",
}
DATE_TYPES = {"date", "date_nanos"}
TEXT_TYPES = {"text", "match_only_text"}
OBJECT_TYPES = {"object", "nested", "flattened"}
CALENDAR_INTERVALS = {
    "minute", "1m", "hour", "1h", "day", "1d", "week", "1w", "month", "1M", "quarter", "1q", "year", "1y",
}
DISTANCE_UNITS_M = {
    "km": 1000.0, "m": 1.0, "cm": 0.01, "mi": 1609.344, "miles": 1609.344, "mile": 1609.344,
    "yd": 0.9144, "ft": 0.3048, "nmi": 1852.0, "nm": 1852.0,
}

LAT_TOKEN = re.compile(r"(?<![a-z0-9])(lat|latitude)(?![a-z0-9])")
LON_TOKEN = re.compile(r"(?<![a-z0-9])(lon|lng|long|longitude)(?![a-z0-9])")
PROJECTED_TOKEN = re.compile(r"(?<![a-z0-9])(x|y|easting|northing)(?![a-z0-9])")
PLACE_TOKEN = re.compile(
    r"(country|state|province|region|city|town|county|district|municipality|address|street|zip|postal|"
    r"postcode|place|locality|continent|iso_?code|admin)"
)
WKT_RE = re.compile(
    r"^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION|BBOX|ENVELOPE)\s*\(",
    re.I,
)
LATLON_STR_RE = re.compile(r"^\s*(-?\d{1,3}(?:\.\d+)?)\s*,\s*(-?\d{1,3}(?:\.\d+)?)\s*$")
RUNTIME_GEO_FIELD = "geo_agent_point"

# One Painless script handles every derived-coordinate format; params.mode picks the branch.
_PAINLESS = """
double toD(def x) { if (x instanceof Number) { return ((Number) x).doubleValue(); } return Double.parseDouble(x.toString().trim()); }
def walk(def src, String full, List path, boolean keepList) {
  if (src instanceof Map && ((Map) src).containsKey(full)) { return ((Map) src).get(full); }
  def v = src;
  for (def p : path) {
    if (v instanceof List && ((List) v).size() > 0) { v = ((List) v).get(0); }
    if (v instanceof Map) { v = ((Map) v).get(p); } else { return null; }
  }
  if (!keepList && v instanceof List && ((List) v).size() > 0) { v = ((List) v).get(0); }
  return v;
}
double la = Double.NaN; double lo = Double.NaN;
try {
  if (params.mode == 'pair_docvalues') {
    if (doc.containsKey(params.lat) && doc.containsKey(params.lon) && doc[params.lat].size() > 0 && doc[params.lon].size() > 0) {
      la = toD(doc[params.lat].value); lo = toD(doc[params.lon].value);
    }
  } else if (params.mode == 'pair_source') {
    def a = walk(params._source, params.lat, params.lat_path, false); def b = walk(params._source, params.lon, params.lon_path, false);
    if (a != null && b != null) { la = toD(a); lo = toD(b); }
  } else if (params.mode == 'latlon_string') {
    def v = walk(params._source, params.field, params.path, false);
    if (v != null) { String s = v.toString(); int i = s.indexOf(','); if (i > 0) { la = toD(s.substring(0, i)); lo = toD(s.substring(i + 1)); } }
  } else if (params.mode == 'geojson_point') {
    def v = walk(params._source, params.field, params.path, true);
    def c = v instanceof Map ? ((Map) v).get('coordinates') : v;
    if (c instanceof List && ((List) c).size() >= 2 && ((List) c).get(0) instanceof Number) { lo = toD(((List) c).get(0)); la = toD(((List) c).get(1)); }
  }
} catch (NumberFormatException e) { }
if (!Double.isNaN(la) && !Double.isNaN(lo) && la >= -90 && la <= 90 && lo >= -180 && lo <= 180) { emit(la, lo); }
"""


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------
class EsError(Exception):
    pass


def _err(exc: Exception) -> str:
    return f"Error: {type(exc).__name__}: {exc}"


def _es_error_text(resp) -> str:
    try:
        payload = resp.json()
    except ValueError:
        return f"Elasticsearch HTTP {resp.status_code}: {resp.text[:500]}"
    err = payload.get("error", payload)
    if isinstance(err, str):
        return f"Elasticsearch HTTP {resp.status_code}: {err[:800]}"
    parts = [f"Elasticsearch HTTP {resp.status_code} {err.get('type', '')}: {err.get('reason', '')}"]
    for shard in err.get("failed_shards", [])[:1]:
        r = shard.get("reason", {})
        parts.append(f"shard failure {r.get('type', '')}: {r.get('reason', '')}")
    cause = err.get("caused_by")
    while cause:  # the deepest cause is usually the most actionable message for the model
        parts.append(f"caused by {cause.get('type', '')}: {cause.get('reason', '')}")
        cause = cause.get("caused_by")
    return " | ".join(parts)[:1500]


def _cloud_id_to_url(cloud_id: str) -> str:
    b64 = cloud_id.split(":", 1)[1] if ":" in cloud_id else cloud_id
    decoded = base64.b64decode(b64 + "=" * (-len(b64) % 4)).decode()
    host, es_uuid = decoded.split("$")[:2]
    port = ""
    if ":" in host:
        host, port = host.split(":", 1)
        port = f":{port}"
    return f"https://{es_uuid}.{host}{port}"


def _norm_name(name: str) -> str:
    """decimalLatitude -> decimal_latitude, so name heuristics work on camelCase too."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def _get_path(src: Any, path: str) -> Any:
    if isinstance(src, dict) and path in src:
        return src[path]
    cur = src
    for part in path.split("."):
        if isinstance(cur, list) and cur:
            cur = cur[0]
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _trim(obj: Any, max_list: int = 10, max_str: int = 300, depth: int = 0) -> Any:
    """Shrink documents for the LLM context: long strings, long lists, polygon vertex arrays."""
    if depth > 8:
        return "..."
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "coordinates" and isinstance(v, list) and v and isinstance(v[0], list):
                out[k] = f"<{_count_vertices(v)} vertices omitted>"
            else:
                out[k] = _trim(v, max_list, max_str, depth + 1)
        return out
    if isinstance(obj, list):
        items = [_trim(v, max_list, max_str, depth + 1) for v in obj[:max_list]]
        if len(obj) > max_list:
            items.append(f"... (+{len(obj) - max_list} more)")
        return items
    if isinstance(obj, str) and len(obj) > max_str:
        return obj[:max_str] + f"... (+{len(obj) - max_str} chars)"
    return obj


def _count_vertices(coords: Any) -> int:
    if isinstance(coords, list) and coords and isinstance(coords[0], (int, float)):
        return 1
    return sum(_count_vertices(c) for c in coords) if isinstance(coords, list) else 0


def _flatten_coords(coords: Any, out: list) -> list:
    if isinstance(coords, list) and len(coords) >= 2 and isinstance(coords[0], (int, float)):
        out.append((coords[1], coords[0]))
    elif isinstance(coords, list):
        for c in coords:
            _flatten_coords(c, out)
    return out


def _geometry_point(geom: Any) -> Optional[tuple]:
    """Representative (lat, lon) for a GeoJSON geometry (vertex mean; fine for display/distance hints)."""
    if not isinstance(geom, dict):
        return None
    if geom.get("type") == "GeometryCollection":
        for g in geom.get("geometries", []):
            p = _geometry_point(g)
            if p:
                return p
        return None
    pts = _flatten_coords(geom.get("coordinates"), [])
    if not pts:
        return None
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _distance_m(distance: str) -> float:
    m = re.match(r"^\s*([\d.]+)\s*([a-zA-Z]*)\s*$", str(distance))
    if not m:
        raise ValueError(f"Unrecognized distance '{distance}'. Use e.g. '10km', '500m', '5mi'.")
    unit = (m.group(2) or "m").lower()
    if unit not in DISTANCE_UNITS_M:
        raise ValueError(f"Unknown distance unit '{unit}'. Use one of: {', '.join(DISTANCE_UNITS_M)}")
    return float(m.group(1)) * DISTANCE_UNITS_M[unit]


def _check_latlon(lat: float, lon: float):
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        raise ValueError(f"Invalid coordinates lat={lat}, lon={lon} (lat must be -90..90, lon -180..180).")


def _parse_json_arg(raw: str, what: str) -> Any:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{what} is not valid JSON: {e}")


def _total(resp: dict) -> Optional[int]:
    t = resp.get("hits", {}).get("total")
    return t.get("value") if isinstance(t, dict) else t


def _round(v: Any, n: int = 4) -> Any:
    return round(v, n) if isinstance(v, float) else v


def _auto_geotile_zoom(bounds: Optional[dict]) -> int:
    if not bounds:
        return 3
    tl, br = bounds["top_left"], bounds["bottom_right"]
    width = (br["lon"] - tl["lon"]) % 360 or 360
    height = max(tl["lat"] - br["lat"], 1e-6)
    extent = max(width, height * 2)
    return int(max(0, min(18, math.floor(math.log2(360 / max(extent, 1e-6))) + 4)))


def _hit_label(hit: dict) -> str:
    parts = [f"_id: {hit.get('_id')}"]
    for k, v in (hit.get("_source") or {}).items():
        if isinstance(v, (str, int, float, bool)) and len(parts) < 6:
            parts.append(f"{k}: {str(v)[:60]}")
    return " | ".join(parts)


def _map_html(title: str, tiles: str, attribution: str, points=None, cells=None, shapes=None, overlay=None) -> str:
    data = {
        "title": title, "tiles": tiles, "attr": attribution,
        "points": points or [], "cells": cells or [], "shapes": shapes or [], "overlay": overlay,
    }
    payload = json.dumps(data, default=str).replace("</", "<\\/")
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<style>
 body {{ margin:0; font-family: system-ui, sans-serif; background: transparent; color: #374151; }}
 #t {{ font-size: 13px; padding: 6px 2px; }}
 #m {{ height: 440px; border-radius: 8px; }}
</style></head><body><div id="t"></div><div id="m"></div>
<script>
const D = {payload};
document.getElementById('t').textContent = D.title;
const map = L.map('m', {{ worldCopyJump: true }});
L.tileLayer(D.tiles, {{ maxZoom: 19, attribution: D.attr }}).addTo(map);
const layers = [];
const pop = (s) => {{ const el = document.createElement('div'); el.textContent = s; return el; }};
D.points.forEach(p => layers.push(L.circleMarker([p[0], p[1]], {{ radius: 5, color: '#1d4ed8', weight: 1, fillOpacity: 0.7 }}).bindPopup(pop(p[2] || ''))));
const maxC = D.cells.reduce((a, c) => Math.max(a, c[2]), 1);
const ramp = (t) => `hsl(${{Math.round(220 - 220 * t)}}, 85%, 50%)`;
D.cells.forEach(c => {{
  const t = Math.sqrt(c[2] / maxC);
  layers.push(L.circleMarker([c[0], c[1]], {{ radius: 4 + 22 * t, color: ramp(t), fillColor: ramp(t), fillOpacity: 0.55, weight: 1 }})
    .bindPopup(pop(c[2].toLocaleString() + ' docs' + (c[3] ? ' (' + c[3] + ')' : ''))));
}});
D.shapes.forEach(s => {{ try {{ layers.push(L.geoJSON(s[0], {{ style: {{ color: '#7c3aed', weight: 1.5, fillOpacity: 0.15 }} }}).bindPopup(pop(s[1] || ''))); }} catch (e) {{}} }});
const o = D.overlay;
if (o && o.circle) layers.push(L.circle([o.circle[0], o.circle[1]], {{ radius: o.circle[2], color: '#dc2626', fill: false, dashArray: '4' }}));
if (o && o.bbox) layers.push(L.rectangle([[o.bbox[0], o.bbox[1]], [o.bbox[2], o.bbox[3]]], {{ color: '#dc2626', fill: false, dashArray: '4' }}));
if (o && o.geojson) layers.push(L.geoJSON(o.geojson, {{ style: {{ color: '#dc2626', fill: false, dashArray: '4' }} }}));
const group = L.featureGroup(layers).addTo(map);
if (layers.length) map.fitBounds(group.getBounds().pad(0.1), {{ maxZoom: 14 }}); else map.setView([20, 0], 2);
function reportHeight() {{ parent.postMessage({{ type: 'iframe:height', height: document.documentElement.scrollHeight }}, '*'); }}
window.addEventListener('load', reportHeight); new ResizeObserver(reportHeight).observe(document.body);
</script></body></html>"""


class GeoRef:
    """A resolved geo source: a real geo field, or a runtime geo_point derived from other fields."""

    def __init__(self, spec: str, field: str, kind: str, runtime: Optional[dict] = None, note: str = ""):
        self.spec, self.field, self.kind, self.runtime, self.note = spec, field, kind, runtime, note

    def describe(self) -> dict:
        d = {"geo_field": self.spec, "kind": self.kind}
        if self.runtime:
            d["derived"] = "runtime geo_point computed at query time (slower on very large indices)"
        if self.note:
            d["note"] = self.note
        return d


def _runtime_geo(params: dict) -> dict:
    return {RUNTIME_GEO_FIELD: {"type": "geo_point", "script": {"source": _PAINLESS, "params": params}}}


# ---------------------------------------------------------------------------
# Open WebUI Tool
# ---------------------------------------------------------------------------
class Tools:
    class Valves(BaseModel):
        ES_URL: str = Field(
            default="",
            description="Elasticsearch URL, e.g. https://es.corp.local:9200. Falls back to the ES_URL env var, then http://localhost:9200.",
        )
        ES_CLOUD_ID: str = Field(default="", description="Elastic Cloud ID (used instead of ES_URL when set). Env: ES_CLOUD_ID.")
        ES_API_KEY: str = Field(default="", description="Base64 API key (preferred over username/password). Env: ES_API_KEY.")
        ES_USERNAME: str = Field(default="", description="Basic-auth username. Env: ES_USERNAME.")
        ES_PASSWORD: str = Field(default="", description="Basic-auth password. Env: ES_PASSWORD.")
        VERIFY_SSL: bool = Field(default=True, description="Verify TLS certificates.")
        CA_CERT_PATH: str = Field(default="", description="Path to a CA bundle inside the Open WebUI container (optional).")
        REQUEST_TIMEOUT: int = Field(default=60, description="HTTP timeout in seconds for each Elasticsearch call.")
        DEFAULT_INDEX: str = Field(default="", description="Index/pattern/alias used when a call omits `index`.")
        INDEX_ALLOWLIST: str = Field(
            default="",
            description="Comma-separated index patterns the agent may touch (e.g. 'geo-*,places'). Empty = any non-system index.",
        )
        ALLOW_SYSTEM_INDICES: bool = Field(default=False, description="Allow indices starting with '.' (security, kibana...).")
        MAX_RESULTS: int = Field(default=50, description="Cap on documents/rows/buckets returned to the model per call.")
        MAX_OUTPUT_CHARS: int = Field(default=24000, description="Truncate tool output beyond this size to protect the context window.")
        READ_ONLY: bool = Field(default=True, description="When true, index/update/delete document tools are refused.")
        ENABLE_MAPS: bool = Field(default=True, description="Render interactive Leaflet maps in chat for geo results.")
        MAX_MAP_POINTS: int = Field(default=2000, description="Maximum markers drawn on a map.")
        MAP_TILE_URL: str = Field(
            default="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            description="Leaflet tile URL template. Point at an internal tile server for air-gapped networks.",
        )
        MAP_ATTRIBUTION: str = Field(default="&copy; OpenStreetMap contributors", description="Tile attribution text.")
        ENABLE_GEOCODING: bool = Field(
            default=False,
            description="Allow the geocode tool to send place names to GEOCODER_URL (an external service by default).",
        )
        GEOCODER_URL: str = Field(
            default="https://nominatim.openstreetmap.org/search", description="Nominatim-compatible search endpoint."
        )

    def __init__(self):
        self.valves = self.Valves()
        self._cache: dict = {}

    # -- configuration / transport ------------------------------------------
    def _cfg(self, name: str) -> str:
        return getattr(self.valves, name) or os.environ.get(name, "")

    def _base_url(self) -> str:
        cloud_id = self._cfg("ES_CLOUD_ID")
        if cloud_id:
            return _cloud_id_to_url(cloud_id)
        return (self._cfg("ES_URL") or "http://localhost:9200").rstrip("/")

    def _request(self, method: str, path: str, body: Any = None, params: Optional[dict] = None) -> Any:
        headers = {"Accept": "application/json"}
        auth = None
        api_key = self._cfg("ES_API_KEY")
        if api_key:
            headers["Authorization"] = f"ApiKey {api_key}"
        elif self._cfg("ES_USERNAME"):
            auth = (self._cfg("ES_USERNAME"), self._cfg("ES_PASSWORD"))
        verify: Any = self.valves.CA_CERT_PATH or self.valves.VERIFY_SSL
        if verify is False:
            requests.packages.urllib3.disable_warnings()
        resp = requests.request(
            method, f"{self._base_url()}/{path.lstrip('/')}", json=body, params=params, headers=headers,
            auth=auth, verify=verify, timeout=self.valves.REQUEST_TIMEOUT,
        )
        if resp.status_code >= 400:
            raise EsError(_es_error_text(resp))
        return resp.json() if resp.content else {}

    def _search(self, index: str, body: dict) -> dict:
        body.setdefault("timeout", f"{max(5, self.valves.REQUEST_TIMEOUT - 5)}s")
        return self._request("POST", f"{quote(index, safe=',*:-_.+')}/_search", body)

    def _cached(self, key: str, ttl: int, fn: Callable) -> Any:
        hit = self._cache.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
        val = fn()
        self._cache[key] = (time.time(), val)
        return val

    def _out(self, obj: Any) -> str:
        text = obj if isinstance(obj, str) else json.dumps(obj, indent=1, default=str, ensure_ascii=False)
        limit = self.valves.MAX_OUTPUT_CHARS
        if len(text) > limit:
            text = text[:limit] + f"\n... [output truncated at {limit} chars; narrow the query, reduce size, or select fields]"
        return text

    def _blocked(self) -> Optional[str]:
        if self.valves.READ_ONLY:
            return "Error: this agent is in READ_ONLY mode; document writes are disabled by the administrator."
        return None

    # -- index access control -------------------------------------------------
    def _allowed(self, name: str) -> bool:
        target = name.split(":", 1)[-1]  # strip cross-cluster prefix
        if target.startswith(".") and not self.valves.ALLOW_SYSTEM_INDICES:
            return False
        allow = [p.strip() for p in self.valves.INDEX_ALLOWLIST.split(",") if p.strip()]
        return not allow or any(fnmatch.fnmatchcase(target, p) for p in allow)

    def _index(self, index: str) -> str:
        index = (index or "").strip() or self.valves.DEFAULT_INDEX.strip()
        if not index:
            allow = self.valves.INDEX_ALLOWLIST.strip()
            if allow:
                return allow
            raise ValueError("Specify `index` (an index name, pattern like 'geo-*', alias, or data stream). Use list_indices.")
        if index in ("*", "_all") and self.valves.INDEX_ALLOWLIST.strip():
            return self.valves.INDEX_ALLOWLIST.strip()
        for part in index.split(","):
            part = part.strip()
            if part.startswith("-"):
                continue
            if part in ("_all",) or not self._allowed(part):
                raise ValueError(f"Index '{part}' is not allowed by this agent's configuration.")
        return index

    def _check_query_sources(self, sources: list):
        for src in sources:
            for part in src.split(","):
                part = part.strip().strip('"`')
                if part and not part.startswith("-") and not self._allowed(part):
                    raise ValueError(f"Index '{part}' is not allowed by this agent's configuration.")

    # -- mapping knowledge ----------------------------------------------------
    def _fcaps(self, index: str) -> dict:
        """field -> {type, searchable, aggregatable, conflict?} merged across all matched indices."""

        def load():
            resp = self._request("GET", f"{quote(index, safe=',*:-_.+')}/_field_caps", params={"fields": "*"})
            out = {}
            for name, by_type in resp.get("fields", {}).items():
                types = [t for t in by_type if t != "unmapped"]
                if not types or name.startswith("_") or types[0].startswith("_"):
                    continue
                info = by_type[types[0]]
                out[name] = {
                    "type": types[0], "searchable": info.get("searchable", False),
                    "aggregatable": info.get("aggregatable", False),
                }
                if len(types) > 1:
                    out[name]["conflict"] = types
            return {"fields": out, "indices": resp.get("indices", [])}

        return self._cached(f"fcaps:{index}", 120, load)

    def _require_field(self, field: str, fc: dict) -> dict:
        fields = fc["fields"]
        if field in fields:
            return fields[field]
        close = difflib.get_close_matches(field, list(fields), n=5, cutoff=0.5)
        hint = f" Did you mean: {', '.join(close)}?" if close else " Use describe_index to see available fields."
        raise ValueError(f"Field '{field}' does not exist in this index.{hint}")

    def _exact(self, field: str, fc: dict) -> str:
        """Aggregatable/term-queryable variant of a field (text -> its .keyword subfield)."""
        info = self._require_field(field, fc)
        if info["aggregatable"] or info["type"] not in TEXT_TYPES:
            return field
        for cand in (f"{field}.keyword", f"{field}.raw"):
            if fc["fields"].get(cand, {}).get("aggregatable"):
                return cand
        for name, sub in fc["fields"].items():
            if name.startswith(field + ".") and sub["aggregatable"]:
                return name
        raise ValueError(f"'{field}' is analyzed text with no keyword subfield; it cannot be aggregated or exact-matched.")

    def _pick_time_field(self, fc: dict, time_field: str = "") -> str:
        if time_field:
            self._require_field(time_field, fc)
            return time_field
        dates = [n for n, i in fc["fields"].items() if i["type"] in DATE_TYPES]
        if not dates:
            raise ValueError("This index has no date fields, so a time range cannot be applied.")
        return "@timestamp" if "@timestamp" in dates else sorted(dates, key=lambda n: (n.count("."), n))[0]

    def _version(self) -> dict:
        return self._cached("version", 600, lambda: self._request("GET", "/").get("version", {}))

    # -- query building -------------------------------------------------------
    def _build_query(self, index: str, query: str = "", filters: str = "", time_field: str = "",
                     start: str = "", end: str = "", extra: Optional[list] = None) -> dict:
        must, flt, must_not = [], list(extra or []), []
        if query and query.strip() not in ("*", "*:*"):
            must.append({"query_string": {"query": query, "default_operator": "AND", "lenient": True}})
        parsed = _parse_json_arg(filters, "filters")
        if parsed:
            if not isinstance(parsed, dict):
                raise ValueError('filters must be a JSON object, e.g. {"country": "FR", "magnitude": {"gte": 5}}')
            fc = self._fcaps(index)
            for field, val in parsed.items():
                info = self._require_field(field, fc)
                if val is None:
                    must_not.append({"exists": {"field": field}})
                elif isinstance(val, dict) and set(val) <= {"exists"}:
                    (flt if val.get("exists", True) else must_not).append({"exists": {"field": field}})
                elif isinstance(val, dict):
                    flt.append({"range": {field: val}})
                elif info["type"] in TEXT_TYPES:
                    try:
                        exact = self._exact(field, fc)
                        flt.append({"terms" if isinstance(val, list) else "term": {exact: val}})
                    except ValueError:
                        vals = val if isinstance(val, list) else [val]
                        flt.append({"bool": {"should": [{"match_phrase": {field: v}} for v in vals], "minimum_should_match": 1}})
                else:
                    flt.append({"terms" if isinstance(val, list) else "term": {field: val}})
        if start or end:
            tf = self._pick_time_field(self._fcaps(index), time_field)
            rng = {}
            if start:
                rng["gte"] = start
            if end:
                rng["lte"] = end
            flt.append({"range": {tf: rng}})
        if not (must or flt or must_not):
            return {"match_all": {}}
        return {"bool": {"must": must, "filter": flt, "must_not": must_not}}

    def _safe_aggs(self, index: str, aggs: dict, query: Optional[dict] = None, runtime: Optional[dict] = None):
        """Run aggregations together; if that fails, run them one by one so one bad agg can't sink the rest."""
        base = {"size": 0, "track_total_hits": True}
        if query:
            base["query"] = query
        if runtime:
            base["runtime_mappings"] = runtime
        try:
            r = self._search(index, {**base, "aggs": aggs})
            return _total(r), r.get("aggregations", {}), {}
        except EsError as e:
            if len(aggs) == 1:
                return None, {}, {next(iter(aggs)): str(e)}
        total, results, errors = None, {}, {}
        for name, agg in aggs.items():
            try:
                r = self._search(index, {**base, "aggs": {name: agg}})
                total, results[name] = _total(r), r["aggregations"][name]
            except EsError as e:
                errors[name] = str(e)
        return total, results, errors

    def _sample(self, index: str, n: int = 20) -> list:
        r = self._search(index, {"size": n, "query": {"match_all": {}}})
        return r.get("hits", {}).get("hits", [])

    def _fmt_hit(self, h: dict, multi: bool) -> dict:
        d = {"_id": h.get("_id")}
        if multi:
            d["_index"] = h.get("_index")
        if h.get("_score") is not None:
            d["_score"] = round(h["_score"], 3)
        d.update(_trim(h.get("_source") or {}))
        return d

    # -- geo detection / resolution ------------------------------------------
    def _geo_candidates(self, index: str) -> dict:
        fc = self._fcaps(index)
        fields = fc["fields"]
        native = [n for n, i in fields.items() if i["type"] in GEO_TYPES]
        cartesian = [n for n, i in fields.items() if i["type"] in CARTESIAN_TYPES]
        lats, lons, proj = {}, {}, []
        for n, i in fields.items():
            if i["type"] in OBJECT_TYPES or i["type"] in GEO_TYPES:
                continue
            norm = _norm_name(n)
            if norm.endswith(".keyword") or norm.endswith(".raw"):
                continue
            if LAT_TOKEN.search(norm):
                lats[LAT_TOKEN.sub("{}", norm, count=1)] = n
            elif LON_TOKEN.search(norm):
                lons[LON_TOKEN.sub("{}", norm, count=1)] = n
            elif PROJECTED_TOKEN.search(norm) and i["type"] in NUMERIC_TYPES:
                proj.append(n)
        pairs = [(lats[s], lons[s]) for s in lats if s in lons]
        geojson = []
        for n, i in fields.items():
            if n.endswith(".coordinates") and i["type"] in NUMERIC_TYPES:
                prefix = n[: -len(".coordinates")]
                if prefix + ".type" in fields and fields.get(prefix, {}).get("type") not in GEO_TYPES:
                    geojson.append(prefix)
        places = [
            n for n, i in fields.items()
            if PLACE_TOKEN.search(_norm_name(n)) and (i["type"] in TEXT_TYPES or i["type"] in {"keyword", "constant_keyword"})
            and not n.endswith(".keyword")
        ]
        return {"native": native, "cartesian": cartesian, "pairs": pairs, "geojson": geojson, "projected": proj,
                "places": places}

    def _pair_ref(self, index: str, lat: str, lon: str, fc: dict) -> GeoRef:
        la, lo = self._require_field(lat, fc), self._require_field(lon, fc)
        spec = f"{lat},{lon}"
        if la["aggregatable"] and lo["aggregatable"]:
            params = {"mode": "pair_docvalues", "lat": lat, "lon": lon}
        else:
            params = {"mode": "pair_source", "lat": lat, "lon": lon, "lat_path": lat.split("."), "lon_path": lon.split(".")}
        return GeoRef(spec, RUNTIME_GEO_FIELD, "point", _runtime_geo(params))

    def _resolve_geo(self, index: str, geo_field: str = "") -> GeoRef:
        fc = self._fcaps(index)
        spec = (geo_field or "").strip()
        if not spec:
            best = self._cached(f"geo_best:{index}", 300, lambda: self._auto_geo_spec(index))
            if not best:
                raise ValueError(
                    "No usable geo data found automatically. Run detect_geo_fields to inspect the data, then pass "
                    "geo_field explicitly (a geo field name, 'lat_field,lon_field', 'latlon_string:field', or 'geojson_point:field')."
                )
            spec = best
        if spec.startswith("latlon_string:") or spec.startswith("geojson_point:"):
            mode, field = spec.split(":", 1)
            params = {"mode": mode, "field": field, "path": field.split(".")}
            return GeoRef(spec, RUNTIME_GEO_FIELD, "point", _runtime_geo(params))
        if "," in spec:
            lat, lon = [s.strip() for s in spec.split(",", 1)]
            return self._pair_ref(index, lat, lon, fc)
        info = self._require_field(spec, fc)
        if info["type"] == "geo_point":
            return GeoRef(spec, spec, "point")
        if info["type"] == "geo_shape":
            return GeoRef(spec, spec, "shape")
        if info["type"] in CARTESIAN_TYPES:
            raise ValueError(f"'{spec}' is a cartesian {info['type']} field (projected x/y), not lat/lon; geo tools cannot use it.")
        raise ValueError(
            f"'{spec}' is a {info['type']} field, not a geo field. For separate latitude/longitude fields pass "
            "'lat_field,lon_field'. Run detect_geo_fields for options."
        )

    def _auto_geo_spec(self, index: str) -> str:
        c = self._geo_candidates(index)
        fc = self._fcaps(index)
        points = [n for n in c["native"] if fc["fields"][n]["type"] == "geo_point"]
        if points:
            return points[0]
        if c["native"]:
            return c["native"][0]
        if c["pairs"]:
            return f"{c['pairs'][0][0]},{c['pairs'][0][1]}"
        if c["geojson"]:
            return f"geojson_point:{c['geojson'][0]}"
        for h in self._sample(index, 10):
            for n, i in fc["fields"].items():
                if i["type"] in TEXT_TYPES | {"keyword"} and not n.endswith(".keyword"):
                    v = _get_path(h.get("_source", {}), n)
                    if isinstance(v, str) and LATLON_STR_RE.match(v):
                        return f"latlon_string:{n}"
        return ""

    def _geo_summary(self, index: str, geo: GeoRef, query: Optional[dict] = None) -> dict:
        aggs = {
            "with_geo": {"filter": {"exists": {"field": geo.field}}},
            "bounds": {"geo_bounds": {"field": geo.field}},
            "centroid": {"geo_centroid": {"field": geo.field}},
        }
        total, res, errors = self._safe_aggs(index, aggs, query, geo.runtime)
        out: dict = {**geo.describe(), "total_docs": total}
        if "with_geo" in res:
            n = res["with_geo"]["doc_count"]
            out["docs_with_geo"] = n
            if total:
                out["geo_coverage_pct"] = round(100 * n / total, 2)
        if res.get("bounds", {}).get("bounds"):
            out["bounds"] = res["bounds"]["bounds"]
        if res.get("centroid", {}).get("location"):
            out["centroid"] = res["centroid"]["location"]
        if errors:
            out["agg_errors"] = errors
        return out

    def _hit_geometry(self, hit: dict, geo: GeoRef) -> Optional[dict]:
        vals = (hit.get("fields") or {}).get(geo.field)
        if vals:
            v = vals[0]
            if isinstance(v, dict):
                return v
            if isinstance(v, str):
                m = LATLON_STR_RE.match(v)
                if m:
                    return {"type": "Point", "coordinates": [float(m.group(2)), float(m.group(1))]}
        return None

    def _geo_hits(self, index: str, geo: GeoRef, q: dict, size: int, fields: str = "", sort: Optional[list] = None):
        size = max(1, min(size, self.valves.MAX_MAP_POINTS))
        body: dict = {"size": size, "query": q, "fields": [geo.field], "track_total_hits": True}
        if fields:
            body["_source"] = [f.strip() for f in fields.split(",") if f.strip()]
        if geo.runtime:
            body["runtime_mappings"] = geo.runtime
        note = ""
        if sort:
            try:
                return self._search(index, {**body, "sort": sort}), ""
            except EsError as e:
                note = f"Server-side distance sort failed ({str(e)[:200]}); results were sorted client-side and may not be the global nearest."
        return self._search(index, body), note

    def _emit_map(self, title: str, **kw) -> Optional[str]:
        if not self.valves.ENABLE_MAPS:
            return None
        return _map_html(title, self.valves.MAP_TILE_URL, self.valves.MAP_ATTRIBUTION, **kw)

    def _hits_for_map(self, hits: list, geo: GeoRef) -> tuple:
        points, shapes = [], []
        for h in hits:
            g = self._hit_geometry(h, geo)
            if not g:
                continue
            label = _hit_label(h)
            if g.get("type") == "Point":
                points.append([g["coordinates"][1], g["coordinates"][0], label])
            elif len(shapes) < 300:
                shapes.append([g, label])
        return points, shapes

    # -- plumbing between async tool methods and sync implementations --------
    async def _run(self, emitter: Optional[Callable], label: str, fn: Callable, *args) -> str:
        async def status(desc: str, done: bool):
            if emitter:
                await emitter({"type": "status", "data": {"description": desc, "done": done, "hidden": False}})

        await status(label, False)
        try:
            result = await asyncio.to_thread(fn, *args)
        except Exception as e:
            await status(f"{label}: failed", True)
            return _err(e)
        html = None
        if isinstance(result, tuple):
            result, html = result
        if html and emitter:
            await emitter({"type": "embeds", "data": {"embeds": [html]}})
            if isinstance(result, dict):
                result["map"] = "An interactive map of these results is already displayed to the user."
        await status(label, True)
        return self._out(result)

    # =======================================================================
    # Discovery
    # =======================================================================
    async def cluster_overview(self, __event_emitter__: Optional[Callable] = None) -> str:
        """Start here. Summarize the Elasticsearch cluster: version, health, and the indices, data streams and
        aliases the agent can see (largest first). Use this to find which index holds the data the user means.
        """
        return await self._run(__event_emitter__, "Reading cluster overview", self._cluster_overview)

    def _cluster_overview(self) -> dict:
        out: dict = {}
        root = self._request("GET", "/")
        out["cluster_name"] = root.get("cluster_name")
        out["version"] = root.get("version", {}).get("number")
        out["distribution"] = root.get("version", {}).get("distribution", "elasticsearch")
        try:
            h = self._request("GET", "_cluster/health")
            out["health"] = {k: h.get(k) for k in ("status", "number_of_nodes", "active_shards", "unassigned_shards")}
        except EsError as e:
            out["health"] = f"unavailable ({str(e)[:120]})"
        rows = self._cat_indices("*")
        out["index_count"] = len(rows)
        out["total_docs"] = sum(r["docs"] for r in rows)
        out["largest_indices"] = sorted(rows, key=lambda r: -r["docs"])[:20]
        try:
            ds = self._request("GET", "_data_stream").get("data_streams", [])
            out["data_streams"] = [d["name"] for d in ds if self._allowed(d["name"])][: self.valves.MAX_RESULTS]
        except EsError:
            pass
        try:
            aliases = self._request("GET", "_cat/aliases", params={"format": "json"})
            out["aliases"] = sorted({a["alias"] for a in aliases if self._allowed(a["alias"])})[: self.valves.MAX_RESULTS]
        except EsError:
            pass
        return out

    def _cat_indices(self, pattern: str) -> list:
        raw = self._request(
            "GET", f"_cat/indices/{quote(pattern, safe=',*:-_.+')}",
            params={"format": "json", "bytes": "b", "h": "index,health,status,docs.count,store.size", "s": "index"},
        )
        rows = []
        for r in raw:
            if not self._allowed(r["index"]):
                continue
            rows.append({
                "index": r["index"], "docs": int(r.get("docs.count") or 0), "health": r.get("health"),
                "size_mb": round(int(r.get("store.size") or 0) / 1048576, 1), "status": r.get("status"),
            })
        return rows

    async def list_indices(self, pattern: str = "*", __event_emitter__: Optional[Callable] = None) -> str:
        """List indices matching a pattern with document counts and size. Backing indices of data streams
        (.ds-*) and system indices are hidden unless the administrator allowed them.

        :param pattern: index name or wildcard pattern, e.g. 'geo*' or 'logs-*,places'. Defaults to all.
        """
        return await self._run(__event_emitter__, f"Listing indices '{pattern}'", self._list_indices, pattern)

    def _list_indices(self, pattern: str) -> dict:
        rows = self._cat_indices(pattern or "*")
        cap = self.valves.MAX_RESULTS * 4
        return {"count": len(rows), "indices": rows[:cap], **({"truncated": True} if len(rows) > cap else {})}

    async def describe_index(self, index: str = "", __event_emitter__: Optional[Callable] = None) -> str:
        """Describe an index's schema: every field grouped by type, date fields, likely geo fields, and two
        sample documents. ALWAYS call this before querying an index you have not described yet, and use the exact
        field names it returns.

        :param index: index name, pattern (e.g. 'geo-*'), alias, or data stream.
        """
        return await self._run(__event_emitter__, "Reading index schema", self._describe_index, index)

    def _describe_index(self, index: str) -> dict:
        index = self._index(index)
        fc = self._fcaps(index)
        by_type: dict = {}
        conflicts = {}
        for n, i in sorted(fc["fields"].items()):
            if i["type"] in OBJECT_TYPES and i["type"] != "nested":
                continue
            by_type.setdefault(i["type"], []).append(n)
            if "conflict" in i:
                conflicts[n] = i["conflict"]
        n_fields = sum(len(v) for v in by_type.values())
        if n_fields > 400:
            by_type = {t: (v if len(v) <= 40 else v[:40] + [f"... +{len(v) - 40} more"]) for t, v in by_type.items()}
        r = self._search(index, {"size": 2, "track_total_hits": True, "query": {"match_all": {}}})
        cand = self._geo_candidates(index)
        geo_hint = {
            "geo_fields": cand["native"],
            "lat_lon_field_pairs": [f"{a},{b}" for a, b in cand["pairs"]],
            "geojson_objects_not_geo_mapped": cand["geojson"],
            "place_name_fields": cand["places"][:15],
        }
        return {
            "index": index,
            "matched_indices": len(fc["indices"]),
            "doc_count": _total(r),
            "field_count": n_fields,
            "fields_by_type": by_type,
            "type_conflicts_across_indices": conflicts or None,
            "date_fields": [n for n, i in fc["fields"].items() if i["type"] in DATE_TYPES],
            "geo_hints": {k: v for k, v in geo_hint.items() if v} or "none found by name/type; run detect_geo_fields",
            "sample_documents": [self._fmt_hit(h, len(fc["indices"]) > 1) for h in r["hits"]["hits"]],
        }

    async def detect_geo_fields(self, index: str = "", __event_emitter__: Optional[Callable] = None) -> str:
        """Figure out HOW geographic information is stored in an index, even when the format is unknown:
        geo_point/geo_shape fields, separate latitude/longitude fields (checks value ranges, swapped lat/lon,
        projected or scaled coordinates, 0,0 placeholders), 'lat,lon' strings, WKT, GeoJSON objects, and place-name
        fields (country, city...). Returns a recommended `geo_field` value for the other geo tools.

        :param index: index name, pattern, alias, or data stream.
        """
        return await self._run(__event_emitter__, "Detecting geo data formats", self._detect_geo_fields, index)

    def _detect_geo_fields(self, index: str) -> dict:
        index = self._index(index)
        fc = self._fcaps(index)
        fields = fc["fields"]
        cand = self._geo_candidates(index)
        samples = [h.get("_source", {}) for h in self._sample(index, 25)]
        out: dict = {"index": index}
        findings = []

        native = []
        for n in cand["native"]:
            geo = GeoRef(n, n, "point" if fields[n]["type"] == "geo_point" else "shape")
            info = {"field": n, "type": fields[n]["type"], **self._geo_summary(index, geo)}
            info.pop("geo_field", None)
            info["source_format"] = self._describe_source_format([_get_path(s, n) for s in samples])
            native.append(info)
            findings.append(f"'{n}' is a native {fields[n]['type']} field ({info['source_format']}).")
        out["native_geo_fields"] = native

        pairs = []
        for lat, lon in cand["pairs"]:
            pairs.append(self._check_pair(index, lat, lon, fc, samples))
            findings.append(f"'{lat}' + '{lon}' look like latitude/longitude: {pairs[-1]['status']}.")
        out["lat_lon_pairs"] = pairs

        strings = []
        for n, i in fields.items():
            if not (i["type"] in TEXT_TYPES or i["type"] == "keyword") or n.endswith(".keyword") or n in cand["places"]:
                continue
            vals = [v for v in (_get_path(s, n) for s in samples) if isinstance(v, str)]
            if not vals:
                continue
            fmt = None
            if sum(bool(LATLON_STR_RE.match(v)) for v in vals) >= max(1, len(vals) // 2):
                fmt, spec = "'lat,lon' string", f"latlon_string:{n}"
            elif sum(bool(WKT_RE.match(v)) for v in vals) >= max(1, len(vals) // 2):
                fmt, spec = "WKT string", None
            elif sum(v.lstrip().startswith('{"type"') for v in vals) >= max(1, len(vals) // 2):
                fmt, spec = "GeoJSON string", None
            if fmt:
                entry = {"field": n, "format": fmt, "example": vals[0][:120]}
                if spec:
                    entry["geo_field_spec"] = spec
                else:
                    entry["note"] = "Not queryable as geo in place; reindex with a geo_shape/geo_point mapping (ES accepts WKT/GeoJSON directly)."
                strings.append(entry)
                findings.append(f"'{n}' stores coordinates as a {fmt}.")
        out["string_coordinates"] = strings

        geojson = []
        for prefix in cand["geojson"]:
            types = sorted({str((_get_path(s, prefix) or {}).get("type")) for s in samples if isinstance(_get_path(s, prefix), dict)})
            entry = {"field": prefix, "geometry_types_in_sample": types,
                     "note": "GeoJSON stored as a plain object (not geo-indexed)."}
            if types == ["Point"]:
                entry["geo_field_spec"] = f"geojson_point:{prefix}"
            geojson.append(entry)
            findings.append(f"'{prefix}' holds GeoJSON ({', '.join(types)}) mapped as a plain object.")
        out["geojson_objects"] = geojson

        if cand["projected"]:
            out["possible_projected_coordinates"] = cand["projected"]
            findings.append(f"Numeric x/y/easting/northing fields found ({', '.join(cand['projected'][:4])}); these may be a projected CRS, not lat/lon.")
        if cand["cartesian"]:
            out["cartesian_fields"] = cand["cartesian"]

        places = []
        for n in cand["places"][:8]:
            try:
                exact = self._exact(n, fc)
            except ValueError:
                continue
            _, res, _ = self._safe_aggs(index, {"t": {"terms": {"field": exact, "size": 5}}, "c": {"cardinality": {"field": exact}}})
            if res.get("t"):
                places.append({"field": exact, "distinct_values": res.get("c", {}).get("value"),
                               "top_values": [[b["key"], b["doc_count"]] for b in res["t"]["buckets"]]})
        out["place_name_fields"] = places
        if places:
            findings.append("Place-name fields: " + ", ".join(p["field"] for p in places) + " (use aggregate to group by them).")

        best = self._auto_geo_spec(index)
        good_pairs = [p for p in pairs if p["status"].startswith("valid")]
        swapped = [p for p in pairs if p["status"].startswith("likely swapped")]
        if not native and not good_pairs and swapped:
            best = swapped[0]["geo_field_spec"]
        out["recommended_geo_field"] = best or None
        if not findings:
            findings.append("No coordinate data detected. The data may only reference places by name/code, or use unusual field names; inspect sample_documents from describe_index.")
        out["summary"] = findings
        self._cache[f"geo_best:{index}"] = (time.time(), best)
        return out

    @staticmethod
    def _describe_source_format(values: list) -> str:
        for v in values:
            if v is None:
                continue
            if isinstance(v, list) and v and isinstance(v[0], (dict, list, str)):
                v = v[0]
            if isinstance(v, dict) and "lat" in v:
                return "stored as {lat, lon} objects"
            if isinstance(v, dict) and "coordinates" in v:
                return f"stored as GeoJSON ({v.get('type')})"
            if isinstance(v, list):
                return "stored as [lon, lat] arrays"
            if isinstance(v, str):
                if LATLON_STR_RE.match(v):
                    return "stored as 'lat,lon' strings"
                if WKT_RE.match(v):
                    return "stored as WKT strings"
                return "stored as geohash strings"
        return "no values in sampled documents"

    def _check_pair(self, index: str, lat: str, lon: str, fc: dict, samples: list) -> dict:
        la, lo = fc["fields"][lat], fc["fields"][lon]
        entry: dict = {"lat_field": lat, "lon_field": lon, "types": [la["type"], lo["type"]]}
        rng = {}
        if la["type"] in NUMERIC_TYPES and lo["type"] in NUMERIC_TYPES:
            aggs = {
                "lat": {"stats": {"field": lat}}, "lon": {"stats": {"field": lon}},
                "zero": {"filter": {"bool": {"filter": [{"term": {lat: 0}}, {"term": {lon: 0}}]}}},
            }
            _, res, _ = self._safe_aggs(index, aggs)
            for k in ("lat", "lon"):
                if res.get(k, {}).get("count"):
                    rng[k] = (res[k]["min"], res[k]["max"])
            entry["docs_with_both"] = min(res.get("lat", {}).get("count", 0), res.get("lon", {}).get("count", 0))
            if res.get("zero", {}).get("doc_count"):
                entry["zero_zero_docs"] = res["zero"]["doc_count"]
        else:
            for k, f in (("lat", lat), ("lon", lon)):
                nums = []
                for s in samples:
                    try:
                        nums.append(float(_get_path(s, f)))
                    except (TypeError, ValueError):
                        pass
                if nums:
                    rng[k] = (min(nums), max(nums))
            entry["note"] = "Values are not numeric-mapped; coordinates are parsed at query time (slower)."
        entry["lat_range"], entry["lon_range"] = rng.get("lat"), rng.get("lon")
        entry["geo_field_spec"] = f"{lat},{lon}"
        if "lat" not in rng or "lon" not in rng:
            entry["status"] = "no values found"
            return entry
        lat_abs = max(abs(rng["lat"][0]), abs(rng["lat"][1]))
        lon_abs = max(abs(rng["lon"][0]), abs(rng["lon"][1]))
        if lat_abs <= 90 and lon_abs <= 180:
            entry["status"] = "valid WGS84 degrees"
        elif lat_abs <= 180 and lon_abs <= 90:
            entry["status"] = "likely swapped (latitude values exceed 90 but longitude values fit latitude range)"
            entry["geo_field_spec"] = f"{lon},{lat}"
        elif lat_abs > 1e5 and lat_abs <= 9e8:
            entry["status"] = "out of range: looks like scaled integers (e.g. degrees x 1e6/1e7) or a projected CRS in metres"
        else:
            entry["status"] = "out of range: probably a projected coordinate system (not lat/lon degrees)"
        if entry.get("zero_zero_docs"):
            entry["status"] += f"; {entry['zero_zero_docs']} docs sit at 0,0 (likely placeholders for missing locations)"
        return entry

    async def profile_index(self, index: str = "", query: str = "", __event_emitter__: Optional[Callable] = None) -> str:
        """Automatic insight report for an index (or a subset of it): document count, time span, top values of the
        most useful categorical fields, statistics of numeric fields, data completeness, and geographic extent/
        centroid/coverage with a density map. Use this when the user asks for insights, a summary, or 'what is in here'.

        :param index: index name, pattern, alias, or data stream.
        :param query: optional Lucene query_string to profile only matching documents, e.g. 'country:FR AND status:active'.
        """
        return await self._run(__event_emitter__, "Profiling index", self._profile_index, index, query)

    def _profile_index(self, index: str, query: str):
        index = self._index(index)
        fc = self._fcaps(index)
        fields = fc["fields"]
        q = self._build_query(index, query)
        out: dict = {"index": index, "query": query or None}

        dates = [n for n, i in fields.items() if i["type"] in DATE_TYPES and i["aggregatable"]]
        dates.sort(key=lambda n: (n != "@timestamp", n.count("."), n))
        keywords = [n for n, i in fields.items() if i["type"] in {"keyword", "ip", "boolean", "constant_keyword"} and i["aggregatable"]]
        numerics = [n for n, i in fields.items() if i["type"] in NUMERIC_TYPES and i["aggregatable"]]
        cand = self._geo_candidates(index)
        pair_fields = {f for p in cand["pairs"] for f in p}
        numerics = [n for n in numerics if n not in pair_fields][:10]

        aggs: dict = {}
        for i, d in enumerate(dates[:3]):
            aggs[f"d{i}_min"] = {"min": {"field": d}}
            aggs[f"d{i}_max"] = {"max": {"field": d}}
        for i, k in enumerate(keywords[:40]):
            aggs[f"k{i}"] = {"cardinality": {"field": k}}
        for i, n in enumerate(numerics):
            aggs[f"n{i}"] = {"stats": {"field": n}}
        total, res, errors = self._safe_aggs(index, aggs, q)
        out["doc_count"] = total
        if not total:
            out["note"] = "No documents match."
            return out

        out["time_span"] = [
            {"field": d, "min": res.get(f"d{i}_min", {}).get("value_as_string"), "max": res.get(f"d{i}_max", {}).get("value_as_string")}
            for i, d in enumerate(dates[:3])
        ] or None
        out["numeric_fields"] = [
            {"field": n, "coverage_pct": round(100 * res[f"n{i}"]["count"] / total, 1),
             **{k: _round(res[f"n{i}"][k]) for k in ("min", "max", "avg")}}
            for i, n in enumerate(numerics) if f"n{i}" in res and res[f"n{i}"]["count"]
        ]

        cards = [(k, res.get(f"k{i}", {}).get("value", 0)) for i, k in enumerate(keywords[:40])]
        cats = sorted([c for c in cards if 2 <= c[1] <= 1000], key=lambda c: (c[0] not in cand["places"] and not any(c[0].startswith(p + ".") for p in cand["places"]), c[1]))[:8]
        if cats:
            _, tres, _ = self._safe_aggs(index, {f"t{i}": {"terms": {"field": k, "size": 6}} for i, (k, _) in enumerate(cats)}, q)
            out["categorical_fields"] = []
            for i, (k, card) in enumerate(cats):
                if f"t{i}" not in tres:
                    continue
                b = tres[f"t{i}"]["buckets"]
                out["categorical_fields"].append({
                    "field": k, "distinct_values": card,
                    "top_values": [[x.get("key_as_string", x["key"]), x["doc_count"], f"{100 * x['doc_count'] / total:.1f}%"] for x in b],
                })
        ids = [k for k, c in cards if c > 1000 and c >= 0.9 * total]
        if ids:
            out["identifier_like_fields"] = ids[:10]
        if errors:
            out["agg_errors"] = errors

        html = None
        try:
            geo = self._resolve_geo(index)
        except ValueError:
            geo = None
        if geo:
            summary = self._geo_summary(index, geo, q)
            out["geo"] = summary
            zoom = _auto_geotile_zoom(summary.get("bounds"))
            cells, _ = self._grid(index, geo, q, "geotile", zoom)
            if cells:
                out["geo"]["densest_areas"] = [
                    {"lat": c["lat"], "lon": c["lon"], "docs": c["count"], "share_pct": round(100 * c["count"] / total, 1)}
                    for c in cells[:5]
                ]
                html = self._emit_map(f"{index}: document density ({total:,} docs)", cells=[[c["lat"], c["lon"], c["count"], None] for c in cells])
        elif cand["places"]:
            out["geo"] = "No coordinates; geography is by place name. See categorical_fields and aggregate by: " + ", ".join(cand["places"][:5])
        return (out, html) if html else out

    async def field_stats(self, index: str, field: str, query: str = "", __event_emitter__: Optional[Callable] = None) -> str:
        """Deep statistics for one field, chosen by its type: numbers (min/max/avg/percentiles/missing), dates
        (range and histogram over time), keywords (distinct count, top 20 values), geo (extent, centroid, coverage).

        :param index: index name, pattern, alias, or data stream.
        :param field: exact field name from describe_index.
        :param query: optional Lucene query_string to restrict the documents.
        """
        return await self._run(__event_emitter__, f"Analyzing field '{field}'", self._field_stats, index, field, query)

    def _field_stats(self, index: str, field: str, query: str) -> dict:
        index = self._index(index)
        fc = self._fcaps(index)
        info = self._require_field(field, fc)
        t = info["type"]
        q = self._build_query(index, query)
        if t in GEO_TYPES:
            return self._geo_summary(index, GeoRef(field, field, "point" if t == "geo_point" else "shape"), q)
        aggs: dict = {"missing": {"missing": {"field": field}}}
        if t in NUMERIC_TYPES:
            aggs["stats"] = {"extended_stats": {"field": field}}
            aggs["pct"] = {"percentiles": {"field": field, "percents": [1, 5, 25, 50, 75, 95, 99]}}
        elif t in DATE_TYPES:
            aggs["min"] = {"min": {"field": field}}
            aggs["max"] = {"max": {"field": field}}
            aggs["hist"] = {"auto_date_histogram": {"field": field, "buckets": 24}}
        else:
            exact = self._exact(field, fc)
            aggs = {"missing": {"missing": {"field": exact}}, "card": {"cardinality": {"field": exact}},
                    "top": {"terms": {"field": exact, "size": 20}}}
            field = exact
        total, res, errors = self._safe_aggs(index, aggs, q)
        out: dict = {"field": field, "type": t, "docs": total, "missing": res.get("missing", {}).get("doc_count")}
        if "stats" in res:
            s = res["stats"]
            out.update({k: _round(s.get(k)) for k in ("count", "min", "max", "avg", "sum", "std_deviation")})
            out["percentiles"] = {k: _round(v) for k, v in res.get("pct", {}).get("values", {}).items()}
        if "min" in res:
            out["earliest"], out["latest"] = res["min"].get("value_as_string"), res["max"].get("value_as_string")
            out["interval"] = res.get("hist", {}).get("interval")
            out["histogram"] = [[b["key_as_string"], b["doc_count"]] for b in res.get("hist", {}).get("buckets", [])]
        if "top" in res:
            out["distinct_values"] = res.get("card", {}).get("value")
            out["top_values"] = [[b.get("key_as_string", b["key"]), b["doc_count"]] for b in res["top"]["buckets"]]
            out["other_docs"] = res["top"].get("sum_other_doc_count")
        if errors:
            out["agg_errors"] = errors
        return out

    # =======================================================================
    # Querying
    # =======================================================================
    async def search(
        self, index: str = "", query: str = "", filters: str = "", fields: str = "", size: int = 10,
        sort: str = "", time_field: str = "", start: str = "", end: str = "",
        __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Find documents. Returns the total match count and the top documents.

        :param index: index name, pattern, alias, or data stream.
        :param query: Lucene query_string, e.g. 'type:earthquake AND magnitude:>5' or '"San Francisco"'. Empty = all documents.
        :param filters: JSON object of exact filters: {"field": "value"}, {"field": ["a","b"]} (any of), {"field": {"gte": 1, "lt": 10}} (range), {"field": null} (missing).
        :param fields: comma-separated fields to return (default: all fields, trimmed).
        :param size: number of documents to return (max limited by the administrator).
        :param sort: e.g. 'magnitude:desc' or '@timestamp:desc,name:asc'.
        :param time_field: date field for start/end (auto-detected if omitted).
        :param start: range start, ISO date or date math like 'now-7d'.
        :param end: range end, ISO date or date math like 'now'.
        """
        return await self._run(__event_emitter__, "Searching", self._search_docs, index, query, filters, fields, size, sort, time_field, start, end)

    def _search_docs(self, index, query, filters, fields, size, sort, time_field, start, end) -> dict:
        index = self._index(index)
        fc = self._fcaps(index)
        body: dict = {
            "size": max(0, min(int(size), self.valves.MAX_RESULTS)), "track_total_hits": True,
            "query": self._build_query(index, query, filters, time_field, start, end),
        }
        if fields:
            body["_source"] = [f.strip() for f in fields.split(",") if f.strip()]
        if sort:
            body["sort"] = []
            for part in sort.split(","):
                name, _, order = part.strip().partition(":")
                name = self._exact(name, fc) if name != "_score" else name
                body["sort"].append({name: {"order": (order or "desc").lower()}})
        r = self._search(index, body)
        multi = len(fc["indices"]) > 1
        return {"total": _total(r), "returned": len(r["hits"]["hits"]), "hits": [self._fmt_hit(h, multi) for h in r["hits"]["hits"]]}

    async def get_document(self, index: str, doc_id: str, __event_emitter__: Optional[Callable] = None) -> str:
        """Fetch one full document by its _id.

        :param index: concrete index or alias holding the document (use _index from search results).
        :param doc_id: the document _id.
        """
        return await self._run(__event_emitter__, f"Fetching document {doc_id}", self._get_document, index, doc_id)

    def _get_document(self, index: str, doc_id: str) -> dict:
        index = self._index(index)
        r = self._request("GET", f"{quote(index, safe=',*:-_.+')}/_doc/{quote(doc_id, safe='')}")
        return {"_index": r.get("_index"), "_id": r.get("_id"), "_source": _trim(r.get("_source"), max_list=50, max_str=2000)}

    async def aggregate(
        self, index: str = "", group_by: str = "", metric: str = "count", metric_field: str = "", query: str = "",
        filters: str = "", size: int = 10, interval: str = "", order_by_metric: bool = False,
        include_geo_centroid: bool = False, time_field: str = "", start: str = "", end: str = "",
        __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Group and summarize documents - the main tool for analytical questions ('how many X per country',
        'average magnitude by month', 'top 10 cities by total population'). Returns one row per group.

        :param index: index name, pattern, alias, or data stream.
        :param group_by: comma-separated fields to group by, nested left to right (max 3), e.g. 'country' or 'country,category'. Date fields make a time series; numeric fields with `interval` make a histogram. Empty = overall metric only.
        :param metric: count, sum, avg, min, max, cardinality (distinct count), median, p90, p95, or p99.
        :param metric_field: numeric (or any, for cardinality) field the metric is computed on; not needed for count.
        :param query: optional Lucene query_string to restrict the documents.
        :param filters: optional JSON exact filters, same format as the search tool.
        :param size: number of groups per level (max limited by the administrator).
        :param interval: for date groups: minute/hour/day/week/month/quarter/year or a fixed span like '15m', '6h'; for numeric groups: bucket width like '10'. Empty date interval = automatic.
        :param order_by_metric: true to rank groups by the metric (highest first) instead of by document count.
        :param include_geo_centroid: true to add the geographic centre of each group (and draw them on a map).
        :param time_field: date field for start/end (auto-detected if omitted).
        :param start: range start, ISO date or date math like 'now-30d'.
        :param end: range end.
        """
        return await self._run(
            __event_emitter__, "Aggregating", self._aggregate, index, group_by, metric, metric_field, query, filters,
            size, interval, order_by_metric, include_geo_centroid, time_field, start, end,
        )

    def _group_agg(self, field: str, fc: dict, size: int, interval: str) -> dict:
        info = self._require_field(field, fc)
        t = info["type"]
        if t in GEO_TYPES:
            raise ValueError(f"'{field}' is a geo field; use geo_heatmap to group by location.")
        if t in DATE_TYPES:
            if interval:
                key = "calendar_interval" if interval in CALENDAR_INTERVALS else "fixed_interval"
                return {"date_histogram": {"field": field, key: interval, "min_doc_count": 1}}
            return {"auto_date_histogram": {"field": field, "buckets": max(2, min(size, 100))}}
        if t in NUMERIC_TYPES and interval:
            return {"histogram": {"field": field, "interval": float(interval), "min_doc_count": 1}}
        return {"terms": {"field": self._exact(field, fc), "size": size}}

    def _aggregate(self, index, group_by, metric, metric_field, query, filters, size, interval,
                   order_by_metric, include_geo_centroid, time_field, start, end):
        index = self._index(index)
        fc = self._fcaps(index)
        size = max(1, min(int(size), self.valves.MAX_RESULTS))
        groups = [g.strip() for g in (group_by or "").split(",") if g.strip()][:3]
        metric = (metric or "count").lower().strip()
        leaf: dict = {}
        order_path = None
        if metric != "count":
            if not metric_field:
                raise ValueError(f"metric '{metric}' needs metric_field.")
            self._require_field(metric_field, fc)
            if metric in ("sum", "avg", "min", "max"):
                leaf["metric"] = {metric: {"field": metric_field}}
                order_path = "metric"
            elif metric == "cardinality":
                leaf["metric"] = {"cardinality": {"field": self._exact(metric_field, fc)}}
                order_path = "metric"
            elif metric in ("median", "p50", "p90", "p95", "p99"):
                pct = 50 if metric in ("median", "p50") else int(metric[1:])
                leaf["metric"] = {"percentiles": {"field": metric_field, "percents": [pct]}}
                order_path = f"metric.{pct}"
            else:
                raise ValueError("metric must be one of count, sum, avg, min, max, cardinality, median, p90, p95, p99.")
        geo = self._resolve_geo(index) if include_geo_centroid else None
        if geo:
            leaf["centroid"] = {"geo_centroid": {"field": geo.field}}
        aggs = leaf
        for i in reversed(range(len(groups))):
            spec = self._group_agg(groups[i], fc, size, interval)
            if order_by_metric and order_path and "terms" in spec:
                spec["terms"]["order"] = {order_path: "desc"}
            if aggs:
                spec["aggs"] = aggs
            aggs = {f"g{i}": spec}
        q = self._build_query(index, query, filters, time_field, start, end)
        body: dict = {"size": 0, "track_total_hits": True, "query": q}
        if aggs:
            body["aggs"] = aggs
        if geo and geo.runtime:
            body["runtime_mappings"] = geo.runtime
        r = self._search(index, body)
        res = r.get("aggregations", {})

        def leaf_row(node: dict, prefix: dict) -> dict:
            row = dict(prefix)
            row["count"] = node.get("doc_count", _total(r))
            if "metric" in node:
                m = node["metric"]
                row[f"{metric}({metric_field})"] = _round(m["value"] if "value" in m else next(iter(m.get("values", {}).values()), None))
            if node.get("centroid", {}).get("location"):
                c = node["centroid"]["location"]
                row["centroid"] = [round(c["lat"], 5), round(c["lon"], 5)]
            return row

        rows: list = []

        def walk(node: dict, level: int, prefix: dict):
            if level == len(groups):
                rows.append(leaf_row(node, prefix))
                return
            for b in node[f"g{level}"]["buckets"]:
                walk(b, level + 1, {**prefix, groups[level]: b.get("key_as_string", b["key"])})

        walk(res, 0, {})
        out: dict = {"total_matching_docs": _total(r), "rows": rows[: self.valves.MAX_RESULTS * 4]}
        if groups and "sum_other_doc_count" in res.get("g0", {}):
            out["docs_in_groups_not_shown"] = res["g0"]["sum_other_doc_count"]
        if groups and "interval" in res.get("g0", {}):
            out["auto_interval"] = res["g0"]["interval"]
        if geo:
            out["geo_field"] = geo.spec
            pts = [[row["centroid"][0], row["centroid"][1], f"{' / '.join(str(row[g]) for g in groups)}: {row['count']:,} docs"] for row in rows if "centroid" in row]
            html = self._emit_map(f"{index}: centre of each group", points=pts) if pts else None
            if html:
                return out, html
        return out

    async def query_dsl(self, index: str, body: str, __event_emitter__: Optional[Callable] = None) -> str:
        """Run a raw Elasticsearch _search request body (Query DSL, aggregations, runtime_mappings, etc.) for
        anything the simpler tools cannot express. Read-only. Size is capped by the administrator.

        :param index: index name, pattern, alias, or data stream.
        :param body: the JSON request body, e.g. {"query": {"range": {"depth": {"gt": 100}}}, "aggs": {...}, "size": 5}.
        """
        return await self._run(__event_emitter__, "Running Query DSL", self._query_dsl, index, body)

    def _query_dsl(self, index: str, body: str) -> dict:
        index = self._index(index)
        parsed = _parse_json_arg(body, "body") or {}
        if not isinstance(parsed, dict):
            raise ValueError("body must be a JSON object.")
        parsed["size"] = max(0, min(int(parsed.get("size", 10)), self.valves.MAX_RESULTS))
        parsed.setdefault("track_total_hits", True)
        r = self._search(index, parsed)
        out: dict = {"total": _total(r), "took_ms": r.get("took"), "timed_out": r.get("timed_out")}
        if r.get("hits", {}).get("hits"):
            out["hits"] = [self._fmt_hit(h, True) for h in r["hits"]["hits"]]
            if any("sort" in h for h in r["hits"]["hits"]):
                for d, h in zip(out["hits"], r["hits"]["hits"]):
                    d["_sort"] = h.get("sort")
        if "aggregations" in r:
            out["aggregations"] = _trim(r["aggregations"], max_list=self.valves.MAX_RESULTS * 2)
        return out

    async def run_esql(self, query: str, __event_emitter__: Optional[Callable] = None) -> str:
        """Run an ES|QL query (Elasticsearch 8.11+), a piped query language well suited to analytics, e.g.
        'FROM quakes | WHERE magnitude > 5 | STATS n = COUNT(*), avg_depth = AVG(depth) BY country | SORT n DESC | LIMIT 10'.
        Supports geo functions like ST_DISTANCE and ST_CENTROID_AGG on geo fields. Prefer `aggregate` for simple group-bys.

        :param query: the ES|QL query, starting with FROM <index>.
        """
        return await self._run(__event_emitter__, "Running ES|QL", self._run_esql, query)

    def _run_esql(self, query: str) -> dict:
        self._check_query_sources(re.findall(r"(?i)\bFROM\s+([^\s|]+(?:\s*,\s*[^\s|]+)*)", query))
        r = self._request("POST", "_query", {"query": query}, params={"format": "json"})
        return self._tabular(r, "values")

    async def run_sql(self, query: str, __event_emitter__: Optional[Callable] = None) -> str:
        """Run an Elasticsearch SQL query (works on older clusters without ES|QL), e.g.
        'SELECT country, COUNT(*) AS n FROM "quakes" GROUP BY country ORDER BY n DESC LIMIT 10'.
        Quote index names containing '-' or '*' with double quotes.

        :param query: the SQL SELECT statement.
        """
        return await self._run(__event_emitter__, "Running SQL", self._run_sql, query)

    def _run_sql(self, query: str) -> dict:
        if not re.match(r"(?is)^\s*(SELECT|SHOW|DESCRIBE|DESC)\b", query):
            raise ValueError("Only SELECT / SHOW / DESCRIBE statements are allowed.")
        self._check_query_sources(re.findall(r'(?i)\bFROM\s+("[^"]+"|[\w.*\-:,]+)', query))
        r = self._request("POST", "_sql", {"query": query, "fetch_size": self.valves.MAX_RESULTS * 4}, params={"format": "json"})
        return self._tabular(r, "rows")

    def _tabular(self, r: dict, key: str) -> dict:
        cols = [c["name"] for c in r.get("columns", [])]
        rows = r.get(key, [])
        cap = self.valves.MAX_RESULTS * 4
        out = {"columns": cols, "row_count": len(rows), "rows": [dict(zip(cols, _trim(row))) for row in rows[:cap]]}
        if len(rows) > cap or r.get("cursor"):
            out["truncated"] = True
        return out

    # =======================================================================
    # Geo
    # =======================================================================
    async def geo_distance_search(
        self, lat: float, lon: float, distance: str = "10km", index: str = "", geo_field: str = "", query: str = "",
        filters: str = "", size: int = 10, fields: str = "", __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Find documents within a distance of a point, nearest first, with their distance in km and a map.
        Use for 'what is near X' / 'within 50 miles of Y'. If the user names a place, use its coordinates
        (from your own knowledge or the geocode tool).

        :param lat: latitude of the centre point in decimal degrees.
        :param lon: longitude of the centre point in decimal degrees.
        :param distance: search radius with unit, e.g. '500m', '10km', '25mi'.
        :param index: index name, pattern, alias, or data stream.
        :param geo_field: geo source from detect_geo_fields (field name, 'lat_field,lon_field', 'latlon_string:f', 'geojson_point:f'). Auto-detected if omitted.
        :param query: optional Lucene query_string to also require.
        :param filters: optional JSON exact filters, same format as the search tool.
        :param size: number of nearest documents to return.
        :param fields: comma-separated fields to return (default all, trimmed).
        """
        return await self._run(__event_emitter__, f"Searching within {distance} of {lat},{lon}", self._geo_distance_search,
                               lat, lon, distance, index, geo_field, query, filters, size, fields)

    def _geo_distance_search(self, lat, lon, distance, index, geo_field, query, filters, size, fields):
        lat, lon = float(lat), float(lon)
        _check_latlon(lat, lon)
        radius_m = _distance_m(distance)
        index = self._index(index)
        geo = self._resolve_geo(index, geo_field)
        clause = {"geo_distance": {"distance": f"{radius_m}m", geo.field: {"lat": lat, "lon": lon}}}
        q = self._build_query(index, query, filters, extra=[clause])
        sort = None
        if geo.kind == "point":
            sort = [{"_geo_distance": {geo.field: {"lat": lat, "lon": lon}, "order": "asc", "unit": "km"}}]
        map_size = min(self.valves.MAX_MAP_POINTS, max(size, 500)) if self.valves.ENABLE_MAPS else size
        r, note = self._geo_hits(index, geo, q, map_size, fields, sort)
        hits = r["hits"]["hits"]
        rows = []
        for h in hits:
            g = self._hit_geometry(h, geo)
            p = _geometry_point(g)
            d = self._fmt_hit(h, True)
            if p:
                d["_distance_km"] = round(_haversine_km(lat, lon, p[0], p[1]), 3)
                d["_location"] = [round(p[0], 6), round(p[1], 6)]
            rows.append(d)
        if note or geo.kind == "shape":
            rows.sort(key=lambda d: d.get("_distance_km", float("inf")))
        out: dict = {**geo.describe(), "center": [lat, lon], "radius": distance, "total_within_radius": _total(r),
                     "hits": rows[: max(1, min(int(size), self.valves.MAX_RESULTS))]}
        if note:
            out["note"] = note
        points, shapes = self._hits_for_map(hits, geo)
        html = self._emit_map(f"{_total(r):,} docs within {distance} of {lat}, {lon}", points=points, shapes=shapes,
                              overlay={"circle": [lat, lon, radius_m]})
        return (out, html) if html else out

    async def geo_bbox_search(
        self, north: float, south: float, east: float, west: float, index: str = "", geo_field: str = "",
        query: str = "", filters: str = "", size: int = 10, fields: str = "", __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Find documents inside a latitude/longitude bounding box (e.g. a country, state or city extent) and map them.

        :param north: top latitude.
        :param south: bottom latitude.
        :param east: right longitude.
        :param west: left longitude (may be greater than east to cross the antimeridian).
        :param index: index name, pattern, alias, or data stream.
        :param geo_field: geo source from detect_geo_fields; auto-detected if omitted.
        :param query: optional Lucene query_string to also require.
        :param filters: optional JSON exact filters, same format as the search tool.
        :param size: number of documents to return.
        :param fields: comma-separated fields to return (default all, trimmed).
        """
        return await self._run(__event_emitter__, "Searching bounding box", self._geo_bbox_search,
                               north, south, east, west, index, geo_field, query, filters, size, fields)

    def _geo_bbox_search(self, north, south, east, west, index, geo_field, query, filters, size, fields):
        north, south, east, west = float(north), float(south), float(east), float(west)
        _check_latlon(north, east)
        _check_latlon(south, west)
        if south > north:
            raise ValueError("south must be less than north.")
        index = self._index(index)
        geo = self._resolve_geo(index, geo_field)
        clause = {"geo_bounding_box": {geo.field: {"top_left": {"lat": north, "lon": west}, "bottom_right": {"lat": south, "lon": east}}}}
        q = self._build_query(index, query, filters, extra=[clause])
        map_size = min(self.valves.MAX_MAP_POINTS, max(size, 500)) if self.valves.ENABLE_MAPS else size
        r, _ = self._geo_hits(index, geo, q, map_size, fields)
        hits = r["hits"]["hits"]
        out = {**geo.describe(), "total_in_box": _total(r),
               "hits": [self._fmt_hit(h, True) for h in hits[: max(1, min(int(size), self.valves.MAX_RESULTS))]]}
        points, shapes = self._hits_for_map(hits, geo)
        html = self._emit_map(f"{_total(r):,} docs in box", points=points, shapes=shapes, overlay={"bbox": [south, west, north, east]})
        return (out, html) if html else out

    async def geo_shape_search(
        self, shape: str, relation: str = "intersects", index: str = "", geo_field: str = "", query: str = "",
        filters: str = "", size: int = 10, fields: str = "", __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Find documents relative to an arbitrary area - a polygon, line or multipolygon - given as GeoJSON
        geometry or WKT. Use for 'inside this region/route corridor/drawn area'.

        :param shape: GeoJSON geometry (e.g. {"type":"Polygon","coordinates":[[[lon,lat],...]]}) or WKT (e.g. 'POLYGON((lon lat, ...))'). Coordinates are lon,lat order.
        :param relation: intersects (default), within, disjoint, or contains (contains only for geo_shape data).
        :param index: index name, pattern, alias, or data stream.
        :param geo_field: geo source from detect_geo_fields; auto-detected if omitted.
        :param query: optional Lucene query_string to also require.
        :param filters: optional JSON exact filters, same format as the search tool.
        :param size: number of documents to return.
        :param fields: comma-separated fields to return (default all, trimmed).
        """
        return await self._run(__event_emitter__, "Searching by shape", self._geo_shape_search,
                               shape, relation, index, geo_field, query, filters, size, fields)

    def _geo_shape_search(self, shape, relation, index, geo_field, query, filters, size, fields):
        relation = (relation or "intersects").lower()
        if relation not in ("intersects", "within", "disjoint", "contains"):
            raise ValueError("relation must be intersects, within, disjoint, or contains.")
        shape = shape.strip()
        geom: Any = shape
        if shape.startswith("{"):
            geom = _parse_json_arg(shape, "shape")
            if geom.get("type") == "Feature":
                geom = geom.get("geometry")
        elif not WKT_RE.match(shape):
            raise ValueError("shape must be a GeoJSON geometry object or a WKT string.")
        index = self._index(index)
        geo = self._resolve_geo(index, geo_field)
        clause = {"geo_shape": {geo.field: {"shape": geom, "relation": relation}}}
        q = self._build_query(index, query, filters, extra=[clause])
        map_size = min(self.valves.MAX_MAP_POINTS, max(size, 500)) if self.valves.ENABLE_MAPS else size
        r, _ = self._geo_hits(index, geo, q, map_size, fields)
        hits = r["hits"]["hits"]
        out = {**geo.describe(), "relation": relation, "total_matching": _total(r),
               "hits": [self._fmt_hit(h, True) for h in hits[: max(1, min(int(size), self.valves.MAX_RESULTS))]]}
        points, shapes = self._hits_for_map(hits, geo)
        overlay = {"geojson": geom} if isinstance(geom, dict) else None
        html = self._emit_map(f"{_total(r):,} docs {relation} shape", points=points, shapes=shapes, overlay=overlay)
        return (out, html) if html else out

    async def geo_heatmap(
        self, index: str = "", geo_field: str = "", query: str = "", filters: str = "", precision: int = 0,
        grid: str = "geotile", time_field: str = "", start: str = "", end: str = "",
        __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Density / hotspot analysis: bins documents into a map grid and returns the busiest cells, how
        concentrated the data is, and a heat map. Use for 'where are most X', 'hotspots', 'clusters', 'spatial distribution'.

        :param index: index name, pattern, alias, or data stream.
        :param geo_field: geo source from detect_geo_fields; auto-detected if omitted.
        :param query: optional Lucene query_string to restrict the documents.
        :param filters: optional JSON exact filters, same format as the search tool.
        :param precision: grid resolution; geotile = map zoom 0-20 (higher = smaller cells), geohash = 1-12. 0 = automatic from data extent.
        :param grid: 'geotile' (square map tiles, default) or 'geohash'.
        :param time_field: date field for start/end (auto-detected if omitted).
        :param start: range start, ISO date or date math like 'now-30d'.
        :param end: range end.
        """
        return await self._run(__event_emitter__, "Computing spatial density", self._geo_heatmap,
                               index, geo_field, query, filters, precision, grid, time_field, start, end)

    def _grid(self, index: str, geo: GeoRef, q: dict, grid: str, precision: int):
        agg_type = "geohash_grid" if grid == "geohash" else "geotile_grid"
        aggs = {"grid": {agg_type: {"field": geo.field, "precision": precision, "size": 2000},
                         "aggs": {"c": {"geo_centroid": {"field": geo.field}}}}}
        total, res, errors = self._safe_aggs(index, aggs, q, geo.runtime)
        if errors:
            raise EsError(errors["grid"])
        cells = []
        for b in res["grid"]["buckets"]:
            loc = b.get("c", {}).get("location")
            if loc:
                cells.append({"cell": b["key"], "count": b["doc_count"], "lat": round(loc["lat"], 5), "lon": round(loc["lon"], 5)})
        return cells, total

    def _geo_heatmap(self, index, geo_field, query, filters, precision, grid, time_field, start, end):
        index = self._index(index)
        grid = "geohash" if str(grid).lower() == "geohash" else "geotile"
        geo = self._resolve_geo(index, geo_field)
        q = self._build_query(index, query, filters, time_field, start, end)
        precision = int(precision or 0)
        if not precision:
            if grid == "geohash":
                precision = 5
            else:
                precision = _auto_geotile_zoom(self._geo_summary(index, geo, q).get("bounds"))
        cells, total = self._grid(index, geo, q, grid, precision)
        in_cells = sum(c["count"] for c in cells)
        out: dict = {**geo.describe(), "grid": grid, "precision": precision, "total_docs": total,
                     "docs_with_location": in_cells, "cells": len(cells)}
        if cells and in_cells:
            top10 = sum(c["count"] for c in cells[:10])
            out["concentration"] = {
                "top_cell_pct": round(100 * cells[0]["count"] / in_cells, 1),
                "top_10_cells_pct": round(100 * top10 / in_cells, 1),
                "median_docs_per_cell": sorted(c["count"] for c in cells)[len(cells) // 2],
            }
            out["hotspots"] = [{**c, "share_pct": round(100 * c["count"] / in_cells, 2)} for c in cells[:25]]
            if len(cells) == 2000:
                out["note"] = "Grid hit the 2000-cell limit; lower precision or filter to see everything."
        html = self._emit_map(f"{index}: density ({grid} precision {precision}, {in_cells:,} located docs)",
                              cells=[[c["lat"], c["lon"], c["count"], c["cell"]] for c in cells]) if cells else None
        return (out, html) if html else out

    async def geo_distance_bands(
        self, lat: float, lon: float, bands: str = "1,5,10,50,100", unit: str = "km", index: str = "",
        geo_field: str = "", query: str = "", filters: str = "", __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Count documents in distance rings around a point (e.g. 0-1 km, 1-5 km, ...). Use for catchment,
        coverage, 'how far from X are most Y' questions.

        :param lat: latitude of the origin.
        :param lon: longitude of the origin.
        :param bands: comma-separated ring boundaries, ascending, e.g. '1,5,10,50'.
        :param unit: km, m, mi, yd, ft, or nmi.
        :param index: index name, pattern, alias, or data stream.
        :param geo_field: geo source from detect_geo_fields; auto-detected if omitted.
        :param query: optional Lucene query_string to restrict the documents.
        :param filters: optional JSON exact filters, same format as the search tool.
        """
        return await self._run(__event_emitter__, "Counting by distance band", self._geo_distance_bands,
                               lat, lon, bands, unit, index, geo_field, query, filters)

    def _geo_distance_bands(self, lat, lon, bands, unit, index, geo_field, query, filters) -> dict:
        lat, lon = float(lat), float(lon)
        _check_latlon(lat, lon)
        unit = unit.lower()
        if unit not in DISTANCE_UNITS_M:
            raise ValueError(f"unit must be one of {', '.join(DISTANCE_UNITS_M)}")
        edges = sorted(float(b) for b in str(bands).split(",") if b.strip())
        ranges, prev = [], 0.0
        for e in edges:
            ranges.append({"from": prev, "to": e})
            prev = e
        ranges.append({"from": prev})
        index = self._index(index)
        geo = self._resolve_geo(index, geo_field)
        es_unit = {"mile": "mi", "miles": "mi", "nm": "nmi"}.get(unit, unit)
        aggs = {"rings": {"geo_distance": {"field": geo.field, "origin": {"lat": lat, "lon": lon}, "unit": es_unit, "ranges": ranges}}}
        total, res, errors = self._safe_aggs(index, aggs, self._build_query(index, query, filters), geo.runtime)
        if errors:
            raise EsError(errors["rings"])
        rows, cum = [], 0
        for b in res["rings"]["buckets"]:
            cum += b["doc_count"]
            label = f"{b.get('from', 0):g}-{b['to']:g} {unit}" if "to" in b else f">{b.get('from', 0):g} {unit}"
            rows.append({"band": label, "docs": b["doc_count"], "cumulative": cum,
                         "cumulative_pct": round(100 * cum / total, 1) if total else None})
        return {**geo.describe(), "origin": [lat, lon], "total_docs": total, "bands": rows}

    async def map_documents(
        self, index: str = "", query: str = "", filters: str = "", geo_field: str = "", size: int = 500,
        time_field: str = "", start: str = "", end: str = "", __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Plot matching documents on an interactive map (points and shapes). Use when the user wants to SEE
        where things are. For very large result sets prefer geo_heatmap.

        :param index: index name, pattern, alias, or data stream.
        :param query: optional Lucene query_string.
        :param filters: optional JSON exact filters, same format as the search tool.
        :param geo_field: geo source from detect_geo_fields; auto-detected if omitted.
        :param size: maximum documents to plot.
        :param time_field: date field for start/end (auto-detected if omitted).
        :param start: range start, ISO date or date math like 'now-7d'.
        :param end: range end.
        """
        return await self._run(__event_emitter__, "Mapping documents", self._map_documents,
                               index, query, filters, geo_field, size, time_field, start, end)

    def _map_documents(self, index, query, filters, geo_field, size, time_field, start, end):
        if not self.valves.ENABLE_MAPS:
            raise ValueError("Maps are disabled by the administrator (ENABLE_MAPS).")
        index = self._index(index)
        geo = self._resolve_geo(index, geo_field)
        q = self._build_query(index, query, filters, time_field, start, end, extra=[{"exists": {"field": geo.field}}])
        r, _ = self._geo_hits(index, geo, q, int(size))
        hits = r["hits"]["hits"]
        points, shapes = self._hits_for_map(hits, geo)
        out = {**geo.describe(), "total_matching": _total(r), "plotted_points": len(points), "plotted_shapes": len(shapes)}
        if (_total(r) or 0) > len(hits):
            out["note"] = f"Only the first {len(hits)} of {_total(r):,} documents are plotted; use geo_heatmap for the full distribution."
        html = self._emit_map(f"{index}: {len(points) + len(shapes):,} of {_total(r):,} matching docs", points=points, shapes=shapes)
        return (out, html) if html else out

    async def geocode(self, place: str, __event_emitter__: Optional[Callable] = None) -> str:
        """Look up coordinates and a bounding box for a place name or address (only if the administrator enabled
        geocoding). For well-known places you may use your own knowledge instead.

        :param place: place name or address, e.g. 'Lyon, France'.
        """
        return await self._run(__event_emitter__, f"Geocoding '{place}'", self._geocode, place)

    def _geocode(self, place: str) -> dict:
        if not self.valves.ENABLE_GEOCODING:
            raise ValueError("Geocoding is disabled (ENABLE_GEOCODING). Use approximate coordinates from your own knowledge and say so.")
        resp = requests.get(
            self.valves.GEOCODER_URL, params={"q": place, "format": "json", "limit": 5},
            headers={"User-Agent": "open-webui-elasticsearch-geo-agent/0.1"}, timeout=15,
        )
        resp.raise_for_status()
        rows = []
        for r in resp.json():
            bb = r.get("boundingbox") or []
            rows.append({
                "name": r.get("display_name"), "lat": float(r["lat"]), "lon": float(r["lon"]), "type": r.get("type"),
                "bbox": {"south": float(bb[0]), "north": float(bb[1]), "west": float(bb[2]), "east": float(bb[3])} if len(bb) == 4 else None,
            })
        return {"query": place, "results": rows}

    # =======================================================================
    # Writes (disabled unless READ_ONLY is turned off)
    # =======================================================================
    async def index_document(self, index: str, document: str, doc_id: str = "", __event_emitter__: Optional[Callable] = None) -> str:
        """Add a new document (or fully replace one when doc_id already exists). Only when the user explicitly asks.

        :param index: target index (not a pattern).
        :param document: the document as a JSON object.
        :param doc_id: optional _id; generated if omitted.
        """
        blocked = self._blocked()
        if blocked:
            return blocked
        return await self._run(__event_emitter__, "Indexing document", self._index_document, index, document, doc_id)

    def _index_document(self, index: str, document: str, doc_id: str) -> dict:
        index = self._index(index)
        if "*" in index or "," in index:
            raise ValueError("Writes need a single concrete index or alias, not a pattern.")
        doc = _parse_json_arg(document, "document")
        if not isinstance(doc, dict):
            raise ValueError("document must be a JSON object.")
        path = f"{quote(index, safe='-_.+')}/_doc" + (f"/{quote(doc_id, safe='')}" if doc_id else "")
        r = self._request("PUT" if doc_id else "POST", path, doc, params={"refresh": "wait_for"})
        return {"result": r.get("result"), "_index": r.get("_index"), "_id": r.get("_id")}

    async def update_document(self, index: str, doc_id: str, fields: str, __event_emitter__: Optional[Callable] = None) -> str:
        """Change some fields of an existing document (partial update). Only when the user explicitly asks.

        :param index: index holding the document.
        :param doc_id: the document _id.
        :param fields: JSON object of fields to set, e.g. {"status": "verified"}.
        """
        blocked = self._blocked()
        if blocked:
            return blocked
        return await self._run(__event_emitter__, f"Updating document {doc_id}", self._update_document, index, doc_id, fields)

    def _update_document(self, index: str, doc_id: str, fields: str) -> dict:
        index = self._index(index)
        doc = _parse_json_arg(fields, "fields")
        if not isinstance(doc, dict) or not doc:
            raise ValueError("fields must be a non-empty JSON object.")
        r = self._request("POST", f"{quote(index, safe='-_.+')}/_update/{quote(doc_id, safe='')}", {"doc": doc}, params={"refresh": "wait_for"})
        return {"result": r.get("result"), "_index": r.get("_index"), "_id": r.get("_id")}

    async def delete_document(self, index: str, doc_id: str, confirm: bool = False, __event_emitter__: Optional[Callable] = None) -> str:
        """Permanently delete one document. Show the user the document first and only set confirm=true after
        they explicitly agree.

        :param index: index holding the document.
        :param doc_id: the document _id.
        :param confirm: must be true, and only after the user confirmed the deletion.
        """
        blocked = self._blocked()
        if blocked:
            return blocked
        if not confirm:
            return "Error: deletion not confirmed. Show the user the document and ask; call again with confirm=true only if they agree."
        return await self._run(__event_emitter__, f"Deleting document {doc_id}", self._delete_document, index, doc_id)

    def _delete_document(self, index: str, doc_id: str) -> dict:
        index = self._index(index)
        r = self._request("DELETE", f"{quote(index, safe='-_.+')}/_doc/{quote(doc_id, safe='')}", params={"refresh": "wait_for"})
        return {"result": r.get("result"), "_index": r.get("_index"), "_id": r.get("_id")}
