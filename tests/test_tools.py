import asyncio
import base64
import importlib.util
import inspect
import json
from pathlib import Path
from unittest.mock import patch

import pytest

spec = importlib.util.spec_from_file_location(
    "elasticsearch_geo_tools", Path(__file__).parent.parent / "tools" / "elasticsearch_geo_tools.py"
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def cap(t, agg=True):
    return {t: {"type": t, "searchable": True, "aggregatable": agg}}


FIELD_CAPS = {
    "indices": ["demo"],
    "fields": {
        "_id": {"_id": {"type": "_id", "searchable": True, "aggregatable": True}},
        "@timestamp": cap("date"),
        "location": cap("geo_point"),
        "boundary": cap("geo_shape", agg=False),
        "decimalLatitude": cap("float"),
        "decimalLongitude": cap("float"),
        "pickup.lat": cap("text", agg=False),
        "pickup.lon": cap("text", agg=False),
        "name": cap("text", agg=False),
        "name.keyword": cap("keyword"),
        "notes": cap("text", agg=False),
        "country": cap("keyword"),
        "magnitude": cap("float"),
        "shape.type": cap("keyword"),
        "shape.coordinates": cap("float"),
        "shape": {"object": {"type": "object", "searchable": False, "aggregatable": False}},
        "easting": cap("double"),
    },
}


class Resp:
    def __init__(self, status, payload):
        self.status_code, self._payload = status, payload
        self.content = json.dumps(payload).encode()
        self.text = self.content.decode()

    def json(self):
        return self._payload


class FakeES:
    """Routes requests by (method, path suffix); records every call."""

    def __init__(self, routes=None):
        self.calls = []
        self.routes = routes or {}

    def __call__(self, method, url, json=None, params=None, **kw):
        path = url.split("9200/", 1)[-1]
        self.calls.append({"method": method, "path": path, "body": json, "params": params, "kw": kw})
        for key, handler in self.routes.items():
            if path.endswith(key) or key in path:
                out = handler(json) if callable(handler) else handler
                return out if isinstance(out, Resp) else Resp(200, out)
        return Resp(404, {"error": {"type": "not_found", "reason": f"no fake route for {path}"}})


@pytest.fixture
def tools():
    t = mod.Tools()
    t.valves.ES_URL = "http://es:9200"
    return t


def run(coro):
    return asyncio.run(coro)


def test_every_public_method_is_async_typed_and_documented(tools):
    for name, fn in inspect.getmembers(tools, inspect.ismethod):
        if name.startswith("_"):
            continue
        assert inspect.iscoroutinefunction(fn), f"{name} should be async"
        assert fn.__doc__, f"{name} lacks a docstring (LLM tool description)"
        for p in inspect.signature(fn).parameters.values():
            assert p.annotation is not inspect.Parameter.empty, f"{name}.{p.name} untyped"
            if not p.name.startswith("__"):
                assert f":param {p.name}:" in fn.__doc__, f"{name}.{p.name} undocumented"


def test_cloud_id_decoding():
    raw = base64.b64encode(b"us-east-1.aws.found.io:443$abc123$kib456").decode()
    assert mod._cloud_id_to_url(f"prod:{raw}") == "https://abc123.us-east-1.aws.found.io:443"


def test_auth_and_tls_settings_are_sent(tools):
    fake = FakeES({"": {"version": {"number": "8.15.0"}}})
    tools.valves.ES_API_KEY = "KEY"
    tools.valves.VERIFY_SSL = False
    with patch.object(mod.requests, "request", fake):
        tools._request("GET", "/")
    kw = fake.calls[0]["kw"]
    assert kw["headers"]["Authorization"] == "ApiKey KEY" and kw["verify"] is False and kw["auth"] is None


def test_env_fallback(monkeypatch):
    monkeypatch.setenv("ES_URL", "https://from-env:9200/")
    assert mod.Tools()._base_url() == "https://from-env:9200"


def test_index_access_rules(tools):
    with pytest.raises(ValueError, match="not allowed"):
        tools._index(".security")
    with pytest.raises(ValueError, match="Specify"):
        tools._index("")
    tools.valves.INDEX_ALLOWLIST = "geo-*,places"
    assert tools._index("geo-2024") == "geo-2024"
    assert tools._index("*") == "geo-*,places"
    assert tools._index("") == "geo-*,places"
    with pytest.raises(ValueError, match="not allowed"):
        tools._index("geo-1,secrets")
    with pytest.raises(ValueError, match="not allowed"):
        tools._run_esql("FROM secrets | LIMIT 5")
    with pytest.raises(ValueError, match="not allowed"):
        tools._run_sql('SELECT * FROM "secrets"')


def test_sql_is_select_only(tools):
    with pytest.raises(ValueError, match="SELECT"):
        tools._run_sql("DELETE FROM x")


def test_geo_candidates_cover_every_format(tools):
    with patch.object(mod.requests, "request", FakeES({"_field_caps": FIELD_CAPS})):
        c = tools._geo_candidates("demo")
    assert c["native"] == ["location", "boundary"]
    assert ("decimalLatitude", "decimalLongitude") in c["pairs"]
    assert ("pickup.lat", "pickup.lon") in c["pairs"]
    assert c["geojson"] == ["shape"]
    assert c["projected"] == ["easting"]
    assert "country" in c["places"]


def test_resolve_geo_variants(tools):
    with patch.object(mod.requests, "request", FakeES({"_field_caps": FIELD_CAPS})):
        assert tools._resolve_geo("demo", "").spec == "location"
        assert tools._resolve_geo("demo", "boundary").kind == "shape"
        pair = tools._resolve_geo("demo", "decimalLatitude,decimalLongitude")
        assert pair.field == mod.RUNTIME_GEO_FIELD
        assert pair.runtime[mod.RUNTIME_GEO_FIELD]["script"]["params"]["mode"] == "pair_docvalues"
        text_pair = tools._resolve_geo("demo", "pickup.lat,pickup.lon")
        assert text_pair.runtime[mod.RUNTIME_GEO_FIELD]["script"]["params"]["lat_path"] == ["pickup", "lat"]
        assert "latlon_string" in tools._resolve_geo("demo", "latlon_string:notes").runtime[mod.RUNTIME_GEO_FIELD]["script"]["params"]["mode"]
        with pytest.raises(ValueError, match="lat_field,lon_field"):
            tools._resolve_geo("demo", "magnitude")
        with pytest.raises(ValueError, match="Did you mean: location"):
            tools._resolve_geo("demo", "locaton")


def _pair_route(lat_rng, lon_rng, zeros=0):
    def handler(body):
        aggs = {
            "lat": {"count": 10, "min": lat_rng[0], "max": lat_rng[1]},
            "lon": {"count": 10, "min": lon_rng[0], "max": lon_rng[1]},
            "zero": {"doc_count": zeros},
        }
        return {"hits": {"total": {"value": 10}}, "aggregations": {k: v for k, v in aggs.items() if k in body["aggs"]}}
    return handler


@pytest.mark.parametrize("lat_rng,lon_rng,zeros,expect,spec", [
    ((-40, 60), (-150, 140), 0, "valid WGS84", "a_lat,a_lon"),
    ((-150, 140), (-40, 60), 0, "likely swapped", "a_lon,a_lat"),
    ((4.4e6, 5.1e6), (2e5, 8e5), 0, "projected", "a_lat,a_lon"),
    ((-40, 60), (-150, 140), 7, "7 docs sit at 0,0", "a_lat,a_lon"),
])
def test_lat_lon_pair_diagnosis(tools, lat_rng, lon_rng, zeros, expect, spec):
    fc = {"fields": {"a_lat": {"type": "float", "aggregatable": True}, "a_lon": {"type": "float", "aggregatable": True}}}
    with patch.object(mod.requests, "request", FakeES({"_search": _pair_route(lat_rng, lon_rng, zeros)})):
        out = tools._check_pair("demo", "a_lat", "a_lon", fc, [])
    assert expect in out["status"] and out["geo_field_spec"] == spec


def test_source_format_description():
    d = mod.Tools._describe_source_format
    assert "{lat, lon}" in d([None, {"lat": 1, "lon": 2}])
    assert "[lon, lat]" in d([[2.3, 48.8]])
    assert "'lat,lon'" in d(["48.8,2.3"])
    assert "WKT" in d(["POINT (2.3 48.8)"])
    assert "GeoJSON (Polygon)" in d([{"type": "Polygon", "coordinates": []}])


def test_build_query_filters(tools):
    with patch.object(mod.requests, "request", FakeES({"_field_caps": FIELD_CAPS})):
        q = tools._build_query("demo", "quake", json.dumps({
            "name": "Tokyo", "country": ["JP", "CL"], "magnitude": {"gte": 5}, "notes": "aftershock", "decimalLatitude": None,
        }), start="now-7d")
        with pytest.raises(ValueError, match="Did you mean: magnitude"):
            tools._build_query("demo", filters='{"magnitud": 1}')
    f = q["bool"]["filter"]
    assert {"term": {"name.keyword": "Tokyo"}} in f
    assert {"terms": {"country": ["JP", "CL"]}} in f
    assert {"range": {"magnitude": {"gte": 5}}} in f
    assert {"range": {"@timestamp": {"gte": "now-7d"}}} in f
    assert any("match_phrase" in json.dumps(x) for x in f)  # text with no keyword subfield
    assert q["bool"]["must_not"] == [{"exists": {"field": "decimalLatitude"}}]
    assert q["bool"]["must"][0]["query_string"]["query"] == "quake"


def test_aggregate_builds_nested_and_flattens(tools):
    seen = {}

    def search(body):
        seen["body"] = body
        return {"hits": {"total": {"value": 100}}, "aggregations": {"g0": {"sum_other_doc_count": 3, "buckets": [
            {"key": "JP", "doc_count": 60, "g1": {"buckets": [
                {"key": 1700000000000, "key_as_string": "2024-01", "doc_count": 60,
                 "metric": {"values": {"95.0": 6.1}}, "centroid": {"location": {"lat": 35.6, "lon": 139.7}}}]}},
        ]}}}

    fake = FakeES({"_field_caps": FIELD_CAPS, "_search": search})
    with patch.object(mod.requests, "request", fake):
        out, html = tools._aggregate("demo", "country,@timestamp", "p95", "magnitude", "", "", 5, "month", True, True, "", "", "")
    g0 = seen["body"]["aggs"]["g0"]
    assert g0["terms"]["order"] == {"metric.95": "desc"}
    assert g0["aggs"]["g1"]["date_histogram"]["calendar_interval"] == "month"
    assert out["rows"] == [{"country": "JP", "@timestamp": "2024-01", "count": 60, "p95(magnitude)": 6.1, "centroid": [35.6, 139.7]}]
    assert out["docs_in_groups_not_shown"] == 3
    assert "leaflet" in html


def test_distance_search_falls_back_when_sort_fails_and_emits_map(tools):
    def search(body):
        if "sort" in body:
            return Resp(400, {"error": {"type": "search_phase_execution_exception", "reason": "all shards failed",
                                        "caused_by": {"type": "illegal_argument_exception", "reason": "can't sort on runtime"}}})
        hits = [
            {"_id": "far", "_source": {"n": 1}, "fields": {mod.RUNTIME_GEO_FIELD: [{"type": "Point", "coordinates": [140.5, 35.6]}]}},
            {"_id": "near", "_source": {"n": 2}, "fields": {mod.RUNTIME_GEO_FIELD: [{"type": "Point", "coordinates": [139.77, 35.68]}]}},
        ]
        return {"hits": {"total": {"value": 2}, "hits": hits}}

    events = []

    async def emitter(e):
        events.append(e)

    with patch.object(mod.requests, "request", FakeES({"_field_caps": FIELD_CAPS, "_search": search})):
        out = json.loads(run(tools.geo_distance_search(35.68, 139.76, "100km", "demo", "decimalLatitude,decimalLongitude",
                                                       __event_emitter__=emitter)))
    assert [h["_id"] for h in out["hits"]] == ["near", "far"]
    assert out["hits"][0]["_distance_km"] < 2 and "sorted client-side" in out["note"]
    assert any(e["type"] == "embeds" for e in events) and "map" in out
    assert events[-1]["data"]["done"] is True


def test_invalid_coordinates_and_distance(tools):
    assert "Invalid coordinates" in run(tools.geo_distance_search(95, 0, "1km", "demo"))
    assert "distance unit" in run(tools.geo_distance_search(1, 0, "10parsecs", "demo"))


def test_errors_are_returned_with_root_cause(tools):
    err = Resp(400, {"error": {"type": "search_phase_execution_exception", "reason": "all shards failed",
                               "failed_shards": [{"reason": {"type": "query_shard_exception", "reason": "bad field"}}],
                               "caused_by": {"type": "number_format_exception", "reason": "For input 'x'"}}})
    with patch.object(mod.requests, "request", FakeES({"_search": err})):
        out = run(tools.query_dsl("demo", '{"query": {"match_all": {}}}'))
    assert out.startswith("Error: EsError") and "bad field" in out and "For input 'x'" in out


def test_query_dsl_caps_size(tools):
    fake = FakeES({"_search": {"hits": {"total": {"value": 0}, "hits": []}}})
    with patch.object(mod.requests, "request", fake):
        run(tools.query_dsl("demo", '{"size": 100000}'))
    assert fake.calls[0]["body"]["size"] == tools.valves.MAX_RESULTS


def test_writes_blocked_when_read_only(tools):
    fake = FakeES()
    with patch.object(mod.requests, "request", fake):
        assert "READ_ONLY" in run(tools.index_document("demo", "{}"))
        assert "READ_ONLY" in run(tools.update_document("demo", "1", '{"a": 1}'))
        assert "READ_ONLY" in run(tools.delete_document("demo", "1", confirm=True))
        tools.valves.READ_ONLY = False
        assert "not confirmed" in run(tools.delete_document("demo", "1"))
        assert "pattern" in run(tools.index_document("demo-*", '{"a": 1}'))
    assert fake.calls == []


def test_output_truncation(tools):
    tools.valves.MAX_OUTPUT_CHARS = 50
    assert "truncated" in tools._out({"x": "y" * 500})


def test_trim_omits_polygon_vertices():
    out = mod._trim({"b": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}, "s": "x" * 400, "l": list(range(30))})
    assert out["b"]["coordinates"] == "<4 vertices omitted>"
    assert out["s"].endswith("(+100 chars)") and out["l"][-1] == "... (+20 more)"


def test_map_html_cannot_break_out_of_script():
    html = mod._map_html("t", "tiles", "a", points=[[1, 2, "</script><script>alert(1)</script>"]])
    assert "</script><script>alert(1)" not in html


def test_auto_zoom_scales_with_extent():
    world = {"top_left": {"lat": 80, "lon": -180}, "bottom_right": {"lat": -80, "lon": 180}}
    city = {"top_left": {"lat": 35.8, "lon": 139.6}, "bottom_right": {"lat": 35.6, "lon": 139.9}}
    assert mod._auto_geotile_zoom(world) < 5 < mod._auto_geotile_zoom(city)


def test_name_normalization_and_haversine():
    assert mod._norm_name("decimalLatitude") == "decimal_latitude"
    assert abs(mod._haversine_km(48.8566, 2.3522, 51.5074, -0.1278) - 343.5) < 1
