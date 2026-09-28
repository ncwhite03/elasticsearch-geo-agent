"""Load synthetic geo datasets into Elasticsearch, one per common storage format, for trying the agent.

    python scripts/load_sample_data.py                      # http://localhost:9200, no auth
    ES_URL=https://es:9200 ES_API_KEY=... python scripts/load_sample_data.py

Creates:
  demo-quakes    geo_point `location` ({lat, lon} objects), magnitude, depth_km, country, @timestamp
  demo-stations  separate `decimalLatitude` / `decimalLongitude` float fields (no geo mapping)
  demo-places    coordinates as a "lat,lon" keyword string in `coords`
  demo-regions   geo_shape polygons in `boundary`
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

import requests

ES = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
HEADERS = {"Content-Type": "application/x-ndjson"}
if os.environ.get("ES_API_KEY"):
    HEADERS["Authorization"] = f"ApiKey {os.environ['ES_API_KEY']}"
AUTH = (os.environ["ES_USERNAME"], os.environ.get("ES_PASSWORD", "")) if os.environ.get("ES_USERNAME") else None

HOTSPOTS = [  # (name, country, lat, lon, spread)
    ("Tokyo", "JP", 35.68, 139.76, 1.5), ("Santiago", "CL", -33.45, -70.66, 2.0), ("San Francisco", "US", 37.77, -122.42, 1.2),
    ("Anchorage", "US", 61.22, -149.9, 2.5), ("Jakarta", "ID", -6.2, 106.85, 2.0), ("Istanbul", "TR", 41.01, 28.98, 1.5),
    ("Lima", "PE", -12.05, -77.04, 1.5), ("Kathmandu", "NP", 27.72, 85.32, 1.0),
]
CATEGORIES = ["hospital", "school", "fire_station", "shelter", "warehouse"]


def bulk(index: str, docs: list, mapping: dict):
    requests.delete(f"{ES}/{index}", headers=HEADERS, auth=AUTH)
    r = requests.put(f"{ES}/{index}", json={"mappings": mapping}, auth=AUTH, headers={k: v for k, v in HEADERS.items() if k != "Content-Type"})
    r.raise_for_status()
    lines = []
    for d in docs:
        lines += [json.dumps({"index": {"_index": index}}), json.dumps(d)]
    r = requests.post(f"{ES}/_bulk?refresh=true", data="\n".join(lines) + "\n", headers=HEADERS, auth=AUTH)
    r.raise_for_status()
    assert not r.json()["errors"], r.json()["items"][:3]
    print(f"{index}: {len(docs)} docs")


def jitter(lat, lon, spread):
    return round(lat + random.gauss(0, spread), 5), round(lon + random.gauss(0, spread), 5)


def main():
    random.seed(7)
    now = datetime.now(timezone.utc)

    quakes = []
    for _ in range(3000):
        name, cc, lat, lon, spread = random.choices(HOTSPOTS, weights=[5, 4, 3, 2, 4, 2, 2, 1])[0]
        la, lo = jitter(lat, lon, spread)
        quakes.append({
            "@timestamp": (now - timedelta(minutes=random.randint(0, 60 * 24 * 365))).isoformat(),
            "location": {"lat": la, "lon": lo}, "magnitude": round(random.expovariate(1.2) + 2.0, 1),
            "depth_km": round(random.uniform(1, 300), 1), "country": cc, "nearest_city": name,
            "reviewed": random.random() > 0.3,
        })
    bulk("demo-quakes", quakes, {"properties": {
        "@timestamp": {"type": "date"}, "location": {"type": "geo_point"}, "magnitude": {"type": "float"},
        "depth_km": {"type": "float"}, "country": {"type": "keyword"}, "nearest_city": {"type": "keyword"},
        "reviewed": {"type": "boolean"},
    }})

    stations = []
    for i in range(800):
        name, cc, lat, lon, spread = random.choice(HOTSPOTS)
        la, lo = jitter(lat, lon, spread / 3)
        stations.append({"stationId": f"ST-{i:04d}", "decimalLatitude": la, "decimalLongitude": lo, "countryCode": cc,
                         "elevationM": random.randint(0, 3000), "status": random.choice(["online", "online", "offline"])})
    stations += [{"stationId": f"ST-X{i}", "decimalLatitude": 0.0, "decimalLongitude": 0.0, "countryCode": "??", "status": "unknown"} for i in range(12)]
    bulk("demo-stations", stations, {"properties": {
        "stationId": {"type": "keyword"}, "decimalLatitude": {"type": "float"}, "decimalLongitude": {"type": "float"},
        "countryCode": {"type": "keyword"}, "elevationM": {"type": "integer"}, "status": {"type": "keyword"},
    }})

    places = []
    for i in range(400):
        name, cc, lat, lon, spread = random.choice(HOTSPOTS)
        la, lo = jitter(lat, lon, spread / 4)
        places.append({"name": f"{name} {random.choice(CATEGORIES).replace('_', ' ')} {i}", "category": random.choice(CATEGORIES),
                       "city": name, "coords": f"{la},{lo}", "capacity": random.randint(10, 2000)})
    bulk("demo-places", places, {"properties": {
        "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}, "category": {"type": "keyword"},
        "city": {"type": "keyword"}, "coords": {"type": "keyword"}, "capacity": {"type": "integer"},
    }})

    regions = []
    for name, cc, lat, lon, spread in HOTSPOTS:
        d = spread * 1.5
        ring = [[lon - d, lat - d], [lon + d, lat - d], [lon + d, lat + d], [lon - d, lat + d], [lon - d, lat - d]]
        regions.append({"region": f"{name} seismic zone", "country": cc, "risk": random.choice(["high", "very high"]),
                        "boundary": {"type": "Polygon", "coordinates": [ring]}})
    bulk("demo-regions", regions, {"properties": {
        "region": {"type": "keyword"}, "country": {"type": "keyword"}, "risk": {"type": "keyword"}, "boundary": {"type": "geo_shape"},
    }})


if __name__ == "__main__":
    main()
