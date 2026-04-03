#!/usr/bin/env python3
"""
Bronze layer: Download OSM POIs from Overpass API and load into DuckDB.

The script builds an Overpass QL query from the configured bounding box and tag
filters, caches the raw JSON response on disk (keyed by a hash of the query), and
loads the parsed elements into the DuckDB table ``raw_osm_elements``.

Environment variables (see pipeline/.env.example for defaults):
  BBOX          "min_lat,min_lon,max_lat,max_lon"  (south,west,north,east)
  OSM_TAGS      Comma-separated tag filters
  OVERPASS_URL  Overpass API endpoint (used as first mirror; overrides default list)
  DB_PATH       Path to DuckDB database file
  CACHE_DIR     Directory for caching downloaded JSON
"""

import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BBOX = os.environ.get("BBOX", "52.490,13.495,52.525,13.545")
OSM_TAGS_RAW = os.environ.get("OSM_TAGS", "attraction=animal")
DB_PATH = os.environ.get("DB_PATH", "pipeline/zoo.duckdb")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "pipeline/cache"))
OVERPASS_TIMEOUT = int(os.environ.get("OVERPASS_TIMEOUT", "300"))

# Ordered list of Overpass endpoints to try; the env var (if set) is tried first.
_DEFAULT_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
_env_url = os.environ.get("OVERPASS_URL")
if _env_url and _env_url not in _DEFAULT_OVERPASS_ENDPOINTS:
    OVERPASS_ENDPOINTS = [_env_url] + _DEFAULT_OVERPASS_ENDPOINTS
else:
    OVERPASS_ENDPOINTS = _DEFAULT_OVERPASS_ENDPOINTS

CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Overpass query builder
# ---------------------------------------------------------------------------

def _tag_filter(tag: str) -> str:
    """Convert a tag spec like 'zoo' or 'tourism=zoo' into an Overpass filter."""
    if "=" in tag:
        key, value = tag.split("=", 1)
        return f'["{key}"="{value}"]'
    return f'["{tag}"]'


def build_overpass_query(bbox: str, tags_raw: str, timeout: int = 120) -> str:
    """Build an Overpass QL query that fetches nodes, ways, and relations."""
    # Overpass bbox order: south,west,north,east  (same as our BBOX convention)
    bbox_str = bbox.strip()
    tags = [t.strip() for t in tags_raw.split(",") if t.strip()]

    unions = []
    for tag in tags:
        tf = _tag_filter(tag)
        for elem_type in ("node", "way", "relation"):
            unions.append(f'  {elem_type}{tf}({bbox_str});')

    body = "\n".join(unions)
    return (
        f"[out:json][timeout:{timeout}];\n"
        f"(\n{body}\n);\n"
        f"out geom;"
    )


# ---------------------------------------------------------------------------
# Caching helpers
# ---------------------------------------------------------------------------

def query_hash(query: str) -> str:
    return hashlib.sha256(query.encode()).hexdigest()[:16]


def make_session(
    retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Build a requests Session with automatic retry and backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_overpass(query: str, cache_path: str | None = None, timeout: int = OVERPASS_TIMEOUT + 30) -> dict:
    """Fetch from Overpass API with mirror rotation and retries, returning parsed JSON.

    The *timeout* is the HTTP request timeout in seconds (defaults to OVERPASS_TIMEOUT + 30
    to give the server time to complete before the client disconnects).

    If *cache_path* is provided and the file exists the cached result is returned
    immediately without contacting Overpass.
    """
    if cache_path:
        p = Path(cache_path)
        if p.exists():
            logger.info("Using cached OSM file at %s", cache_path)
            return json.loads(p.read_text())

    session = make_session(retries=5, backoff_factor=1.0)
    last_exc: Exception | None = None

    for endpoint in OVERPASS_ENDPOINTS:
        try:
            logger.info("Posting Overpass query to %s", endpoint)
            resp = session.post(
                endpoint,
                data={"data": query},
                timeout=timeout,
                headers={"User-Agent": "zoomap-pipeline/1.0 (https://github.com/PapaBravo/zoomap)"},
            )
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError:
                raise RuntimeError("Overpass returned a non-JSON response")
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            logger.warning("Overpass mirror %s failed: %s", endpoint, exc)
            time.sleep(2)

    logger.error("All Overpass mirrors failed. Last exception: %s", last_exc)
    raise RuntimeError(f"Overpass queries failed after trying all mirrors; last error: {last_exc}")


def load_or_fetch(query: str) -> dict:
    """Return cached Overpass result, or fetch and cache it."""
    h = query_hash(query)
    cache_file = CACHE_DIR / f"osm_{h}.json"

    if cache_file.exists():
        logger.info("Cache hit: %s", cache_file)
        with cache_file.open() as f:
            return json.load(f)

    logger.info("Cache miss, fetching from Overpass …")
    data = fetch_overpass(query, cache_path=None)
    with cache_file.open("w") as f:
        json.dump(data, f)
    logger.info("Cached response to %s", cache_file)
    return data


# ---------------------------------------------------------------------------
# Element parsing
# ---------------------------------------------------------------------------

def _centroid_from_geometry(geometry: list) -> tuple[float, float]:
    """Compute centroid (lat, lon) from a list of {lat, lon} dicts."""
    lats = [p["lat"] for p in geometry if "lat" in p and "lon" in p]
    lons = [p["lon"] for p in geometry if "lat" in p and "lon" in p]
    if not lats:
        return (None, None)
    return (sum(lats) / len(lats), sum(lons) / len(lons))


def _coords_from_geometry(geometry: list) -> list:
    """Convert Overpass geometry list to GeoJSON coordinate pairs [lon, lat]."""
    return [[p["lon"], p["lat"]] for p in geometry if "lat" in p and "lon" in p]


def parse_elements(elements: list) -> list[dict]:
    """Parse Overpass elements into a flat list of dicts for DuckDB loading."""
    rows = []
    for elem in elements:
        osm_id = elem.get("id")
        osm_type = elem.get("type")
        tags = elem.get("tags", {})

        if osm_type == "node":
            lat = elem.get("lat")
            lon = elem.get("lon")
            if lat is None or lon is None:
                continue
            geom_type = "Point"
            geom_coords = json.dumps([lon, lat])

        elif osm_type == "way":
            geometry = elem.get("geometry", [])
            if not geometry:
                continue
            lat, lon = _centroid_from_geometry(geometry)
            if lat is None:
                continue
            coords = _coords_from_geometry(geometry)
            # Closed way → Polygon; open way → LineString
            if len(coords) >= 4 and coords[0] == coords[-1]:
                geom_type = "Polygon"
                geom_coords = json.dumps([coords])
            else:
                geom_type = "LineString"
                geom_coords = json.dumps(coords)

        elif osm_type == "relation":
            # Relations are complex; skip for now unless they have a center
            center = elem.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")
            if lat is None:
                continue
            geom_type = "Point"
            geom_coords = json.dumps([lon, lat])

        else:
            continue

        rows.append({
            "osm_id": osm_id,
            "osm_type": osm_type,
            "centroid_lat": lat,
            "centroid_lon": lon,
            "tags": json.dumps(tags),
            "geom_type": geom_type,
            "geom_coords": geom_coords,
        })

    return rows


# ---------------------------------------------------------------------------
# DuckDB loading
# ---------------------------------------------------------------------------

def load_into_duckdb(rows: list[dict]) -> None:
    """Create (or replace) the raw_osm_elements table in DuckDB."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
            CREATE OR REPLACE TABLE raw_osm_elements (
                osm_id      BIGINT,
                osm_type    VARCHAR,
                centroid_lat DOUBLE,
                centroid_lon DOUBLE,
                tags        VARCHAR,
                geom_type   VARCHAR,
                geom_coords VARCHAR
            )
        """)

        if rows:
            con.executemany(
                """
                INSERT INTO raw_osm_elements VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["osm_id"],
                        r["osm_type"],
                        r["centroid_lat"],
                        r["centroid_lon"],
                        r["tags"],
                        r["geom_type"],
                        r["geom_coords"],
                    )
                    for r in rows
                ],
            )

        count = con.execute("SELECT COUNT(*) FROM raw_osm_elements").fetchone()[0]
        logger.info("Loaded %d elements into raw_osm_elements", count)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    query = build_overpass_query(BBOX, OSM_TAGS_RAW, timeout=OVERPASS_TIMEOUT)
    logger.info("Overpass query:\n%s\n", query)

    data = load_or_fetch(query)
    elements = data.get("elements", [])
    logger.info("Fetched %d raw elements from Overpass", len(elements))

    rows = parse_elements(elements)
    logger.info("Parsed %d valid elements", len(rows))

    load_into_duckdb(rows)


if __name__ == "__main__":
    main()
