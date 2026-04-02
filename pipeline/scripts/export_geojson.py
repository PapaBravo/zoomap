#!/usr/bin/env python3
"""
Gold layer export: Query the gold_animals dbt model from DuckDB and write the
result as a GeoJSON FeatureCollection to ``$OUTPUT_PATH``.

Environment variables (see pipeline/.env.example):
  DB_PATH      Path to DuckDB database file
  OUTPUT_PATH  Destination GeoJSON file (relative to repo root)
  LANG         Wikipedia language for URL construction (default: en)
"""

import json
import os
import re
import sys
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("DB_PATH", "pipeline/zoo.duckdb")
OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH", "data/animals.geojson"))
WIKIDATA_LANG = os.environ.get("WIKIDATA_LANG", "en")


# ---------------------------------------------------------------------------
# GeoJSON building
# ---------------------------------------------------------------------------

def _build_geometry(geom_type: str, geom_coords: str) -> dict | None:
    """Reconstruct a GeoJSON geometry object."""
    try:
        coords = json.loads(geom_coords)
    except (json.JSONDecodeError, TypeError):
        return None

    if geom_type == "Point":
        return {"type": "Point", "coordinates": coords}
    if geom_type == "Polygon":
        return {"type": "Polygon", "coordinates": coords}
    if geom_type == "LineString":
        return {"type": "LineString", "coordinates": coords}
    return None


def _slugify(text: str) -> str:
    """Convert a name into a URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = re.sub(r"^-+|-+$", "", text)
    return text


def build_feature(row: dict) -> dict | None:
    """Build a single GeoJSON Feature from a gold_animals row."""
    geometry = _build_geometry(row["geom_type"], row["geom_coords"])
    if geometry is None:
        return None

    feature_id = row["feature_id"] or _slugify(row["name"] or str(row["osm_id"]))

    properties = {
        "id": feature_id,
        "name": row["name"],
        "species": row["species"],
        "description": row["description"],
        "enclosure": row["enclosure"],
        "wikipedia": row["wikipedia"],
        "image": row["image"],
    }
    # Remove None values so the output stays clean
    properties = {k: v for k, v in properties.items() if v is not None}

    return {"type": "Feature", "geometry": geometry, "properties": properties}


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export() -> None:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        rows = con.execute("""
            SELECT
                feature_id,
                name,
                species,
                description,
                enclosure,
                wikipedia,
                image,
                geom_type,
                geom_coords,
                osm_id
            FROM gold_animals
            ORDER BY name
        """).fetchall()
        columns = [
            "feature_id", "name", "species", "description", "enclosure",
            "wikipedia", "image", "geom_type", "geom_coords", "osm_id",
        ]
    finally:
        con.close()

    features = []
    skipped = 0
    for raw_row in rows:
        row = dict(zip(columns, raw_row))
        feature = build_feature(row)
        if feature:
            features.append(feature)
        else:
            skipped += 1
            print(f"Skipped row with osm_id={row['osm_id']} (invalid geometry)", flush=True)

    geojson = {"type": "FeatureCollection", "features": features}

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(
        f"Exported {len(features)} features to {OUTPUT_PATH} "
        f"({skipped} skipped)",
        flush=True,
    )


def validate(path: Path) -> None:
    """Basic schema validation of the written GeoJSON file."""
    with path.open() as f:
        data = json.load(f)
    assert data.get("type") == "FeatureCollection", "Root type must be FeatureCollection"
    for feat in data.get("features", []):
        assert feat.get("type") == "Feature", "Each feature must have type=Feature"
        assert feat.get("geometry") is not None, "Feature must have geometry"
        props = feat.get("properties", {})
        assert "id" in props, "Feature must have 'id' property"
        assert "name" in props, "Feature must have 'name' property"
    print(f"Validation passed: {len(data['features'])} valid features", flush=True)


def main() -> None:
    export()
    validate(OUTPUT_PATH)


if __name__ == "__main__":
    main()
