#!/usr/bin/env python3
"""
Enrichment layer: Fetch Wikipedia and Wikidata metadata for silver-layer POIs
and load the results into the DuckDB table ``wiki_enrichment``.

Each Wikidata entity and Wikipedia article is cached individually on disk so
that re-runs only fetch what is missing (respecting API rate limits).

Environment variables (see pipeline/.env.example):
  DB_PATH        Path to DuckDB database file
  CACHE_DIR      Directory for caching downloaded JSON
  WIKIDATA_LANG  Wikipedia language code (default: en)
"""

import json
import os
import re
import time
from pathlib import Path
from urllib.parse import quote

import duckdb
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("DB_PATH", "pipeline/zoo.duckdb")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "pipeline/cache"))
LANG = os.environ.get("WIKIDATA_LANG", "en")

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIPEDIA_API = f"https://{LANG}.wikipedia.org/api/rest_v1/page/summary"

# Politeness delay between API calls (seconds)
REQUEST_DELAY = 1.0

CACHE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "zoomap-pipeline/1.0 (https://github.com/PapaBravo/zoomap)"
})


# ---------------------------------------------------------------------------
# Wikidata helpers
# ---------------------------------------------------------------------------

def fetch_wikidata_entity(qid: str) -> dict | None:
    """Fetch a Wikidata entity by QID. Returns the entity dict or None."""
    cache_file = CACHE_DIR / f"wikidata_{qid}.json"
    if cache_file.exists():
        with cache_file.open() as f:
            return json.load(f)

    print(f"Fetching Wikidata {qid} …", flush=True)
    try:
        resp = SESSION.get(
            WIKIDATA_API,
            params={
                "action": "wbgetentities",
                "ids": qid,
                "format": "json",
                "props": "claims|sitelinks|labels",
                "sitefilter": f"{LANG}wiki",
                "languages": LANG,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        entity = data.get("entities", {}).get(qid)
        if entity and entity.get("id") == qid:
            with cache_file.open("w") as f:
                json.dump(entity, f)
            time.sleep(REQUEST_DELAY)
            return entity
    except requests.RequestException as exc:
        print(f"Warning: Wikidata fetch failed for {qid}: {exc}", flush=True)
    return None


def extract_wikidata_info(entity: dict, qid: str) -> dict:
    """Extract common name, scientific name, image filename, and Wikipedia sitelink from entity."""
    result = {
        "wikidata_id": qid,
        "common_name": None,
        "scientific_name": None,
        "image_filename": None,
        "wikipedia_title": None,
    }

    # English label: common name of the species
    labels = entity.get("labels", {})
    if LANG in labels:
        result["common_name"] = labels[LANG].get("value")

    claims = entity.get("claims", {})

    # P225: taxon name (scientific name)
    taxon_claims = claims.get("P225", [])
    if taxon_claims:
        try:
            result["scientific_name"] = (
                taxon_claims[0]["mainsnak"]["datavalue"]["value"]
            )
        except (KeyError, TypeError):
            pass

    # P18: image (Wikimedia Commons filename)
    image_claims = claims.get("P18", [])
    if image_claims:
        try:
            result["image_filename"] = (
                image_claims[0]["mainsnak"]["datavalue"]["value"]
            )
        except (KeyError, TypeError):
            pass

    # Sitelinks: get Wikipedia title
    sitelinks = entity.get("sitelinks", {})
    site_key = f"{LANG}wiki"
    if site_key in sitelinks:
        result["wikipedia_title"] = sitelinks[site_key].get("title")

    return result


def commons_image_url(filename: str, width: int = 320) -> str | None:
    """Return a Wikimedia Commons thumbnail URL for the given filename."""
    if not filename:
        return None
    # Normalise underscores/spaces
    norm = filename.replace(" ", "_")
    return (
        f"https://commons.wikimedia.org/wiki/Special:FilePath/"
        f"{quote(norm, safe='')}?width={width}"
    )


# ---------------------------------------------------------------------------
# Wikipedia helpers
# ---------------------------------------------------------------------------

def fetch_wikipedia_summary(title: str) -> dict | None:
    """Fetch the Wikipedia REST summary for *title*. Returns the summary dict or None."""
    # Sanitise title: replace spaces with underscores for cache key
    safe_title = title.replace(" ", "_")
    cache_file = CACHE_DIR / f"wikipedia_{re.sub(r'[^A-Za-z0-9_-]', '_', safe_title)}.json"
    if cache_file.exists():
        with cache_file.open() as f:
            return json.load(f)

    print(f"Fetching Wikipedia summary for '{title}' …", flush=True)
    try:
        url = f"{WIKIPEDIA_API}/{quote(title, safe='')}"
        resp = SESSION.get(url, timeout=30)
        if resp.status_code == 404:
            print(f"Wikipedia page not found: {title}", flush=True)
            return None
        resp.raise_for_status()
        data = resp.json()
        with cache_file.open("w") as f:
            json.dump(data, f)
        time.sleep(REQUEST_DELAY)
        return data
    except requests.RequestException as exc:
        print(f"Warning: Wikipedia fetch failed for '{title}': {exc}", flush=True)
    return None


def extract_wikipedia_info(summary: dict) -> dict:
    """Extract description and thumbnail URL from a Wikipedia summary response."""
    description = summary.get("extract", "")
    # Truncate to 500 characters at a sentence boundary
    if len(description) > 500:
        truncated = description[:500]
        last_period = truncated.rfind(".")
        description = truncated[: last_period + 1] if last_period > 200 else truncated

    thumbnail = summary.get("thumbnail", {})
    image_url = thumbnail.get("source")

    return {"description": description, "image_url": image_url}


# ---------------------------------------------------------------------------
# Parse OSM wikipedia tag  (format: "en:African bush elephant")
# ---------------------------------------------------------------------------

def parse_osm_wikipedia_tag(tag: str) -> str | None:
    """Return the article title from an OSM wikipedia tag, or None."""
    if not tag:
        return None
    if ":" in tag:
        lang, title = tag.split(":", 1)
        if lang == LANG:
            return title.strip()
        # Different language — skip (could add cross-lang resolution later)
        return None
    return tag.strip()


# ---------------------------------------------------------------------------
# Main enrichment logic
# ---------------------------------------------------------------------------

def enrich() -> list[dict]:
    """Read silver_pois, enrich each row, return list of enrichment dicts.

    When the OSM ``species:wikidata`` tag contains multiple QIDs separated by
    semicolons (e.g. ``Q1;Q2``), one enrichment record is produced per QID so
    that each species gets its own GeoJSON feature with the same geometry.
    """
    con = duckdb.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT osm_id, species_wikidata_id, wikidata_id, wikipedia_tag FROM silver_pois"
        ).fetchall()
    finally:
        con.close()

    enrichments = []
    for osm_id, species_wikidata_id, wikidata_id, wikipedia_tag in rows:
        # Split species:wikidata on ';' to handle multiple species per enclosure
        species_qids = (
            [q.strip() for q in species_wikidata_id.split(";") if q.strip()]
            if species_wikidata_id
            else []
        )

        # Produce one record per species QID; fall back to a single record using
        # the enclosure-level wikidata tag when no species QIDs are present.
        effective_qids = species_qids if species_qids else [wikidata_id]

        for qid in effective_qids:
            record = {
                "osm_id": osm_id,
                "wikidata_id": qid,
                "common_name": None,
                "scientific_name": None,
                "wikipedia_title": None,
                "description": None,
                "image_url": None,
            }

            # --- Wikidata lookup ---
            if qid:
                entity = fetch_wikidata_entity(qid)
                if entity:
                    wd_info = extract_wikidata_info(entity, qid)
                    record["common_name"] = wd_info["common_name"]
                    record["scientific_name"] = wd_info["scientific_name"]
                    if wd_info["wikipedia_title"]:
                        record["wikipedia_title"] = wd_info["wikipedia_title"]
                    if wd_info["image_filename"]:
                        record["image_url"] = commons_image_url(wd_info["image_filename"])

            # --- Wikipedia (from Wikidata sitelink or OSM tag) ---
            wp_title = record["wikipedia_title"] or parse_osm_wikipedia_tag(wikipedia_tag)
            if wp_title:
                summary = fetch_wikipedia_summary(wp_title)
                if summary:
                    wp_info = extract_wikipedia_info(summary)
                    record["wikipedia_title"] = wp_title
                    record["description"] = wp_info["description"]
                    # Prefer Wikidata P18 image; fall back to Wikipedia thumbnail
                    if not record["image_url"]:
                        record["image_url"] = wp_info["image_url"]

            enrichments.append(record)

    return enrichments


def save_enrichments(enrichments: list[dict]) -> None:
    """Write enrichment records into the wiki_enrichment table in DuckDB."""
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
            CREATE OR REPLACE TABLE wiki_enrichment (
                osm_id          BIGINT,
                wikidata_id     VARCHAR,
                common_name     VARCHAR,
                scientific_name VARCHAR,
                wikipedia_title VARCHAR,
                description     VARCHAR,
                image_url       VARCHAR
            )
        """)
        if enrichments:
            con.executemany(
                "INSERT INTO wiki_enrichment VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        r["osm_id"],
                        r["wikidata_id"],
                        r["common_name"],
                        r["scientific_name"],
                        r["wikipedia_title"],
                        r["description"],
                        r["image_url"],
                    )
                    for r in enrichments
                ],
            )
        count = con.execute("SELECT COUNT(*) FROM wiki_enrichment").fetchone()[0]
        print(f"Saved {count} enrichment records into wiki_enrichment", flush=True)
    finally:
        con.close()


def main() -> None:
    enrichments = enrich()
    save_enrichments(enrichments)


if __name__ == "__main__":
    main()
