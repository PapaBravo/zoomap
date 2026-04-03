-- Silver model: filter, normalise, and structure bronze POIs.
--
-- Filtering rules:
--   1. Must have attraction=animal (the standard OSM tag for zoo animals).
--
-- All tag values are extracted from the JSON tags column into typed columns.

{{ config(materialized='table') }}

with extracted as (
    select
        osm_id,
        osm_type,
        centroid_lat,
        centroid_lon,
        geom_type,
        geom_coords,
        -- Name field
        json_extract_string(tags, '$.name')                    as name,
        -- Species Wikidata QID (primary enrichment pointer)
        json_extract_string(tags, '$."species:wikidata"')      as species_wikidata_id,
        -- Free-text species tag (fallback)
        json_extract_string(tags, '$.species')                 as species,
        -- OSM wikipedia tag (fallback enrichment pointer)
        json_extract_string(tags, '$.wikipedia')               as wikipedia_tag,
        -- Wikidata QID of the feature itself (kept for debugging)
        json_extract_string(tags, '$.wikidata')                as wikidata_id,
        -- Attraction classification (used for filtering)
        json_extract_string(tags, '$.attraction')              as attraction,
        -- Raw tags kept for debugging / future use
        tags
    from {{ ref('bronze_osm_pois') }}
)

select *
from extracted
where attraction = 'animal'
