-- Silver model: filter, normalise, and structure bronze POIs.
--
-- Filtering rules:
--   1. Must have at least one of: zoo, animal, species, wikidata, wikipedia tags.
--   2. Exclude the outer zoo boundary (tourism=zoo without any animal-specific tag).
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
        -- Name fields
        json_extract_string(tags, '$.name')        as name,
        coalesce(
            json_extract_string(tags, '$."name:en"'),
            json_extract_string(tags, '$.name')
        )                                          as name_en,
        -- Animal / taxonomy tags
        json_extract_string(tags, '$.species')     as species,
        json_extract_string(tags, '$.animal')      as animal_tag,
        -- Zoo-specific classification
        json_extract_string(tags, '$.zoo')         as zoo_tag,
        -- Enrichment pointer tags
        json_extract_string(tags, '$.wikidata')    as wikidata_id,
        json_extract_string(tags, '$.wikipedia')   as wikipedia_tag,
        -- Tourism classification (used to exclude outer boundary)
        json_extract_string(tags, '$.tourism')     as tourism_tag,
        -- Raw tags kept for debugging / future use
        tags
    from {{ ref('bronze_osm_pois') }}
)

select *
from extracted
where
    -- Must carry at least one animal-identifying tag
    (
        zoo_tag        is not null
        or animal_tag  is not null
        or species     is not null
        or wikidata_id is not null
        or (wikipedia_tag is not null and wikipedia_tag <> '')
    )
    -- Exclude the outer zoo boundary polygon (tourism=zoo with no animal tags)
    and not (
        tourism_tag = 'zoo'
        and zoo_tag        is null
        and animal_tag     is null
        and species        is null
        and wikidata_id    is null
    )
