-- Gold model: join silver POIs with Wikipedia/Wikidata enrichment and shape
-- the result to match the data/animals.geojson schema expected by the frontend.
--
-- Output columns:
--   feature_id   Slugified identifier (used as GeoJSON properties.id)
--   name         Display name (English preferred)
--   species      Scientific name
--   description  Wikipedia extract (≤500 chars)
--   enclosure    Enclosure / exhibit name
--   wikipedia    Full Wikipedia URL
--   image        Image URL
--   geom_type    GeoJSON geometry type
--   geom_coords  JSON-serialised coordinates
--   osm_id       Source OSM element ID (for traceability)
--   osm_type     Source OSM element type

{{ config(materialized='table') }}

with silver as (
    select * from {{ ref('silver_pois') }}
),

wiki as (
    select * from {{ source('wiki', 'wiki_enrichment') }}
),

joined as (
    select
        s.osm_id,
        s.osm_type,
        s.geom_type,
        s.geom_coords,
        coalesce(s.name_en, s.name)                             as name,
        coalesce(w.scientific_name, s.species)                  as species,
        w.description                                           as description,
        -- Use element name as the enclosure name for enclosure ways;
        -- for animal nodes the enclosure is usually their own name
        coalesce(s.name_en, s.name)                             as enclosure,
        -- Build Wikipedia URL from enrichment title or OSM tag
        case
            when w.wikipedia_title is not null
                then 'https://' || '{{ var("wikipedia_lang", "en") }}' || '.wikipedia.org/wiki/'
                     || replace(w.wikipedia_title, ' ', '_')
            when s.wikipedia_tag is not null
                 and s.wikipedia_tag like '%:%'
                then 'https://'
                     || split_part(s.wikipedia_tag, ':', 1)
                     || '.wikipedia.org/wiki/'
                     || replace(split_part(s.wikipedia_tag, ':', 2), ' ', '_')
            else null
        end                                                     as wikipedia,
        w.image_url                                             as image,
        s.wikidata_id
    from silver s
    left join wiki w on s.osm_id = w.osm_id
    where coalesce(s.name_en, s.name) is not null
),

slugified as (
    select
        -- Slugify the name: lowercase, replace non-alphanumeric runs with '-'
        regexp_replace(
            regexp_replace(
                lower(coalesce(name, 'unknown-' || osm_id::varchar)),
                '[^a-z0-9]+', '-'
            ),
            '^-+|-+$', ''
        )                                                       as feature_id,
        name,
        species,
        description,
        enclosure,
        wikipedia,
        image,
        geom_type,
        geom_coords,
        osm_id,
        osm_type,
        wikidata_id
    from joined
)

select * from slugified
