-- Bronze model: expose raw OSM elements with lightweight casting.
-- This model is the single source of truth for downstream silver/gold models.

{{ config(materialized='table') }}

select
    osm_id::bigint                          as osm_id,
    osm_type                                as osm_type,
    centroid_lat::double                    as centroid_lat,
    centroid_lon::double                    as centroid_lon,
    tags                                    as tags,
    geom_type                               as geom_type,
    geom_coords                             as geom_coords
from {{ source('osm', 'raw_osm_elements') }}
where osm_id is not null
  and centroid_lat is not null
  and centroid_lon is not null
