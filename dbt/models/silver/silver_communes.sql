{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}

-- One row per French commune, with a valid PostGIS polygon (WGS 84).
-- Only the latest loaded file is kept; communes without geometry are dropped.
with latest_file as (
    select file_sha256
    from {{ source('bronze', 'communes') }}
    order by ingested_at desc
    limit 1
)

select
    commune_code,
    commune_name,
    -- Département code: 2 characters (01 to 95, 2A and 2B for Corsica)
    left(commune_code, 2) as departement_code,
    -- ST_MakeValid repairs self-intersections of the simplified file; keep polygons only
    ST_Multi(ST_CollectionExtract(ST_MakeValid(
        ST_SetSRID(ST_GeomFromGeoJSON(geometry_geojson), 4326)), 3)) as geom
from {{ source('bronze', 'communes') }}
where file_sha256 = (select file_sha256 from latest_file)
  and geometry_geojson is not null
