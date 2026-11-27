-- FACT_WATER: one row per drinking water point, with the tile it lies in.
-- Factless fact table: counting its rows measures how much water a tile holds.
-- Incremental: a run only writes new points (or points moved to another tile);
-- the post-hook removes the points that disappeared from the latest file.
{{ config(
    materialized='incremental',
    unique_key='water_resource_id',
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} f WHERE NOT EXISTS (SELECT 1 FROM {{ ref('silver_water') }} w
               WHERE {{ water_resource_id('w.osm_type', 'w.osm_id') }} = f.water_resource_id)"
) }}

with points as (
    select
        {{ water_resource_id('osm_type', 'osm_id') }} as water_resource_id,
        {{ tile_id('ST_X(geom)', 'ST_Y(geom)') }} as tile_id
    from {{ ref('silver_water') }}
),

facts as (
    select
        p.tile_id,
        p.water_resource_id,
        s.source_id
    from points p
    -- Only points in a French tile (the tiles cover metropolitan France)
    join {{ ref('silver_tiles') }} t on t.tile_id = p.tile_id
    cross join (select source_id from {{ ref('dim_source') }} where source_name like 'Huwise%') s
)

select f.*
from facts f
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} o
    where o.water_resource_id = f.water_resource_id and o.tile_id = f.tile_id
)
{% endif %}
