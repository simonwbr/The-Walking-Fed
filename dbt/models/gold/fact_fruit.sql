-- FACT_FRUIT: one row per fruit resource found in a tile (one Falling Fruit location x type).
-- Factless fact table: counting its rows measures how much food a tile holds.
-- Incremental, tile by tile: a run only rewrites the tiles whose fruit resources changed
-- (delete the tile's rows, insert its current rows); the post-hook empties the tiles that
-- no longer hold any fruit resource.
{{ config(
    materialized='incremental',
    unique_key='tile_id',
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} f WHERE NOT EXISTS (SELECT 1 FROM {{ ref('silver_fruit') }} s
               WHERE s.tile_id = f.tile_id)"
) }}

with facts as (
    select
        f.tile_id,
        f.type_id as fruit_resource_id,
        s.source_id
    from {{ ref('silver_fruit') }} f
    join {{ ref('dim_fruit_resource') }} r on r.fruit_resource_id = f.type_id
    cross join (select source_id from {{ ref('dim_source') }} where source_name = 'Falling Fruit') s
)

{% if is_incremental() %}
, changed_tiles as (
    -- Tiles where the number of resources of some type differs between the new data and the table
    select coalesce(n.tile_id, o.tile_id) as tile_id
    from (select tile_id, fruit_resource_id, count(*) as n from facts group by 1, 2) n
    full join (select tile_id, fruit_resource_id, count(*) as n from {{ this }} group by 1, 2) o
      on o.tile_id = n.tile_id and o.fruit_resource_id = n.fruit_resource_id
    where n.n is distinct from o.n
    group by 1
)

select * from facts where tile_id in (select tile_id from changed_tiles)
{% else %}
select * from facts
{% endif %}
