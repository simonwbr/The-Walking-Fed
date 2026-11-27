{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}

-- One row per Falling Fruit location x type, in France, for forageable plant types only.
-- A location can list several types ("3, 19, 23"): it gives one row per type.
with latest_file as (
    select file_sha256
    from {{ source('bronze', 'fruit_locations') }}
    order by ingested_at desc
    limit 1
),

locations as (
    select *
    from {{ source('bronze', 'fruit_locations') }}
    where file_sha256 = (select file_sha256 from latest_file)
      and coalesce(hidden, 'false') <> 'true'               -- hidden by Falling Fruit moderators
),

location_types as (
    -- distinct: some locations list the same type twice ("3, 3")
    select distinct on (l.id, trim(t.type_id)::int)
        l.*,
        trim(t.type_id)::int as type_id
    from locations l
    cross join lateral unnest(string_to_array(l.type_ids, ',')) as t(type_id)
    order by l.id, trim(t.type_id)::int
),

located as (
    select
        lt.*,
        {{ tile_id('lt.longitude', 'lt.latitude') }} as tile_id
    from location_types lt
)

select
    l.id::bigint || '-' || l.type_id as fruit_key,
    l.id::bigint as location_id,
    l.type_id,
    ty.en_name,
    ty.fr_name,
    ty.scientific_name,
    l.tile_id,
    tiles.commune_code,
    coalesce(l.unverified, 'false') <> 'true' as is_verified,
    coalesce(l.access, 'Unknown') as access,
    l.season_start,
    l.season_stop,
    left(l.created_at, 10)::date as created_date,
    left(l.updated_at, 10)::date as updated_date,
    l.import_link,
    l.longitude,
    l.latitude,
    ST_SetSRID(ST_MakePoint(l.longitude, l.latitude), 4326) as geom,
    l.file_sha256
from located l
join {{ ref('silver_fruit_types') }} ty
  on ty.type_id = l.type_id
 and ty.is_forager and not ty.is_freegan and not ty.is_pending
-- Only points in a French cell: drops the neighbouring countries of the study-area rectangle
join {{ ref('silver_tiles') }} tiles
  on tiles.tile_id = l.tile_id
