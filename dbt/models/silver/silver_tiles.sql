{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}

-- One row per 1 km population cell whose centre lies in a French commune.
-- Cells outside France (sea, neighbouring countries) are dropped by the join.
with latest_file as (
    select file_sha256
    from {{ source('bronze', 'population_tiles') }}
    order by extracted_at desc
    limit 1
),

tiles as (
    select *
    from {{ source('bronze', 'population_tiles') }}
    where file_sha256 = (select file_sha256 from latest_file)
),

located as (
    -- A centre exactly on a border between two communes would match both: keep one
    select distinct on (t.tile_id)
        t.*,
        c.commune_code,
        c.commune_name,
        c.departement_code
    from tiles t
    join {{ ref('silver_communes') }} c
      on ST_Within(ST_SetSRID(ST_MakePoint(t.center_lon, t.center_lat), 4326), c.geom)
    order by t.tile_id, c.commune_code
)

select
    tile_id,
    grid_row,
    grid_col,
    center_lon as longitude,
    center_lat as latitude,
    population,
    cell_size,
    commune_code,
    commune_name,
    departement_code,
    'France' as country,
    epoch,
    file_sha256,
    -- Cell square, built in the grid projection (Mollweide) then converted to WGS 84
    ST_Transform(ST_MakeEnvelope(x_min, y_max - cell_size, x_min + cell_size, y_max, 54009), 4326) as geom
from located
