-- Cleaned drinking water points: latest downloaded file only, metropolitan France,
-- one row per OSM element, typed columns and a PostGIS point geometry.

with latest_file as (
    -- bronze can hold several file versions: keep the most recent one
    select file_sha256
    from {{ source('bronze', 'water_points') }}
    order by ingested_at desc
    limit 1
),

deduplicated as (
    select
        b.*,
        row_number() over (
            partition by b.meta_osm_type, b.meta_osm_id
            order by b.loaded_at desc
        ) as row_num
    from {{ source('bronze', 'water_points') }} as b
    join latest_file using (file_sha256)
)

select
    meta_osm_type || '/' || meta_osm_id                    as water_point_key,
    meta_osm_type                                           as osm_type,
    meta_osm_id::bigint                                     as osm_id,
    nullif(trim(name), '')                                  as name,
    case lower(fee)
        when 'yes' then 'paid'
        when 'no' then 'free'
        else 'unknown'
    end                                                     as access_fee,
    case lower(indoor)
        when 'yes' then 'indoor'
        when 'no' then 'outdoor'
        else 'unknown'
    end                                                     as location_type,
    case lower(accepts_bottle)
        when 'yes' then true
        when 'no' then false
    end                                                     as accepts_bottle,
    meta_code_com                                           as commune_code,
    meta_name_com                                           as commune_name,
    meta_code_dep                                           as departement_code,
    meta_name_dep                                           as departement_name,
    meta_code_reg                                           as region_code,
    left(meta_first_update, 10)::date                       as first_update_date,
    left(meta_last_update, 10)::date                        as last_update_date,
    meta_versions_number::numeric::int                      as versions_number,
    meta_users_number::numeric::int                         as users_number,
    longitude,
    latitude,
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)     as geom,
    file_sha256,
    ingested_at
from deduplicated
where row_num = 1
  -- overseas regions (Guadeloupe, Martinique, Guyane, La Réunion, Mayotte) are out of scope
  and meta_code_reg not in ('01', '02', '03', '04', '06')