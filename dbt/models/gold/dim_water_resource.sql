-- DIM_WATER_RESOURCE: one row per drinking water point, SCD type 1.
-- Incremental: a run only writes the points that are new or whose attributes changed
-- (type 1: the old values are overwritten), and the post-hook removes the points that
-- disappeared from the latest OpenStreetMap file.
-- quality counts three criteria that matter when survival depends on it:
--   free to use, reachable outdoors (no building to enter), and recently checked on OpenStreetMap
--   (updated in the last 5 years). 3 criteria = good, 2 = medium, 0 or 1 = bad.
{{ config(
    materialized='incremental',
    unique_key='water_resource_id',
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} d WHERE NOT EXISTS (SELECT 1 FROM {{ ref('silver_water') }} w
               WHERE {{ water_resource_id('w.osm_type', 'w.osm_id') }} = d.water_resource_id)"
) }}

with points as (
    select
        {{ water_resource_id('osm_type', 'osm_id') }} as water_resource_id,
        name,
        access_fee,
        location_type,
        last_update_date
    from {{ ref('silver_water') }}
),

scored as (
    select
        *,
        (case when access_fee <> 'paid' then 1 else 0 end)
      + (case when location_type <> 'indoor' then 1 else 0 end)
      + (case when last_update_date >= current_date - interval '5 years' then 1 else 0 end) as criteria_met
    from points
),

resources as (
    select
        water_resource_id,
        case
            when access_fee = 'paid' then 'paid drinking water'
            when location_type = 'indoor' then 'indoor drinking water'
            else 'public drinking water'
        end as category,
        case criteria_met when 3 then 'good' when 2 then 'medium' else 'bad' end as quality,
        coalesce(name, 'Drinking water point') as name
    from scored
)

select r.*
from resources r
{% if is_incremental() %}
-- Only new points, or points whose category, quality or name changed
where not exists (
    select 1 from {{ this }} d
    where d.water_resource_id = r.water_resource_id
      and d.category = r.category and d.quality = r.quality and d.name = r.name
)
{% endif %}
