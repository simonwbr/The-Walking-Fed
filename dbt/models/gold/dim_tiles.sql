-- DIM_TILES: one row per 1 km tile of metropolitan France, SCD type 1.
-- Binds the two fact tables and carries the precomputed survivability score.
{% set fw = var('fruit_quality_weights') %}
{% set ww = var('water_quality_weights') %}

with food as (
    select
        f.tile_id,
        sum(case r.quality when 'good' then {{ fw['good'] }} when 'medium' then {{ fw['medium'] }}
                           else {{ fw['bad'] }} end) as food_points
    from {{ ref('fact_fruit') }} f
    join {{ ref('dim_fruit_resource') }} r using (fruit_resource_id)
    group by f.tile_id
),

water as (
    select
        f.tile_id,
        sum(case r.quality when 'good' then {{ ww['good'] }} when 'medium' then {{ ww['medium'] }}
                           else {{ ww['bad'] }} end) as water_points
    from {{ ref('fact_water') }} f
    join {{ ref('dim_water_resource') }} r using (water_resource_id)
    group by f.tile_id
),

scores as (
    select
        t.tile_id,
        t.latitude,
        t.longitude,
        t.commune_name as city,
        t.country,
        t.population,
        -- Each score grows with the amount and tends to 1 (x / (x + half) = 0.5 when x = half)
        coalesce(food.food_points, 0) / (coalesce(food.food_points, 0) + {{ var('food_half_saturation') }}) as food_score,
        coalesce(water.water_points, 0) / (coalesce(water.water_points, 0) + {{ var('water_half_saturation') }}) as water_score,
        t.population / (t.population + {{ var('zombie_half_population') }}) as zombie_threat
    from {{ ref('silver_tiles') }} t
    left join food using (tile_id)
    left join water using (tile_id)
)

select
    tile_id,
    latitude,
    longitude,
    city,
    country,
    population,
    -- 0 to 100: you need both food and water (geometric mean), and fewer zombies is safer
    round((100 * sqrt(food_score * water_score) * (1 - zombie_threat))::numeric, 2)::double precision
        as survivability_score
from scores
