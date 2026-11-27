-- The Walking Fed: answers to the business questions of Assignment 1, on the gold layer.
-- Run: docker compose exec -T postgres psql -U walkingfed -d walking_fed < sql/business_questions.sql

\echo '=== Q1. Are highly populated areas safer than rural areas? ==='
-- Threshold: 1,500 inhabitants per km2 (Eurostat's density threshold for urban centres)
SELECT
    CASE WHEN population > 1500 THEN 'highly_populated' ELSE 'low_population' END AS population_group,
    COUNT(*) AS tiles,
    ROUND(AVG(survivability_score)::numeric, 2) AS avg_survivability_score,
    ROUND(100.0 * AVG(CASE WHEN survivability_score > 0 THEN 1 ELSE 0 END), 1) AS pct_tiles_with_food_and_water
FROM gold.dim_tiles
WHERE country = 'France'
GROUP BY population_group
ORDER BY avg_survivability_score DESC;

\echo '=== Q2. What is the most prevalent food type in France? ==='
SELECT r.name, r.category, r.quality, COUNT(*) AS occurrences
FROM gold.fact_fruit f
JOIN gold.dim_fruit_resource r ON r.fruit_resource_id = f.fruit_resource_id
JOIN gold.dim_tiles t ON t.tile_id = f.tile_id
WHERE t.country = 'France'
GROUP BY r.name, r.category, r.quality
ORDER BY occurrences DESC
LIMIT 5;

\echo '--- Q2 by category'
SELECT r.category, COUNT(*) AS occurrences
FROM gold.fact_fruit f
JOIN gold.dim_fruit_resource r ON r.fruit_resource_id = f.fruit_resource_id
GROUP BY r.category
ORDER BY occurrences DESC;

\echo '=== Q3. Should I flee my current location or stay? ==='
-- Change the coordinates of "me" below. Neighbouring tiles: tile_id +/- 1 (west/east)
-- and +/- 1,000,000 (north/south), so the 8 neighbours have a fixed offset.
WITH me AS (
    SELECT 4.8357 AS longitude, 45.7640 AS latitude       -- Lyon, place Bellecour
),
my_tile AS (
    SELECT
        floor((9000000 - ST_Y(p)) / 1000)::bigint * 1000000 + floor((ST_X(p) + 18041000) / 1000)::bigint AS tile_id
    FROM (SELECT ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 54009) AS p FROM me) m
),
neighbourhood AS (
    SELECT t.*, (t.tile_id = m.tile_id) AS is_my_tile
    FROM gold.dim_tiles t, my_tile m
    WHERE t.tile_id IN (
        m.tile_id, m.tile_id - 1, m.tile_id + 1,
        m.tile_id - 1000000, m.tile_id - 1000000 - 1, m.tile_id - 1000000 + 1,
        m.tile_id + 1000000, m.tile_id + 1000000 - 1, m.tile_id + 1000000 + 1)
)
SELECT
    tile_id, city, ROUND(latitude::numeric, 4) AS latitude, ROUND(longitude::numeric, 4) AS longitude,
    ROUND(population::numeric) AS population, survivability_score, is_my_tile,
    CASE
        WHEN is_my_tile AND survivability_score >= MAX(survivability_score) OVER () THEN 'STAY: no neighbour is better'
        WHEN is_my_tile THEN 'you are here'
        WHEN survivability_score = MAX(survivability_score) OVER ()
         AND survivability_score > MAX(CASE WHEN is_my_tile THEN survivability_score END) OVER () THEN 'FLEE HERE'
    END AS advice
FROM neighbourhood
ORDER BY survivability_score DESC, is_my_tile DESC;

\echo '=== Q4. Which city is the safest in France? ==='
-- Average over the city's tiles; cities with at least 5 tiles (5 km2), so that one lucky tile
-- does not make a village "the safest city"
SELECT city, COUNT(*) AS tiles, ROUND(SUM(population)::numeric) AS population,
       ROUND(AVG(survivability_score)::numeric, 2) AS avg_survivability
FROM gold.dim_tiles
WHERE country = 'France'
GROUP BY city
HAVING COUNT(*) >= 5
ORDER BY avg_survivability DESC
LIMIT 10;

\echo '=== Q5. How many of the water sources are of good quality? ==='
SELECT
    COUNT(*) AS water_sources,
    COUNT(CASE WHEN r.quality = 'good' THEN 1 END) AS good_sources,
    ROUND(COUNT(CASE WHEN r.quality = 'good' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS good_water_pct
FROM gold.fact_water f
JOIN gold.dim_water_resource r ON r.water_resource_id = f.water_resource_id;

\echo '--- Q5 by quality'
SELECT r.quality, COUNT(*) AS water_sources
FROM gold.fact_water f
JOIN gold.dim_water_resource r ON r.water_resource_id = f.water_resource_id
GROUP BY r.quality
ORDER BY r.quality;

\echo '=== KPI 1. Data Ingestion Rate: rows ingested into bronze per source, last load ==='
SELECT 'Falling Fruit locations' AS source, COUNT(*) AS rows_ingested, MAX(ingested_at) AS last_ingested_at FROM bronze.fruit_locations
UNION ALL SELECT 'Falling Fruit types', COUNT(*), MAX(ingested_at) FROM bronze.fruit_types
UNION ALL SELECT 'OSM drinking water', COUNT(*), MAX(ingested_at) FROM bronze.water_points
UNION ALL SELECT 'GHSL population cells', COUNT(*), MAX(extracted_at) FROM bronze.population_tiles
UNION ALL SELECT 'Communes', COUNT(*), MAX(ingested_at) FROM bronze.communes;

\echo '=== KPI 2. Usable Data Percentage: share of bronze records that reach the gold layer ==='
WITH fruit_links AS (
    -- One record per location x type, as in fact_fruit
    SELECT COUNT(DISTINCT (id, trim(t))) AS n
    FROM bronze.fruit_locations, unnest(string_to_array(type_ids, ',')) AS t
)
SELECT 'Falling Fruit (location x type)' AS source, (SELECT n FROM fruit_links) AS bronze_records,
       (SELECT COUNT(*) FROM gold.fact_fruit) AS gold_records,
       ROUND(100.0 * (SELECT COUNT(*) FROM gold.fact_fruit) / (SELECT n FROM fruit_links), 1) AS usable_pct
UNION ALL
SELECT 'OSM drinking water', (SELECT COUNT(*) FROM bronze.water_points), (SELECT COUNT(*) FROM gold.fact_water),
       ROUND(100.0 * (SELECT COUNT(*) FROM gold.fact_water) / (SELECT COUNT(*) FROM bronze.water_points), 1)
UNION ALL
SELECT 'GHSL population cells', (SELECT COUNT(*) FROM bronze.population_tiles), (SELECT COUNT(*) FROM gold.dim_tiles),
       ROUND(100.0 * (SELECT COUNT(*) FROM gold.dim_tiles) / (SELECT COUNT(*) FROM bronze.population_tiles), 1);
