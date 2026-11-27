-- DIM_SOURCE: one row per data source. Static (SCD type 0): the sources are chosen once.
select *
from (values
    (1, 'Falling Fruit', 'https://fallingfruit.org/about/data'),
    (2, 'Huwise / OpenStreetMap - France drinking water', 'https://hub.huwise.com/explore/assets/osm-france-drinking-water/'),
    (3, 'GHSL - GHS-POP R2023A (1 km)', 'https://human-settlement.emergency.copernicus.eu/download.php?ds=pop'),
    (4, 'france-geojson - communes', 'https://github.com/gregoiredavid/france-geojson')
) as sources (source_id, source_name, source_url)
