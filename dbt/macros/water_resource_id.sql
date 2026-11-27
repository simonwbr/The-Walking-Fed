{#- Stable integer id of an OSM water point: the OSM id, with the OSM object type as last digit
    (node 1, way 2, relation 3). Shared by dim_water_resource and fact_water. -#}
{% macro water_resource_id(osm_type, osm_id) -%}
    ({{ osm_id }} * 10 + case {{ osm_type }} when 'node' then 1 when 'way' then 2 else 3 end)
{%- endmacro %}
