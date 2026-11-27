{#- Id of the GHSL grid cell that contains a WGS 84 point: same formula as the population extraction
    (grid_row * 1,000,000 + grid_col, world grid origin -18,041,000 / 9,000,000 in Mollweide metres). -#}
{% macro tile_id(longitude, latitude) -%}
    (
        floor((9000000 - ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint({{ longitude }}, {{ latitude }}), 4326), 54009)))
              / {{ var('cell_size') }})::bigint * 1000000
      + floor((ST_X(ST_Transform(ST_SetSRID(ST_MakePoint({{ longitude }}, {{ latitude }}), 4326), 54009)) + 18041000)
              / {{ var('cell_size') }})::bigint
    )
{%- endmacro %}
