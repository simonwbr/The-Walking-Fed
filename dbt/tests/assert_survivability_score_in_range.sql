-- The survivability score must stay between 0 and 100
select * from {{ ref('dim_tiles') }} where survivability_score < 0 or survivability_score > 100
