-- One row per Falling Fruit type, latest file only, with clean names and flags.
with latest_file as (
    select file_sha256
    from {{ source('bronze', 'fruit_types') }}
    order by ingested_at desc
    limit 1
)

select
    id::int as type_id,
    nullif(parent_id, '')::int as parent_type_id,
    scientific_name,
    coalesce(taxonomic_rank, 'Unknown') as taxonomic_rank,
    en_name,
    fr_name,
    en_wikipedia_url,
    coalesce(category_mask, '') as category_mask,
    -- forager: plants you can pick; freegan: dumpsters, markets... (not a plant)
    coalesce(category_mask, '') like '%forager%' as is_forager,
    coalesce(category_mask, '') like '%freegan%' as is_freegan,
    pending = 'true' as is_pending
from {{ source('bronze', 'fruit_types') }}
where file_sha256 = (select file_sha256 from latest_file)
