-- DIM_FRUIT_RESOURCE: one row per kind of fruit resource (Falling Fruit type), SCD type 1.
-- category and quality are set from the genus (first word of the scientific name):
--   quality = how much the resource helps to survive (good: real food; medium: needs work or
--   is a side food, e.g. acorns, linden leaves; bad: little food value, e.g. ash seeds, maple sap).
with types as (
    select
        type_id,
        initcap(coalesce(en_name, scientific_name)) as name,
        split_part(scientific_name, ' ', 1) as genus
    from {{ ref('silver_fruit_types') }}
    where is_forager and not is_freegan and not is_pending
),

categorised as (
    select
        *,
        case
            when genus in ('Malus', 'Pyrus', 'Prunus', 'Ficus', 'Cydonia', 'Mespilus', 'Eriobotrya',
                           'Diospyros', 'Citrus', 'Olea', 'Punica', 'Ziziphus', 'Actinidia', 'Asimina')
                then 'tree fruit'
            when genus in ('Rubus', 'Morus', 'Sambucus', 'Ribes', 'Vaccinium', 'Fragaria', 'Amelanchier',
                           'Sorbus', 'Crataegus', 'Celtis', 'Arbutus', 'Rosa', 'Vitis', 'Cornus',
                           'Elaeagnus', 'Mahonia', 'Berberis', 'Hippophae')
                then 'berry'
            when genus in ('Juglans', 'Corylus', 'Castanea', 'Carya', 'Fagus', 'Quercus', 'Pinus', 'Ginkgo')
                then 'nut'
            when genus in ('Tilia', 'Robinia', 'Cercis', 'Urtica', 'Allium', 'Taraxacum', 'Malva',
                           'Portulaca', 'Rumex', 'Chenopodium', 'Plantago')
                then 'leaves and flowers'
            else 'other'
        end as category
    from types
)

select
    type_id as fruit_resource_id,
    category,
    case
        when category in ('tree fruit', 'berry') then 'good'
        when genus in ('Juglans', 'Corylus', 'Castanea', 'Carya') then 'good'
        when category in ('nut', 'leaves and flowers') then 'medium'
        else 'bad'
    end as quality,
    name
from categorised
