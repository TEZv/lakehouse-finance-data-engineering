select * from {{ ref('fct_events') }} where quantity <= 0 or version <= 0
