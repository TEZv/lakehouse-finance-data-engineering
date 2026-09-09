select * from {{ ref('mart_event_summary') }}
where event_count <> (select count(*) from {{ ref('fct_events') }})
   or total_quantity <> (select coalesce(sum(quantity), 0) from {{ ref('fct_events') }})
