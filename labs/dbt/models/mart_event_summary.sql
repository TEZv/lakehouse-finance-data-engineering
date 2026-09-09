-- Quantity is a synthetic unit count, not revenue or cross-currency exposure.
select count(*) as event_count, coalesce(sum(quantity), 0) as total_quantity
from {{ ref('fct_events') }}
