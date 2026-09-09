{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='delete+insert') }}

-- Contract: staging contains exactly one current version per event.
-- Per-key comparison avoids losing corrections behind a global watermark.
select source.*
from {{ ref('stg_events') }} source
{% if is_incremental() %}
left join {{ this }} target on source.event_id = target.event_id
where target.event_id is null or source.version > target.version
{% endif %}
