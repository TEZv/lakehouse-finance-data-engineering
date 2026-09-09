select
    cast(event_id as varchar) as event_id,
    cast(version as bigint) as version,
    cast(quantity as bigint) as quantity
from read_json_auto('{{ env_var("DBT_INPUT_PATH") | replace("'", "''") }}')
