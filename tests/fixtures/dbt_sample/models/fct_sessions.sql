with events as (
    select * from {{ ref('stg_events') }}
)
select id
from events
