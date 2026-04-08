{{ config(materialized='view') }}
select id, user_id
from {{ source('raw', 'events') }}
