{{ config(materialized='view', transient=true) }}

select *
from persons_raw
where status = 'NOTPROCESSED'
