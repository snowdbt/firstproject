{{ config(materialized='table', transient=true) }}

select *
from persons_raw
where status = 'NOTPROCESSED'
