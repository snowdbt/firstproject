{{ config(materialized='incremental', unique_key='PERSONID') }}
select *
from {{ ref('PERSONS_RAW_stg') }}