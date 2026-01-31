{{config(materialized='view')}}

with source_data as (
    select * from {{ source('raw_data', 'prices') }}
)
select * from source_data