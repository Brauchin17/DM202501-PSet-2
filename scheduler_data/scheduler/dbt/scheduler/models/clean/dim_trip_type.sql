{{ config(materialized='table') }}

with trips as (
    select distinct
        cast(trip_type as int) as trip_type_id,  -- Cas tea si es float con .0
        trip_type_desc
    from NY_TAXI.SILVER.STG_TRIPS
)
select
    row_number() over (order by trip_type_id) as trip_type_sk,
    *
from trips
order by trip_type_id