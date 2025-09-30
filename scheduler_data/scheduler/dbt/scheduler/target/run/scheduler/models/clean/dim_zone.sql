
  
    

        create or replace transient table NY_TAXI.GOLD.dim_zone
         as
        (

with zones as (
    select distinct
        pulocationid as location_id,
        pickup_borough as borough,
        pickup_zone as zone,
        pickup_service_zone as service_zone
    from NY_TAXI.SILVER.STG_TRIPS
    union
    select distinct
        dolocationid,
        dropoff_borough,
        dropoff_zone,
        dropoff_service_zone
    from NY_TAXI.SILVER.STG_TRIPS
)
select
    row_number() over (order by location_id) as zone_sk,
    *
from zones
order by location_id
        );
      
  