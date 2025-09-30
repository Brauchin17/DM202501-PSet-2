
  
    

        create or replace transient table NY_TAXI.GOLD.dim_service_type
         as
        (

with services as (
    select distinct
        service_type
    from NY_TAXI.SILVER.STG_TRIPS
)
select
    row_number() over (order by service_type) as service_type_sk,
    service_type
from services
order by service_type
        );
      
  