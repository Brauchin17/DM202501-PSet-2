
  
    

        create or replace transient table NY_TAXI.GOLD.dim_rate_code
         as
        (

with rates as (
    select distinct
        ratecodeid as rate_code_id,
        ratecode_desc as rate_code_desc
    from NY_TAXI.SILVER.STG_TRIPS
)
select
    row_number() over (order by rate_code_id) as rate_code_sk,
    *
from rates
order by rate_code_id
        );
      
  