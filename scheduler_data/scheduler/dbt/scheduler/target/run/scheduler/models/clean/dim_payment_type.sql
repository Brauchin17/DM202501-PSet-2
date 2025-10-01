
  
    

        create or replace transient table NY_TAXI.GOLD.dim_payment_type
         as
        (

with payments as (
    select distinct
        payment_type as payment_type_id,
        payment_type_desc as payment_type_desc
    from NY_TAXI.SILVER.STG_TRIPS
)
select
    row_number() over (order by payment_type_id) as payment_type_sk,
    *
from payments
order by payment_type_id
        );
      
  