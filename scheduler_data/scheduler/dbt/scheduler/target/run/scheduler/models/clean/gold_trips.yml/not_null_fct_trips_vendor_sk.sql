select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select vendor_sk
from NY_TAXI.GOLD.fct_trips
where vendor_sk is null



      
    ) dbt_internal_test