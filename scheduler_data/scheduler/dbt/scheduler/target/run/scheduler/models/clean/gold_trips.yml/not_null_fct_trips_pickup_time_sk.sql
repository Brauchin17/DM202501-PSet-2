select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pickup_time_sk
from NY_TAXI.GOLD.fct_trips
where pickup_time_sk is null



      
    ) dbt_internal_test