select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dropoff_date_sk
from NY_TAXI.GOLD.fct_trips
where dropoff_date_sk is null



      
    ) dbt_internal_test