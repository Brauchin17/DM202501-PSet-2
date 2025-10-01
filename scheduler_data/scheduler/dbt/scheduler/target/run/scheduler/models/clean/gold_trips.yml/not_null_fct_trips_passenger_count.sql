select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select passenger_count
from NY_TAXI.GOLD.fct_trips
where passenger_count is null



      
    ) dbt_internal_test