select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trip_type_sk
from NY_TAXI.GOLD.fct_trips
where trip_type_sk is null



      
    ) dbt_internal_test