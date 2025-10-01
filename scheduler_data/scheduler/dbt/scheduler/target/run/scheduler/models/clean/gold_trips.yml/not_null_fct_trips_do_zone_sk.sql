select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select do_zone_sk
from NY_TAXI.GOLD.fct_trips
where do_zone_sk is null



      
    ) dbt_internal_test