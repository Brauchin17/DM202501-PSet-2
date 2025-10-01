select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select tolls_amount
from NY_TAXI.GOLD.fct_trips
where tolls_amount is null



      
    ) dbt_internal_test