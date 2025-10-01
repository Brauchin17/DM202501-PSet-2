select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select payment_type_sk as from_field
    from NY_TAXI.GOLD.fct_trips
    where payment_type_sk is not null
),

parent as (
    select payment_type_sk as to_field
    from NY_TAXI.GOLD.dim_payment_type
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test