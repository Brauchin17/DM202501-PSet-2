
    
    

with child as (
    select rate_code_sk as from_field
    from NY_TAXI.GOLD.fct_trips
    where rate_code_sk is not null
),

parent as (
    select rate_code_sk as to_field
    from NY_TAXI.GOLD.dim_rate_code
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


