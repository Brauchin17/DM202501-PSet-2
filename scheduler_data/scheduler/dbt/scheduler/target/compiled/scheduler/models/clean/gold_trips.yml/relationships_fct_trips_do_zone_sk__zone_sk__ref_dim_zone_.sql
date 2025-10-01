
    
    

with child as (
    select do_zone_sk as from_field
    from NY_TAXI.GOLD.fct_trips
    where do_zone_sk is not null
),

parent as (
    select zone_sk as to_field
    from NY_TAXI.GOLD.dim_zone
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


