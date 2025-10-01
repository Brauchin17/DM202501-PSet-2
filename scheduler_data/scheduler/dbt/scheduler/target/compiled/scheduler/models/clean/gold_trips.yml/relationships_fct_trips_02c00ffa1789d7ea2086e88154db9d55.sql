
    
    

with child as (
    select service_type_sk as from_field
    from NY_TAXI.GOLD.fct_trips
    where service_type_sk is not null
),

parent as (
    select service_type_sk as to_field
    from NY_TAXI.GOLD.dim_service_type
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


