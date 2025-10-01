
    
    

with child as (
    select dropoff_date_sk as from_field
    from NY_TAXI.GOLD.fct_trips
    where dropoff_date_sk is not null
),

parent as (
    select date_sk as to_field
    from NY_TAXI.GOLD.dim_date
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


