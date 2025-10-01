

with vendors as (
    select distinct
        vendorid as vendor_id,
        vendorid_desc as vendor_name
    from NY_TAXI.SILVER.STG_TRIPS
)
select
    row_number() over (order by vendor_id) as vendor_sk,
    *
from vendors
order by vendor_id