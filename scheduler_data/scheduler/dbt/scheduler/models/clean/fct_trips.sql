{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} CLUSTER BY (PICKUP_DATE_SK, PU_ZONE_SK)"
) }}

SELECT
    -- Llave primaria o ID único del viaje
    -- Clave primaria generada
    row_number() over (order by st.pickup_datetime, st.pulocationid) as trip_id,
    
    -- Dimensiones (joins con dims para obtener SKs)
    dd_pickup.date_sk AS pickup_date_sk,  -- Join con dim_date
    dt_pickup.time_sk AS pickup_time_sk,  -- Join con dim_time
    dd_dropoff.date_sk AS dropoff_date_sk,
    dt_dropoff.time_sk AS dropoff_time_sk,
    dz_pu.zone_sk AS pu_zone_sk,  -- Pickup zone
    dz_do.zone_sk AS do_zone_sk,  -- Dropoff zone
    dv.vendor_sk AS vendor_sk,
    dr.rate_code_sk AS rate_code_sk,
    dp.payment_type_sk AS payment_type_sk,
    ds.service_type_sk AS service_type_sk,
    dtt.trip_type_sk AS trip_type_sk,
    
    -- Medidas (facts)
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    passenger_count,
    trip_duration_minutes,
    -- Otros campos como congestion_surcharge, etc., si aplican

FROM NY_TAXI.SILVER.STG_TRIPS AS st  -- Fuente desde Silver
LEFT JOIN {{ ref('dim_date') }} AS dd_pickup ON DATE(st.pickup_datetime) = dd_pickup.full_date
LEFT JOIN {{ ref('dim_time') }} AS dt_pickup ON TIME(st.pickup_datetime) = dt_pickup.full_time
LEFT JOIN {{ ref('dim_date') }} AS dd_dropoff ON DATE(st.dropoff_datetime) = dd_dropoff.full_date
LEFT JOIN {{ ref('dim_time') }} AS dt_dropoff ON TIME(st.dropoff_datetime) = dt_dropoff.full_time
LEFT JOIN {{ ref('dim_zone') }} AS dz_pu ON st.pulocationid = dz_pu.location_id
LEFT JOIN {{ ref('dim_zone') }} AS dz_do ON st.dolocationid = dz_do.location_id
LEFT JOIN {{ ref('dim_vendor') }} AS dv ON st.vendorid = dv.vendor_id
LEFT JOIN {{ ref('dim_rate_code') }} AS dr ON st.ratecodeid = dr.rate_code_id
LEFT JOIN {{ ref('dim_payment_type') }} AS dp ON st.payment_type = dp.payment_type_id
LEFT JOIN {{ ref('dim_service_type') }} AS ds ON st.service_type = ds.service_type
LEFT JOIN {{ ref('dim_trip_type') }} AS dtt ON st.trip_type = dtt.trip_type_id