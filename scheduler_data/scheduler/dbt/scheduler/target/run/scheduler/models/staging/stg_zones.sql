
  
    

        create or replace transient table NY_TAXI.SILVER.stg_zones
         as
        (
-- unir tablas
with yellow as (
  select
    "run_id" as RUN_ID,
    "VendorID" as VENDORID,
    "tpep_pickup_datetime" as PICKUP_DATETIME,
    "tpep_dropoff_datetime" as DROPOFF_DATETIME,
    "passenger_count" as PASSENGER_COUNT,
    "trip_distance" as TRIP_DISTANCE,
    "RatecodeID" as RATECODEID,
    "store_and_fwd_flag" as STORE_AND_FWD_FLAG,
    "PULocationID" as PULOCATIONID,
    "DOLocationID" as DOLOCATIONID,
    "payment_type" as PAYMENT_TYPE,
    "fare_amount" as FARE_AMOUNT,
    "extra" as EXTRA,
    "mta_tax" as MTA_TAX,
    "tip_amount" as TIP_AMOUNT,
    "tolls_amount" as TOLLS_AMOUNT,
    "improvement_surcharge" as IMPROVEMENT_SURCHARGE,
    "total_amount" as TOTAL_AMOUNT,
    "congestion_surcharge" as CONGESTION_SURCHARGE,
    "cbd_congestion_fee" as CBD_CONGESTION_FEE,
    null as EHAIL_FEE,
    null as TRIP_TYPE,
    "Airport_fee" as AIRPORT_FEE,
    "fuente" as SERVICE_TYPE,
    "ventana_temporal" as INGEST_TIMESTAMP,
    "tamano" as FILE_SIZE,
    "lote_mes" as BATCH_YEAR_MONTH
  from NY_TAXI.RAW."yellow_taxi_trip"
),
green as (
  select
    "run_id" as RUN_ID,
    "VendorID" as VENDORID,
    "lpep_pickup_datetime" as PICKUP_DATETIME,
    "lpep_dropoff_datetime" as DROPOFF_DATETIME,
    "passenger_count" as PASSENGER_COUNT,
    "trip_distance" as TRIP_DISTANCE,
    "RatecodeID" as RATECODEID,
    "store_and_fwd_flag" as STORE_AND_FWD_FLAG,
    "PULocationID" as PULOCATIONID,
    "DOLocationID" as DOLOCATIONID,
    "payment_type" as PAYMENT_TYPE,
    "fare_amount" as FARE_AMOUNT,
    "extra" as EXTRA,
    "mta_tax" as MTA_TAX,
    "tip_amount" as TIP_AMOUNT,
    "tolls_amount" as TOLLS_AMOUNT,
    "improvement_surcharge" as IMPROVEMENT_SURCHARGE,
    "total_amount" as TOTAL_AMOUNT,
    "congestion_surcharge" as CONGESTION_SURCHARGE,
    "cbd_congestion_fee" as CBD_CONGESTION_FEE,
    "ehail_fee" as EHAIL_FEE,
    "trip_type" as TRIP_TYPE,
    null as AIRPORT_FEE,
    "fuente" as SERVICE_TYPE,
    "ventana_temporal" as INGEST_TIMESTAMP,
    "tamano" as file_size,
    "lote_mes" as BATCH_YEAR_MONTH
  from NY_TAXI.RAW."green_taxi_trip"
),
unioned_trips as (
  select * from green
  union all
  select * from yellow
),
-- Estandarización de zonas horarias y normalización
standardized_trips as (
    select
        *,
        -- Estandarizar zonas horarias
        convert_timezone('UTC', 'America/New_York', PICKUP_DATETIME) as PICKUP_DATETIME_EST,
        convert_timezone('UTC', 'America/New_York', DROPOFF_DATETIME) as DROPOFF_DATETIME_EST,
        
        -- Normalizar
        case VENDORID
            when 1 then 'Creative Mobile Technologies, LLC'
            when 2 then 'Curb Mobility, LLC'
            when 6 then 'Myle Technologies Inc'
            when 7 then 'Helix'
            else 'Not specified'
        end as VENDORID_DESC,

        case RATECODEID
            when 1 then 'Standard rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated fare'
            when 6 then 'Group ride'
            else 'Unknown'
        end as RATECODE_DESC,

        case PAYMENT_TYPE
            when 0 then 'Flex Fare trip '
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
            else 'Not specified'
        end as PAYMENT_TYPE_DESC,       
        
        case TRIP_TYPE
            when 1 then 'Street-hall'
            when 2 then 'Dispatch'
            else 'Unknown'
        end as TRIP_TYPE_DESC,

        case STORE_AND_FWD_FLAG
            when 'Y' then 'Yes'
            when 'N' then 'No'
            else 'Unknown'
        end as STORE_AND_FWD_FLAG_DESC,

        -- Calidad de datos: casos con valores negativos
        case 
            when TRIP_DISTANCE < 0 then 0
            else TRIP_DISTANCE
        end as TRIP_DISTANCE_CLEANED,
        
        case 
            when FARE_AMOUNT < 0 then 0
            else FARE_AMOUNT
        end as FARE_AMOUNT_CLEANED,
        
        case 
            when TIP_AMOUNT < 0 then 0
            else TIP_AMOUNT
        end as TIP_AMOUNT_CLEANED,
        
        -- Validación de fechas: timing correcto
        case 
            when PICKUP_DATETIME > DROPOFF_DATETIME then 1
            else 0
        end as HAS_INVALID_TIMING,
        
        -- Duración del viaje en minutos
        datediff('minute', PICKUP_DATETIME, DROPOFF_DATETIME) as TRIP_DURATION_MINUTES,
        
        -- Validación de datos requeridos
        case 
            when PULOCATIONID is null or DOLOCATIONID is null then 1
            else 0
        end as HAS_MISSING_LOCATION_IDS

    from unioned_trips
),
-- Enriquecer con Taxi Zones
enriched_with_zones as (
    select
        st.*,
        -- Información de pickup location
        pz."Zone" as PICKUP_ZONE,
        pz."Borough" as PICKUP_BOROUGH, --distritos gubernamentales
        pz."service_zone" as PICKUP_SERVICE_ZONE,
        
        -- Información de dropoff location  
        dz."Zone" as DROPOFF_ZONE,
        dz."Borough" as DROPOFF_BOROUGH,
        dz."service_zone" as DROPOFF_SERVICE_ZONE,
        
        -- Flags de calidad basados en zonas
        case 
            when pz."LocationID" is null then 1
            else 0
        end as HAS_INVALID_PICKUP_ZONE,
        
        case 
            when dz."LocationID" is null then 1
            else 0
        end as HAS_INVALID_DROPOFF_ZONE

    from standardized_trips st
    left join "NY_TAXI"."RAW"."taxi_zones" pz 
        on st.PULOCATIONID = pz."LocationID"
    left join "NY_TAXI"."RAW"."taxi_zones" dz 
        on st.DOLOCATIONID = dz."LocationID"
),
-- Métricas adicionales y limpieza final
final as (
    select
        -- Identificadores y metadatos
        RUN_ID,
        INGEST_TIMESTAMP,
        SERVICE_TYPE,
        
        -- Fechas y tiempos
        BATCH_YEAR_MONTH,
        PICKUP_DATETIME_EST as PICKUP_DATETIME,
        DROPOFF_DATETIME_EST as DROPOFF_DATETIME,
        TRIP_DURATION_MINUTES,
        
        -- Datos del viaje
        VENDORID,
        VENDORID_DESC,
        PASSENGER_COUNT,
        TRIP_DISTANCE_CLEANED as TRIP_DISTANCE,
        RATECODEID,
        RATECODE_DESC,
        STORE_AND_FWD_FLAG_DESC,
        -- Información de ubicación
        PULOCATIONID,
        PICKUP_ZONE,
        PICKUP_BOROUGH,
        PICKUP_SERVICE_ZONE,
        
        DOLOCATIONID, 
        DROPOFF_ZONE,
        DROPOFF_BOROUGH,
        DROPOFF_SERVICE_ZONE,
        
        -- Información de pago
        PAYMENT_TYPE,
        PAYMENT_TYPE_DESC,
        FARE_AMOUNT_CLEANED as FARE_AMOUNT,
        TIP_AMOUNT_CLEANED as TIP_AMOUNT,
        EXTRA,
        MTA_TAX,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        CONGESTION_SURCHARGE,
        CBD_CONGESTION_FEE,
        EHAIL_FEE,
        TOTAL_AMOUNT,
        AIRPORT_FEE,
        
        -- Campos específicos
        TRIP_TYPE,
        TRIP_TYPE_DESC,
        FILE_SIZE,
    from enriched_with_zones
)

select * from final
        );
      
  