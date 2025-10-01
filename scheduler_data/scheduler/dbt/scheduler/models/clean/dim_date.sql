{{ config(materialized='table') }}

with dates as (
  -- Obtenemos todas las fechas distintas desde la columna de pickup
  select distinct date_trunc('day', PICKUP_DATETIME) as the_date
  from NY_TAXI.SILVER.STG_TRIPS
  where PICKUP_DATETIME is not null

  union  -- combinamos con las fechas de dropoff

  -- Obtenemos todas las fechas distintas desde la columna de dropoff
  select distinct date_trunc('day', DROPOFF_DATETIME) as the_date
  from NY_TAXI.SILVER.STG_TRIPS
  where DROPOFF_DATETIME is not null
)

-- Ahora transformamos esas fechas en una tabla de dimensión calendario
select
  -- Surrogate key en formato YYYYMMDD, útil como PK para joins
  cast(to_char(the_date,'YYYYMMDD') as integer) as date_sk,

  -- Fecha completa (tipo date)
  the_date as full_date,

  -- Componentes básicos de la fecha
  date_part(year, the_date) as year,
  date_part(month, the_date) as month,
  date_part(day, the_date) as day,

  -- Día de la semana (dependiendo de Snowflake: 1=Lunes … 7=Domingo)
  dayofweek(the_date) as day_of_week,

  -- Flag booleano: true si sábado (6) o domingo (7), false en caso contrario
  case when dayofweek(the_date) in (0,6) then true else false end as is_weekend

from dates
order by the_date
