
  
    

        create or replace transient table NY_TAXI.GOLD.dim_time
         as
        (


with raw_times as (
    select 
        date_part(hour, pickup_datetime) as hour,
        date_part(minute, pickup_datetime) as minute
    from NY_TAXI.SILVER.STG_TRIPS
    union all  -- UNION ALL + DISTINCT es más eficiente que UNION
    select 
        date_part(hour, dropoff_datetime) as hour,
        date_part(minute, dropoff_datetime) as minute
    from NY_TAXI.SILVER.STG_TRIPS
),
unique_times as (
    select distinct
        hour,
        minute
    from raw_times
)
select
    (hour * 100 + minute) as time_sk,  -- Tu SK original en formato HHMM
    to_time(
        lpad(to_varchar(hour), 2, '0') || ':' || 
        lpad(to_varchar(minute), 2, '0') || ':00'
    ) as full_time,  -- Columna TIME para joins
    hour,  -- 0-23
    minute,  -- 0-59
    0 as second,  -- Fijo, ya que no extraes segundos
    hour * 60 + minute as minute_of_day,  -- Minutos transcurridos en el día (0-1439)
    floor(minute / 15) + 1 as quarter_hour,  -- Cuarto de hora (1: 0-14, 2: 15-29, 3: 30-44, 4: 45-59)
    floor(minute / 30) + 1 as half_hour,  -- Media hora (1: 0-29, 2: 30-59)
    case 
        when hour < 12 then 'AM' 
        else 'PM' 
    end as am_pm,  -- AM/PM
    case 
        when mod(hour, 12) = 0 then 12 
        else mod(hour, 12) 
    end as hour_12_format,  -- Hora en formato 12h
    case 
        when hour between 6 and 18 then 'day'
        else 'night'
    end as day_period, 
from unique_times
order by hour, minute
        );
      
  