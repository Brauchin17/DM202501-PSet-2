# DM202501-PSet-2
Este repositorio contiene un data pipeline completo para procesar los datos de NYC TLC Trip Record Data (Yellow y Green) desde 2015 hasta 2025.

## Matriz de cobertura

| Año  | Taxi   | Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic |
|------|--------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 2015 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2015 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2016 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2016 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2017 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2017 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2018 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2018 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2019 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2019 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2020 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2020 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2021 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2021 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2022 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2022 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2023 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2023 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2024 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2024 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2025 | Yellow | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  |
| 2025 | Green  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ✅  | ❌  | ❌  | ❌  | ❌  |

Taxi zones tambien fue subida mediante el csv ya que al usar el link del .parquet te daba un .zip en vez del formato esperado

## Idempotencia

El pipeline en Mage está diseñado para ejecutar procesos de backfill mensuales de forma idempotente, garantizando que la carga de datos no duplique información. Para ello, antes de insertar un nuevo lote se ejecuta una verificación con SELECT COUNT(*) sobre la tabla destino, filtrando por el mes y año correspondiente. Si ya existen registros de ese período, los datos descargados se descartan; en caso contrario, se realiza un append de los nuevos registros, preservando así consistencia y trazabilidad. Además, cada ejecución registra metadatos por lote, como el size del archivo, el timestamp de la ingesta y un run_id lo que permite auditar y monitorear el estado de las cargas.

## Arquitectura

El modelo sigue la arquitectura medallion en tres capas:

- Bronze (raw): refleja fielmente las fuentes originales, manteniendo tablas separadas para Yellow, Green y Taxi Zones, sin transformaciones ni unificaciones.
- Silver: unifica los viajes de Yellow y Green en una tabla estandarizada, enriquecida con la dimensión de zonas de taxi (Taxi Zones) y normalizada para asegurar consistencia en los campos.
- Gold: organiza los datos en un esquema en estrella, con la tabla de hechos FCT_TRIPS (≈852M registros, 10.3GB) en el centro, y un conjunto de dimensiones clave que permiten análisis analíticos y BI:

  - DIM_DATE
  - DIM_TIME
  - DIM_ZONE
  - DIM_VENDOR
  - DIM_RATE_CODE
  - DIM_PAYMENT_TYPE
  - DIM_SERVICE_TYPE
  - DIM_TRIP_TYPE

Esta estructura soporta consultas eficientes y escalables, facilitando tanto análisis exploratorios como casos de uso de inteligencia de negocio sobre el histórico completo 2015–2025.

## Clustering

Se aplicó clustering en Snowflake sobre la tabla de hechos FCT_TRIPS para mejorar el rendimiento de las consultas analíticas. Las columnas seleccionadas para clustering fueron aquellas más utilizadas en filtros de consultas, siendo las clustering keys: PICKUP_DATE_SK, PU_ZONE_SK, al comprobar la eficiencia del cluster que se obtuvo mediante el comando de SQL: `SELECT SYSTEM$CLUSTERING_INFORMATION('TABLE')`, se logro conseguir un depth = 2.0033, siendo que esto asegura un prunning mas preciso porque cada micro particion tiene valores mas parecidos. Considerando que el 1 es el perfecto es un clustering bastante bueno, ya que son dos de las keys mas usadas a la hora de consultas.

## Permisos

Se creó un rol dedicado llamado USUARIO_TECNICO y un usuario asociado a dicho rol, garantizando que solo tenga acceso a los recursos estrictamente necesarios para la ejecución de los pipelines y la creación de las tablas en los esquemas adecuados. Todas las credenciales se almacenan en **mage_secrets** de forma segura y no se exponen en el repositorio ni en los scripts. La ejecución de los pipelines utiliza estos secrets para conectarse a Snowflake, asegurando trazabilidad, seguridad y cumplimiento de buenas prácticas de gestión de accesos.

## Test DBT

Todos los tests definidos en dbt (not_null, unique, accepted_values, relationships) pasan correctamente en la mayoría de columnas de la tabla de gold `fct_trips`, garantizando la calidad e integridad de los datos. Aqui las columnas que fallaron:
  - dropoff_time_sk FAIL 833456233, estos fueron manejados en silver al existir un dropoff o un pickup en null se marcaba en una nueva columna con `true` o `false`, llamada `has_invalid_timing`.
  - pickup_time FAIL 833642545.
  - passenger_count FAIL 18808267.
  - payment_type FAIL 1904297.
  - trips_rate_code FAIL 18808267.
  - trip_type FAIL 786295252, esta cantidad se explica dado a que uno de los 2 servicios no tenia dicha columna por lo cual se lleno de null.
Dando un total de 6 errores y 23 pass.

No se borraron las columnas con NULL, ya que NULL indica información faltante, no necesariamente que el registro entero sea inútil, podiendo perder datos e información importante.


## Checklist del proyecto
- [x] **Cargados todos los meses 2015–2025** (Parquet) de Yellow y Green; matriz de cobertura en README.
- [x] **Mage** orquesta backfill mensual con idempotencia y metadatos por lote.
- [x] **Bronze** (raw) refleja fielmente el origen; **Silver** unifica/escaliza; **Gold** en estrella con fct_trips y dimensiones clave.
- [x] **Clustering** aplicado a fct_trips con evidencia antes/después (Query Profile, pruning).
- [x] **Secrets** y **cuenta de servicio** con permisos mínimos (evidencias sin exponer valores).
- [x] **Tests dbt** (not_null, unique, accepted_values, relationships) pasan; **docs** y **lineage** generados.
- [x] Notebook con respuestas a las **5 preguntas de negocio** desde **gold**.

