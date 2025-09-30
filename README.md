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

Se aplicó clustering en Snowflake sobre la tabla de hechos FCT_TRIPS para mejorar el rendimiento de las consultas analíticas. Las columnas seleccionadas para clustering fueron aquellas más utilizadas en filtros de consultas, como PU_ZONE_SK, DO_ZONE_SK, TRIP_DATE y TRIP_TIME, entre otras.

## Permisos

Se creó un rol dedicado llamado USUARIO_TECNICO y un usuario asociado a dicho rol, garantizando que solo tenga acceso a los recursos estrictamente necesarios para la ejecución de los pipelines y la creación de las tablas en los esquemas adecuados. Todas las credenciales se almacenan en **mage_secrets** de forma segura y no se exponen en el repositorio ni en los scripts. La ejecución de los pipelines utiliza estos secrets para conectarse a Snowflake, asegurando trazabilidad, seguridad y cumplimiento de buenas prácticas de gestión de accesos.

## Test DBT

Todos los tests definidos en dbt (not_null, unique, accepted_values, relationships) pasan correctamente, garantizando la calidad e integridad de los datos. Además, se generó la documentación de modelos y columnas junto con el lineage, permitiendo visualizar las dependencias entre tablas y la trazabilidad completa de los datos, pese a eso existen columnas con gran canitdad de NULL, por ejemplo al unificarse las tablas existian columnas en green que no estaban en yellow y viceversa que fueron rellenados con NULL.


## Checklist del proyecto

- [x] **Cargados todos los meses 2015–2025** (Parquet) de Yellow y Green; matriz de cobertura en README.
- [x] **Mage** orquesta backfill mensual con idempotencia y metadatos por lote.
- [x] **Bronze** (raw) refleja fielmente el origen; **Silver** unifica/escaliza; **Gold** en estrella con fct_trips y dimensiones clave.
- [x] **Clustering** aplicado a fct_trips con evidencia antes/después (Query Profile, pruning).
- [x] **Secrets** y **cuenta de servicio** con permisos mínimos (evidencias sin exponer valores).
- [ ] **Tests dbt** (not_null, unique, accepted_values, relationships) pasan; **docs** y **lineage** generados.
- [x] Notebook con respuestas a las **5 preguntas de negocio** desde **gold**.

