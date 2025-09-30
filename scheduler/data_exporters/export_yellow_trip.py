
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Snowflake warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#snowflake
    """
    fuente = kwargs['fuente']
    mes = kwargs['mes']
    anio = kwargs['anio']
    fecha_extraida = f"{mes}-{anio}"
    table_name = f'"{fuente}_taxi_trip"'
    database = 'NY_TAXI'
    schema = 'RAW'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        #Verificar si ya existen datos para esa fecha
        result = loader.execute(f"""
            SELECT COUNT(*) 
            FROM {database}.{schema}.{table_name}
            WHERE "lote_mes" = '{fecha_extraida}'
        """)

        if result[0][0] == 0:  # Si no existen datos para la fecha se sube
            loader.export(
                df,
                table_name,
                database,
                schema,
                if_exists='append',  # Specify resolution policy if table already exists
            )
        else: 
            print(f"\n Datos para la fecha {fecha_extraida} ya existen, no se realizará la carga.")

