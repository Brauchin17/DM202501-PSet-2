import io
import pandas as pd
import requests
import time
import gc

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    mes = kwargs['mes']
    anio = kwargs['anio']
    fuente = kwargs['fuente']
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{fuente}_tripdata_{anio}-{mes}.parquet'
    print("iniciando descarga")
    data = pd.read_parquet(url)
    print(f"Data del mes {mes} y anio {anio} ingestada correctamente")
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
