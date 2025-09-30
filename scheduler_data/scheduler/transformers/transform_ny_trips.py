from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
from datetime import datetime, timezone
import uuid

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.AVERAGE

    Docs: https://docs.mage.ai/guides/transformer-blocks#aggregation-actions
    """
    mes = kwargs.get('mes')
    anio = kwargs.get('anio')
    fuente = kwargs.get('fuente')
    now_utc = datetime.now(timezone.utc)
    # Convertir a formato ISO 8601 sin microsegundos
    iso_timestamp = now_utc.replace(microsecond=0).isoformat()

    
    run_id = kwargs.get('run_id', str(uuid.uuid4()))  # Genera un ID único si no se pasa run_id
    ventana_temporal = datetime.now()
    lote_mes = f"{mes}-{anio}"  # Formato MM-YYYY, ajusta si necesitas otro formato
    
    # Agregar columnas de metadatos al DataFrame
    df['run_id'] = run_id
    df['ventana_temporal'] = iso_timestamp
    df['lote_mes'] = lote_mes
    df['tamano'] = df.memory_usage(deep=True).sum()
    df['fuente'] = kwargs['fuente']
    
    print(f"Metadatos añadidos al dataFrame")
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
