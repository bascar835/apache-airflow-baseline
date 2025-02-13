from pathlib import Path
import pandas as pd
from Proceso_ETL.src import extract, parsers

def extract_data_pib(**kwargs):
    # Cargar configuración
    config = parsers.Parser.parse(Path("/opt/airflow/dags/Proceso_ETL/config.yml"))

    pib_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['pib']}")

    pib_df = extract.ExtractorExcel.extract(pib_path, sheet_name="Full data")

    kwargs['ti'].xcom_push(key='pib_df', value=pib_df)
