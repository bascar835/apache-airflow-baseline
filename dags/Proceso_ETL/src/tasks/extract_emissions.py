from pathlib import Path
import pandas as pd
from Proceso_ETL.src import extract, parsers

def extract_data_emissions(**kwargs):
    # Cargar configuración
    config = parsers.Parser.parse(Path("/opt/airflow/dags/Proceso_ETL/config.yml"))
    
    
    emissions_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['emissions']}")

    
    emissions_df = extract.ExtractorCSV.extract(emissions_path)

    kwargs['ti'].xcom_push(key='emissions_df', value=emissions_df)
