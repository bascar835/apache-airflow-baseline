from pathlib import Path
import pandas as pd
from Proceso_ETL.src import extract, parsers

def extract_data_worldp(**kwargs):
    # Cargar configuración
    config = parsers.Parser.parse(Path("/opt/airflow/dags/Proceso_ETL/config.yml"))
    
    world_population_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['world_population']}")

    world_population_df = extract.ExtractorCSV.extract(world_population_path)

    kwargs['ti'].xcom_push(key='world_population_df', value=world_population_df)
