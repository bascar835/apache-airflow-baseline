from pathlib import Path
import pandas as pd
from Proceso_ETL.src import extract, parsers

def extract_data_energy(**kwargs):
    # Cargar configuración
    config = parsers.Parser.parse(Path("/opt/airflow/dags/Proceso_ETL/config.yml"))
    
    energy_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['energy']}")
    

    energy_df = extract.ExtractorCSV.extract(energy_path)

    kwargs['ti'].xcom_push(key='energy_df', value=energy_df)
