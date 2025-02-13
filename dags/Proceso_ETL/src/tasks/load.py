from pathlib import Path
from Proceso_ETL.src import load, parsers

def load_data(**kwargs):
    # Cargar configuración desde el archivo config.yml
    config = parsers.Parser.parse(Path("/opt/airflow/dags/Proceso_ETL/config.yml"))

    # Obtener la ruta de la base de datos desde la configuración
    database_file = Path(f"/opt/airflow/dags/Proceso_ETL/{config['output']['database_file']}")

    ti = kwargs['ti']
    merged_df = ti.xcom_pull(task_ids='transform_data_merge_2', key='merged_df')

    load.Loader.to_sqlite(merged_df, database_file, "etl_results")

    kwargs['ti'].xcom_push(key='merged_df', value=merged_df)
