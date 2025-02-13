from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Agregar el directorio de scripts ETL al path
#ETL_PATH = "/Proceso_ETL/Proceso ETL/src"
#sys.path.append(ETL_PATH)

from Proceso_ETL.src import parsers, extract, transform, load
import pandas as pd

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL pipeline with Airflow',
    schedule_interval=timedelta(days=1),
)

# Cargar configuración
config = parsers.Parser.parse(Path("/opt/airflow/dags/Proceso_ETL/config.yml"))


# Definir variables de rutas desde config.yml
energy_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['energy']}")
pib_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['pib']}")
emissions_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['emissions']}")
world_population_path = Path(f"/opt/airflow/dags/Proceso_ETL/{config['files']['world_population']}")
database_file =  Path(f"/opt/airflow/dags/Proceso_ETL/{config['output']['database_file']}")


# Funciones para las tareas del DAG
def extract_data(**kwargs):
    energy_df = extract.ExtractorCSV.extract(energy_path)
    pib_df = extract.ExtractorExcel.extract(pib_path, sheet_name="Full data")
    emissions_df = extract.ExtractorCSV.extract(emissions_path)
    world_population_df = extract.ExtractorCSV.extract(world_population_path)

    # Enviar los datos a XComs
    kwargs['ti'].xcom_push(key='energy_df', value=energy_df)
    kwargs['ti'].xcom_push(key='pib_df', value=pib_df)
    kwargs['ti'].xcom_push(key='emissions_df', value=emissions_df)
    kwargs['ti'].xcom_push(key='world_population_df', value=world_population_df)


def transform_data(**kwargs):
    ti = kwargs['ti']

    # Recuperar los DataFrames de XComs
    energy_df = ti.xcom_pull(task_ids='extract_data', key='energy_df')
    pib_df = ti.xcom_pull(task_ids='extract_data', key='pib_df')
    emissions_df = ti.xcom_pull(task_ids='extract_data', key='emissions_df')
    world_population_df = ti.xcom_pull(task_ids='extract_data', key='world_population_df')

    energy_df_transformed = transform.TransformEnergy.transform(energy_df, world_population_df)
    emissions_df_transformed = transform.TransformEmissions.transform(emissions_df, world_population_df)
    pib_df_transformed = transform.TransformPib.transform(pib_df)

    merged_df = pd.merge(energy_df_transformed, emissions_df_transformed, on="Country", how="inner")
    merged_df = pd.merge(merged_df, pib_df_transformed, on="Country", how="inner")

    # Pasar merged_df a XComs
    ti.xcom_push(key='merged_df', value=merged_df)



def load_data(**kwargs):
    ti = kwargs['ti']

    # Recuperar merged_df de XComs
    merged_df = ti.xcom_pull(task_ids='transform_data', key='merged_df')

    # Cargar el DataFrame a la base de datos
    load.Loader.to_sqlite(merged_df, database_file, "etl_results")


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,  # Permite el acceso a kwargs
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Permite el acceso a kwargs
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Permite el acceso a kwargs
    dag=dag,
)


# Definir la secuencia de ejecución
extract_task >> transform_task >> load_task
