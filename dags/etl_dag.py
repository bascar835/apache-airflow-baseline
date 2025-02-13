from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Agregar el directorio de scripts ETL al path
from Proceso_ETL.src import parsers

#Extract
from Proceso_ETL.src.tasks.extract_energy import extract_data_energy
from Proceso_ETL.src.tasks.extract_pib import extract_data_pib
from Proceso_ETL.src.tasks.extract_emissions import extract_data_emissions
from Proceso_ETL.src.tasks.extract_worldp import extract_data_worldp

#Transform
from Proceso_ETL.src.tasks.transform_energy_wp import transform_data_energy_wp
from Proceso_ETL.src.tasks.transform_emissions_wp import transform_data_emissions_wp
from Proceso_ETL.src.tasks.transform_pib import transform_data_pib

#Merge
from Proceso_ETL.src.tasks.transform_merge_energy_em import transform_data_merge_1
from Proceso_ETL.src.tasks.transform_merge_df import transform_data_merge_2

#Load
from Proceso_ETL.src.tasks.load import load_data

#CSV
from Proceso_ETL.src.tasks.to_csv import to_csv



# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'etl_dag_2',
    default_args=default_args,
    description='ETL pipeline with Airflow',
    schedule_interval=timedelta(days=1),
)


extract_task = PythonOperator(
    task_id='extract_data_energy',
    python_callable=extract_data_energy,
    dag=dag,
)

extract_task1 = PythonOperator(
    task_id='extract_data_pib',
    python_callable=extract_data_pib,
    dag=dag,
)

extract_task2 = PythonOperator(
    task_id='extract_data_emissions',
    python_callable=extract_data_emissions,
    dag=dag,
)

extract_task3 = PythonOperator(
    task_id='extract_data_worldp',
    python_callable=extract_data_worldp,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data_energy_wp',
    python_callable=transform_data_energy_wp,
    dag=dag,
)

transform_task2 = PythonOperator(
    task_id='transform_data_emissions_wp',
    python_callable=transform_data_emissions_wp,
    dag=dag,
)

transform_task3 = PythonOperator(
    task_id='transform_data_pib',
    python_callable=transform_data_pib,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='transform_data_merge_1',
    python_callable=transform_data_merge_1,
    dag=dag,
)

merge_task2 = PythonOperator(
     task_id='transform_data_merge_2',
     python_callable=transform_data_merge_2,
     provide_context=True,
     dag=dag,
 )



load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

to_csv_task = PythonOperator(
    task_id='to_csv',
    python_callable=to_csv,
    dag=dag,
)

# Definir la secuencia de ejecución
# Definir la secuencia de ejecución correcta

# PIB
extract_task1 >> transform_task3

# Energy y World Population
#[extract_task, extract_task3] >> transform_task
extract_task >> transform_task

extract_task3 >> transform_task

# Emissions y World Population
#[extract_task2,extract_task3] >> transform_task2

extract_task2 >> transform_task2

extract_task3 >> transform_task2

# Merge 1 (une Energy y Emissions transformados)

[transform_task,transform_task2] >> merge_task

transform_task3 >> merge_task2

# transform_task >> merge_task

# transform_task2 >> merge_task

# transform_task3 >> merge_task

merge_task >> merge_task2

merge_task2 >> load_task

load_task >> to_csv_task

# # Merge 2 (agrega PIB)
# merge_task >> merge_task2

# transform_task3 >> merge_task2

# Cargar datos a la base
#merge_task2 >> load_task


# extract_task >> transform_task >> merge_task >> merge_task2 >> load_task

# extract_task3 >> transform_task >> merge_task >> merge_task2 >> load_task

# extract_task2 >> transform_task2 >> merge_task >> merge_task2 >> load_task

# extract_task3 >> transform_task2 >> merge_task >> merge_task2 >> load_task

# extract_task1 >> transform_task3 >> merge_task >> merge_task2 >> load_task




