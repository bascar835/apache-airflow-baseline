from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract_data_callable
from transform import transform_data_callable
from load import load_data_callable

with DAG(
    dag_id="weather_etl",
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True
) as dag:
    
    extract_data = PythonOperator(
        dag=dag,
        task_id="extract_data",
        python_callable=extract_data_callable
    )
    
    transform_data = PythonOperator(
        dag=dag,
        task_id="transform_data",
        python_callable=transform_data_callable,
        op_kwargs={"raw_data": "{{ ti.xcom_pull(task_ids='extract_data') }}"}
    )
    
    load_data = PythonOperator(
        dag=dag,
        task_id="load_data",
        python_callable=load_data_callable,
        op_kwargs={"transformed_data": "{{ ti.xcom_pull(task_ids='transform_data') }}"}
    )

    # Set dependencies between tasks
    extract_data >> transform_data >> load_data
