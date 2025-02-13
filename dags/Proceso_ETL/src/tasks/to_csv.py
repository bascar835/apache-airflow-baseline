from pathlib import Path
from Proceso_ETL.src import load

def to_csv(**kwargs):
    
    # Obtener la ruta de la base de datos desde la configuración
    csv_file = Path(f"/opt/airflow/dags/Proceso_ETL/output/merged.csv")

    ti = kwargs['ti']
    merged_df = ti.xcom_pull(task_ids='load_data', key='merged_df')

    merged_df.to_csv(csv_file, index=False)


    