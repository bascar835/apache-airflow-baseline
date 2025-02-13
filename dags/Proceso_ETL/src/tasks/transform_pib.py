import pandas as pd
from Proceso_ETL.src import transform

def transform_data_pib(**kwargs):
    ti = kwargs['ti']
    
    pib_df = ti.xcom_pull(task_ids='extract_data_pib', key='pib_df')

    pib_df_transformed = transform.TransformPib.transform(pib_df)

    kwargs['ti'].xcom_push(key='pib_df_transformed', value=pib_df_transformed)
