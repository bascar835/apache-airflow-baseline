import pandas as pd
from Proceso_ETL.src import transform

def transform_data_merge_2(**kwargs):
    ti = kwargs['ti']

    pib_df_transformed = kwargs['ti'].xcom_pull(task_ids='transform_data_pib', key='pib_df_transformed')
    merged_df = kwargs['ti'].xcom_pull(task_ids='transform_data_merge_1', key='merged_df')

    merged_df = pd.merge(merged_df, pib_df_transformed, on="Country", how="inner")

    ti.xcom_push(key='merged_df', value=merged_df)

