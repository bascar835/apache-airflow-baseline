import pandas as pd
from Proceso_ETL.src import transform

def transform_data_merge_1(**kwargs):
    ti = kwargs['ti']

    emissions_df_transformed = kwargs['ti'].xcom_pull(task_ids='transform_data_emissions_wp', key='emissions_df_transformed')
    energy_df_transformed = kwargs['ti'].xcom_pull(task_ids='transform_data_energy_wp', key='energy_df_transformed')
    #pib_df_transformed = kwargs['ti'].xcom_pull(task_ids='transform_data_pib', key='pib_df_transformed') 

    merged_df = pd.merge(energy_df_transformed, emissions_df_transformed, on="Country", how="inner")
    #merged_df = pd.merge(merged_df, pib_df_transformed, on="Country", how="inner") 

    ti.xcom_push(key='merged_df', value=merged_df)

