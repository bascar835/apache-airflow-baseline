import pandas as pd
from Proceso_ETL.src import transform

def transform_data_emissions_wp(**kwargs):
    ti = kwargs['ti']
    
    emissions_df = ti.xcom_pull(task_ids='extract_data_emissions', key='emissions_df')
    world_population_df = ti.xcom_pull(task_ids='extract_data_worldp', key='world_population_df')

    emissions_df_transformed = transform.TransformEmissions.transform(emissions_df, world_population_df)

    kwargs['ti'].xcom_push(key='emissions_df_transformed', value=emissions_df_transformed)
    
