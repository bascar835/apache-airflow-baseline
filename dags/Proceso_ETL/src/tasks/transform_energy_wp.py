import pandas as pd
from Proceso_ETL.src import transform

def transform_data_energy_wp(**kwargs):
    ti = kwargs['ti']
    
    energy_df = ti.xcom_pull(task_ids='extract_data_energy', key='energy_df')
    world_population_df = ti.xcom_pull(task_ids='extract_data_worldp', key='world_population_df')

    energy_df_transformed = transform.TransformEnergy.transform(energy_df, world_population_df)

    kwargs['ti'].xcom_push(key='energy_df_transformed', value=energy_df_transformed)
