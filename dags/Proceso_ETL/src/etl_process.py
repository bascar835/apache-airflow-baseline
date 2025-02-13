# etl_process.py
#  Instancio la clase Energy -> Lee datos, hacemos los transforms

import pandas as pd
from pathlib import Path
import parsers, extract, transform, load
import sqlite3
import sys

# Añadir el directorio raíz del proyecto al sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))    # Comentar esta linea si no se ejecuta el programa

def main():
    # Cargar configuración
    config = parsers.Parser.parse(Path("config.yml"))

    # Asegurar que la carpeta de salida exista
    Path(config['paths']['output_dir']).mkdir(parents=True, exist_ok=True)

    # Rutas desde config.yml
    
    energy_path = Path(config['files']['energy'])
    pib_path = Path(config['files']['pib'])
    emissions_path = Path(config['files']['emissions'])
    world_population_path = Path(config['files']['world_population'])
    database_file = Path(config['output']['database_file'])

    # Extracción de datos
    energy_df = extract.ExtractorCSV.extract(energy_path)   #rows=4??
    pib_df = extract.ExtractorExcel.extract(pib_path, sheet_name="Full data")
    emissions_df = extract.ExtractorCSV.extract(emissions_path)
    world_population_df = extract.ExtractorCSV.extract(world_population_path)

    # Transformación de datos
    
    energy_df = transform.TransformEnergy.transform(energy_df, world_population_df)
    emissions_df = transform.TransformEmissions.transform(emissions_df, world_population_df)
    pib_df = transform.TransformPib.transform(pib_df)

    # Combinación de datos
    merged_df = pd.merge(energy_df, emissions_df, on="Country", how="inner")
    merged_df = pd.merge(merged_df, pib_df, on="Country", how="inner")

    # Carga de datos procesados
    load.Loader.to_sqlite(merged_df, database_file, "etl_results")

if __name__ == "__main__":
    main()

