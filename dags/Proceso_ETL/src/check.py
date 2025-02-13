#check.py
import pandas as pd
from pathlib import Path
import sqlite3
import parsers

def main():
    # Cargar configuración
    config = parsers.Parser.parse(Path("config.yml"))
    database_file = Path(config['output']['database_file'])
    merged_file = Path(config['output']['merged_file'])

    # Leer base de datos SQLite
    with sqlite3.connect(database_file) as conn:
        df = pd.read_sql_query("SELECT * FROM etl_results", conn)

    # Guardar en CSV
    df.to_csv(merged_file, index=False)

if __name__ == "__main__":
    main()

