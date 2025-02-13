#load.py
from pathlib import Path
import sqlite3
import pandas as pd

class Loader:
    @staticmethod
    def to_sqlite(data: pd.DataFrame, database_file: Path, table_name: str):
        """
        Guarda un DataFrame en una base de datos SQLite.
        """
        with sqlite3.connect(database_file) as conn:
            data.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

    @staticmethod
    def to_csv(data: pd.DataFrame, output_file: Path):
        """
        Guarda un DataFrame en un archivo CSV.
        """
        data.to_csv(output_file, index=False) 
