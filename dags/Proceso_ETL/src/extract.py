#extract.py

from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd

class Extractor(ABC):
    @abstractmethod
    def extract(self, path):
        pass

class ExtractorCSV(Extractor):
    @staticmethod
    def extract(path: Path, rows=0):
        """
        Extrae datos de un archivo CSV en un DataFrame.
        """
        return pd.read_csv(path, skiprows=rows)

class ExtractorExcel(Extractor):
    @staticmethod
    def extract(path: Path, sheet_name: str):
        """
        Extrae datos de un archivo Excel en un DataFrame.
        """
        return pd.read_excel(path, sheet_name=sheet_name)

'''
class ExtractorSQLite(Extractor):
    @staticmethod
    def extract(path: Path):
        """
        Extrae tablas de una base de datos SQLite en un diccionario de DataFrames.
        """
        import sqlite3

        with sqlite3.connect(path) as dbcon:
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table';", dbcon
            )['name'].tolist()
            return {table: pd.read_sql_query(f"SELECT * FROM {table}", dbcon) for table in tables}

'''