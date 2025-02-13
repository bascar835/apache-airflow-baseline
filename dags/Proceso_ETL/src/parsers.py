# parsers.py
from pathlib import Path
import yaml

class Parser:
    @staticmethod
    def parse(path: Path):
        """
        Carga y devuelve el contenido de un archivo YAML como un diccionario.
        """
        with open(path, 'r') as file:
            return yaml.safe_load(file) 
