import duckdb
import os
import logging
import asyncio
from config import load_settings
from logging_config import setup_logging

setup_logging()

conn = duckdb.connect(database='cnpj.duckdb')

class Municipios:

    async def processa_municipios():
        
        settings = await load_settings()

        logging.info("Início do processamento")
        
        try:
            
            conn.execute("""CREATE TABLE municipios (
                codigo VARCHAR PRIMARY KEY,
                descricao VARCHAR
            );""")
            
            municipios_file_path = os.path.join(settings.path_file_csv, 'municipios.csv')
            
            conn.execute(f"""
            COPY municipios FROM '{municipios_file_path}' 
                (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
            """)

        except Exception as e:
            logging.error(f"Ocorreu um erro: {e}")
            
        finally:
            conn.close()

        logging.info("Fim do processamento")

if __name__ == '__main__':
    asyncio.run(Municipios.processa_municipios())