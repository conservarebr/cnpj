import duckdb
import os
import logging
import asyncio
from config import load_settings
from logging_config import setup_logging

setup_logging()

conn = duckdb.connect(database='cnpj.duckdb')

class Estabelecimentos:
    
    async def processa_estabelecimentos():
        
        settings = await load_settings()
        
        logging.info("Início do processamento")

        try:
            estabelecimentos_files = [os.path.join(settings.path_file_csv, f'estabelecimentos_{i}.csv') for i in range(10)]
            estabelecimentos_files_str = ', '.join([f"'{file}'" for file in estabelecimentos_files])

            conn.execute(f"""
                CREATE TABLE CNPJ AS
                SELECT DISTINCT
                    CONCAT(column00, column01, column02) AS cnpj_completo,
                    column13 AS tipo_logradouro
                    column14 AS logradouro,
                    column15 AS numero
                    column17 AS bairro
                    column18 AS cep,
                    column19 AS uf
                    column20 AS muncipio,
                    column11 AS cnae_primaria,
                    TRIM(value) AS cnae_secundaria
                FROM read_csv_auto(
                    [{estabelecimentos_files_str}],
                    sep = ';',
                    header = false,
                    ignore_errors = true,
                    union_by_name = true,
                    filename = true
                ) AS
                CROSS JOIN UNNEST(string_split(column12, ',')) AS cnae_secundaria(value)
                WHERE column05 = '02';
            """)

            saida = os.path.join(settings.path_file_csv, 'sss.csv')

            conn.execute(f"""
                COPY CNPJ TO '{saida}' 
                (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
            """)

            logging.info(f"A tabela 'Estabelecimentos' foi salva em {saida}")

        except Exception as e:
            logging.error(f"Ocorreu um erro: {e}")
        
        finally:
            conn.close()

        logging.info("Fim do processamento")

if __name__ == '__main__':
    asyncio.run(Estabelecimentos.processa_estabelecimentos())
