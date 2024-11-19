import duckdb
import os
import logging
from config import load_settings
from logging_config import setup_logging

setup_logging()

class Resultado:

    async def processa_resultado():
        
        settings = await load_settings()
        db_path = settings.path_db_openaddress

        logging.info("Início do processamento")

        try:
            conn = duckdb.connect(db_path)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS nominatim (
                    cnpj_completo VARCHAR,
                    cnae_primaria VARCHAR,
                    cnae_secundaria VARCHAR,
                    endereco_editado VARCHAR,
                    cep VARCHAR,
                    cep_editado VARCHAR
                );
            """)

            nominatim_file_path = os.path.join(settings.path_file_cnpj, 'endereco.csv')

            conn.execute(f"""
            COPY nominatim FROM '{nominatim_file_path}'
                (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"',
                ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
            """)

            estados = settings.brasil
            union_queries = []

            for uf in estados:
                estado = f'"{uf.lower()}"' if uf.lower() == "to" else uf.lower()

                query = f"""
                SELECT
                    e.cnpj_completo,
                    e.cnae_primaria,
                    e.cnae_secundaria,
                    e.endereco_editado,
                    e.cep_editado,
                    {estado}.postcode
                FROM
                    nominatim e
                JOIN
                    {estado} uf
                    ON e.cep_editado = {estado}.postcode
                WHERE
                    e.endereco_editado LIKE '%{uf.upper()}'
                """
                union_queries.append(query)

            final_query = " UNION ".join(union_queries)

            result = conn.execute(final_query).fetchall()

            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS resultado AS {final_query};
            """)

        except Exception as e:
            logging.error(f"Ocorreu um erro: {e}")

        finally:
            conn.close()

        logging.info("Fim do processamento")

if __name__ == '__main__':
    import asyncio
    asyncio.run(Resultado.processa_resultado())
