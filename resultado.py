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
                    endereco_editado VARCHAR,
                    cep VARCHAR,
                    cep_editado VARCHAR,
                    resultado_geocodificacao_endereco_editado VARCHAR,
                    resultado_geocodificacao_cep VARCHAR
                );
            """)

            nominatim_file_path = os.path.join(settings.path_file_cnpj, 'nominatim.csv')

            conn.execute(f"""
            COPY nominatim FROM '{nominatim_file_path}'
                (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"',
                ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
            """)

            estados = settings.brasil
            union_queries = []

            for uf in estados:
                estado = f'"{uf.lower()}"' # if uf.lower() != "to" else '"to"' # Agora, todos os estados são tratados de forma uniforme

                query = f"""
                SELECT
                    e.cnpj_completo,
                    e.endereco_editado,
                    e.cep_editado,
                    e.resultado_geocodificacao_endereco_editado,
                    e.resultado_geocodificacao_cep,
                    uf.postcode,
                    uf.geom
                FROM
                    nominatim e
                LEFT JOIN
                    {estado} uf
                    ON e.cep_editado = uf.postcode
                WHERE
                    (e.resultado_geocodificacao_endereco_editado IS NULL OR e.resultado_geocodificacao_endereco_editado = '')
                    AND
                    (e.resultado_geocodificacao_cep IS NULL OR e.resultado_geocodificacao_cep = '')
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
    