import duckdb
import os
import logging
import asyncio
from logging_config import setup_logging
from config import load_settings

setup_logging()  
logger = logging.getLogger() 

class OpenAddress:
    
    async def processa_openaddress():
        
        settings = await load_settings()
        db_path = settings.path_db_openaddress
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        logger.info("Iniciando o processamento do script.")

        try:
            
            con = duckdb.connect(db_path)
            
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
            
            con.execute(f"SET s3_region='{settings.s3_region}';")
            con.execute(f"SET s3_access_key_id='{settings.s3_access_key_id}';")
            con.execute(f"SET s3_secret_access_key='{settings.s3_secret_access_key}';")
            con.execute(f"SET s3_endpoint='{settings.s3_endpoint}';")
            con.execute(f"SET s3_use_ssl={str(settings.s3_use_ssl).lower()};")
            con.execute(f"SET s3_url_style='{settings.s3_url_style}';")

            brasil = settings.brasil
            campos_desnecessarios = settings.campos_desnecessarios
            
            for uf in brasil:
                if uf == "to":
                    uf = '"to"' 
                sql = f"""
                CREATE TABLE {uf} AS 
                SELECT * 
                FROM st_read('s3://geoserver/br/{uf}/statewide-addresses-state.geojson');
                """
                con.execute(sql)
                for cpo in campos_desnecessarios:
                    con.execute(f"ALTER TABLE {uf} DROP COLUMN {cpo};")
                logger.info(f"Tabela criada para a UF: {uf}")
            
            for arquivo in settings.arquivos:
                tbl_name = arquivo[7:].replace("-addresses-city.geojson", "")
                sql = f"""
                CREATE TABLE {tbl_name} AS 
                SELECT * 
                FROM st_read('s3://geoserver/{arquivo}');
                """
                con.execute(sql)
                for cpo in campos_desnecessarios:
                    con.execute(f"ALTER TABLE {tbl_name} DROP COLUMN {cpo};")
                logger.info(f"Tabela criada para a cidade: {tbl_name}")

        except Exception as e:
            logger.error(f"Ocorreu um erro durante o processamento: {e}")
        finally:
            if 'con' in locals():
                con.close()
                logger.info("Conexão com o banco de dados fechada.")

        logger.info("Processamento do script finalizado.")
        
if __name__ == '__main__':
    asyncio.run(OpenAddress.processa_openaddress())