import duckdb
import logging
import os
from config import load_settings
from logging_config import setup_logging

setup_logging()

conn = duckdb.connect(database='cnpj.duckdb')
class Endereco:
    
    async def processa_endereco():
        
        settings = await load_settings()

        logging.info("Início do processamento de endereço")
        
        try:
            conn.execute("""
                CREATE TABLE endereco AS
                SELECT DISTINCT
                    e.cnpj_completo,
                    e.tipo_logradouro,
                    e.logradouro,
                    e.numero,
                    e.bairro,
                    e.uf,
                    e.municipio,
                    m.codigo AS municipio_codigo,
                    m.descricao AS municipio_nome,
                    e.cnae_primaria,
                    e.cnae_secundaria,
                    CONCAT(e.tipo_logradouro, ' ', e.logradouro, ' ', e.numero, ' ',
                    e.bairro, ' ', e.municipio, ' ', e.uf) AS endereco_editado,
                    e.cep
                FROM estabelecimentos AS e
                LEFT JOIN municipios AS m
                    ON e.municipio = m.descricao;
            """)

            conn.execute("ALTER TABLE endereco DROP COLUMN tipo_logradouro;")
            conn.execute("ALTER TABLE endereco DROP COLUMN logradouro;")
            conn.execute("ALTER TABLE endereco DROP COLUMN numero;")
            conn.execute("ALTER TABLE endereco DROP COLUMN bairro;")
            conn.execute("ALTER TABLE endereco DROP COLUMN municipio;")
            conn.execute("ALTER TABLE endereco DROP COLUMN municipio_nome;")
            conn.execute("ALTER TABLE endereco DROP COLUMN municipio_codigo;")
            conn.execute("ALTER TABLE endereco DROP COLUMN uf;")
            
            saida = os.path.join(settings.path_file_cnpj, 'endereco.csv')
            conn.execute(f"""
                COPY endereco TO '{saida}' 
                (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
            """)
            
            logging.info(f"A tabela 'endereço' foi salva em {saida}")

        except Exception as e:
            logging.error(f"Ocorreu um erro ao processar o endereço: {e}")
        
        finally:
            conn.close()

        logging.info("Fim do processamento de endereço")

if __name__ == '__main__':
    import asyncio
    asyncio.run(Endereco.processa_endereco())
