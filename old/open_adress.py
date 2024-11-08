import duckdb
import os
import logging
from logging_config import setup_logging

# Configuração do logging
setup_logging()  
logger = logging.getLogger() 

# Caminho do banco de dados
db_path = '/home/fribeiro/bases/CNPJ/openaddress.db'

# Criar o diretório se não existir
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Início do processamento
logger.info("Iniciando o processamento do script.")

try:
    # Conectar ao banco de dados DuckDB (ele será criado se não existir)
    con = duckdb.connect(db_path)

    # Instalar e carregar extensões
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configurações do MinIO
    con.execute("SET s3_region='us-west-rack';")
    con.execute("SET s3_access_key_id='U597zxiH0ZXgx68Atlad';")
    con.execute("SET s3_secret_access_key='tdh4j4PJkerzxWEfnqo5d2bbXfLOq0JfCidOrLhd';")
    con.execute("SET s3_endpoint='s3.iocasta.com.br';")
    con.execute("SET s3_use_ssl=true;")
    con.execute("SET s3_url_style='path';")

    # Ler os dados dos endereços estaduais
    brasil = "ac,al,am,ap,ba,ce,df,es,go,ma,mg,ms,mt,pa,pb,pe,pi,pr,rj,rn,ro,rr,rs,sc,se,sp,to"
    campos_desnecessarios = ["hash", "unit", "region", "id", "city", "district"]

    # Criar tabelas para cada UF
    for uf in brasil.split(","):
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

    # Lista de arquivos para as cidades específicas
    arquivos = [
        "br/es/vitoria-addresses-city.geojson",
        "br/mg/belo_horizonte-addresses-city.geojson",
        "br/ms/campo_grande-addresses-city.geojson",
        "br/rj/rio_de_janeiro-addresses-city.geojson",
        "br/pe/recife-addresses-city.geojson",
        "br/sp/sao-paulo-city-addresses-GeoSampa.geojson",
        "br/sp/santos-addresses-city.geojson",
        "br/sp/guarulhos-addresses-city.geojson",
        "br/pr/curitiba-addresses-city.geojson",
        "br/sc/joinville-addresses-city.geojson",
        "br/rs/caxias_do_sul-addresses-city.geojson",
        "br/rs/canoas-addresses-city.geojson"
    ]

    # Criar tabelas para cada cidade
    for arquivo in arquivos:
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
    # Fechar a conexão
    if 'con' in locals():
        con.close()
        logger.info("Conexão com o banco de dados fechada.")

# Fim do processamento
logger.info("Processamento do script finalizado.")