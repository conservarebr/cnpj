import duckdb
import os
import logging
from datetime import datetime

# Configurando o logging para exibir no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

data_fribeiro = "/home/fribeiro/bases/CNPJ"
conn = duckdb.connect(database=':memory:')

# Registro de início do processamento
logging.info("Início do processamento")

try:
    #### CNAE ####
    conn.execute("""CREATE TABLE cnae (
        codigo VARCHAR PRIMARY KEY,
        descricao VARCHAR
    );""")
    cnae_file_path = os.path.join(data_fribeiro, 'cnae.csv')
    conn.execute(f"""
    COPY cnae FROM '{cnae_file_path}' 
        (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
    """)

    #### Municípios ####
    conn.execute("""CREATE TABLE municipios (
        codigo VARCHAR PRIMARY KEY,
        descricao VARCHAR
    );""")
    municipios_file_path = os.path.join(data_fribeiro, 'municipios.csv')
    conn.execute(f"""
    COPY municipios FROM '{municipios_file_path}' 
        (FORMAT CSV, DELIMITER ';', HEADER TRUE, QUOTE '"', ESCAPE '"', ENCODING 'UTF8', IGNORE_ERRORS TRUE);
    """)

    #### Estabelecimentos ####
    estabelecimentos_files = [os.path.join(data_fribeiro, f'estabelecimentos_{i}.csv') for i in range(10)]
    estabelecimentos_files_str = ', '.join([f"'{file}'" for file in estabelecimentos_files])

    conn.execute(f"""
    CREATE TABLE CNPJ AS
    SELECT DISTINCT
        CONCAT(e.column00, e.column01, e.column02) AS cnpj_completo,
        CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19, ' ', e.column18) AS endereco,
        CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19) AS endereco_editado,
        e.column18 AS cep,
        e.column11 AS cnae_primaria,
        TRIM(value) AS cnae_secundaria,
        CONCAT(
            CONCAT(e.column00, e.column01, e.column02),
            '|http://venus.iocasta.com.br:8080/search.php?q=',
            TRIM(CONCAT(e.column13, ' ', e.column14, ' ', e.column15, ' ', e.column17, ' ', m.descricao, ' ', e.column19))
        ) AS colecao
    FROM read_csv_auto(
        [{estabelecimentos_files_str}],
        sep = ';',
        header = false,
        ignore_errors = true,
        union_by_name = true,
        filename = true
    ) AS e
    JOIN municipios m ON e.column20 = m.codigo
    CROSS JOIN UNNEST(string_split(e.column12, ',')) AS cnae_secundaria(value)
    WHERE e.column05 = '02';
    """)

    #### Salvando em CSV ####
    saida = os.path.join(data_fribeiro, 'CNPJ.csv')
    conn.execute(f"""
    COPY CNPJ TO '{saida}' 
        (FORMAT CSV, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8');
    """)

    logging.info(f"A tabela 'CNPJ' foi salva em {saida}")

except Exception as e:
    logging.error(f"Ocorreu um erro: {e}")
finally:
    conn.close()

# Registro de fim do processamento
logging.info("Fim do processamento")
