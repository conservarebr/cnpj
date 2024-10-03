
----- Cnae -----

CREATE TABLE cnae (
    codigo VARCHAR PRIMARY key,
    descricao VARCHAR
);

COPY cnae FROM 'mnt/disk1/data/cnpj/cnaes.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, ENCODING 'UTF8');

select count (*) from cnae

----- Municipios -----

CREATE TABLE municipios (
    codigo VARCHAR PRIMARY key,
    descricao VARCHAR
);

COPY municipios FROM 'mnt/disk1/data/cnpj/municipios.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, ENCODING 'UTF8');

SELECT COUNT (*) from municipios

----- Estabelecimentos -----

CREATE TABLE estabelecimentos (
    cnpj_basico VARCHAR,
    cnpj_ordem VARCHAR, 
    cnpj_dv VARCHAR,
    identificador_matriz_filial VARCHAR,
    nome_fantasia VARCHAR,
    situacao_cadastral VARCHAR,
    data_situacao_cadastral VARCHAR,
    motivo_situacao_cadastral VARCHAR,
    nome_da_cidade_no_exterior VARCHAR,
    pais VARCHAR,
    data_de_inicio_atividade VARCHAR,
    cnae_fiscal_principal VARCHAR,
    cnae_fiscal_secundaria VARCHAR,
    tipo_de_logradouro VARCHAR,
    logradouro VARCHAR,
    numero VARCHAR,
    complemento VARCHAR,
    bairro VARCHAR,
    cep VARCHAR,
    uf VARCHAR,
    municipio VARCHAR,
    ddd_1 VARCHAR,
    telefone_1 VARCHAR,
    ddd_2 VARCHAR,
    telefone_2 VARCHAR,
    ddd_do_fax VARCHAR,
    fax VARCHAR,
    correio_eletronico VARCHAR,
    situacao_especial VARCHAR,
    data_da_situacao_especial VARCHAR
);

COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos0.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos1.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos2.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos3.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos4.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos5.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos6.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos7.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos8.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);
COPY estabelecimentos FROM 'mnt/disk1/data/cnpj/estabelecimentos9.csv' WITH (FORMAT csv, DELIMITER ';', HEADER false, IGNORE_ERRORS true);

-- Etapa 01: Filtrando somente os estabelecimentos ativos e concatenando o cnpj:

CREATE TABLE estabelecimentos_ativos AS
SELECT *
FROM estabelecimentos
WHERE situacao_cadastral = '02';

ALTER TABLE estabelecimentos_ativos ADD COLUMN cnpj_completo VARCHAR;
UPDATE estabelecimentos_ativos
SET cnpj_completo = CONCAT(cnpj_basico, cnpj_ordem, cnpj_dv);

-- Etapa 02: Selecionando somente as variaveis úteis:

CREATE TABLE variaveis_uteis AS
SELECT 
    e.cnpj_completo, 
    e.situacao_cadastral, 
    e.cnae_fiscal_principal, 
    e.unnest(string_split(cnae_fiscal_secundaria, ',')) AS cnae_secundaria,
    e.tipo_de_logradouro, 
    e.logradouro, 
    e.numero, 
    e.complemento, 
    e.bairro, 
    e.cep, 
    e.uf, 
    m.descricao AS municipio_descricao
FROM 
    estabelecimentos_ativos e
LEFT JOIN 
    municipios m ON e.municipio = m.codigo;
   
-- Etapa 03: Selecionando o CNAE desejado:
   
CREATE TABLE filtro_cnae AS
SELECT *
FROM variaveis_uteis
WHERE cnae_fiscal_principal IN (4110700, 6435201, 6470101, 6470103, 6810201, 6810202, 6810203, 6821801, 6821802, 6822600, 7490104)
OR cnae_secundaria IN (4110700, 6435201, 6470101, 6470103, 6810201, 6810202, 6810203, 6821801, 6821802, 6822600, 7490104);

ALTER TABLE filtro_cnae ADD COLUMN endereco VARCHAR;
UPDATE filtro_cnae
SET endereco = CONCAT(tipo_de_logradouro, ' ', logradouro, ' ', numero, ' ', bairro, ' ', municipio_descricao, ' ', uf, ' ', cep);
ALTER TABLE filtro_cnae ADD COLUMN endereco_editado_1 VARCHAR;
UPDATE filtro_cnae
SET endereco_editado_1 = CONCAT(tipo_de_logradouro, ' ', logradouro, ' ', numero, ' ', bairro, ' ', municipio_descricao, ' ', uf);
ALTER TABLE filtro_cnae ADD COLUMN endereco_editado_2 VARCHAR;
UPDATE filtro_cnae
SET endereco_editado_2 = CONCAT(cep);

CREATE TABLE cnae AS
SELECT DISTINCT cnpj_completo, endereco, endereco_editado_1, endereco_editado_2
FROM filtro_cnae;