# openaddress


URL:  https://batch.openaddresses.io/data#map=0/0/0

Instruções:
- Diretório no MinIo: s3://geoserver/br/
- Criar banco de dados DuckDb: /mnt/disk1/data/openaddress/openaddress.db
- Quando conectar fornecer o banco de dados "openaddress"
- Nome da rua tem que ser em maiúscula e sem acento;
- CEP tem que ser no formato 30340-620;


Obs:
- Estou pensando em não passar o nome da cidade, somente CEP e nome da rua;
- Se ficar mais rápido podemos copiar do S3 e colocar os arquivos em um diretório;

```py
INSTALL spatial;
LOAD spatial;

INSTALL httpfs;
LOAD httpfs;
set s3_region='us-west-rack';
SET s3_access_key_id='U597zxiH0ZXgx68Atlad';
set s3_secret_access_key='tdh4j4PJkerzxWEfnqo5d2bbXfLOq0JfCidOrLhd';
set s3_endpoint='s3.iocasta.com.br';
set s3_region='us-west-rack';
set s3_use_ssl=true;
set s3_url_style='path';

select * from st_read('s3://geoserver/br/ac/statewide-addresses-state.geojson') ;




campos_desnecessarios= ["hash", "unit", "region", "id", "city", "district"]

brasil:str = "ac,al,am,ap,ba,ce,df,es,go,ma,mg,ms,mt,pa,pb,pe,pi,pr,rj,rn,ro,rr,rs,sc,se,sp,to"
for uf in ufs list[str] = split(brasil,","):
	sql =  f"create table {uf} as "\
			"select * " \
			"from st_read('s3://geoserver/br/{uf}/statewide-addresses-state.geojson');"
	execute(sql)
	for cpo in campos_desnecessarios:
		sql = f"ALTER TABLE {tbl_name} DROP COLUMN cpo;"
		execute(sql)


arquivos = ["br/es/vitoria-addresses-city.geojson",
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
			"br/rs/canoas-addresses-city.geojson"]
for arquivo in ufs list[str] = arquivos:
	tbl_name: str = (arquivo[7:]).replace("-addresses-city.geojson", "")
	sql =  f"create table {tbl_name} as "\
			"select * " \
			"from st_read('s3://geoserver/{arquivo}');"
	execute(sql)
	for cpo in campos_desnecessarios:
		sql = f"ALTER TABLE {tbl_name} DROP COLUMN cpo;"
		execute(sql)



Busca --->  passar street,
	    	number, ??????
			postcode 
receber-->	geometry
```
