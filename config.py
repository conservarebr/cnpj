from settings import Settings
import os

async def load_settings() -> Settings:
    return Settings(
        
        # s3
        path_s3=os.getenv('S3_PATH', 's3://dblocation/cnpj'),
        s3_access_key_id=os.getenv('S3_ACCESS_KEY_ID', 'U597zxiH0ZXgx68Atlad'),
        s3_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY', 'tdh4j4PJkerzxWEfnqo5d2bbXfLOq0JfCidOrLhd'),
        s3_endpoint=os.getenv('S3_ENDPOINT', 's3.iocasta.com.br'),
        s3_region=os.getenv('S3_REGION', 'us-west-rack'),
        s3_use_ssl=True,
        s3_url_style='path',
        
        # cnae.py, municipios.py, estabelecimentos.py, endereco.py
        path_file_cnpj=r"/home/fribeiro/bases/CNPJ",

        # nominatim.py
        path_file_endereco=r"/home/fribeiro/bases/CNPJ/endereco.csv",
        
        # open_address.py
        path_db_openaddress=r"/home/fribeiro/bases/CNPJ/openaddress.db",
        brasil=["ac", "al", "am", "ap", "ba", "ce", "df", "es", "go", "ma", "mg", "ms", "mt", "pa", 
                "pb", "pe", "pi", "pr", "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to"],
        campos_desnecessarios=["hash", "unit", "region", "id", "city", "district"],
        arquivos=[
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
    )
