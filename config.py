from settings import Settings

async def load_settings() -> Settings:
    return Settings(
        path_s3="s3://dblocation/cnpj",
        s3_access_key_id='U597zxiH0ZXgx68Atlad',
        s3_secret_access_key='tdh4j4PJkerzxWEfnqo5d2bbXfLOq0JfCidOrLhd',
        s3_endpoint='s3.iocasta.com.br',
        s3_region='us-west-rack',
        s3_use_ssl=True,
        s3_url_style='path',
        path_file_cnpj=r"/home/fribeiro/bases/CNPJ",
        path_file_endereco=r"/home/fribeiro/bases/CNPJ/endereco.csv",
        cnae=['4110700', '6435201', '6470101', '6470103', '6810201', '6810202', '6810203', '6821801', '6821802', '6822600', '7490104'],
        geocode_url="http://venus.iocasta.com.br:8080/search.php?q=", 
        geocode_timeout=10 
        num_linhas_max= 10
    )
