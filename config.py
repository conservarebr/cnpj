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
        cnae=[]
    )
