from pydantic import BaseModel

class Settings(BaseModel):
    path_s3:str
    s3_access_key_id:str
    s3_secret_access_key:str
    s3_endpoint:str
    s3_region:str 
    s3_use_ssl:bool
    s3_url_style:str
    path_file_cnpj: str
    path_file_endereco:str
    cnae: list[str]
    geocode_url: str
    geocode_timeout:int
    num_linhas_max: int