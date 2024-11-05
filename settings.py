from pydantic import BaseModel
from typing import Optional

class Settings(BaseModel):
    path_s3:str
    s3_access_key_id:str
    s3_secret_access_key:str
    s3_endpoint:str
    s3_region:str 
    s3_use_ssl:bool
    s3_url_style:str
    path_arquivos_baixados: str 
    path_arquivos_saida: str 
    filter_field: Optional[list[str]] = None