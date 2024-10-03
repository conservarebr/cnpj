from dotenv import load_dotenv
from pydantic_settings import BaseSettings
import os

load_dotenv("/home/fribeiro/src/cnpj/example.env")

class Settings(BaseSettings):
    data_path: str

settings = Settings()