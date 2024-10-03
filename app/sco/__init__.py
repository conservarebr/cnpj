
from dotenv import load_dotenv
import os

# Carregar o arquivo .env
load_dotenv("/home/fribeiro/src/cnpj/example.env")

from app.core.config import Settings

# Carregar as configurações
settings = Settings()


