from dotenv import load_dotenv

from app.core.config import Settings
from app.core.translate import Translate


load_dotenv("/home/fribeiro/src/cnpj/example.env")

settings = Settings()
objtranslate = Translate()
