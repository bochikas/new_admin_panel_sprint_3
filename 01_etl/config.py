import os

from dotenv import load_dotenv
from pydantic import BaseSettings, Field

dotenv_path = os.path.join(os.path.dirname(__file__), 'addons/.env')

load_dotenv(dotenv_path)

index_name = os.getenv('ES_INDEX')

es_schema_path = os.path.abspath('addons/es_schema.json')
last_state_file_path = os.path.abspath('addons/last_state.json')


class PostgresSettings(BaseSettings):
    dbname: str = Field(..., env='POSTGRES_DB')
    user: str = Field(..., env='POSTGRES_USER')
    password: str = Field(..., env='POSTGRES_PASSWORD')
    host: str = Field(..., env='POSTGRES_HOST')
    port: str = Field(..., env='POSTGRES_PORT')
    options: str = '-c search_path=content'


class ESSettings(BaseSettings):
    host: str = Field(..., env='ES_HOST')
    index: str = Field(..., env='ES_INDEX')
