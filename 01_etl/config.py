import os

from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), 'addons/.env')

load_dotenv(dotenv_path)

index_name = os.getenv('INDEX_NAME')

es_schema_path = os.path.abspath('addons/es_schema.json')
last_state_file_path = os.path.abspath('addons/last_state.json')

pg_dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT')
}

es_dsl = {
        'scheme': os.getenv('ES_SCHEME'),
        'host': os.getenv('ES_HOST'),
        'port': os.getenv('ES_PORT')
}
