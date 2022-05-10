import logging

from utils import backoff
from config import es_schema_path, index_name

logger = logging.getLogger(__name__)


@backoff(loger=logger)
def create_index(es):
    with open(es_schema_path, 'r') as file:
        data = file.read()
        try:
            es.indices.create(index=index_name, body=data)
        except Exception as ex:
            logger.error(ex)
        else:
            logger.info('Создание индекса завершено')

