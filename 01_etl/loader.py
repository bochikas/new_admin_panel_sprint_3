import logging
import datetime

from config import es_schema_path, last_state_key
from backoff import backoff

logger = logging.getLogger(__name__)


class ESLoader:
    def __init__(self, connection, index_name):
        self.es_conn = connection
        self.index_name = index_name

    @backoff(loger=logger)
    def create_index(self):
        with open(es_schema_path, 'r') as file:
            data = file.read()
            try:
                self.es_conn.indices.create(index=self.index_name, body=data)
            except Exception as ex:
                logger.error(ex)
            else:
                logger.info('Создание индекса завершено')

    @backoff(loger=logger)
    def bulk_create(self, entries, state):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.set_state(last_state_key, now)

        return self.es_conn.bulk(index=self.index_name, body=entries)
