import logging
import datetime
from typing import List

from elasticsearch.client import Elasticsearch

from backoff import backoff
from config import es_schema_path, last_state_key
from state import State

logger = logging.getLogger(__name__)


class ESLoader:
    def __init__(self, connection: Elasticsearch, index_name: str) -> None:
        self.es_conn = connection
        self.index_name = index_name

    @backoff(loger=logger)
    def create_index(self) -> None:
        """
        Создание индекса
        :return: None
        """
        with open(es_schema_path, 'r') as file:
            data = file.read()

        self.es_conn.indices.create(index=self.index_name, body=data)
        logger.info('Создание индекса завершено')

    @backoff(loger=logger)
    def bulk_create(self, entries: List[dict], state: State) -> None:
        """
        Массовое создание документов в Elasticsearch

        :param entries: список записей в виде объектов модели Movie
        :param state: объект класса State для хранения состояний
        :return: None
        """
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.set_state(last_state_key, now)

        self.es_conn.bulk(index=self.index_name, body=entries)
