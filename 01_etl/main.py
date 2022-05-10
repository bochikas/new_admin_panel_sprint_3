import logging
import time
from typing import Tuple

import elasticsearch
import psycopg2
from elasticsearch.client import Elasticsearch
from psycopg2.extensions import connection

from backoff import backoff
from config import (
    index_name, ESSettings, PostgresSettings, state_file_path, last_state_key, batch_size)
from extractor import PostgresExtractor
from loader import ESLoader
from state import JsonFileStorage, State
from transformer import Transformer

logger = logging.getLogger(__name__)


def main() -> None:
    es_conn, pg_conn = connect()
    storage = JsonFileStorage(state_file_path)
    state = State(storage)
    extractor = PostgresExtractor(pg_conn, batch_size)
    transformer = Transformer()
    loader = ESLoader(es_conn, index_name)

    if not es_conn.indices.exists(index=index_name):
        loader.create_index()

    last_update_time = state.get_state(last_state_key)
    pg_data = extractor.get_data(last_update_time)

    for batch in pg_data:
        entries = transformer.compile_data(batch)
        loader.bulk_create(entries, state)


@backoff(loger=logger)
def connect() -> Tuple[Elasticsearch, connection]:
    """
    Подключение к Elasticsearch и PostgreSQL

    :return: Клиенты соединений Postgre и Elastic
    """
    es_conn = elasticsearch.Elasticsearch([ESSettings().host], request_timeout=300)
    pg_conn = psycopg2.connect(**PostgresSettings().dict())
    return es_conn, pg_conn


if __name__ == '__main__':
    while True:
        main()

        time.sleep(10)
