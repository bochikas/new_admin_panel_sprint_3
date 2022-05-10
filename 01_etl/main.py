import datetime
import logging
import time

import elasticsearch
import psycopg2

from config import index_name, ESSettings, PostgresSettings
from utils import backoff, get_last_update
from loader import ESLoader
from transformer import Transformer
from extractor import PostgresExtractor

logger = logging.getLogger(__name__)


def main():
    es_conn, pg_conn = connect()
    extractor = PostgresExtractor(pg_conn)
    transformer = Transformer()
    loader = ESLoader(es_conn, index_name)

    if not es_conn.indices.exists(index=index_name):
        loader.create_index()
        initial_data = extractor.get_data()
        entries = transformer.compile_data(initial_data)

        return loader.bulk_create(entries)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_updated_time = get_last_update()
    updated_data = extractor.get_data(last_updated_time)

    if now > last_updated_time and updated_data:
        entries = transformer.compile_data(updated_data)

        return loader.bulk_create(entries)


@backoff(loger=logger)
def connect():
    es_conn = elasticsearch.Elasticsearch([ESSettings().host], request_timeout=300)
    pg_conn = psycopg2.connect(**PostgresSettings().dict())
    return es_conn, pg_conn


if __name__ == '__main__':
    while True:
        main()

        time.sleep(10)
