import datetime
import logging
import time

import elasticsearch
import psycopg2

from config import index_name, ESSettings, PostgresSettings
from utils import backoff, get_last_update
from loader import create_index
from transformer import create_data_for_elastic
from extractor import PostgresExtractor

logger = logging.getLogger(__name__)


def main():
    es_conn, pg_conn = connect()
    extractor = PostgresExtractor(pg_conn)

    if not es_conn.indices.exists(index=index_name):
        create_index(es_conn)
        initial_data = extractor.get_data()
        entries = create_data_for_elastic(initial_data)

        return es_conn.bulk(index=index_name, body=entries)

    now_plus_5_min = datetime.datetime.now() + datetime.timedelta(minutes=5)
    last_updated_time = get_last_update()

    if now_plus_5_min.strftime("%Y-%m-%d %H:%M:%S") > last_updated_time:
        updated_data = extractor.get_data(last_updated_time)
        entries = create_data_for_elastic(updated_data)

        return es_conn.bulk(index=index_name, body=entries)


@backoff(loger=logger)
def connect():
    es_conn = elasticsearch.Elasticsearch([ESSettings().host], request_timeout=300)
    pg_conn = psycopg2.connect(**PostgresSettings().dict())
    return es_conn, pg_conn


if __name__ == '__main__':
    while True:
        main()

        time.sleep(10)
