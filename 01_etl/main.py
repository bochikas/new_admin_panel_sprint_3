import datetime
import time

import elasticsearch
import psycopg2

from config import index_name, ESSettings, PostgresSettings
from utils import backoff, get_last_update
from loader import create_index
from transformer import create_data_for_elastic
from extractor import get_postgres_data, get_updated_data


@backoff()
def main():
    if not es_conn.indices.exists(index=index_name):
        create_index(es_conn)
        initial_data = get_postgres_data(pg_conn)
        entries = create_data_for_elastic(initial_data)

        return es_conn.bulk(index=index_name, body=entries)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_updated_time = get_last_update()

    if now > last_updated_time:
        updated_data = get_updated_data(pg_conn, last_updated_time)
        entries = create_data_for_elastic(updated_data)

        return es_conn.bulk(index=index_name, body=entries)


if __name__ == '__main__':
    es_conn = elasticsearch.Elasticsearch([ESSettings().host], request_timeout=300)
    pg_conn = psycopg2.connect(**PostgresSettings().dict())

    while True:
        main()

        time.sleep(10)
