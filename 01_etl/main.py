import datetime

import elasticsearch

from config import es_dsl, index_name
from backoff import backoff
from loader import create_index, get_updated_data, get_last_update
from transformer import create_data_for_elastic
from extractor import get_postgres_data


@backoff()
def main():
    if not es.indices.exists(index=index_name):
        create_index()
        initial_data = get_postgres_data()
        entries = create_data_for_elastic(initial_data)

        return es.bulk(index=index_name, body=entries)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_updated_time = get_last_update()

    if now > last_updated_time:
        updated_data = get_updated_data(last_updated_time)
        entries = create_data_for_elastic(updated_data)

        return es.bulk(index=index_name, body=entries)


if __name__ == '__main__':
    es = elasticsearch.Elasticsearch([es_dsl], request_timeout=300)

    main()
