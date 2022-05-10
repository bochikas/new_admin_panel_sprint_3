import json
import logging
import datetime
import psycopg2

import elasticsearch

from backoff import backoff
from config import es_dsl, es_schema_path, index_name, last_state_file_path, pg_dsl
from sql_queries import last_update_query

logger = logging.getLogger(__name__)


def set_last_update():
    with open(last_state_file_path, 'w') as f:
        now = datetime.datetime.now()
        data_template = {"last_update": now.strftime("%Y-%m-%d %H:%M:%S")}
        json.dump(data_template, f, indent=4)


def get_last_update():
    with open(last_state_file_path, 'r') as f:
        data = json.load(f)
        return data['last_update']


def get_updated_data(last_update_time):
    conn = psycopg2.connect(**pg_dsl)
    cursor = conn.cursor()
    try:
        cursor.execute(last_update_query % last_update_time)
        row = cursor.fetchall()
        set_last_update()
        return row
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    finally:
        if conn:
            conn.close()


@backoff()
def create_index():
    with open(es_schema_path, 'r') as file:
        data = file.read()
        es = elasticsearch.Elasticsearch([es_dsl], request_timeout=300)
        try:
            es.indices.create(index=index_name, body=data)
        except Exception as ex:
            logger.error(ex)
        else:
            logger.info('Создание индекса завершено')

