import logging

from psycopg2 import DatabaseError

from utils import set_last_update
from sql_queries import all_data_query, last_update_query

logger = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self, connection):
        self.connection = connection


def get_postgres_data(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(all_data_query)
        row = cursor.fetchall()
        set_last_update()
        return row
    except (Exception, DatabaseError) as error:
        logger.error(error)


def get_updated_data(conn, last_update_time):
    cursor = conn.cursor()
    try:
        cursor.execute(last_update_query % last_update_time)
        row = cursor.fetchall()
        set_last_update()
        return row
    except (Exception, DatabaseError) as error:
        logger.error(error)

