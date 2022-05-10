import logging

from psycopg2 import DatabaseError

from backoff import backoff
from sql_queries import all_data_query, last_update_query

logger = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self, connection):
        self.connection = connection

    @backoff(loger=logger)
    def get_data(self, last_update_time=''):
        cursor = self.connection.cursor()
        try:
            if last_update_time:
                cursor.execute(last_update_query % last_update_time)
            else:
                cursor.execute(all_data_query)
            row = cursor.fetchall()
            return row
        except (Exception, DatabaseError) as error:
            logger.error(error)

