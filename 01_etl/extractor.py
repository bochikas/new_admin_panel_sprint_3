import logging

from backoff import backoff
from sql_queries import all_data_query

logger = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self, connection, batch_size):
        self.connection = connection
        self.limit = batch_size

    @backoff(loger=logger)
    def get_data(self, last_update_time):
        cursor = self.connection.cursor()

        try:
            cursor.execute(all_data_query % last_update_time)

            while data := cursor.fetchmany(self.limit):
                yield data

        except Exception as error:
            logger.error(error)

