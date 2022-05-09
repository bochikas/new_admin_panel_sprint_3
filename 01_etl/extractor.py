import logging
import psycopg2

from config import pg_dsl
from loader import set_last_update
from sql_queries import all_data_query

logger = logging.getLogger(__name__)


def get_postgres_data():
    conn = psycopg2.connect(**pg_dsl)
    cursor = conn.cursor()
    try:
        cursor.execute(all_data_query)
        row = cursor.fetchall()
        set_last_update()
        return row
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()

