"""Class for interacting with AWS Redshift."""
import logging

import psycopg2


logger = logging.getLogger(__name__)


class Redshift(object):
    """Redshift psycopg2 interface."""

    def __init__(self, config):
        """Instantiate with required configuration and credentials."""
        self.config = config
        self.connection = psycopg2.connect(
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            sslmode="require",
        )
        # Disable autocommit; force the user to commit explicitly through the code
        self.connection.set_session(autocommit=False)
        self.cursor = self.connection.cursor()

    def execute(self, query, data=None):
        """Pass the SQL and data on to be executed to the database."""
        self.cursor.execute(query, data)
        logger.info(self.cursor.statusmessage)

    def commit(self):
        """Commit the db."""
        self.connection.commit()

    def rollback(self):
        """Rollback pending transactions."""
        self.connection.rollback()

    def __enter__(self):
        """Enter context manager."""
        self.connection.__enter__()
        self.cursor.__enter__()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Exit context manager."""
        try:
            self.cursor.__exit__(exception_type, exception_value, traceback)
        finally:
            self.connection.__exit__(exception_type, exception_value, traceback)
