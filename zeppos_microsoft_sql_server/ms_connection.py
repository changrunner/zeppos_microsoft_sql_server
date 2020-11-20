from zeppos_microsoft_sql_server.ms_connection_string import MsConnectionString
from zeppos_logging.app_logger import AppLogger
import pyodbc
from urllib import parse
from sqlalchemy import create_engine

class MsConnection:
    def __init__(self, connection_string):
        self.connection_string = MsConnectionString(connection_string)
        self._engine = None
        self._conn = None

    def __del__(self):
        AppLogger.logger.info("Closing and destroying connection.")
        try:
            if self._engine:
                self._engine.dispose()
        except:
            pass

        try:
            if self._conn:
                self._conn.close()
        except:
            pass
        AppLogger.logger.info("Closed and destroyed connection.")

    @property
    def pyodbc_connection(self):
        if self._conn:
            return self._conn
        self._conn = pyodbc.connect(self.connection_string.value)
        return self._conn

    @property
    def sqlalchemy_connection(self):
        if self._engine:
            return self._engine
        params = parse.quote(self.connection_string.value)
        self._engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        return self._engine
