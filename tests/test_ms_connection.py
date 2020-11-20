import unittest
from zeppos_microsoft_sql_server.ms_connection import MsConnection

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        ms_connection = MsConnection(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")

        self.assertEqual("{ODBC Driver 13 for SQL Server}", ms_connection.connection_string.odbc_driver)
        self.assertEqual("localhost\sqlexpress", ms_connection.connection_string.server_name)
        self.assertEqual("master", ms_connection.connection_string.database_name)
        self.assertEqual(True, ms_connection.connection_string.trusted_connection)

    def test_pyodbc_connection_method(self):
        ms_connection = MsConnection(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual("<class 'pyodbc.Connection'>", str(type(ms_connection.pyodbc_connection)))

    def test_sqlalchemy_connection_method(self):
        ms_connection = MsConnection(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual("<class 'sqlalchemy.engine.base.Engine'>", str(type(ms_connection.sqlalchemy_connection)))


if __name__ == '__main__':
    unittest.main()
