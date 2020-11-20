import unittest
from zeppos_microsoft_sql_server.ms_connection_string import MsConnectionString

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        ms_connection_string = MsConnectionString(
                "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;"
        )
        self.assertEqual("{ODBC Driver 13 for SQL Server}", ms_connection_string.odbc_driver)
        self.assertEqual("localhost\sqlexpress", ms_connection_string.server_name)
        self.assertEqual("master", ms_connection_string.database_name)
        self.assertEqual(True, ms_connection_string.trusted_connection)


if __name__ == '__main__':
    unittest.main()
