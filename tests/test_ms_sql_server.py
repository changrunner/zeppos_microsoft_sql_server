import unittest
from zeppos_microsoft_sql_server.ms_sql_server import MsSqlServer
import pandas as pd
import pyodbc

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_methods(self):
        self.assertEqual("<class 'zeppos_microsoft_sql_server.ms_sql_server.MsSqlServer'>", str(type(MsSqlServer(""))))

    def test_execute_sql_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual(True, ms_sql.execute_sql("drop table if exists #tmp"))

    def test_drop_table_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual(True, ms_sql.drop_table("dbo", "table_does_not_exist"))

    def test_save_dataframe_by_record_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        ms_sql.drop_table("dbo", "test_table")
        ms_sql.execute_sql("create table dbo.test_table (column_1 int)")

        # test
        df_actual = pd.DataFrame({'column_1': [3600]}, columns=['column_1'])
        self.assertEqual(True, ms_sql.save_dataframe_by_record(df_actual, "dbo", "test_table"))
        self.assertEqual(1, pd.read_sql("SELECT TOP 1 column_1 FROM dbo.test_table", pyodbc.connect(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")).shape[0])

    def test_save_dataframe_in_bulk_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")

        # test
        df_actual = pd.DataFrame({'column_1': [3600]}, columns=['column_1'])
        self.assertEqual(True, ms_sql.save_dataframe_in_bulk(df_actual, "dbo", "test_table"))
        self.assertEqual(1, pd.read_sql("SELECT TOP 1 column_1 FROM dbo.test_table", pyodbc.connect(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")).shape[
            0])

    def test_read_data_into_dataframe_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual(1,
            ms_sql.read_data_into_dataframe("SELECT TOP 1 COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS").shape[0])


if __name__ == '__main__':
    unittest.main()
