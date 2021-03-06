import unittest
from zeppos_microsoft_sql_server.ms_sql_server import MsSqlServer
import pandas as pd
import pyodbc
import os

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

    def test_create_table_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        df = pd.DataFrame({'column_1': [3600],
                           'column_2': ['12'],
                           'column_3': [23]
                           }, columns=['column_1', 'column_2', 'column_3'])
        df['column_1'] = df['column_1'].astype(object)
        df['column_2'] = df['column_2'].astype(str)
        df['column_3'] = df['column_3'].astype(int)
        ms_sql.drop_table("dbo", "table_does_not_exist")
        self.assertEqual(True, ms_sql.create_table("dbo", "table_does_not_exist", df))

    def test_does_table_exists(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual(False, ms_sql.does_table_exists('dbo', 'test123456123'))

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

    def test_1_read_data_into_dataframe_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual(1,
            ms_sql.read_data_into_dataframe("SELECT TOP 1 COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS").shape[0])

    def test_2_read_data_into_dataframe_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        self.assertEqual(1, ms_sql.read_data_into_dataframe("""
                SET NOCOUNT ON; -- This has to be here.
                
                DROP TABLE IF EXISTS #tmp
                
                SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME into #tmp 
                FROM INFORMATION_SCHEMA.COLUMNS
            
                SELECT count(1) as RECORD_COUNT from #tmp
            """).shape[0])

    def test_extract_to_csv_method(self):
        ms_sql = MsSqlServer(
            "DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        csv_file = ms_sql.extract_to_csv("select table_schema, table_name from information_schema.tables", r"c:\temp", "test.csv")
        self.assertEqual(True, os.path.exists(csv_file.full_file_name))
        df = pd.read_csv(csv_file.full_file_name, sep="|")
        self.assertGreater(df.shape[0], 0)
        os.remove(csv_file.full_file_name)


if __name__ == '__main__':
    unittest.main()
