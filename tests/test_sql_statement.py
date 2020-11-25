import unittest
from zeppos_microsoft_sql_server.ms_sql_statement import MsSqlStatement
import pandas as pd
import os

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        self.assertEqual("<class 'zeppos_microsoft_sql_server.ms_sql_statement.MsSqlStatement'>", str(type(MsSqlStatement())))

    def test__get_columns_create_definition_method(self):
        df = pd.DataFrame({'column_1': [3600],
                           'column_2': ['12'],
                           'column_3': [23]
                           }, columns=['column_1', 'column_2', 'column_3'])
        self.assertEqual("column_1 varchar(50), \ncolumn_2 varchar(50), \ncolumn_3 varchar(50)",
                         MsSqlStatement._get_columns_create_definition(df))

    def test_get_table_create_statement(self):
        df = pd.DataFrame({'column_1': [3600],
                           'column_2': ['12'],
                           'column_3': [23]
                           }, columns=['column_1', 'column_2', 'column_3'])
        self.assertEqual("CREATE TABLE [dbo].[test_table] (\ncolumn_1 varchar(50),\ncolumn_2 varchar(50),\ncolumn_3 varchar(50)\n)",
                         MsSqlStatement.get_table_create_statement('dbo', 'test_table', df))

    def test_get_does_table_exist_statement_method(self):
        self.assertEqual("select count(1) as record_count\nfrom INFORMATION_SCHEMA.tables\nwhere TABLE_SCHEMA = 'dbo'\nand TABLE_NAME = 'test'",
                         MsSqlStatement.get_does_table_exist_statement('dbo', 'test'))

    def test_get_from_file_method(self):
        os.makedirs(r"c:\temp", exist_ok=True)
        with open(r"c:\temp\query_test_get_from_file_method.sql", 'w') as fl:
            fl.write("test sql")
        self.assertEqual("test sql", MsSqlStatement.get_from_file(r"c:\temp\query_test_get_from_file_method.sql"))
        if os.path.exists(r"c:\temp\query_test_get_from_file_method.sql"):
            os.remove(r"c:\temp\query_test_get_from_file_method.sql")


if __name__ == '__main__':
    unittest.main()