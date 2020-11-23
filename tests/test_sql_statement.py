import unittest
from zeppos_microsoft_sql_server.sql_statement import SqlStatement
import pandas as pd

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        self.assertEqual("<class 'zeppos_microsoft_sql_server.sql_statement.SqlStatement'>", str(type(SqlStatement())))

    def test__get_columns_create_definition_method(self):
        df = pd.DataFrame({'column_1': [3600],
                           'column_2': ['12'],
                           'column_3': [23]
                           }, columns=['column_1', 'column_2', 'column_3'])
        self.assertEqual("column_1 varchar(50), \ncolumn_2 varchar(50), \ncolumn_3 varchar(50)",
                         SqlStatement._get_columns_create_definition(df))

    def test_get_table_create_statement(self):
        df = pd.DataFrame({'column_1': [3600],
                           'column_2': ['12'],
                           'column_3': [23]
                           }, columns=['column_1', 'column_2', 'column_3'])
        self.assertEqual("CREATE TABLE [dbo].[test_table] (\ncolumn_1 varchar(50),\ncolumn_2 varchar(50),\ncolumn_3 varchar(50)\n)",
            SqlStatement.get_table_create_statement('dbo', 'test_table', df))

    def test_get_does_table_exist_statement(self):
        self.assertEqual("select count(1) as record_count\nfrom INFORMATION_SCHEMA.tables\nwhere TABLE_SCHEMA = 'dbo'\nand TABLE_NAME = 'test'",
            SqlStatement.get_does_table_exist_statement('dbo','test'))


if __name__ == '__main__':
    unittest.main()