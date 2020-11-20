from zeppos_logging.app_logger import AppLogger
import pandas as pd
from zeppos_microsoft_sql_server.ms_connection import MsConnection

class MsSqlServer:
    def __init__(self, connection_string):
        self.connection = MsConnection(connection_string)

    def execute_sql(self, sql):
        try:
            crs = self.connection.pyodbc_connection.cursor()
            crs.execute(sql)
            crs.commit()
            crs.close()
            return True
        except:
            return False

    def drop_table(self, table_schema, table_name):
        try:
            AppLogger.logger.info(f'Drop table [{table_schema}].[{table_name}]')
            self.execute_sql(f'DROP TABLE IF EXISTS [{table_schema}].[{table_name}]')
            return True
        except:
            return False

    def save_dataframe_by_record(self, df, table_schema, table_name, batch_size=500):
        """
        save_dataframe will use sql insert statements. One per record.
        This will make the process of inserting rather slow.
        Use with small datasets to direct insert into a table.
        """
        try:
            AppLogger.logger.info(f"Saving data to sql server. Record_count: [{len(df)}]")
            for i in range(0, len(df), batch_size):
                after = i + (batch_size - 1)
                if after > len(df):
                    after = len(df) - 1

                temp = df.truncate(before=i, after=after)
                temp.to_sql(table_name, self.connection.sqlalchemy_connection, if_exists='append', index=False, schema=table_schema)
            return True
        except Exception as error:
            return False

    def read_data_into_dataframe(self, sql_statement):
        try:
            return pd.read_sql(sql_statement, self.connection.pyodbc_connection)
        except:
            return pd.DataFrame()

    # @staticmethod
    # def insert_using_bcp(df, server_name, database_name,
    #                      staging_table_schema, staging_table_name,
    #                      username=None, password=None,
    #                      use_existing_sql_table=False):
    #     sql_config = {
    #         'server': server_name,
    #         'database': database_name,
    #         'username': username,
    #         'password': password
    #     }
    #     print(sql_config)
    #     bdf = bcpy.DataFrame(df)
    #     sql_table = bcpy.SqlTable(sql_config, table=staging_table_name, schema_name=staging_table_schema,
    #                               batch_size=1000)  # bcp default batchsize is 1000
    #     bdf.to_sql(sql_table, use_existing_sql_table=use_existing_sql_table)

