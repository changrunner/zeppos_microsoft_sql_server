from zeppos_logging.app_logger import AppLogger
import pandas as pd
from zeppos_microsoft_sql_server.ms_connection import MsConnection
from zeppos_bcpy.sql_configuration import SqlConfiguration
from zeppos_bcpy.dataframe import Dataframe
from zeppos_microsoft_sql_server.sql_statement import SqlStatement

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

    def create_table(self, table_schema, table_name, df):
        try:
            if isinstance(df, pd.core.frame.DataFrame):
                if not self.does_table_exists(table_schema, table_name):
                    AppLogger.logger.info(f'Create table [{table_schema}].[{table_name}]')
                    self.execute_sql(SqlStatement.get_table_create_statement(table_schema, table_name, df))

            return True
        except:
            return False

    def does_table_exists(self, table_schema, table_name):
        df = self.read_data_into_dataframe(
            SqlStatement.get_does_table_exist_statement(table_schema,table_name)
        )
        return df.iloc[0]['record_count'] > 0

    def save_dataframe_by_record(self, df, table_schema, table_name, batch_size=500):
        """
        save_dataframe will use sql insert statements. One per record.
        This will make the process of inserting rather slow.
        Use with small datasets to direct insert into a table.
        """
        try:
            AppLogger.logger.info(f"Saving data to sql server "
                                  f"[{table_schema}].[{table_name}]. Record_count: [{len(df)}]")
            for i in range(0, len(df), batch_size):
                after = i + (batch_size - 1)
                if after > len(df):
                    after = len(df) - 1

                temp = df.truncate(before=i, after=after)
                temp.to_sql(table_name, self.connection.sqlalchemy_connection, if_exists='append', index=False, schema=table_schema)
            return True
        except Exception as error:
            AppLogger.logger.error(f"Could not save dataframe by record: {error}")
            return False

    def save_dataframe_in_bulk(self, df, schema_name, table_name, use_existsing=False):
        try:
            sql_configuration = SqlConfiguration(
                server_type="microsoft",
                server_name=self.connection.connection_string.server_name,
                database_name=self.connection.connection_string.database_name,
                schema_name=schema_name,
                table_name=table_name
            )
            Dataframe.to_sqlserver_creating_instance(df, sql_configuration)
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

