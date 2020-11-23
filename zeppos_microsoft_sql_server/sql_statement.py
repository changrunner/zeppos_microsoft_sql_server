from zeppos_data_manager.data_cleaner import DataCleaner

class SqlStatement:
    @staticmethod
    def get_table_create_statement(table_schema, table_name, df):
        return \
            DataCleaner.strip_content(
                f"""
                    CREATE TABLE [{table_schema}].[{table_name}] (
                    {SqlStatement._get_columns_create_definition(df)}       
                    )
                """
            )

    @staticmethod
    def _get_columns_create_definition(df):
        definition = ""
        for column_name in df.columns:
            definition += f"{column_name} varchar(50), \n"

        return definition[:-3]

    @staticmethod
    def get_does_table_exist_statement(table_schema, table_name):
        return DataCleaner.strip_content("select count(1) as record_count \n" \
               "from INFORMATION_SCHEMA.tables \n" \
               f"where TABLE_SCHEMA = '{table_schema}' \n" \
               f"  and TABLE_NAME = '{table_name}'")
