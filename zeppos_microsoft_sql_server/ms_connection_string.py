class MsConnectionString:
    def __init__(self, connection_string):
        self.value = connection_string
        self.odbc_driver = None
        self.server_name = None
        self.database_name = None
        self.trusted_connection = False
        self.password = None
        self.user_name = None
        self._parse_connection_string(connection_string)

    def _parse_connection_string(self, connection_string):
        conn_array = connection_string.split(';')

        for conn_value_pair in conn_array:
            conn_value_pair_array = conn_value_pair.split('=')
            if len(conn_value_pair_array) == 2:
                key = conn_value_pair_array[0].strip().upper()
                value = conn_value_pair_array[1]

                if key == "DRIVER":
                    self.odbc_driver = value
                if key == "SERVER":
                    self.server_name = value
                if key == "DATABASE":
                    self.database_name = value
                if key == "TRUSTED_CONNECTION":
                    self.trusted_connection = True

    @staticmethod
    def get_pyodbc_connection_string(server_name, database_name="master", odbc_version=17):
        return f"DRIVER={{ODBC Driver {odbc_version} for SQL Server}}; SERVER={server_name}; " \
               f"DATABASE={database_name}; Trusted_Connection=yes;"


