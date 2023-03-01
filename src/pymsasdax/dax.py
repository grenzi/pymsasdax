import clr
import pandas as pd
import dateparser


class Connection:
    """A class for running DAX queries against Microsoft SQL Server Analysis Services Tabular Models, Azure Analysis Services, or Power BI XMLA endpoints

    Args:
    initial_catalog (str): Required if conn_str is not specified. Initial Catalog of the server.
    data_source (str): Required if conn_str is not specified. Data Source of the server.
    uid (str): User ID for authentication. Default is empty string.
    password (str): Password for authentication. Default is empty string.
    effective_user_name (str): Username to impersonate, usually a UPN, such as joe@contoso.com
    tidy_column_names (bool): Flag for tidying column names. Default is True.
    tidy_map_function (function): A function to tidy column names. Default is to use the internal one, which leaves capitalization alone, replaces spaces with underscores, and removes square brackets.
    timeout (int): Timeout period (in seconds) for running queries. Default is 30.
    conn_str (str): Optional connection string for the connection.
    **kwargs: Additional keyword value pairs, will be added to the connection string. 

    Raises:
    ValueError: If initial_catalog and data_source are not specified when conn_str is not passed.

    Methods:
    _handle_oledb_field(f): Handles different data types returned from OleDbConnection query.
    _cleanup_column_name(c): Cleans up the column name by replacing some characters with underscores.
    __enter__(): Opens the connection when entering the context.
    __exit__(exc_type, exc_value, traceback): Closes the connection when exiting the context.
    query(daxcmd): Executes a DAX query on the connected server and returns the result as a Pandas DataFrame.

    Returns: Nothing
    """
    def __init__(
        self,
        initial_catalog=None,
        data_source=None,
        uid="",
        password="",
        effective_user_name=None,
        tidy_column_names=True,
        tidy_map_function=None,
        timeout=30,
        conn_str=None,
        **kwargs
    ):
        """Initializes the Connection object."""
        if conn_str is not None:
            self._connection_string = conn_str
        else:
            if not "initial_catalog" or not "data_source":
                raise ValueError(
                    f"initial_catalog and data_source must be specificed if not passing a connection string"
                )
            self._connection_string = f"Provider=MSOLAP;Persist Security Info=True;Initial Catalog={initial_catalog};Data Source={data_source};Timeout={timeout};UID={uid};Password={password}"
            if effective_user_name:
                self._connection_string += f"EffectiveUserName={effective_user_name};"
            for key, value in kwargs.items():
                self._connection_string += f"{key}={value};"

        self._connection = None
        self._tidy_column_names = tidy_column_names
        self._tidy_map_function = tidy_map_function
        clr.AddReference("System.Data")

    def _handle_oledb_field(self, f):
        """Handles different data types returned from OleDbConnection query, converting to the appropriate python type
        Attempts type sniffing by getting the Class of the type from .NET

        Args:
        f: A field from the query result.

        Returns:
        The field converted to the appropriate data type.
        """
        mytype = str(type(f))
        if mytype == "<class 'System.DBNull'>":
            return None
        if mytype == "<class 'int'>":
            return int(f)
        if mytype == "<class 'System.Decimal'>":
            return float(f.ToString())
        if mytype == "<class 'float'>":
            return float(f)
        if mytype == "<class 'str'>":
            return str(f)
        if mytype == "<class 'System.DateTime'>":
            return dateparser.parse(str(f))
        if mytype == "<class 'bool'>":
            return f
        raise Exception("Unknown Type " + mytype)

    def _cleanup_column_name(self, c):
        """Cleans up the column name by replacing some characters with underscores.

        Args:
        c (str): A column name.

        Returns:
        The cleaned-up column name.
        """
        newname = c.replace("[", "_").replace("]", "_").replace(" ", "_")
        return newname.strip("_")

    def __enter__(self):
        """Opens the connection when entering the context.
        While the class normally tries to be lazy about connecting, entering the context forces immediate connection.

        Returns:
        The Connection object.
        """
        if self._connection is None:
            import System.Data.OleDb as ADONET

            self._connection = ADONET.OleDbConnection(self._connection_string)
            self._connection.Open()
            return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Closes the connection when exiting the context."""
        if self._connection is not None:
            self._connection.Close()

    def query(self, daxcmd):
        """Executes a DAX query on the connected server and returns the result as a Pandas DataFrame.

        Args:
        daxcmd (str): A DAX query.

        Returns:
        A Pandas DataFrame representing the result of the query.
        """
        # lazy connection if not already initialized (via enter, etc)
        if self._connection is None:
            import System.Data.OleDb as ADONET

            self._connection = ADONET.OleDbConnection(self._connection_string)
            self._connection.Open()

        command = self._connection.CreateCommand()
        command.CommandText = daxcmd
        reader = command.ExecuteReader()
        schema_table = reader.GetSchemaTable()

        # schema_table rows are query result columns
        columns = []
        for r in schema_table.Rows:
            columns.append(r["ColumnName"])

        rows = [None] * reader.get_RecordsAffected()
        for x in range(0, reader.get_RecordsAffected()):
            reader.Read()
            # https://docs.microsoft.com/en-us/dotnet/api/system.typecode?view=netframework-4.7.2
            # but ints dont have them
            # rows[x] = list( [ reader[c] if reader[c].GetTypeCode() != 2 else None for c in columns] )
            rows[x] = list([self._handle_oledb_field(reader[c]) for c in columns])
        df = pd.DataFrame.from_records(
            columns=columns,
            data=rows,
            coerce_float=True,
        )
        if self._tidy_column_names:
            map_func = self._tidy_map_function or self._cleanup_column_name
            df.rename(columns={c: map_func(c) for c in columns}, inplace=True)
        del rows
        del reader
        return df
