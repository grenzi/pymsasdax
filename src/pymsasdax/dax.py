import clr
import pandas as pd
import dateparser


class Connection:
    def __init__(
        self,
        conn_str=None,
        initial_catalog=None,
        data_source=None,
        uid="",
        password="",
        tidy_column_names=True,
        tidy_map_function=None,
        timeout=30,
    ):
        if conn_str is not None:
            self._connection_string = conn_str
        else:
            if not "initial_catalog" or not "data_source":
                raise ValueError(
                    f"initial_catalog and data_source must be specificed if not passing a connection string"
                )
            self._connection_string = f"Provider=MSOLAP;Persist Security Info=True;Initial Catalog={initial_catalog};Data Source={data_source};Timeout={timeout};UID={uid};Password={password}"

        self._connection = None
        self._tidy_column_names = tidy_column_names
        self._tidy_map_function = tidy_map_function
        clr.AddReference("System.Data")

    def _handle_oledb_field(self, f):
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
        newname = c.replace("[", "_").replace("]", "_").replace(" ", "_")
        return newname.strip("_")

    def __enter__(self):
        # don't be lazy - force connection if we're entering with with
        if self._connection is None:
            import System.Data.OleDb as ADONET

            self._connection = ADONET.OleDbConnection(self._connection_string)
            self._connection.Open()
            return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._connection.Close()

    def query(self, daxcmd):
        # lazy connection
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
