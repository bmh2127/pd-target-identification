from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster import FilesystemIOManager

# Configure your DuckDB I/O manager for DataFrames
pd_duckdb_io_manager = DuckDBPandasIOManager(
    database="pd_target_identification.duckdb",
    schema="pd_research"
)

# Default I/O manager for other data types (dictionaries, etc.)
default_io_manager = FilesystemIOManager()