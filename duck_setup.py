import duckdb
from pathlib import Path
import os
# ToDo
# setup for s3 (modules and shit)
# View generator

class DuckDBBuilder:
    """Builds a DuckDB instance that uses data allocated in a S3 instance.
    Make sure to close your Database viewer.
    """
    def __init__(self) -> None:
        self.facts_path = self.get_facts_path()


    def get_facts_tables(self):
        return os.listdir(self.facts_path)

    
    def get_facts_path(self) -> Path:
        """Returns the path where the data is located

        Returns:
            Path: _description_
        """
        if os.getenv("MODE", "dev") == "prod":
            # ToDo
            return None
        else:
            return Path(__file__).parent.joinpath('tmp', 'analytics-dev', 
                                                  'facts')
        
    
    def generate_views(self, conn:duckdb.DuckDBPyConnection) -> None:
        sql_template = "CREATE OR REPLACE VIEW {table} \
                        AS SELECT * FROM '{path}/*.parquet';"
        for table in self.get_facts_tables():
            table_path = self.facts_path.joinpath(table)
            conn.execute(sql_template.format(table=table, path=table_path))


    def build_duck_db(self):
        conn = duckdb.connect(database=str(Path(__file__).parent.
                            joinpath('movi-analytics.duckdb')), read_only=False)
        self.generate_views(conn)


if __name__ == '__main__':
    """Executes the builder
    """
    DuckDBBuilder().build_duck_db()

