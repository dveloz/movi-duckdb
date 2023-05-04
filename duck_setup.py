import duckdb
from pathlib import Path
import os
from typing import List
from s3path import S3Path
import logging


class DuckDBBuilder:
    """Builds a DuckDB instance that uses data allocated in a S3 instance.
    Make sure to close your Database viewer.
    """
    def __init__(self) -> None:
        self.access_key = input("Escribe tu AWS_ACCESS_KEY_ID: ")
        self.secret_key = input("Escribe tu AWS_SECRET_ACCESS_KEY: ")
        self._verify_aws_settings()
        self.facts_path = self._get_facts_path()
        self.duck_db_path = Path().home().joinpath('movicar-duckdb')
        self.logger = logging.getLogger("MoviDuckDBInstaller")

    
    def _verify_aws_settings(self) -> None:
        """Verifies that the aws settings exist and generates it in case they 
        do not exist.
        """
        aws_shared_credentials_path = Path().home().joinpath('.aws')
        os.makedirs(aws_shared_credentials_path, exist_ok=True)
        
        aws_config_path = Path().home().joinpath('.aws', 'config')
        aws_credentials_path = Path().home().joinpath('.aws', 'credentials')
        self._write_aws_config(aws_config_path)
        self._write_aws_credentials(aws_credentials_path)

    
    def _write_aws_config(self, config_path:Path) -> None:
        """Writes the credentials configuration file"""
        with open(config_path, 'w') as file:
            file.write("[default]\nregion = us-west-1")

    
    def _write_aws_credentials(self, creds_path:Path) -> None:
        """Writes the credentials file"""
        output = f"[default]\naws_access_key_id = {self.access_key}\n"
        output += f"aws_secret_access_key = {self.secret_key}"
        with open(creds_path, 'w') as file:
            file.write(output)

    
    def _crawl_tables_paths(self) -> List[str]:
        """Returns a list of directories containing tables.

        Returns:
            List[str]: List of table names.
        """
        return list(self.facts_path.iterdir())

    
    def _get_facts_path(self) -> Path:
        """Returns the path where the 'facts' data is located

        Returns:
            Path: Path with of the 'facts'.
        """
        return S3Path(f'/movi-data-lake').joinpath("analytics-prod", "facts")
    
    
    def _setup_views(self, conn:duckdb.DuckDBPyConnection) -> None:
        """Creates a set of Views in a given DuckDB instance.

        Args:
            conn (duckdb.DuckDBPyConnection): A connection to the DuckDB 
            instance where the views will be created.
        """
        self.logger.info("Exploring Movicar data...")
        sql_template = """CREATE OR REPLACE VIEW {table}
                          AS SELECT * FROM '{path}/*.parquet';"""
        for table_path in self._crawl_tables_paths():
            try:
                conn.execute(sql_template.format(table=table_path.name, 
                                             path=table_path.as_uri()))
            except duckdb.IOException as e:
                error_msg = f"{e}\n\nCould not connect to S3. Please verify credentials"
                raise IOError(error_msg)


    def _load_s3_deps(self, conn:duckdb.DuckDBPyConnection) -> None:
        """ Executes the necessary commands to create a connection to S3.
        """
        self.logger.info("Installing DuckDB dependencies...")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_region='us-west-1';")
    
        conn.execute(f"SET s3_access_key_id='{self.access_key}';")
        conn.execute(f"SET s3_secret_access_key='{self.secret_key}';")
        

    def build_duck_db(self):
        """Runner that setups a Movicar DuckDB instance.
        """
        self.logger.info("Creating DuckDB instance...")
        os.makedirs(self.duck_db_path, exist_ok=True)
        conn = duckdb.connect(
                 database=str(self.duck_db_path.joinpath('analytics-prod.duckdb')),
                 read_only=False
                )
        self._load_s3_deps(conn)
        self._setup_views(conn)
        self.logger.info("DuckDB setup correctly")


if __name__ == '__main__':
    """Executes the builder
    """
    logging.basicConfig(level=logging.INFO)

    DuckDBBuilder().build_duck_db()

