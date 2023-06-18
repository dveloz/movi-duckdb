import duckdb
from pathlib import Path
import os
from typing import List, Tuple
from s3path import S3Path
import logging

# ToDo: Add custom profile to aws config file instead of overwriting.

class DuckDBBuilder:
    """Builds a DuckDB instance that uses data allocated in a S3 instance.
    Make sure to close your Database viewer.
    """
    def __init__(self, in_memory=False) -> None:
        if os.getenv('MODE', 'dev') == 'prod':
            self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
            self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            self._write_aws_config()
            self._write_aws_credentials()
        else:
            self.access_key = None  # populated after calling _verify_aws_settings
            self.secret_key = None  # populated after calling _verify_aws_settings
            self._verify_aws_settings()
        self.s3_db_path = S3Path(f'/movi-data-lake').joinpath("analytics-prod")
        self.duck_db_path = Path().home().joinpath('movicar-duckdb')
        self.logger = logging.getLogger("MoviDuckDBInstaller")
        self.in_memory = in_memory
    
    def _verify_aws_settings(self) -> None:
        """Verifies that the aws settings exist and generates it in case they 
        do not exist.
        """
        aws_shared_credentials_path = Path().home().joinpath('.aws')
        os.makedirs(aws_shared_credentials_path, exist_ok=True)
        if self._creds_exist():
            creds = self._read_creds_vars()
            self.access_key = creds['aws_access_key_id']
            self.secret_key = creds['aws_secret_access_key']
        else:            
            self._prompt_for_credentials()


    def _prompt_for_credentials(self) -> None:
        self.access_key = input("Escribe tu AWS_ACCESS_KEY_ID: ")
        self.secret_key = input("Escribe tu AWS_SECRET_ACCESS_KEY: ")
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_key
        os.environ["AWS_DEFAULT_REGION"] = "us-west-1"


    def _save_credentials(self) -> None:
        """Asks the user if they want to save their credentials. Saves them in 
        case they agree.
        """
        while True:
            save_creds = input("Do you want to save your credentiales (y/n): ")
            if save_creds.lower() in ['y','yes']:
                self._write_aws_config()
                self._write_aws_credentials()
                break
            elif save_creds.lower() in ['n', 'no']:
                break
            else:
                self.logger.error(f"Invalid response '{save_creds}'")


    def _write_aws_config(self) -> None:
        """Writes the credentials configuration file"""
        config_path = Path().home().joinpath('.aws', 'config')
        
        os.makedirs(config_path.parent, exist_ok=True)
        with open(config_path, 'w') as file:
            file.write("[default]\nregion = us-west-1")

    
    def _write_aws_credentials(self) -> None:
        """Writes the credentials file"""
        creds_path = Path().home().joinpath('.aws', 'credentials')
        
        os.makedirs(creds_path.parent, exist_ok=True)
        output = f"[default]\naws_access_key_id = {self.access_key}\n"
        output += f"aws_secret_access_key = {self.secret_key}"
        with open(creds_path, 'w') as file:
            file.write(output)
    

    def _creds_exist(self) -> bool:
        """Checks whether the AWS credentials file exists.

        Returns:
            bool: _description_
        """
        if not Path().home().joinpath('.aws', 'credentials').exists():
            return False
        
        if not Path().home().joinpath('.aws', 'config').exists():
            return False
        
        return True


    def _read_creds_vars(self) -> dict:
        """Reads variables defined in a folder.

        Returns:
            dict: A dictionary with the variables defined in the .aws file
        """
        variables = {}
        creds_path = Path().home().joinpath('.aws', 'credentials')
        with open(creds_path, 'r') as file:
            for line in file:
                line = line.strip()
                if '=' in line:
                    variable, value = line.split('=')
                    variables[variable.strip()] = value.strip()
                
        return variables



    def _crawl_schemas(self) -> List[str]:
        """Returns a dict where each key is a schema of the database

        Returns:
            dict: Dictionary where each key is a schema, values are empty lists
        """
        schemas = []
        for path in self.s3_db_path.iterdir():
            if path.is_dir():
                schemas.append(path.name)
        return schemas


    def _crawl_tables_paths(self, schema:str) -> List[str]:
        """Returns a list of directories containing tables.

        Returns:
            List[str]: List of table names.
        """
        return list(self.s3_db_path.joinpath(schema).iterdir()) 

    
    def _get_schema_map(self) -> dict:
        """Crawls the S3 data to infer the schemas and existing tables.

        Returns:
            dict: dictionary where each key is a schema and the value is a 
        list with the tables in the schema
        """
        schema_map = {}
        self.logger.info("Exploring Movicar data...")

        schemas = self._crawl_schemas()
        for schema in schemas:
            schema_map[schema] = self._crawl_tables_paths(schema)
        return schema_map
    

    def _create_schema(self, schema:str, conn:duckdb.DuckDBPyConnection) -> None:
        if schema != 'main':
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


    def _create_table(self, schema:str, table_path:S3Path, 
                      conn:duckdb.DuckDBPyConnection) -> None:
        if schema != 'main':
            sql_template = """CREATE OR REPLACE VIEW {schema}.{table}
                          AS SELECT * FROM '{path}/*.parquet';"""
            conn.execute(sql_template.format(schema=schema, 
                                             table=table_path.name, 
                                             path=table_path.as_uri()
                                                    ))
        else:
            sql_template = """CREATE OR REPLACE VIEW {table}
                          AS SELECT * FROM '{path}/*.parquet';"""
            conn.execute(sql_template.format(table=table_path.name, 
                                             path=table_path.as_uri()
                                                    ))

    def _setup_views(self, conn:duckdb.DuckDBPyConnection, 
                     schema_map:dict) -> None:
        """Creates a set of Views in a given DuckDB instance.
        Args:
            conn (duckdb.DuckDBPyConnection): A connection to the DuckDB 
            instance where the views will be created.
        """

        for schema in schema_map:
            self._create_schema(schema, conn)
            try:
                for table_path in schema_map[schema]:
                    self._create_table(schema, table_path, conn)
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
        

    def build_duck_db(self, return_instance:bool=False) -> duckdb.DuckDBPyConnection:
        """Runner that setups a Movicar DuckDB instance.
        """
        self.logger.info("Creating DuckDB instance...")
        os.makedirs(self.duck_db_path, exist_ok=True)

        if self.in_memory:
            database_file = ':memory:'
        else:
            database_file = str(self.duck_db_path.joinpath('analytics-prod.duckdb'))

        conn = duckdb.connect(
                 database=database_file,
                 read_only=False
                )
        
        self._load_s3_deps(conn)
        # we crawl our database
        schema_map = self._get_schema_map()
        # we save creds locally
        if not self._creds_exist():
            self._save_credentials()
        # we setup our database
        self._setup_views(conn, schema_map)
        self.logger.info("DuckDB setup correctly")
        if return_instance:
            return conn


if __name__ == '__main__':
    """Executes the builder
    """
    logging.basicConfig(level=logging.INFO)

    DuckDBBuilder().build_duck_db()

