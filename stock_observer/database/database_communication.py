import mysql.connector as mysql
from log_setup import get_logger
from configparser import ConfigParser
from sqlalchemy import create_engine
from pandas import DataFrame, read_sql

logger = get_logger(__name__)


class MySQL_Connection:
    def __init__(self, config: ConfigParser):
        self.config = config
        # connecting to the database using 'connect()' method
        self.server = config['MySQL']['server']
        self.username = config['MySQL']['username']
        self.password = config['MySQL']['password']
        self.database = config['MySQL']['database']
        self.connection = mysql.connect(host=self.server,
                                        user=self.username,
                                        passwd=self.password,
                                        database=self.database)
        # creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def create_table(self, table_name: str, table_type: str, derivative_features=None):
        global query
        logger.info(f"Start creating {table_type} table with {table_name} name in the database")
        if table_type == 'stage':
            query = f"""CREATE TABLE {table_name} (
                        id VARCHAR(20) NOT NULL PRIMARY KEY, 
                        ticker VARCHAR(10) NOT NULL, 
                        date date NOT NULL, 
                        open double, 
                        high double, 
                        low double, 
                        close double, 
                        volume bigint(20));"""
        elif table_type == 'main':
            cols = " double, ".join([str(i) for i in derivative_features]) + " double"
            query = f"""CREATE TABLE {table_name} (
                        id VARCHAR(20) NOT NULL PRIMARY KEY, 
                        ticker VARCHAR(10) NOT NULL, 
                        date date NOT NULL, 
                        open double, 
                        high double, 
                        low double, 
                        close double, 
                        volume bigint(20), 
                        {cols});"""
        elif table_type == 'analysis':
            cols = " double, ".join([str(i) for i in derivative_features]) + " double"
            query = f"""CREATE TABLE {table_name} (
                        id VARCHAR(20) NOT NULL PRIMARY KEY, 
                        ticker VARCHAR(10) NOT NULL, 
                        date date NOT NULL, 
                        {cols});"""
        self.cursor.execute(query)

    def insert(self, table_name, data_df: DataFrame) -> None:
        """
        :param table_name:
        :param data_df: storing values in a variable
                values = ("Hafeez", "hafeez")
        :return: None
        """
        values = [tuple(x) for x in data_df.to_numpy()]
        cols = ", ".join([str(i) for i in data_df.columns.tolist()])
        try:
            query = f"INSERT INTO {table_name} (" + cols + ") VALUES (" + "%s," * (data_df.shape[1] - 1) + "%s)"
            # executing the query with values
            self.cursor.execute(query, values)
            # to make final output we have to run the 'commit()' method of the database object
            self.connection.commit()

            logger.info(f"{self.cursor.rowcount} record inserted")
        except Exception as e:
            logger.error(f"insert error: {e}")
            raise e

    def insert_many(self, table_name, data_df: DataFrame) -> None:
        """
        :param data_df: storing values in a variable
                values = [("Peter", "peter"), ("Amy", "amy"), ("Michael", "michael"), ("Hennah", "hennah")]
        :param table_name:
        :return: None
        """
        # data_df.drop(columns='Date', inplace=True)
        values_list = [tuple(x) for x in data_df.to_numpy()]
        cols = ", ".join([str(i) for i in data_df.columns.tolist()])
        try:
            query = f"INSERT INTO {table_name} (" + cols + ") VALUES (" + "%s," * (data_df.shape[1] - 1) + "%s)"
            # executing the query with values
            self.cursor.executemany(query, values_list)
            # to make final output we have to run the 'commit()' method of the database object
            self.connection.commit()
            logger.info(f"{self.cursor.rowcount} record inserted")
        except Exception as e:
            logger.error(f"insert many error: {e}")
            raise e

    def insert_df(self, data_df: DataFrame, table_name: str, primary_key: str, if_exists: str = 'fail'):
        engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.server}/{self.database}')
        db_connection = engine.connect()
        try:
            # query = f"""SELECT `COLUMN_NAME`
            #                     FROM `INFORMATION_SCHEMA`.`COLUMNS`
            #                     WHERE `TABLE_SCHEMA`='{self.database}'
            #                     AND `TABLE_NAME`='{table_name}';"""
            # col = self.select(query)  # TODO I want to check the column before insert into the data base
            data_df.to_sql(name=table_name, con=db_connection, if_exists=if_exists, index=False,
                           index_label=primary_key)
        except ValueError as vx:
            logger.error(vx)
            raise vx
        except Exception as ex:
            logger.error(ex)
            raise ex
        else:
            logger.info(f"Data insertion into the {table_name} is done")
        finally:
            db_connection.close()

    def select(self, query: str) -> DataFrame:
        engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.server}/{self.database}')
        db_connection = engine.connect()
        try:
            data = read_sql(query, db_connection)
            return data
        except Exception as e:
            logger.warning(f"select error: {e}")
        finally:
            db_connection.close()

    def delete(self, query) -> None:
        """
        :param query: query = "DELETE FROM users WHERE id = 5"
        :return: None
        """
        try:
            # executing the query
            self.cursor.execute(query)
            # final step to tell the database that we have changed the table data
            self.cursor.commit()
        except Exception as e:
            logger.error(f"delete error: {e}")
