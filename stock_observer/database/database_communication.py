# Connecting to the database
# importing 'mysql.connector' as mysql for convenient
from configparser import ConfigParser
import mysql.connector as mysql
from pandas import DataFrame
from log_setup import get_logger

logger = get_logger(__name__)


class MySQL_Connection:
    def __init__(self, config: ConfigParser):
        self.config = config
        # connecting to the database using 'connect()' method
        self.server = config['MySQL']['server']
        self.username = config['MySQL']['username']
        self.password = config['MySQL']['password']
        self.database = config['MySQL']['database']
        self.conection = mysql.connect(host=self.server,
                                       user=self.username,
                                       passwd=self.password,
                                       database=self.database)
        # creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'

        self.cursor = self.conection.cursor()

    def __del__(self):
        self.cursor.close()
        self.conection.close()

    def show_db(self):
        try:
            self.cursor.execute("SHOW DATABASES")
            # 'fetchall()' method fetches all the rows from the last executed statement
            databases = self.cursor.fetchall()  # it returns a list of all databases present

            # printing the list of databases
            print(databases)
        except Exception as e:
            logger.error("show database error")
            logger.error(e)

    def create_table(self, table_name: str) -> None:
        try:
            self.cursor.execute(f"CREATE TABLE {table_name} ("
                                f"ticker VARCHAR(255), "
                                f"date date,"
                                f"open float,"
                                f"high float,"
                                f"low float,"
                                f"close float,"
                                f"volume float,"
                                f"dividends float,"
                                f"stock_splits float);")
        except Exception as e:
            logger.error("create table error")
            logger.error(e)

    def show_table(self) -> None:
        try:
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()  # it returns list of tables present in the database
            # showing all the tables one by one
            for table in tables:
                print(table)
        except Exception as e:
            logger.error("show table error")
            logger.error(e)

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
            self.conection.commit()

            logger.info(f"{self.cursor.rowcount} record inserted")
        except Exception as e:
            logger.error("insert error")
            logger.error(e)

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
            self.conection.commit()
            logger.info(f"{self.cursor.rowcount} record inserted")
        except Exception as e:
            logger.error("insert many error")
            logger.error(e)

    def select(self, query) -> DataFrame:
        try:
            # getting records from the table
            self.cursor.execute(query)
            # fetching all records from the 'cursor' object
            records = self.cursor.fetchall()
            data = DataFrame(records, columns=['ticker', 'date', 'open', 'high', 'low',
                                               'close', 'volume', 'dividends', 'stock_splits'])
            return data
        except Exception as e:
            logger.error("select error")
            logger.error(e)

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
            logger.error("delete error")
            logger.error(e)

    def update(self, query) -> None:
        """
        :param query: query = "UPDATE users SET name = 'Kareem' WHERE id = 1"
        :return: None
        """
        try:
            # executing the query
            self.cursor.execute(query)
            # final step to tell the database that we have changed the table data
            self.cursor.commit()
        except Exception as e:
            logger.error("update error")
            logger.error(e)