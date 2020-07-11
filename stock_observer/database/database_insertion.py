from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class DB_Insertion:
    def __init__(self, config: ConfigParser):
        self.config = config

    def insertion(self, data_df: DataFrame, table_name: str) -> None:
        mysql = MySQL_Connection(config=self.config)
        test = mysql.select(f"SELECT * FROM {table_name} LIMIT 3;")
        if test is None:
            logger.info(f"Start creating {table_name} table in the database")
            mysql.insert_df(data_df=data_df, table_name=table_name, if_exists='fail')
        else:
            logger.info(f"Update {table_name} table in the database")
            mysql.insert_df(data_df=data_df, table_name=table_name, if_exists='replace')
