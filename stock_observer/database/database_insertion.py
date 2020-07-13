from pandas import DataFrame
from log_setup import get_logger
from configparser import ConfigParser
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class DB_Insertion:
    def __init__(self, config: ConfigParser):
        self.config = config

    def insertion(self, data_df: DataFrame, table_name: str, table_type: str, derivative_features=None) -> None:
        if not data_df.empty:
            mysql = MySQL_Connection(config=self.config)
            test = mysql.select(f"SELECT * FROM {table_name} LIMIT 3;")
            if test is None:
                mysql.create_table(table_name=table_name, table_type=table_type, derivative_features=derivative_features)
                mysql.insert_df(data_df=data_df, table_name=table_name, primary_key='id', if_exists='append')
            else:
                logger.info(f"Update {table_name} table in the database")
                mysql.insert_df(data_df=data_df, table_name=table_name, primary_key='id', if_exists='append')
        else:
            logger.warning("Database insertion received empty data frame")
