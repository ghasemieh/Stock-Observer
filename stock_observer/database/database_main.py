import yfinance as yf
from pathlib import Path
from utils import save_csv
from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, to_datetime
from stock_observer.database.database_communication import MySQL_Connection
from sqlalchemy import create_engine

import pymysql

import pandas as pd

logger = get_logger(__name__)


class Main_DB:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.main_table_name = config['MySQL']['main table name']

    def insertion(self, data_df: DataFrame):
        mysql = MySQL_Connection(config=self.config)
        test = mysql.select(f"SELECT * FROM {self.main_table_name} LIMIT 3;")
        if test is None:
            logger.warning(f"There is no {self.main_table_name} table in the database")
            mysql.insert_df(data_df=data_df, if_exists='fail')
        else:
            mysql = MySQL_Connection(config=self.config)
            mysql.update(table_name=self.main_table_name, data_df=data_df)
