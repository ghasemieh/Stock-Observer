from log_setup import get_logger
from configparser import ConfigParser
import numpy as np
import matplotlib.pyplot as plt
import mysql.connector as mysql
from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import sympy as sp
from datetime import datetime, timedelta, date

from stock_observer.database.database_communication import MySQL_Connection
from utils import save_csv

logger = get_logger(__name__)


class Analyzer:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.main_table_name = self.config['MySQL']['main table name']

    def analysis(self) -> str:
        data_df = self.data_load(main_table_name=self.main_table_name, day_shift=30)
        s1 = self.BB_check(data_df=data_df)
        s2 = self.MA_cross_angle_diff(data_df=data_df)
        s3 = self.ATR_slope_change(data_df=data_df)
        s4 = self.ATR_range(data_df=data_df)
        s5 = self.CCI_change(data_df=data_df)
        result = self.alert_message_generator(s1=s1, s2=s2, s3=s3, s4=s4, s5=s5)
        return result

    def data_load(self, main_table_name: str, day_shift: int) -> DataFrame:
        logger.info("Data loading from main database")
        starting_date = date.today() - timedelta(days=(day_shift + 3))
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {main_table_name} WHERE date > '{starting_date}';")
        return data_df

    def BB_check(self, data_df: DataFrame) -> int:
        logger.info("Bollinger band check")

    def MA_cross_angle_diff(self, data_df: DataFrame) -> int:
        logger.info("MA-5 and MA-20 cross point angle check")

    def ATR_slope_change(self, data_df: DataFrame) -> int:
        logger.info("ATR slope change check")

    def ATR_range(self, data_df: DataFrame) -> int:
        logger.info("ATR 1.5 range check")

    def CCI_change(self, data_df: DataFrame) -> int:
        logger.info("CCI change check")

    def alert_message_generator(self, s1: int, s2: int, s3: int, s4: int, s5: int) -> str:
        message = ""
        return message
