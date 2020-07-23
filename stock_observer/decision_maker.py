from typing import Tuple

from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame
import pandas as pd
import sympy as sp
import configuration
from datetime import datetime, timedelta, date
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class Decision_Maker:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.result_table_name = self.config['MySQL']['result table name']
        self.result_file_path = f"{self.config['Data_Sources']['analysis equity price csv']}_{date.today()}.csv"

    def decide(self) -> Tuple[str, str]:
        data_df = self.data_load()
        alert_message = self.alert_message_generator(result_df=data_df)
        result_file_path =""
        return alert_message, self.result_file_path

    def data_load(self) -> DataFrame:
        logger.info("Data loading from analysis database")
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {self.result_table_name} WHERE date = '{date.today()}';")
        return data_df

    @staticmethod
    def alert_message_generator(result_df: DataFrame) -> str:
        logger.info("Alert message generator started")
        message = ""

        return message


if __name__ == '__main__':
    decision_maker = Decision_Maker(config=configuration.get())
    result = decision_maker.decide()
