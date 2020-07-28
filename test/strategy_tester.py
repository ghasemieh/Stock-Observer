from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, read_csv
import configuration
from stock_observer.database.database_communication import MySQL_Connection
from stock_observer.analyzer import Analyzer
from pathlib import Path
from datetime import date, datetime, time

from utils import save_csv

logger = get_logger(__name__)


class Strategy_Tester:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.main_table_name = self.config['MySQL']['main table name']
        self.ticker_list = Path(config['Data_Sources']['test tickers list csv'])
        self.strategy_test_result = Path(config['Data_Sources']['strategy test result csv'])

    def strategy_tester(self):
        data_df = self.data_load(start='2019-01-01')
        analyzer = Analyzer(self.config)
        # result = analyzer.analysis(data=data_df)
        save_csv(data_df, Path(f"{self.strategy_test_result}_{datetime.now().date()}_"
                               f"{datetime.now().hour}-{datetime.now().minute}.csv"))

    def data_load(self, start: str) -> DataFrame:
        logger.info("Loading data from main database")
        ticker_list = read_csv(self.ticker_list)
        data_df = DataFrame()
        for row in ticker_list.iterrows():
            ticker = row[1]['ticker']
            mysql = MySQL_Connection(config=self.config)
            data = mysql.select(f"SELECT * FROM {self.main_table_name} "
                                f"WHERE ticker = '{ticker}'"
                                f"and date >= '{start}';")
            data_df = data_df.append(data, ignore_index=True)
        return data_df


if __name__ == '__main__':
    tester = Strategy_Tester(config=configuration.get())
    tester.strategy_tester()
