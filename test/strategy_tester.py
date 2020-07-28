from copy import deepcopy
from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, read_csv
import configuration
from stock_observer.database.database_communication import MySQL_Connection
from stock_observer.analyzer import Analyzer
from pathlib import Path
from datetime import datetime

from utils import save_csv

logger = get_logger(__name__)


class Strategy_Tester:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.main_table_name = self.config['MySQL']['main table name']
        self.ticker_list = Path(config['Data_Sources']['test tickers list csv'])
        self.strategy_test_result = Path(config['Data_Sources']['strategy test result csv'])

    def strategy_tester(self):
        data_df = self.data_load(start='2018-12-01')
        ticker_list = data_df.ticker.unique()
        result = DataFrame()
        for ticker in ticker_list:
            temp_df = data_df[data_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=True)
            temp_df.reset_index(drop=True, inplace=True)
            total_record = len(temp_df)
            num_of_record_in_each_chunk = 30
            for i in range(0, total_record - 1):
                data_df_chunk = deepcopy(temp_df[i:(i + num_of_record_in_each_chunk)])
                analyzer = Analyzer(self.config)
                BB_result_df = analyzer.BB_check(data_df=data_df_chunk)
                MA_result_df = analyzer.MA_cross_angle_diff(data_df=data_df_chunk)
                ATR_slope_result_df = analyzer.ATR_slope_change(data_df=data_df_chunk)
                ATR_range_result_df = analyzer.ATR_range(data_df=data_df_chunk)
                CCI_result_df = analyzer.CCI_change(data_df=data_df_chunk)
                result_df = analyzer.result_integrator(BB=BB_result_df, MA=MA_result_df, ATR_S=ATR_slope_result_df,
                                                       ATR_R=ATR_range_result_df, CCI=CCI_result_df)
                result = result.append(result_df)

        save_csv(result, Path(f"{self.strategy_test_result}_{datetime.now().date()}_"
                              f"{datetime.now().hour}-{datetime.now().minute}.csv"))
        return result

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
