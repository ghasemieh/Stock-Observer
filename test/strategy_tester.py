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
        self.strategy_csv_path = Path(config['Data_Sources']['strategy test result csv'])
        self.strategy_table_name = self.config['MySQL']['strategy tester table name']

    def strategy_tester(self):
        data_df = self.data_load(start='2018-12-01')
        ticker_list = data_df.ticker.unique()
        data = DataFrame()
        for ticker in ticker_list:
            logger.info(f"---------- Analyzing {ticker} -----------")
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
                price_change_result_df = analyzer.price_change(data_df=data_df_chunk)
                analyzed_data = analyzer.result_integrator(BB=BB_result_df, MA=MA_result_df, ATR_S=ATR_slope_result_df,
                                                           ATR_R=ATR_range_result_df, CCI=CCI_result_df,
                                                           PC=price_change_result_df)
                data = data.append(analyzed_data)
        result = data_df.merge(data, on=['id', 'ticker', 'date'], how='outer')
        result.drop_duplicates(subset='id', inplace=True)
        save_csv(result, Path(f"{self.strategy_csv_path}_{datetime.now().date()}_"
                              f"{datetime.now().hour}-{datetime.now().minute}.csv"))
        self.result_logger(data_df=result, table_name=self.strategy_table_name, table_type="analysis")
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

    def result_logger(self, table_name: str, table_type: str, data_df: DataFrame) -> None:
        if not data_df.empty:
            latest_date = max(data_df.date)
            logger.info(f"Saving analysis csv file in {self.strategy_csv_path}_{latest_date}.csv")
            save_csv(data_df, Path(f"{self.strategy_csv_path}_{latest_date}.csv"))

            logger.info(f"Inserting analysis result in database {table_name} table")
            mysql = MySQL_Connection(config=self.config)
            test = mysql.select(f"SELECT * FROM {table_name} LIMIT 3;")
            if test is None:
                derivative_features = list(data_df.columns)[3:]
                mysql.create_table(table_name=table_name, table_type=table_type,
                                   derivative_features=derivative_features)
                mysql.insert_df(data_df=data_df, table_name=table_name, primary_key='id', if_exists='append')
            else:
                logger.info(f"Update {table_name} table in the database")
                mysql.insert_df(data_df=data_df, table_name=table_name, primary_key='id', if_exists='append')
        else:
            logger.warning("Database insertion received empty data frame")


if __name__ == '__main__':
    tester = Strategy_Tester(config=configuration.get())
    tester.strategy_tester()
