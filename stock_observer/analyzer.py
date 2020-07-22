from log_setup import get_logger
from configparser import ConfigParser
import numpy as np
import matplotlib.pyplot as plt
from pandas import DataFrame, read_csv
import pandas as pd
import sympy as sp
import configuration
from datetime import datetime, timedelta, date
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class Analyzer:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.main_table_name = self.config['MySQL']['main table name']
        self.result_table_name = self.config['MySQL']['result table name']

    def analysis(self) -> str:
        data_df = self.data_load(main_table_name=self.main_table_name, day_shift=30)
        # BB_result_df = self.BB_check(data_df=data_df)
        # MA_result_df = self.MA_cross_angle_diff(data_df=data_df)
        ATR_slope_result_df = self.ATR_slope_change(data_df=data_df)
        ATR_range_result_df = self.ATR_range(data_df=data_df)
        CCI_result_df = self.CCI_change(data_df=data_df)
        # result_df = self.result_integrator(BB=BB_result_df, MA=MA_result_df, ATR_S=ATR_slope_result_df,
        #                                    ATR_R=ATR_range_result_df, CCI=CCI_result_df)
        # self.result_logger(table_name=self.result_table_name, result_df=result_df)
        # result_message = self.alert_message_generator(result_df=result_df)
        # return result_message

    def data_load(self, main_table_name: str, day_shift: int) -> DataFrame:
        logger.info("Data loading from main database")
        starting_date = date.today() - timedelta(days=(day_shift + 3))
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {main_table_name} WHERE date > '{starting_date}';")
        return data_df

    @staticmethod
    def BB_check(data_df: DataFrame):
        logger.info("Bollinger band check")
        BB_df = data_df[data_df.date == date.today()][['id', 'ticker', 'date', 'open', 'close', '20_BB_U', '20_BB_L']]
        BB_df.reset_index(drop=True, inplace=True)
        result_L = []
        result_U = []
        for tup in BB_df.itertuples():
            max_price = max(tup.open, tup.close)
            min_price = min(tup.open, tup.close)

            if max_price > tup._5:
                s_u = 1
                logger.warning(f"{tup.ticker} price broke the BB upper band")
            else:
                s_u = 0
            result_U.append(s_u)

            if min_price < tup._6:
                s_l = 1
                logger.warning(f"{tup.ticker} price broke the BB lower band")
            else:
                s_l = 0
            result_L.append(s_l)

        result_L_df = DataFrame(result_L, columns=['BB_L_signal'])
        result_U_df = DataFrame(result_U, columns=['BB_U_signal'])
        result_df = BB_df.join(result_U_df.join(result_L_df))
        return result_df

    @staticmethod
    def MA_cross_angle_diff(data_df: DataFrame) -> DataFrame:
        logger.info("MA-5 and MA-20 cross point angle check")
        MA_df = data_df[data_df.date >= (date.today() - timedelta(days=3))][['id', 'ticker', 'date', '5_MA', '20_MA',
                                                                             '5_MA_alpha', '20_MA_alpha']]
        MA_df.reset_index(drop=True, inplace=True)
        ticker_list = MA_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            temp_df = MA_df[MA_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True)
            temp_df.reset_index(drop=True, inplace=True)

            if len(temp_df) == 2:
                MA_5_y = temp_df.iloc[0]['5_MA']
                MA_20_y = temp_df.iloc[0]['20_MA']

                MA_5_t = temp_df.iloc[1]['5_MA']
                MA_20_t = temp_df.iloc[1]['20_MA']
                MA_5_alpha_t = temp_df.iloc[1]['5_MA_alpha']
                MA_20_alpha_t = temp_df.iloc[1]['20_MA_alpha']

                if ((MA_5_y < MA_20_y) and (MA_5_t > MA_20_t)) or ((MA_5_y > MA_20_y) and (MA_5_t < MA_20_t)):
                    angle_diff = abs(MA_5_alpha_t - MA_20_alpha_t)
                    if angle_diff <= 10:
                        signal = 0
                    elif 10 < angle_diff < 45:
                        signal = 1
                        logger.warning(f"Signal {signal}: {ticker} MA_5 and MA_20 angle difference is {angle_diff}")
                    elif 45 <= angle_diff < 60:
                        signal = 2
                        logger.warning(f"Signal {signal}: {ticker} MA_5 and MA_20 angle difference is {angle_diff}")
                    elif 60 <= angle_diff < 75:
                        signal = 3
                        logger.warning(f"Signal {signal}: {ticker} MA_5 and MA_20 angle difference is {angle_diff}")
                    elif 75 <= angle_diff < 90:
                        signal = 4
                        logger.warning(f"Signal {signal}: {ticker} MA_5 and MA_20 angle difference is {angle_diff}")
                    elif angle_diff >= 90:
                        signal = 5
                        logger.warning(f"Signal {signal}: {ticker} MA_5 and MA_20 angle difference is {angle_diff}")
                else:
                    signal = 0
                result.append((ticker, temp_df.iloc[0]['date'], signal))
            else:
                logger.warning(f"Ticker {ticker} does't have 2 days data in last 3 days")
                signal = 0
                result.append((ticker, temp_df.iloc[0]['date'], signal))
        result_df = DataFrame(result, columns=['ticker', 'date', 'MA_signal'])
        return result_df

    @staticmethod
    def ATR_slope_change(data_df: DataFrame) -> DataFrame:
        logger.info("ATR slope change check")
        ATR_S_df = data_df[data_df.date >= (date.today() - timedelta(days=3))][['id', 'ticker', 'date', '20_ATR_alpha']]
        ATR_S_df.reset_index(drop=True, inplace=True)
        ticker_list = ATR_S_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            temp_df = ATR_S_df[ATR_S_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True)
            temp_df.reset_index(drop=True, inplace=True)

            if len(temp_df) == 2:
                ATR_alpha_t = temp_df.iloc[0]['20_ATR_alpha']
                ATR_alpha_y = temp_df.iloc[1]['20_ATR_alpha']
                angle_diff = abs(ATR_alpha_t - ATR_alpha_y)
                if angle_diff <= 30:
                    signal = 0
                elif 30 < angle_diff < 45:
                    signal = 1
                    logger.warning(f"Signal {signal}: {ticker} ATR slope break is {angle_diff}")
                elif 45 <= angle_diff < 60:
                    signal = 2
                    logger.warning(f"Signal {signal}: {ticker} ATR slope break is {angle_diff}")
                elif 60 <= angle_diff < 75:
                    signal = 3
                    logger.warning(f"Signal {signal}: {ticker} ATR slope break is {angle_diff}")
                elif 75 <= angle_diff < 90:
                    signal = 4
                    logger.warning(f"Signal {signal}: {ticker} ATR slope break is {angle_diff}")
                elif angle_diff >= 90:
                    signal = 5
                    logger.warning(f"Signal {signal}: {ticker} ATR slope break is {angle_diff}")

                result.append((ticker, temp_df.iloc[0]['date'], signal))
            else:
                logger.warning(f"Ticker {ticker} does't have 2 days data in last 3 days")
                signal = 0
                result.append((ticker, temp_df.iloc[0]['date'], signal))

        result_df = DataFrame(result, columns=['ticker', 'date', 'MA_signal'])

        return result_df


    @staticmethod
    def ATR_range(data_df: DataFrame) -> int:
        logger.info("ATR 1.5 range check")
        signal = 0

        return signal

    @staticmethod
    def CCI_change(data_df: DataFrame) -> int:
        logger.info("CCI change check")
        signal = 0

        return signal

    @staticmethod
    def result_integrator(BB: DataFrame, MA: DataFrame, ATR_S: DataFrame, ATR_R: DataFrame,
                          CCI: DataFrame) -> DataFrame:
        None

    @staticmethod
    def alert_message_generator(result_df: DataFrame, s1: int, s2: int, s3: int, s4: int, s5: int) -> str:
        logger.info("Alert message generator started")
        message = ""
        return message

    @staticmethod
    def result_logger(table_name: str, result_df: DataFrame) -> None:
        None


if __name__ == '__main__':
    analyzer = Analyzer(config=configuration.get())
    result = analyzer.analysis()
