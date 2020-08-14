from copy import deepcopy
from typing import Tuple
from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, read_csv
import configuration
from pathlib import Path
from datetime import date, timedelta, datetime
from stock_observer.database.database_communication import MySQL_Connection
from utils import save_csv

logger = get_logger(__name__)

"""
1. [(Price2-Price1)/Price1>3%] & [BB_U_Signal]
2. [(Price2-Price1)/Price1<-3%] & [BB_L_Signal]
3. [(Price2-Price1)/Price1>3%] & [ATR_Candle_size_signal]
4. [(Price2-Price1)/Price1<-3%] & [ATR_Candle_size_signal]
5. [MA_5=MA_20] & [MA_5_alpha - MA_20_alpha > 30]
6. Within 10 days: [ATR_angle_dif > 30] & [CCI_signal]
7. Within 10 days: [ATR_angle_dif > 30] & [MA_5=MA_20]
8. Within 10 days:  [CCI_signal] & [MA_5=MA_20]
"""


class Decision_Maker:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.ticker_list_path = Path(config['Data_Sources']['test tickers list csv']) # TODO
        self.analysis_table_name = self.config['MySQL']['analysis table name']
        self.decision_table_name = self.config['MySQL']['decision table name']
        self.path = config['Data_Sources']['decision equity price csv']

    def decide(self) -> Tuple[str, str]:
        data_df = self.data_load(day_shift=20)
        ticker_list = data_df.ticker.unique()
        data = DataFrame()
        for ticker in ticker_list:
            logger.info(f"---------- Analyzing {ticker} -----------")
            temp_df = data_df[data_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=True)
            temp_df.reset_index(drop=True, inplace=True)
            total_record = len(temp_df)
            num_of_record_in_each_chunk = 30
            for i in range(0, total_record - num_of_record_in_each_chunk + 1):
                data_df_chunk = deepcopy(temp_df[i:(i + num_of_record_in_each_chunk)])
                BB_U_PD_result_df = self.BB_U_PD(data_df=data_df_chunk)
                BB_L_PD_result_df = self.BB_L_PD(data_df=data_df_chunk)
                ATR_candle_PD_result_df = self.ATR_candle_PD(data_df=data_df_chunk)
                MA_angle_diff_result_df = self.MA_angle_diff(data_df=data_df_chunk)
                ATR_angle_CCI_result_df = self.ATR_angle_CCI(data_df=data_df_chunk)
                ATR_angle_MA_result_df = self.ATR_angle_MA(data_df=data_df_chunk)
                CCI_MA_result_df = self.CCI_MA(data_df=data_df_chunk)

                result_df = self.result_integrator(BB_U_PD_result_df, BB_L_PD_result_df, ATR_candle_PD_result_df,
                                                   MA_angle_diff_result_df, ATR_angle_CCI_result_df, ATR_angle_MA_result_df,
                                                   CCI_MA_result_df)
                data = data.append(result_df)
        result = data_df.merge(data, on=['id', 'ticker', 'date'], how='outer')
        result.drop_duplicates(subset='id', inplace=True)
        result.dropna(inplace=True)
        result.reset_index(drop=True, inplace=True)
        self.result_logger(table_name=self.decision_table_name, table_type='analysis', data_df=result)
        alert_message = self.alert_message_generator(result_df=data_df)
        result_file_path = ""
        return alert_message

    def data_load(self, day_shift=10) -> DataFrame:
        logger.info("Data loading from analysis database")
        ticker_list = read_csv(self.ticker_list_path)
        data_df = DataFrame()
        for row in ticker_list.iterrows():
            ticker = row[1]['ticker']
            mysql = MySQL_Connection(config=self.config)
            latest_date_df = mysql.select(
                f"SELECT max(date) FROM {self.decision_table_name} WHERE ticker = '{ticker}';")

            if latest_date_df is not None:
                latest_date_in_db = latest_date_df.iloc[0][0]
                logger.info(f"{ticker} latest update is {latest_date_in_db}")
            else:
                latest_date_in_db = datetime.strptime('2018-12-01', "%Y-%m-%d").date()

            starting_date = str(latest_date_in_db - timedelta(days=(day_shift + 3)))
            data = mysql.select(f"SELECT * FROM {self.analysis_table_name} "
                                f"WHERE ticker = '{ticker}' AND date > '{starting_date}';")
            data_df = data_df.append(data, ignore_index=True)
        return data_df

    @staticmethod
    def BB_U_PD(data_df: DataFrame):
        """
        [(Price2-Price1)/Price1>3%] & [BB_U_Signal]
        :param data_df:
        :return:
        """
        logger.info("Bollinger upper band and price difference check")
        latest_date = max(data_df.date)
        df = data_df[data_df.date == latest_date][['id', 'ticker', 'date', 'BB_U_signal', 'price_diff_signal']]
        df.reset_index(drop=True, inplace=True)
        result = []
        for tup in df.itertuples():
            if tup.price_diff_signal > 0:
                signal = tup.BB_U_signal * tup.price_diff_signal
            else:
                signal = 0
            result.append([tup.id, tup.ticker, tup.date, signal])
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'BB_U_PD_signal'])
        return result_df

    @staticmethod
    def BB_L_PD(data_df: DataFrame):
        """
        [(Price2-Price1)/Price1<-3%] & [BB_L_Signal]
        :param data_df:
        :return:
        """
        logger.info("Bollinger lower band and price difference check")
        latest_date = max(data_df.date)
        df = data_df[data_df.date == latest_date][['id', 'ticker', 'date', 'BB_L_signal', 'price_diff_signal']]
        df.reset_index(drop=True, inplace=True)
        result = []
        for tup in df.itertuples():
            if tup.price_diff_signal < 0:
                signal = tup.BB_L_signal * tup.price_diff_signal
            else:
                signal = 0
            result.append([tup.id, tup.ticker, tup.date, signal])
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'BB_L_PD_signal'])
        return result_df

    @staticmethod
    def ATR_candle_PD(data_df: DataFrame):
        """
        [(Price2-Price1)/Price1>3%] & [ATR_Candle_size_signal]
        [(Price2-Price1)/Price1<-3%] & [ATR_Candle_size_signal]
        :param data_df:
        :return:
        """
        logger.info("ATR candle size and price difference check")
        latest_date = max(data_df.date)
        df = data_df[data_df.date == latest_date][
            ['id', 'ticker', 'date', 'ATR_candle_size_signal', 'price_diff_signal']]
        df.reset_index(drop=True, inplace=True)
        result = []
        for tup in df.itertuples():
            signal = tup.ATR_candle_size_signal * tup.price_diff_signal
            result.append([tup.id, tup.ticker, tup.date, signal])
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'ATR_candle_PD_signal'])
        return result_df

    @staticmethod
    def MA_angle_diff(data_df: DataFrame):
        """
        [MA_5=MA_20] & [MA_5_alpha - MA_20_alpha > 30]
        :param data_df:
        :return:
        """
        logger.info("5 days and 20 days moving average angle check")
        latest_date = max(data_df.date)
        result_df = data_df[data_df.date == latest_date][['id', 'ticker', 'date', 'MA_signal']]
        return result_df

    @staticmethod
    def ATR_angle_CCI(data_df: DataFrame):
        """
        Within 10 days: [ATR_angle_dif > 30] & [CCI_signal]
        :param data_df:
        :return:
        """
        logger.info("ATR angle and CCI change check")
        data_df.reset_index(drop=True, inplace=True)
        ticker_list = data_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            signal: int = 0
            a: bool = False
            b: bool = False
            ATR_signal_max = 0
            temp_df = data_df[data_df.ticker == ticker][
                ['id', 'ticker', 'date', 'ATR_slope_change_signal', 'CCI_signal']].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=False)
            temp_df = temp_df[:10]
            temp_df.sort_values(by=['date'], inplace=True, ascending=True)
            temp_df.reset_index(drop=True, inplace=True)
            for tup in temp_df.itertuples():
                CCI_signal = tup.CCI_signal
                ATR_signal = tup.ATR_slope_change_signal

                if CCI_signal == 1:
                    CCI_max = CCI_signal
                    a = True
                if CCI_signal == -1:
                    CCI_min = CCI_signal
                    b = True
                ATR_signal_max = max([ATR_signal_max, ATR_signal])

                if a is True:
                    signal = CCI_max * ATR_signal_max
                    a = False
                if b is True:
                    signal = CCI_min * ATR_signal_max
                    b = False

            result.append((tup.id, ticker, tup.date, signal))
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'ATR_angle_CCI_signal'])
        return result_df

    @staticmethod
    def ATR_angle_MA(data_df: DataFrame):
        """
        Within 10 days: [ATR_angle_dif > 30] & [MA_5=MA_20]
        :param data_df:
        :return:
        """
        logger.info("ATR angle and MA-5 and MA-20 cross check")
        data_df.reset_index(drop=True, inplace=True)
        ticker_list = data_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            signal: int = 0
            ATR_signal_max = 0
            MA_max = 0
            temp_df = data_df[data_df.ticker == ticker][
                ['id', 'ticker', 'date', 'ATR_slope_change_signal', 'MA_signal']].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=False)
            temp_df = temp_df[:10]
            temp_df.sort_values(by=['date'], inplace=True, ascending=True)
            temp_df.reset_index(drop=True, inplace=True)
            for tup in temp_df.itertuples():
                MA_signal = tup.MA_signal
                ATR_signal = tup.ATR_slope_change_signal
                MA_max = max([MA_max, MA_signal])
                ATR_signal_max = max([ATR_signal_max, ATR_signal])
                signal = MA_max * ATR_signal_max

            result.append((tup.id, ticker, tup.date, signal))
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'ATR_angle_MA_signal'])
        return result_df

    @staticmethod
    def CCI_MA(data_df: DataFrame):
        """
        Within 10 days:  [CCI_signal] & [MA_5=MA_20]
        :param data_df:
        :return:
        """
        logger.info("CCI and MA-5 and MA-20 cross check")
        data_df.reset_index(drop=True, inplace=True)
        ticker_list = data_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            signal: int = 0
            a: bool = False
            b: bool = False
            MA_signal_max = 0
            temp_df = data_df[data_df.ticker == ticker][
                ['id', 'ticker', 'date', 'MA_signal', 'CCI_signal']].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=False)
            temp_df = temp_df[:10]
            temp_df.sort_values(by=['date'], inplace=True, ascending=True)
            temp_df.reset_index(drop=True, inplace=True)
            for tup in temp_df.itertuples():
                CCI_signal = tup.CCI_signal
                MA_signal = tup.MA_signal

                if CCI_signal == 1:
                    CCI_max = CCI_signal
                    a = True
                if CCI_signal == -1:
                    CCI_min = CCI_signal
                    b = True
                MA_signal_max = max([MA_signal_max, MA_signal])

                if a is True:
                    signal = CCI_max * MA_signal_max
                    a = False
                if b is True:
                    signal = CCI_min * MA_signal_max
                    b = False

            result.append((tup.id, ticker, tup.date, signal))
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'CCI_MA_signal'])
        return result_df

    @staticmethod
    def result_integrator(s1: DataFrame, s2: DataFrame, s3: DataFrame, s4: DataFrame, s5: DataFrame,
                          s6: DataFrame, s7: DataFrame) -> DataFrame:
        data_df = s1.merge(s2, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(s3, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(s4, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(s5, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(s6, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(s7, on=['id', 'ticker', 'date'], how='outer')
        data_df.sort_values(by=['id', 'ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        return data_df

    def result_logger(self, table_name: str, table_type: str, data_df: DataFrame) -> None:
        if not data_df.empty:
            latest_date = max(data_df.date)
            logger.info(f"Saving trade signal csv file in {self.path}_{latest_date}.csv")
            save_csv(data_df, Path(f"{self.path}_{datetime.now().date()}_"
                                   f"{datetime.now().hour}-{datetime.now().minute}.csv"))

            logger.info(f"Inserting trade signal in database {table_name} table")
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

    @staticmethod
    def alert_message_generator(result_df: DataFrame) -> str:
        logger.info("Alert message generator started")
        message = ""

        return message


if __name__ == '__main__':
    decision_maker = Decision_Maker(config=configuration.get())
    result = decision_maker.decide()
