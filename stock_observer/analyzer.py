import configuration
from pathlib import Path
from pandas import DataFrame
from typing import Tuple, Any
from log_setup import get_logger
from configparser import ConfigParser
from datetime import timedelta, date
from stock_observer.database.database_communication import MySQL_Connection
from utils import save_csv

logger = get_logger(__name__)


class Analyzer:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.main_table_name = self.config['MySQL']['main table name']
        self.result_table_name = self.config['MySQL']['result table name']
        self.path = config['Data_Sources']['analysis equity price csv']

    def analysis(self) -> None:
        data_df, latest_date = self.data_load(day_shift=30)
        BB_result_df = self.BB_check(data_df=data_df, latest_date=latest_date)
        MA_result_df = self.MA_cross_angle_diff(data_df=data_df, latest_date=latest_date)
        ATR_slope_result_df = self.ATR_slope_change(data_df=data_df, latest_date=latest_date)
        ATR_range_result_df = self.ATR_range(data_df=data_df, latest_date=latest_date)
        CCI_result_df = self.CCI_change(data_df=data_df, latest_date=latest_date)
        result_df = self.result_integrator(BB=BB_result_df, MA=MA_result_df, ATR_S=ATR_slope_result_df,
                                           ATR_R=ATR_range_result_df, CCI=CCI_result_df)
        self.result_logger(table_name=self.result_table_name, table_type='analysis', data_df=result_df, latest_date=latest_date)

    def data_load(self, day_shift: int) -> Tuple[DataFrame, Any]:
        logger.info("Data loading from main database")
        starting_date = date.today() - timedelta(days=(day_shift + 3))
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {self.main_table_name} WHERE date > '{starting_date}';")
        latest_date = max(data_df.date)
        return data_df, latest_date

    @staticmethod
    def BB_check(data_df: DataFrame, latest_date: date):
        logger.info("Bollinger band check")
        BB_df = data_df[data_df.date == latest_date][['id', 'ticker', 'date', 'open', 'close', '20_BB_U', '20_BB_L']]
        BB_df.reset_index(drop=True, inplace=True)
        result_L = []
        result_U = []
        for tup in BB_df.itertuples():
            max_price = max(tup.open, tup.close)
            min_price = min(tup.open, tup.close)

            if max_price > tup._6:
                s_u = 1
                logger.warning(f"{tup.ticker} price broke the BB upper band")
            else:
                s_u = 0
            result_U.append(int(s_u))

            if min_price < tup._7:
                s_l = 1
                logger.warning(f"{tup.ticker} price broke the BB lower band")
            else:
                s_l = 0
            result_L.append(int(s_l))

        result_L_df = DataFrame(result_L, columns=['BB_L_signal'])
        result_U_df = DataFrame(result_U, columns=['BB_U_signal'])
        result_df = BB_df.join(result_U_df.join(result_L_df))
        result_df.drop(columns=['open', 'close', '20_BB_U', '20_BB_L'], inplace=True)
        return result_df

    @staticmethod
    def MA_cross_angle_diff(data_df: DataFrame, latest_date: date) -> DataFrame:
        logger.info("MA-5 and MA-20 cross point angle check")
        MA_df = data_df[data_df.date >= (latest_date - timedelta(days=3))][['id', 'ticker', 'date', '5_MA', '20_MA',
                                                                            '5_MA_alpha', '20_MA_alpha']]
        MA_df.reset_index(drop=True, inplace=True)
        ticker_list = MA_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            temp_df = MA_df[MA_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=False)
            temp_df.reset_index(drop=True, inplace=True)

            if len(temp_df) >= 2:
                MA_5_y = temp_df.iloc[1]['5_MA']
                MA_20_y = temp_df.iloc[1]['20_MA']

                MA_5_t = temp_df.iloc[0]['5_MA']
                MA_20_t = temp_df.iloc[0]['20_MA']
                MA_5_alpha_t = temp_df.iloc[0]['5_MA_alpha']
                MA_20_alpha_t = temp_df.iloc[0]['20_MA_alpha']
                angle_diff = int(round(abs(MA_5_alpha_t - MA_20_alpha_t), 0))

                if ((MA_5_y < MA_20_y) and (MA_5_t > MA_20_t)) or ((MA_5_y > MA_20_y) and (MA_5_t < MA_20_t)):
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
                result.append((temp_df.iloc[0]['id'], ticker, temp_df.iloc[0]['date'], angle_diff, signal))
            else:
                logger.warning(f"Ticker {ticker} does't have 2 days data in last 3 days")
                signal = 0
                result.append((temp_df.iloc[0]['id'], ticker, temp_df.iloc[0]['date'], angle_diff, signal))
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'MA_angle_diff', 'MA_signal'])
        return result_df

    @staticmethod
    def ATR_slope_change(data_df: DataFrame, latest_date: date) -> DataFrame:
        logger.info("ATR slope change check")
        ATR_S_df = data_df[data_df.date >= (latest_date - timedelta(days=3))][['id', 'ticker', 'date', '20_ATR_alpha']]
        ATR_S_df.reset_index(drop=True, inplace=True)
        ticker_list = ATR_S_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            temp_df = ATR_S_df[ATR_S_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=False)
            temp_df.reset_index(drop=True, inplace=True)

            if len(temp_df) >= 2:
                ATR_alpha_t = temp_df.iloc[0]['20_ATR_alpha']
                ATR_alpha_y = temp_df.iloc[1]['20_ATR_alpha']
                angle_diff = int(round(abs(ATR_alpha_t - ATR_alpha_y), 0))
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

                result.append((temp_df.iloc[0]['id'], ticker, temp_df.iloc[0]['date'], angle_diff, signal))
            else:
                logger.warning(f"Ticker {ticker} does't have 2 days data in last 3 days")
                signal = 0
                result.append((temp_df.iloc[0]['id'], ticker, temp_df.iloc[0]['date'], angle_diff, signal))

        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'ATR_angle_diff', 'ATR_slope_change_signal'])

        return result_df

    @staticmethod
    def ATR_range(data_df: DataFrame, latest_date: date) -> DataFrame:
        logger.info("ATR 1.5 range check")
        ATR_R_df = data_df[data_df.date >= (latest_date - timedelta(days=3))][['id', 'ticker', 'date', 'open',
                                                                               'close', '20_ATR']]
        ATR_R_df.reset_index(drop=True, inplace=True)
        ticker_list = ATR_R_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            temp_df = ATR_R_df[ATR_R_df.ticker == ticker].copy()
            temp_df.sort_values(by=['date'], inplace=True, ascending=False)
            temp_df.reset_index(drop=True, inplace=True)

            if len(temp_df) >= 2:
                open_t = temp_df.iloc[0]['open']
                close_t = temp_df.iloc[0]['close']
                ATR_y = temp_df.iloc[1]['20_ATR']
                candal_size = abs(open_t - close_t)
                if candal_size <= ATR_y:
                    signal = 0
                elif ATR_y < candal_size < 1.55 * ATR_y:
                    signal = 1
                    logger.warning(
                        f"Signal {signal}: {ticker} Candal size is {round(candal_size / ATR_y, 2)} times of ATR")
                elif 1.55 * ATR_y <= candal_size < 1.7 * ATR_y:
                    signal = 2
                    logger.warning(
                        f"Signal {signal}: {ticker} Candal size is {round(candal_size / ATR_y, 2)} times of ATR")
                elif 1.7 * ATR_y <= candal_size < 1.85 * ATR_y:
                    signal = 3
                    logger.warning(
                        f"Signal {signal}: {ticker} Candal size is {round(candal_size / ATR_y, 2)} times of ATR")
                elif 1.85 * ATR_y <= candal_size < 2 * ATR_y:
                    signal = 4
                    logger.warning(
                        f"Signal {signal}: {ticker} Candal size is {round(candal_size / ATR_y, 2)} times of ATR")
                elif candal_size >= 2 * ATR_y:
                    signal = 5
                    logger.warning(
                        f"Signal {signal}: {ticker} Candal size is {round(candal_size / ATR_y, 2)} times of ATR")

                result.append(
                    (temp_df.iloc[0]['id'], ticker, temp_df.iloc[0]['date'], round(candal_size / ATR_y, 2), signal))
            else:
                logger.warning(f"Ticker {ticker} does't have 2 days data in last 3 days")
                signal = 0
                result.append(
                    (temp_df.iloc[0]['id'], ticker, temp_df.iloc[0]['date'], round(candal_size / ATR_y, 2), signal))

        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'candal_ATR_ratio', 'ATR_candal_size_signal'])
        return result_df

    @staticmethod
    def CCI_change(data_df: DataFrame, latest_date: date) -> DataFrame:
        logger.info("CCI change check")
        data_df.reset_index(drop=True, inplace=True)
        ticker_list = data_df.ticker.unique()
        result = []
        for ticker in ticker_list:
            trigger = False
            status = 0
            signal: int = 0
            temp_df = data_df[data_df.ticker == ticker][['id', 'ticker', 'date', '30_CCI']].copy()
            temp_df.sort_values(by=['date'], inplace=True)
            temp_df.reset_index(drop=True, inplace=True)
            for tup in temp_df.itertuples():
                CCI = tup._4
                if CCI > 100:
                    trigger = True
                    status = 1
                if CCI < -100:
                    trigger = True
                    status = 2
                if trigger:
                    if CCI < -50 and status == 1:
                        signal = 1
                        logger.warning(f"{ticker} CCI drop from +100 to -50")
                    if CCI > 50 and status == 2:
                        signal = 1
                        logger.warning(f"{ticker} CCI raise from -50 to +100")
                result.append((tup.id, ticker, tup.date, signal))
                signal = 0
        result_df = DataFrame(result, columns=['id', 'ticker', 'date', 'CCI_signal'])
        result_df = result_df[result_df.date == latest_date]
        return result_df

    @staticmethod
    def result_integrator(BB: DataFrame, MA: DataFrame, ATR_S: DataFrame, ATR_R: DataFrame,
                          CCI: DataFrame) -> DataFrame:
        data_df = BB.merge(MA, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(ATR_S, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(ATR_R, on=['id', 'ticker', 'date'], how='outer')
        data_df = data_df.merge(CCI, on=['id', 'ticker', 'date'], how='outer')
        data_df.sort_values(by=['id', 'ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        return data_df

    def result_logger(self, table_name: str, table_type: str, data_df: DataFrame, latest_date: date) -> None:
        if not data_df.empty:
            logger.info(f"Saving analysis csv file in {self.path}_{latest_date}.csv")
            save_csv(data_df, Path(f"{self.path}_{latest_date}.csv"))

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
    analyzer = Analyzer(config=configuration.get())
    analyzer.analysis()
