import numpy as np
from pathlib import Path
from utils import save_csv
from pandas import DataFrame
from datetime import timedelta
from utils import str_to_datetime
from log_setup import get_logger
from configparser import ConfigParser
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class Transformer:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.path = Path(config['Data_Sources']['processed equity price csv'])
        self.stage_table_name = self.config['MySQL']['stage table name']
        self.moving_avg_period_1 = int(self.config['Indicator']['moving average 1st period'])
        self.moving_avg_period_2 = int(self.config['Indicator']['moving average 2nd period'])
        self.cci_period = int(self.config['Indicator']['cci period'])
        self.atr_period = int(self.config['Indicator']['atr period'])
        self.bollinger_bands_period = int(self.config['Indicator']['bollinger bands period'])

    def transform(self, data: DataFrame = DataFrame()) -> DataFrame:
        # if data.empty:
        #     logger.warning("Transformation received empty data frame")
        #     return DataFrame()

        data_df = self.data_load(data, day_shift=30)

        data_df = self.add_moving_avg(data_df=data_df, n_days=self.moving_avg_period_1)
        data_df = self.add_moving_avg(data_df=data_df, n_days=self.moving_avg_period_2)
        # data_df = self.add_cci(data_df=data_df, n_days=self.cci_period)
        data_df = self.add_atr(data_df=data_df, n_days=self.atr_period)
        data_df = self.add_bollinger_bands(data_df=data_df, n_days=self.bollinger_bands_period)
        data_df = self.add_angle(data_df=data_df, feature='MA_5')
        data_df = self.add_angle(data_df=data_df, feature='MA_20')
        data_df = self.add_angle(data_df=data_df, feature='ATR_20')

        # data_df = data_df[data_df['date'] >= min(data.date)]  # TODO uncomment
        data_df = data_df.round(3)
        logger.info(f"Saving transformed data in csv file at {self.path}")
        save_csv(data_df, self.path)
        return data_df

    def data_load(self, data: DataFrame, day_shift: int) -> DataFrame:
        logger.info("Data loading from staging database")
        if data.empty:
            least_date = str_to_datetime('2000-01-01').date()  # TODO
        else:
            least_date = min(data.date)
        starting_date = least_date - timedelta(days=(day_shift + 3))
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {self.stage_table_name} "
                               f"WHERE date > '{starting_date}';")  # TODO
        return data_df

    @staticmethod
    def add_rolling_ave(data_df: DataFrame, n_days: int, feature: str) -> DataFrame:
        data_df.sort_values(by=['date'], inplace=True)
        data_df.index = data_df.date
        rolling_avg_df = data_df.groupby(by='ticker')[feature].rolling(window=n_days, min_periods=n_days) \
            .mean().reset_index(drop=False)
        rolling_avg_df.rename(columns={feature: f'rolling_result_{n_days}'}, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(rolling_avg_df, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        return data_df

    def add_moving_avg(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info(f"Calculating Moving Average for {n_days} days")
        data_df['op'] = (data_df['open'] + data_df['close']) / 2
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='op')
        data_df.rename(columns={f'rolling_result_{n_days}': f'MA_{n_days}'}, inplace=True)
        data_df.drop(columns='op', inplace=True)
        return data_df

    def add_cci(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating CCI")
        # Find the lowest and highest price in the last x days
        data_df.index = data_df.date
        highest = data_df.groupby(by='ticker')['high'].rolling(window=n_days, min_periods=n_days).max().reset_index(drop=False)
        lowest = data_df.groupby(by='ticker')['low'].rolling(window=n_days, min_periods=n_days).min().reset_index(drop=False)

        highest.rename(columns={'high': f'highest_{n_days}'}, inplace=True)
        lowest.rename(columns={'low': f'lowest_{n_days}'}, inplace=True)

        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(highest, on=['ticker', 'date'], how='inner')
        data_df = data_df.merge(lowest, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)

        # compute typical price: (Hn+Ln+C)/3
        data_df['typical_price'] = (data_df[f'highest_{n_days}'] + data_df['close'] + data_df[f'lowest_{n_days}']) / 3

        # compute moving average on typical price: sum(typical_price)/30
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='typical_price')  # TODO shift 1 day back
        data_df.rename(columns={f'rolling_result_{n_days}': f'moving_avg_of_typical_price_{n_days}'},
                       inplace=True)

        # compute mean deviation: sum(|typical_price - moving average|)/30
        data_df['tp_mv'] = abs(data_df['typical_price'] - data_df[f'moving_avg_of_typical_price_{n_days}'])
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='tp_mv')
        data_df.rename(columns={f'rolling_result_{n_days}': f'mean_deviation_{n_days}'}, inplace=True)

        # compute Commodity Channel Index (CCI): (typical_price - moving_average)/(0.015 * mean_deviation)
        data_df[f'mean_deviation_{n_days}'] = data_df[f'mean_deviation_{n_days}'] \
            .map(lambda x: 0.00001 if x == 0 else x)
        data_df[f'CCI_{n_days}'] = (data_df['typical_price'] - data_df[
            f'moving_avg_of_typical_price_{n_days}']) / (0.015 * data_df[f'mean_deviation_{n_days}'])

        # data_df.drop(columns=['typical_price', 'tp_mv', f'moving_avg_of_typical_price_{n_days}',
        #                       f'mean_deviation_{n_days}', f'lowest_{n_days}', f'highest_{n_days}'], inplace=True)
        return data_df

    def add_atr(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating ATR")
        data_df['H_L'] = abs(data_df['high'] - data_df['low'])
        data_df['H_P'] = abs(data_df['high'] - data_df['close'].shift(1))
        data_df['L_P'] = abs(data_df['close'].shift(1) - data_df['low'])
        max_list = []
        for index, row in data_df.iterrows():
            max_list.append(max(row['H_L'], row['H_P'], row['L_P']))
        data_df['true_range'] = DataFrame(max_list, columns=['true_range'])
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='true_range')
        data_df.rename(columns={f'rolling_result_{n_days}': f'ATR_{n_days}'}, inplace=True)
        data_df.drop(columns=['H_L', 'H_P', 'L_P', 'true_range'], inplace=True)
        return data_df

    @staticmethod
    def add_bollinger_bands(data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating Bollinger Bands")
        data_df.sort_values(by=['date'], inplace=True)
        data_df.index = data_df.date
        data_df['op'] = (data_df['open'] + data_df['close']) / 2
        rolling_sd_df = data_df.groupby(by='ticker')['op'].rolling(window=n_days, min_periods=n_days) \
            .std().reset_index(drop=False)
        rolling_sd_df.rename(columns={'op': f'standard_deviation_{n_days}'}, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(rolling_sd_df, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)

        data_df[f'BB_L_{n_days}'] = data_df['MA_20'] - 2 * data_df[f'standard_deviation_{n_days}']
        data_df[f'BB_U_{n_days}'] = data_df['MA_20'] + 2 * data_df[f'standard_deviation_{n_days}']
        data_df.drop(columns=['op', f'standard_deviation_{n_days}'], inplace=True)
        return data_df

    @staticmethod
    def slope_cal(points) -> int:
        y = []
        for tup in points:
            y.append(tup)

        x = np.array([1, 2, 3, 4, 5])
        y = np.array(y)

        max_x = max(x)
        min_x = min(x)

        max_y = max(y)
        min_y = min(y)

        d_x = max_x - min_x
        d_y = max_y - min_y

        if d_y == 0:
            d_y = 0.01

        x_n = (x - min_x) / d_x
        y_n = (y - min_y) / d_y

        s1 = (y_n[4] - y_n[3]) / (x_n[4] - x_n[3])
        s2 = (y_n[3] - y_n[2]) / (x_n[3] - x_n[2])
        s3 = (y_n[2] - y_n[1]) / (x_n[2] - x_n[1])
        s4 = (y_n[1] - y_n[0]) / (x_n[1] - x_n[0])

        slope = (s1 * 4 + s2 * 3 + s3 * 2 + s4 * 1) / 10
        result = np.arctan(slope) / np.pi * 180
        return result

    def add_angle(self, data_df: DataFrame, feature: str):
        logger.info(f'Calculating line angle for {feature}')
        data = data_df
        data.set_index(data["date"], inplace=True)
        slope = data_df.groupby(by='ticker')[feature].rolling(window=5, min_periods=5) \
            .apply(self.slope_cal).reset_index(drop=False)
        slope.rename(columns={feature: f'{feature}_alpha'}, inplace=True)
        slope = slope.round(0)
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(slope, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        return data_df
