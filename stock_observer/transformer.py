from pathlib import Path
from utils import save_csv
from pandas import DataFrame
import numpy as np
from log_setup import get_logger
from configparser import ConfigParser
from datetime import timedelta
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

    def transform(self, data: DataFrame) -> DataFrame:
        if data.empty:
            logger.warning("Transformation received empty data frame")
            return DataFrame()

        data_df = self.data_load(data, self.stage_table_name, day_shift=30)
        # data_df = data

        data_df = self.add_moving_avg(data_df=data_df, n_days=self.moving_avg_period_1)
        data_df = self.add_moving_avg(data_df=data_df, n_days=self.moving_avg_period_2)
        data_df = self.add_cci(data_df=data_df, n_days=self.cci_period)
        data_df = self.add_atr(data_df=data_df, n_days=self.atr_period)
        data_df = self.add_bollinger_bands(data_df=data_df, n_days=self.bollinger_bands_period)
        data_df = self.add_angle(data_df=data_df, feature='5_MA')
        data_df = self.add_angle(data_df=data_df, feature='20_MA')
        data_df = self.add_angle(data_df=data_df, feature='20_ATR')
        data_df = self.add_angle(data_df=data_df, feature='30_CCI')

        data_df = data_df[data_df['date'] >= min(data.date)]
        data_df = data_df.round(4)
        save_csv(data_df, self.path)
        return data_df

    def data_load(self, data: DataFrame, stage_table_name: str, day_shift: int) -> DataFrame:
        logger.info("Data loading from staging database")
        least_date = min(data.date)
        starting_date = least_date - timedelta(days=(day_shift + 3))
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {stage_table_name} WHERE date > '{starting_date}';")
        return data_df

    @staticmethod
    def add_rolling_ave(data_df: DataFrame, n_days: int, feature: str) -> DataFrame:
        data_df.sort_values(by=['date'], inplace=True)
        data_df.index = data_df.date
        rolling_avg_df = data_df.groupby(by='ticker')[feature].rolling(window=n_days, min_periods=n_days) \
            .mean().reset_index(drop=False)
        rolling_avg_df.rename(columns={feature: f'{n_days}_days_rolling_result'}, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(rolling_avg_df, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        return data_df

    def add_moving_avg(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info(f"Calculating Moving Average for {n_days} days")
        data_df['op'] = (data_df['open'] + data_df['close']) / 2
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='op')
        data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_MA'}, inplace=True)
        data_df.drop(columns='op', inplace=True)
        return data_df

    def add_cci(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating CCI")
        # compute typical price: (H+L+C)/3
        data_df['typical_price'] = (data_df['high'] + data_df['close'] + data_df['low']) / 3

        # compute moving average on typical price: sum(typical_price)/30
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='typical_price')
        data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_moving_avg_of_typical_price'},
                       inplace=True)

        # compute mean deviation: sum(|typical_price - moving average|)/30
        data_df['tp-mv'] = abs(data_df['typical_price'] - data_df[f'{n_days}_days_moving_avg_of_typical_price'])
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='tp-mv')
        data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_mean_deviation'}, inplace=True)

        # compute Commodity Channel Index (CCI): (typical_price - moving_average)/(0.015 * mean_deviation)
        data_df[f'{n_days}_days_mean_deviation'] = data_df[f'{n_days}_days_mean_deviation'] \
            .map(lambda x: 0.00001 if x == 0 else x)
        data_df[f'{n_days}_CCI'] = (data_df['typical_price'] - data_df[
            f'{n_days}_days_moving_avg_of_typical_price']) / (0.015 * data_df[f'{n_days}_days_mean_deviation'])

        data_df.drop(columns=['typical_price', 'tp-mv',
                              f'{n_days}_days_moving_avg_of_typical_price',
                              f'{n_days}_days_mean_deviation'], inplace=True)
        return data_df

    def add_atr(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating ATR")
        data_df['H-L'] = abs(data_df['high'] - data_df['low'])
        data_df['H-P'] = abs(data_df['high'] - data_df['close'].shift(1))
        data_df['L-P'] = abs(data_df['close'].shift(1) - data_df['low'])
        max_list = []
        for index, row in data_df.iterrows():
            max_list.append(max(row['H-L'], row['H-P'], row['L-P']))
        data_df['true_range'] = DataFrame(max_list, columns=['true_range'])
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='true_range')
        data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_ATR'}, inplace=True)
        data_df.drop(columns=['H-L', 'H-P', 'L-P', 'true_range'], inplace=True)
        return data_df

    @staticmethod
    def add_bollinger_bands(data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating Bollinger Bands")
        data_df.sort_values(by=['date'], inplace=True)
        data_df.index = data_df.date
        data_df['op'] = (data_df['open'] + data_df['close']) / 2
        rolling_sd_df = data_df.groupby(by='ticker')['op'].rolling(window=n_days, min_periods=n_days) \
            .std().reset_index(drop=False)
        rolling_sd_df.rename(columns={'op': f'{n_days}_standard_deviation_result'}, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(rolling_sd_df, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)

        data_df[f'{n_days}_BB_L'] = data_df['20_MA'] - 2 * data_df[f'{n_days}_standard_deviation_result']
        data_df[f'{n_days}_BB_U'] = data_df['20_MA'] + 2 * data_df[f'{n_days}_standard_deviation_result']
        data_df.drop(columns=['op', f'{n_days}_standard_deviation_result'], inplace=True)
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
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(slope, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        return data_df
