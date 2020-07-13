from pathlib import Path
from utils import save_csv
from pandas import DataFrame
from log_setup import get_logger
from configparser import ConfigParser
from datetime import datetime, timedelta
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class Transformer:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.path = Path(config['Data_Sources']['processed equity price csv'])
        self.stage_table_name = self.config['MySQL']['stage table name']

    def transform(self, data: DataFrame) -> DataFrame:
        if data.empty:
            logger.warning("Transformation received empty data frame")
            return DataFrame()
        data_df = self.data_load(data, self.stage_table_name, day_shift=30)

        data_df = self.add_moving_avg(data_df=data_df, n_days=5)
        data_df = self.add_moving_avg(data_df=data_df, n_days=20)
        data_df = self.add_cci(data_df=data_df, n_days=30)
        data_df = self.add_atr(data_df=data_df, n_days=20)
        data_df = self.add_bollinger_bands(data_df=data_df, n_days=20)

        data_df = data_df[data_df['date'] >= datetime.strptime(min(data.date), '%Y-%m-%d').date()]
        data_df = data_df.round(4)
        save_csv(data_df, self.path)
        return data_df

    def data_load(self, data: DataFrame, stage_table_name: str, day_shift: int) -> DataFrame:
        logger.info("Data loading from staging database")
        least_date = datetime.strptime(min(data.date), '%Y-%m-%d').date()
        starting_date = least_date - timedelta(days=(day_shift+3))
        mysql = MySQL_Connection(config=self.config)
        data_df = mysql.select(f"SELECT * FROM {stage_table_name} WHERE date > '{starting_date}';")
        return data_df

    @staticmethod
    def add_rolling_ave(data_df: DataFrame, n_days: int, feature: str) -> DataFrame:
        data_df.sort_values(by=['date'], inplace=True)
        data_df.index = data_df.date
        rolling_avg_df = data_df.groupby(by='ticker')[feature].rolling(window=n_days, min_periods=1).mean().reset_index(
            drop=False)
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
        data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_moving_avg'}, inplace=True)
        data_df.drop(columns='op', inplace=True)
        return data_df

    def add_cci(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating CCI")
        # compute typical price: (H+L+C)/3
        data_df['typical_price'] = (data_df['high'] + data_df['close'] + data_df['close']) / 3

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
        data_df[f'{n_days}_days_CCI'] = (data_df['typical_price'] - data_df[
            f'{n_days}_days_moving_avg_of_typical_price']) / (0.015 * data_df[f'{n_days}_days_mean_deviation'])

        data_df.drop(columns=['typical_price', 'tp-mv',
                              f'{n_days}_days_moving_avg_of_typical_price',
                              f'{n_days}_days_mean_deviation'], inplace=True)
        return data_df

    def add_atr(self, data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating ATR")
        data_df['H-L'] = abs(data_df['high'] - data_df['low'])
        data_df['H-P'] = abs(data_df['high'] - data_df['close'])
        data_df['L-P'] = abs(data_df['close'] - data_df['low'])
        max_list = []
        for index, row in data_df.iterrows():
            max_list.append(max(row['H-L'], row['H-P'], row['L-P']))
        data_df['true_range'] = DataFrame(max_list, columns=['true_range'])
        data_df = self.add_rolling_ave(data_df=data_df, n_days=n_days, feature='true_range')
        data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_ATR'}, inplace=True)
        data_df.drop(columns=['H-L', 'H-P', 'L-P', 'true_range'], inplace=True)
        return data_df

    @staticmethod
    def add_bollinger_bands(data_df: DataFrame, n_days: int) -> DataFrame:
        logger.info("Calculating Bollinger Bands")
        data_df.sort_values(by=['date'], inplace=True)
        data_df.index = data_df.date
        rolling_sd_df = data_df.groupby(by='ticker')['20_days_moving_avg'].rolling(window=n_days, min_periods=1) \
            .std().reset_index(drop=False)
        rolling_sd_df.rename(columns={'20_days_moving_avg': f'{n_days}_standard_deviation_result'}, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df = data_df.merge(rolling_sd_df, on=['ticker', 'date'], how='inner')
        data_df.sort_values(by=['ticker', 'date'], inplace=True)
        data_df.reset_index(drop=True, inplace=True)

        data_df['bollinger_lower_band'] = data_df['20_days_moving_avg'] - 2 * data_df[
            f'{n_days}_standard_deviation_result']
        data_df['bollinger_upper_band'] = data_df['20_days_moving_avg'] + 2 * data_df[
            f'{n_days}_standard_deviation_result']
        return data_df
