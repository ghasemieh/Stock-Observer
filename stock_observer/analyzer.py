from pathlib import Path
from utils import save_csv
from pandas import DataFrame, to_datetime
from log_setup import get_logger
from configparser import ConfigParser

logger = get_logger(__name__)


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


def add_moving_avg(data_df: DataFrame, n_days: int) -> DataFrame:
    data_df['op'] = (data_df['open'] + data_df['close']) / 2
    data_df = add_rolling_ave(data_df=data_df, n_days=n_days, feature='op')
    data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_moving_avg'}, inplace=True)
    data_df.drop(columns='op', inplace=True)
    return data_df


def add_cci(data_df: DataFrame, n_days: int) -> DataFrame:
    # compute typical price: (H+L+C)/3
    data_df['typical_price'] = (data_df['high'] + data_df['close'] + data_df['close']) / 3

    # compute moving average on typical price: sum(typical_price)/30
    data_df = add_rolling_ave(data_df=data_df, n_days=n_days, feature='typical_price')
    data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_moving_avg_of_typical_price'},
                   inplace=True)

    # compute mean deviation: sum(|typical_price - moving average|)/30
    data_df['tp-mv'] = abs(data_df['typical_price'] - data_df[f'{n_days}_days_moving_avg_of_typical_price'])
    data_df = add_rolling_ave(data_df=data_df, n_days=n_days, feature='tp-mv')
    data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_days_mean_deviation'}, inplace=True)

    # compute Commodity Channel Index (CCI): (typical_price - moving_average)/(0.015 * mean_deviation)
    data_df['CCI'] = (data_df['typical_price'] - data_df[f'{n_days}_days_moving_avg_of_typical_price']) / (
            0.015 * data_df[f'{n_days}_days_mean_deviation'])

    data_df.drop(columns=['typical_price', 'tp-mv',
                          f'{n_days}_days_moving_avg_of_typical_price',
                          f'{n_days}_days_mean_deviation'], inplace=True)
    return data_df


def add_atr(data_df: DataFrame, n_days: int) -> DataFrame:
    data_df['H-L'] = abs(data_df['high'] - data_df['low'])
    data_df['H-P'] = abs(data_df['high'] - data_df['close'])
    data_df['L-P'] = abs(data_df['close'] - data_df['low'])
    for tuple in data_df.itertuples():
        tuple
    data_df['true_range'] = max(data_df['H-L'], data_df['H-P'], data_df['L-P'])

    data_df = add_rolling_ave(data_df=data_df, n_days=n_days, feature='true_range')
    data_df.rename(columns={f'{n_days}_days_rolling_result': f'{n_days}_ATR'}, inplace=True)
    return data_df


class Analyzer:
    def __init__(self, confi: ConfigParser):
        None

    def analysis(self, data_df: DataFrame) -> DataFrame:
        # data_df = add_moving_avg(data_df=data_df, n_days=5)
        # data_df = add_moving_avg(data_df=data_df, n_days=20)
        # data_df = add_cci(data_df=data_df, n_days=30)
        data_df = add_atr(data_df=data_df, n_days=20)
        return data_df
