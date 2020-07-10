from pathlib import Path
from utils import save_csv
from pandas import DataFrame, to_datetime
from log_setup import get_logger
from configparser import ConfigParser

logger = get_logger(__name__)


def add_moving_avg(data_df: DataFrame, n_days: int) -> DataFrame:
    data_df.sort_values(by=['date'], inplace=True)
    data_df['tmp'] = (data_df['open'] + data_df['close']) / 2
    data_df.index = data_df.date
    moving_avg_df = data_df.groupby(by='ticker').tmp.rolling(window=5, min_periods=1).mean().reset_index(drop=False)
    moving_avg_df.rename(columns={'tmp': f'{n_days}_days_moving_avg'}, inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    data_df = data_df.merge(moving_avg_df, on=['ticker', 'date'], how='inner')
    data_df.sort_values(by=['ticker', 'date'], inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    return data_df


class Analyzer:
    def __init__(self, confi: ConfigParser):
        None

    def analysis(self, data_df: DataFrame):
        data_df = add_moving_avg(data_df=data_df, n_days=5)
        data_df = add_moving_avg(data_df=data_df, n_days=20)
