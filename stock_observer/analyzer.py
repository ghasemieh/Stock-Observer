from log_setup import get_logger
from configparser import ConfigParser
from sklearn import preprocessing
import numpy as np
import matplotlib.pyplot as plt
import mysql.connector as mysql
from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import sympy as sp
from datetime import datetime

from utils import save_csv

logger = get_logger(__name__)


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

    # plt.plot(x, y, 'x', x, y)
    # plt.xlim([x[0] - 1, x[-1] + 1])
    # plt.show()
    return result


def analyer(data_df: DataFrame):
    data = data_df
    data.set_index(data["date"], inplace=True)
    ma_slope = data_df.groupby(by='ticker')['5_days_moving_avg'].rolling(window=5, min_periods=5) \
        .apply(slope_cal).reset_index(drop=False)
    ma_slope.rename(columns={'5_days_moving_avg': f'alpha'}, inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    data_df = data_df.merge(ma_slope, on=['ticker', 'date'], how='inner')
    data_df.sort_values(by=['ticker', 'date'], inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    return data_df

df = pd.read_csv('data/processed/processed_equity_price.csv')

df['date'] = pd.to_datetime(df['date'])
processed_df = analyer(df)
save_csv(processed_df,'data/processed/alpha.csv')
