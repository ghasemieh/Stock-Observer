import yfinance as yf
from configparser import ConfigParser
from pandas import DataFrame
from log_setup import get_logger

logger = get_logger(__name__)


class Downloader:
    def __init__(self, config: ConfigParser):
        self.config = config

    def download(self, ticker_list) -> DataFrame:
        data_df = DataFrame()
        for item in ticker_list:
            logger.info(f'Retrieving data for {item}')
            item_obj = yf.Ticker(item)

            # get stock info
            # print(item_obj.info)
            # print(item_obj.earnings)

            # get historical market data
            data = item_obj.history(period="5d")
            data['Ticker'] = item
            data.reset_index(level=0, inplace=True)
            cols = list(data.columns)
            cols = [cols[-1]] + cols[:-1]
            data = data[cols]
            data_df = data_df.append(data)
            # print(data)
        return data_df
