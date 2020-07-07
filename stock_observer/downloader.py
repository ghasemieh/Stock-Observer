import yfinance as yf
from pathlib import Path
from utils import save_csv
from pandas import DataFrame
from log_setup import get_logger
from configparser import ConfigParser

logger = get_logger(__name__)


class Downloader:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.path = Path(config['Data_Sources']['equity price csv'])

    def download(self, ticker_list: DataFrame) -> DataFrame:
        data_df = DataFrame()
        try:
            for index, item in ticker_list.iterrows():
                ticker = item['ticker']
                logger.info(f"Retrieving data for {ticker}")
                item_obj = yf.Ticker(ticker)

                # get historical market data
                data = item_obj.history(period="5d")
                data['Ticker'] = ticker
                data.reset_index(level=0, inplace=True)

                cols = list(data.columns)
                cols = [cols[-1]] + cols[:-1]
                data = data[cols]

                data_df = data_df.append(data)
        except Exception as e:
            logger.error(e)

        save_csv(data_df, self.path)
        return data_df
