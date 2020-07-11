import yfinance as yf
from pathlib import Path
from utils import save_csv
from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, to_datetime
from stock_observer.database.database_communication import MySQL_Connection

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
                record = item_obj.history(period="max")
                record['Ticker'] = ticker
                record.reset_index(level=0, inplace=True)

                cols = list(record.columns)
                cols = [cols[-1]] + cols[:-1]
                record = record[cols]
                record = record.rename(columns={'Ticker': 'ticker', 'Date': 'date', 'Open': 'open', 'Low': 'low',
                                                'High': 'high', 'Close': 'close', 'Volume': 'volume',
                                                'Dividends': 'dividends', 'Stock Splits': 'stock_splits'})

                if not record.empty:
                    data_df = data_df.append(record)
                else:
                    logger.warning(f"{ticker}: No data found for this date range, symbol may be delisted")
        except Exception as e:
            logger.error(e)
        data_df['date'] = to_datetime(data_df['date'])
        data_df['date'] = data_df['date'].map(lambda x: x.date())
        logger.info(f"Data size is {data_df.shape}")
        save_csv(data_df, self.path)
        return data_df
