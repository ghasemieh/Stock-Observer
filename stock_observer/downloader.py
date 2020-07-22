import yfinance as yf
from pathlib import Path
from utils import save_csv
from datetime import datetime, timedelta
from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, to_datetime
from stock_observer.database.database_communication import MySQL_Connection

logger = get_logger(__name__)


class Downloader:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.path = Path(config['Data_Sources']['equity price csv'])
        self.stage_table_name = self.config['MySQL']['stage table name']

    def download(self, ticker_list: DataFrame) -> DataFrame:
        mysql = MySQL_Connection(config=self.config)
        test = mysql.select(f"SELECT * FROM {self.stage_table_name} LIMIT 3;")
        if test is None:
            data_df = self.bulk_downloader(ticker_list=ticker_list)
        else:
            data_df = self.updates_downloader(ticker_list=ticker_list)
        if not data_df.empty:
            data_df = data_df[['id', 'ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        return data_df

    def bulk_downloader(self, ticker_list) -> DataFrame:
        data_df = DataFrame()
        for index, item in ticker_list.iterrows():
            ticker = item['ticker']
            logger.info(f"Retrieving data for {ticker}")
            try:
                item_obj = yf.Ticker(ticker)
                # get historical market data
                record = item_obj.history(period="max")
                record['Ticker'] = ticker
                record.reset_index(level=0, inplace=True)

                cols = list(record.columns)
                cols = [cols[-1]] + cols[:-1]
                record = record[cols]
                record = record.rename(columns={'Ticker': 'ticker', 'Date': 'date', 'Open': 'open', 'Low': 'low',
                                                'High': 'high', 'Close': 'close', 'Volume': 'volume'})
                if not record.empty:
                    data_df = data_df.append(record)
                else:
                    logger.warning(f"{ticker}: No data found for this date range, symbol may be delisted")
            except Exception as e:
                logger.error(e)
        data_df = self.add_primary_key(data_df)
        logger.info(f"Data size is {data_df.shape}")
        save_csv(data_df, self.path)
        return data_df

    def updates_downloader(self,ticker_list):
        mysql = MySQL_Connection(config=self.config)
        max_date = mysql.select(f"SELECT max(date) FROM {self.stage_table_name};")
        start_date = str(max_date.iloc[0][0] + timedelta(days=1))
        end_date = str(datetime.today().date())
        if start_date == end_date:
            return DataFrame()
        data_df = DataFrame()
        for index, item in ticker_list.iterrows():
            ticker = item['ticker']
            logger.info(f"Retrieving data for {ticker}")
            try:
                record = yf.download(ticker, start=start_date, end=end_date)
                record['Ticker'] = ticker
                record.reset_index(level=0, inplace=True)

                cols = list(record.columns)
                cols = [cols[-1]] + cols[:-1]
                record = record[cols]
                record = record.rename(columns={'Ticker': 'ticker', 'Date': 'date', 'Open': 'open', 'Low': 'low',
                                                'High': 'high', 'Close': 'close', 'Volume': 'volume'})
                if not record.empty:
                    data_df = data_df.append(record)
                else:
                    logger.warning(f"{ticker}: No data found for this date range, symbol may be delisted")
            except Exception as e:
                logger.error(e)
        if not data_df.empty:
            data_df = self.add_primary_key(data_df)
            data_df = data_df[data_df['date'] > max_date.iloc[0][0]]
            logger.info(f"Data size is {data_df.shape}")
            logger.info(f"Saving downloaded data in csv file at {self.path}")
            save_csv(data_df, self.path)
            return data_df
        else:
            return DataFrame()

    @staticmethod
    def add_primary_key(data: DataFrame) -> DataFrame:
        data_df = data.copy()
        logger.info("Adding primary key")
        data_df['date'] = to_datetime(data_df['date'])
        data_df['date'] = data_df['date'].map(lambda x: x.date())
        data_df['date_str'] = data_df['date'].map(lambda x: str(x))
        data_df['id'] = data_df['ticker'] + "-" + data_df['date_str']
        cols = list(data_df.columns)
        cols = [cols[-1]] + cols[:-1]
        data_df = data_df[cols]
        data_df.drop(columns='date_str', inplace=True)
        return data_df
