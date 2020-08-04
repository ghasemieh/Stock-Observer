import yfinance as yf
from pathlib import Path
from utils import save_csv
from datetime import datetime, timedelta
from log_setup import get_logger
from configparser import ConfigParser
from pandas import DataFrame, to_datetime
import requests
import time
from stock_observer.database.database_communication import MySQL_Connection
from utils import str_to_datetime

logger = get_logger(__name__)


class Downloader:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.path = Path(config['Data_Sources']['equity price csv'])
        self.stage_table_name = self.config['MySQL']['stage table name']
        self.alphavantage_api_key = self.config['API']['alphavantage API key']
        self.marketstack_api_key = self.config['API']['marketstack API key']

    def download(self, ticker_list: DataFrame) -> DataFrame:
        mysql = MySQL_Connection(config=self.config)
        test = mysql.select(f"SELECT * FROM {self.stage_table_name} LIMIT 3;")
        if test is None:
            data_df = self.bulk_downloader(ticker_list=ticker_list)
        else:
            data_df = self.updates_downloader(ticker_list=ticker_list)
        if not data_df.empty:
            data_df = data_df[['id', 'ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'CCI']]
        return data_df

    def bulk_downloader(self, ticker_list) -> DataFrame:
        data_df = self.API_downloader_AV(ticker_list=ticker_list, table_name=self.stage_table_name, output_size='full',
                                         mode='buk')
        data_df = self.add_primary_key(data_df)
        logger.info(f"Data size is {data_df.shape}")
        save_csv(data_df, self.path)
        return data_df

    def updates_downloader(self, ticker_list):
        data_df = self.API_downloader_AV(ticker_list=ticker_list, table_name=self.stage_table_name,
                                         output_size='compact', mode='update')
        if not data_df.empty:
            data_df = self.add_primary_key(data_df)
            logger.info(f"Data size is {data_df.shape}")
            logger.info(f"Saving downloaded data in csv file at {self.path}")
            save_csv(data_df, self.path)
            return data_df
        else:
            logger.warning("No new data received")
            return DataFrame()

    def API_downloader_AV(self, ticker_list: list, table_name: str, output_size: str, mode: str) -> DataFrame:
        """
        This function is responsible for check the database for each ticker and get the updates from Quandl
        :param output_size:
        :param mode:
        :param ticker_list: the list of tickers
        :param table_name: The name of the SQl table
        :return: Dataframe
        """
        try:
            data_df = DataFrame()
            for index, item in ticker_list.iterrows():
                ticker = item['ticker']

                price = self.price_API_AV(ticker=ticker, output_size=output_size)
                cci = self.cci_indicators_API_AV(ticker=ticker)
                record = price.merge(cci, on=['ticker', 'date'], how='inner')

                if not record.empty:
                    record['date'] = record['date'].map(lambda x: datetime.strptime(x, "%Y-%m-%d").date())
                    if mode == 'update':
                        mysql = MySQL_Connection(config=self.config)
                        latest_date_df = mysql.select(
                            f"""select max(date) from {table_name} where ticker = '{ticker}';""")
                        if latest_date_df is not None:
                            latest_date_in_db = latest_date_df.iloc[0][0]
                            logger.info(f"{ticker} latest update is {latest_date_in_db}")
                        else:
                            latest_date_in_db = datetime.strptime('2000-01-01', "%Y-%m-%d").date()

                        record = record[record['date'] > latest_date_in_db]
                    data_df = data_df.append(record)
                else:
                    logger.warning(f"{ticker}: No data found for this date range, symbol may be delisted")
                time.sleep(30)
            return data_df

        except Exception as e:
            logger.error("API downloader error")
            logger.error(e)

    def price_API_AV(self, ticker: str, output_size: str, period: int = 30):
        logger.info(f"Retrieving data for {ticker}")
        # Extract data from quandl REST API
        controller = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}' \
                     f'&outputsize={output_size}&apikey={self.alphavantage_api_key}'
        response = requests.get(controller)
        response_dict = response.json()
        record = DataFrame.from_dict(response_dict['Time Series (Daily)'], orient='index')
        record['ticker'] = ticker
        record.reset_index(level=0, inplace=True)

        cols = list(record.columns)
        cols = [cols[-1]] + cols[:-1]
        record = record[cols]
        record = record.rename(columns={'index': 'date', '1. open': 'open', '3. low': 'low',
                                        '2. high': 'high', '4. close': 'close', '5. volume': 'volume'})
        return record

    def cci_indicators_API_AV(self, ticker: str, interval: str = 'daily', period: int = 30):
        logger.info(f"Retrieving cci indicator for {ticker}")
        # Extract data from quandl REST API
        controller = f'https://www.alphavantage.co/query?function=CCI&symbol={ticker}&interval={interval}' \
                     f'&time_period={period}&apikey={self.alphavantage_api_key}'
        response = requests.get(controller)
        response_dict = response.json()
        record = DataFrame.from_dict(response_dict['Technical Analysis: CCI'], orient='index')
        record['ticker'] = ticker
        record.reset_index(level=0, inplace=True)

        cols = list(record.columns)
        cols = [cols[-1]] + cols[:-1]
        record = record[cols]
        record = record.rename(columns={'index': 'date'})
        if record.empty:
            logger.warning(f"{ticker}: No indicator found")
        return record

    def API_downloader_MS(self, ticker_list: list, table_name: str) -> DataFrame:
        """
        This function is responsible for check the database for each ticker and get the updates from Quandl
        :param ticker_list: the list of tickers
        :param table_name: The name of the SQl table
        :return: Dataframe
        """
        try:
            data_df = DataFrame()
            for index, item in ticker_list.iterrows():
                ticker = item['ticker']
                logger.info(f"Retrieving data for {ticker}")
                # Extract data from quandl REST API
                controller = f'http://api.marketstack.com/v1/eod?access_key={self.marketstack_api_key}' \
                             f'&symbols={ticker}' \
                             f'&date_from=2000-01-01' \
                             f'&date_to={datetime.today().date()}'
                response = requests.get(controller)
                response_dict = response.json()
                updates = response_dict['data']
                record = DataFrame(updates)
                if not record.empty:
                    record['date'] = record['date'].map(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S+%f").date())
                    record.reset_index(level=0, inplace=True, drop=True)
                    record = record.rename(columns={'symbol': 'ticker'})
                    cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
                    record = record[cols]
                    data_df = data_df.append(record)
                else:
                    logger.warning(f"{ticker}: No data found for this date range, symbol may be delisted")

            mysql = MySQL_Connection(config=self.config)
            latest_data_df = mysql.select(f"""select max(date) from {table_name};""")

            if latest_data_df is not None:
                latest_date_in_db = latest_data_df.date[0]
            else:
                latest_date_in_db = datetime.strptime('2000-01-01', "%Y-%m-%d").date()

            data_df = data_df[data_df['date'] > latest_date_in_db]

            return data_df
        except Exception as e:
            logger.error("API downloader error")
            logger.error(e)

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
