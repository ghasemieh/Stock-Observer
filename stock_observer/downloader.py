import yfinance as yf
from pathlib import Path
from utils import save_csv
from pandas import DataFrame
from log_setup import get_logger
from configparser import ConfigParser
from stock_observer.database_communication import MySQL_Connection

logger = get_logger(__name__)


class Downloader:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.path = Path(config['Data_Sources']['equity price csv'])
        self.table_name = config['MySQL']['table name']

    def download(self, ticker_list: DataFrame) -> DataFrame:
        data_df = DataFrame()
        try:
            for index, item in ticker_list.iterrows():
                ticker = item['ticker']
                logger.info(f"Retrieving data for {ticker}")
                item_obj = yf.Ticker(ticker)

                # get historical market data
                record = item_obj.history(period="5d")
                record['Ticker'] = ticker
                record.reset_index(level=0, inplace=True)

                cols = list(record.columns)
                cols = [cols[-1]] + cols[:-1]
                record = record[cols]

                data_df = data_df.append(record)

                try:
                    mysql = MySQL_Connection(config=self.config)
                    test = mysql.select(f"SELECT * FROM {self.table_name} LIMIT 3;")
                    if test is None:
                        logger.warning(f"There is no {self.table_name} table in the database")
                        mysql.create_table(table_name=self.table_name)
                    mysql.insert_many(table_name=self.table_name, data_df=data_df)
                except Exception as e:
                    logger.error(e)


        except Exception as e:
            logger.error(e)

        save_csv(data_df, self.path)
        return data_df
