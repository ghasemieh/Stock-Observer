import sys
import argparse
import configuration
from stock_observer.analyzer import Analyzer
from stock_observer.database.database_main import Main_DB
from stock_observer.database.database_staging import Stage_DB
from stock_observer.notifier import Notifier
from utils import read_csv
from typing import List
from pathlib import Path
from argparse import Namespace
from log_setup import get_logger
from argparse import ArgumentParser
from configuration import ConfigParser
from pipeline_report import PipelineReport
from stock_observer.downloader import Downloader

logger = get_logger(__name__)


class Stock_Observer_Pipeline:
    def __init__(self, config: ConfigParser):
        self.pipeline_report = PipelineReport()
        self.config = config
        self.ticker_list_path = Path(config['Data_Sources']['tickers list csv'])

    def stock_observer_pipeline(self, arguments: List[str]) -> None:
        global data_df
        logger.info("+----------------------------------+")
        logger.info("| Stock observer pipeline started. |")
        logger.info("+----------------------------------+")

        self.pipeline_report.arguments = ' '.join(arguments)

        parser: ArgumentParser = argparse.ArgumentParser(description=__doc__)

        parser.add_argument("-D", "--download", help="download raw data files", action="store_true")
        parser.add_argument("-S", "--stage_db", help="download raw data files", action="store_true")
        parser.add_argument("-A", "--analyze", help="download raw data files", action="store_true")
        parser.add_argument("-M", "--main_db", help="download raw data files", action="store_true")
        parser.add_argument("-N", "--notify", help="download raw data files", action="store_true")

        args: Namespace = parser.parse_args(args=arguments)

        try:
            if args.download:
                logger.info("Downloader started.")
                pipeline_report_step = self.pipeline_report.create_step("Downloader")
                try:
                    download = Downloader(self.config)
                    ticker_list = read_csv(self.ticker_list_path)
                    data_df = download.download(ticker_list=ticker_list)
                except BaseException as e:
                    pipeline_report_step.mark_failure(str(e))
                    raise e

            if args.stage_db:
                logger.info("Database staging started.")
                pipeline_report_step = self.pipeline_report.create_step("Database Stagger")
                try:
                    stage_db = Stage_DB(self.config)
                    stage_db.insertion(data_df=data_df)
                except BaseException as e:
                    pipeline_report_step.mark_failure(str(e))
                    raise e

            if args.analyze:
                logger.info("Analyzer started.")
                pipeline_report_step = self.pipeline_report.create_step("Analyzer")
                try:
                    # import pandas as pd
                    # data_df = pd.read_csv('data/downloaded/equity_price.csv')
                    analyzer = Analyzer(self.config)
                    processed_data_df = analyzer.analysis(data_df=data_df)
                except BaseException as e:
                    pipeline_report_step.mark_failure(str(e))
                    raise e

            if args.main_db:
                logger.info("Main database insertion started.")
                pipeline_report_step = self.pipeline_report.create_step("Main DB Insertion")
                try:
                    import pandas as pd
                    processed_data_df = pd.read_csv('data/processed/processed_equity_price.csv')
                    main_db = Main_DB(self.config)
                    main_db.insertion(data_df=processed_data_df)
                except BaseException as e:
                    pipeline_report_step.mark_failure(str(e))
                    raise e

            if args.notify:
                logger.info("Notifier started.")
                pipeline_report_step = self.pipeline_report.create_step("Notifier")
                try:
                    notifier = Notifier(self.config)
                    notifier.notifier()
                except BaseException as e:
                    pipeline_report_step.mark_failure(str(e))
                    raise e

            self.pipeline_report.mark_success()
        except BaseException as e:
            self.pipeline_report.mark_failure()
            raise e

        finally:
            self.pipeline_report.log()


if __name__ == '__main__':
    pipeline: Stock_Observer_Pipeline = Stock_Observer_Pipeline(configuration.get())
    pipeline.stock_observer_pipeline(sys.argv[1:])
