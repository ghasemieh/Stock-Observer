import sys
import argparse
import configuration
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
        logger.info("Stock Observer Pipeline started.")

        self.pipeline_report.arguments = ' '.join(arguments)

        parser: ArgumentParser = argparse.ArgumentParser(description=__doc__)

        parser.add_argument("-D", "--download", help="download raw data files", action="store_true")
        parser.add_argument("-A", "--analyze", help="download raw data files", action="store_true")
        parser.add_argument("-N", "--notify", help="download raw data files", action="store_true")

        args: Namespace = parser.parse_args(args=arguments)

        try:
            if args.download:
                logger.info("Downloader started.")
                pipeline_report_step = self.pipeline_report.create_step("Downloader")
                try:
                    download = Downloader(self.config)
                    ticker_list = read_csv(self.ticker_list_path)
                    # ticker_list = {'MSFT'}
                    data_df = download.download(ticker_list=ticker_list)
                except BaseException as e:
                    pipeline_report_step.mark_failure(str(e))
                    raise e

            if args.analyze:
                logger.info("Analyzer started.")
                pipeline_report_step = self.pipeline_report.create_step("Analyzer")
                try:
                    None
                    # analyzer = Analyzer(self.config)
                    # analyzer.analysis(data_df=data_df)
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
