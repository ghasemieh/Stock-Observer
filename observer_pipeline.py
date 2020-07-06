import sys
import argparse
import configuration
from typing import List
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

    def stock_observer_pipeline(self, arguments: List[str]) -> None:
        logger.info("Stock Observer Pipeline started.")

        self.pipeline_report.arguments = ' '.join(arguments)

        parser: ArgumentParser = argparse.ArgumentParser(description=__doc__)

        parser.add_argument("-D", "--download", help="download raw data files", action="store_true")
        parser.add_argument("-A", "--analyse", help="download raw data files", action="store_true")
        parser.add_argument("-N", "--notify", help="download raw data files", action="store_true")

        args: Namespace = parser.parse_args(args=arguments)

        try:
            if args.download:
                logger.info("Downloader started.")
                download = Downloader(self.config)
                ticker_list = {'MSFT'}
                data_df = download.download(ticker_list=ticker_list)
                print(data_df)


        except BaseException as e:
            self.pipeline_report.mark_failure()
            raise e

        finally:
            self.pipeline_report.log()


if __name__ == '__main__':
    pipeline: Stock_Observer_Pipeline = Stock_Observer_Pipeline(configuration.get())
    pipeline.stock_observer_pipeline(sys.argv[1:])
