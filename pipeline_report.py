"""PipelineReport class to summarize the results of the pipeline.
"""
from datetime import datetime
from datetime import timedelta
from log_setup import get_logger
from utils import format_timedelta

logger = get_logger(__name__)


class PipelineReportStep:
    def __init__(self, name):
        self.start_time = datetime.now()
        self.name = name
        self.status = "Unknown"
        self.details = []
        self.end_time = None
        self._pipeline_report_ = None

    def set_pipeline_report(self, pipeline_report):
        self._pipeline_report_ = pipeline_report

    def mark_success(self):
        self._mark_status("Success")

    def mark_failure(self, message: str):
        self._mark_status("Failure")
        self.add_detail("Failure reason", message)
        self._pipeline_report_.mark_failure()

    def mark_skipped(self, message: str):
        self._mark_status("Skipped")
        self.add_detail("Skipped reason", message)

    def _mark_status(self, status: str):
        self.status = status
        self.end_time = datetime.now()

    def add_detail(self, key: str, value: str):
        self.details.append((key, value))

    def add_info_detail(self, message: str):
        logger.info(message)
        self.add_detail("Info", message)

    def add_warning_detail(self, message: str):
        logger.warning(message)
        self.add_detail("Warning", message)

    def calculate_elapsed_time(self) -> timedelta:
        pipeline_step_end_time = self.end_time if self.end_time else datetime.now()
        return pipeline_step_end_time - self.start_time

    def log(self):
        logger.info("------------------------------------------------------------------------")
        logger.info(f"Step           : {self.name}")
        logger.info(f"Elapsed time   : {format_timedelta(self.calculate_elapsed_time())}")
        for detail in self.details:
            logger.info(f"{detail[0]:15}: {detail[1]}")


class PipelineReport:
    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        self.arguments = None
        self.status = "Unknown"
        self.steps = []

    def create_step(self, name: str) -> PipelineReportStep:
        step = PipelineReportStep(name)
        step._pipeline_report_ = self
        self.steps.append(step)
        return step

    def calculate_elapsed_time(self) -> timedelta:
        pipeline_end_time = self.end_time if self.end_time else datetime.now()
        return pipeline_end_time - self.start_time

    def mark_success(self):
        self._mark_status("Success")

    def mark_failure(self):
        self._mark_status("Failure")

    def _mark_status(self, status: str):
        self.status = status
        self.end_time = datetime.now()

    def log(self):
        logger.info("------------------------------------------------------------------------")
        logger.info(f"stock_observer_pipeline.py {self.arguments} started {self.start_time}")
        for pipeline_report_step in self.steps:
            pipeline_report_step.log()
        logger.info("------------------------------------------------------------------------")
        logger.info(f"ETL Pipeline complete, status: {self.status}. Elapsed time {format_timedelta(self.calculate_elapsed_time())}")
        logger.info("------------------------------------------------------------------------")
