"""
Centralize logging setup.
"""
import logging
import logging.handlers
import os

logging.getLogger().setLevel(logging.INFO)

logFormatter = logging.Formatter("%(asctime)s - %(levelname)-7s - %(message)s")

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

fileHandler = logging.handlers.RotatingFileHandler("logs/pipeline.log", maxBytes=1000000, backupCount=5)
fileHandler.setFormatter(logFormatter)


def get_logger(name):
    """
    import this function --> logger = get_logger(__name__) --> logger.info(f"message: {variable}.")
    :param name:
    :return:
    """
    logger = logging.getLogger(name)
    logger.addHandler(consoleHandler)
    if 'HISTORY_SCRIPT' in os.environ:
        if bool(os.environ['HISTORY_SCRIPT']):
            pass
        else:
            logger.addHandler(fileHandler)
    else:
        logger.addHandler(fileHandler)
    logger.propagate = False
    return logger


class LoggerStream(object):
    """
    Used to redirect stdout and stderr to logger instances.
    https://stackoverflow.com/questions/19425736/how-to-redirect-stdout-and-stderr-to-logger-in-python
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level

    def write(self, message):
        for line in message.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass
