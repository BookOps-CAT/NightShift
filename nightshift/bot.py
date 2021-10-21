"""
Launches NightShift application
"""

import logging
import logging.config
import loggly.handlers

from nightshift.config.logging_conf import log_conf
from nightshift.manager import process_resources

conf = log_conf()
logging.config.dictConfig(conf)
logger = logging.getLogger("nightshift")


def run():
    logger.info("Initiating NightShift...")

    # process_resources()

    logger.info("Processing resources completed.")


if __name__ == "__main__":
    run()
