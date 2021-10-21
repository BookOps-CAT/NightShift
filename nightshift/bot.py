"""
Launches NightShift application
"""

import logging
import logging.config
import loggly.handlers

from nightshift.config.logging_conf import log_conf

conf = log_conf()
logging.config.dictConfig(conf)
logger = logging.getLogger("nightshift")


def run():
    logger.info("Initiating NightShift...error")
    try:
        2 / 0
    except Exception as e:
        logger.error(e, exc_info=True)


if __name__ == "__main__":
    run()
