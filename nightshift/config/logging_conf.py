"""
Configures logging
"""
import os
from typing import Dict, List


def get_handlers() -> List:
    """
    Retrieves from env variables logging handlers to be used and
    returns them as a list

    Returns:
        handlers
    """
    return os.getenv("LOG_HANDLERS").split(",")


def get_token() -> str:
    """
    Retrieves from env variables loggly customer token

    Returns:
        token
    """
    return os.getenv("LOGGLY_TOKEN")


def log_conf() -> Dict:
    """
    Returns dictionary with logger configuration. Depends on
    environmental variables to access loggly token and specify logger.

    Returns:
        logging_config
    """
    log_token = get_token()
    handlers = get_handlers()

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "brief": {
                "format": "%(name)s-%(asctime)s-%(filename)s-%(lineno)s-%(levelname)s-%(message)s"
            },
            "json": {
                "format": '{"app":"%(name)s", "asciTime":"%(asctime)s", "fileName":"%(filename)s", "lineNo":"%(lineno)d", "levelName":"%(levelname)s", "message":"%(message)s", "exc_info":"%(exc_info)s"}'
            },
        },
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "brief",
            },
            "file": {
                "level": "DEBUG",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "ns_log.log",
                "formatter": "brief",
                "maxBytes": 1024 * 1024,
                "backupCount": 5,
            },
            "loggly": {
                "level": "INFO",
                "class": "loggly.handlers.HTTPSHandler",
                "formatter": "json",
                "url": f"https://logs-01.loggly.com/inputs/{log_token}/tag/python",
            },
        },
        "loggers": {
            "nightshift": {
                "handlers": handlers,
                "level": "DEBUG",
                "propagate": True,
            }
        },
    }
    return logging_config