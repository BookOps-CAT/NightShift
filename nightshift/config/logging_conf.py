"""
Configures app logging
"""

import os


def log_conf() -> dict:
    """
    Returns dictionary with logger configuration. Depends on
    environmental variables to access loggly token and specify logger.

    Returns:
        logging_config
    """
    log_token = os.environ["LOGGLY_TOKEN"]
    log_handlers = os.environ["LOG_HANDLERS"]
    handlers = log_handlers.split(",")
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "brief": {
                "format": "%(name)s-%(asctime)s-%(filename)s-%(lineno)s-%(levelname)s-%(message)s"  # noqa: E501
            },
            "json": {
                "format": '{"app":"%(name)s", "asciTime":"%(asctime)s", "fileName":"%(filename)s", "lineNo":"%(lineno)d", "levelName":"%(levelname)s", "message":"%(message)s"}'  # noqa: E501
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
                "filename": "nightshift.log",
                "formatter": "brief",
                "maxBytes": 10 * 1024 * 1024,  # ~5k records per file
                "backupCount": 5,
                "encoding": "utf8",
            },
            "loggly": {
                "level": "WARN",
                "class": "loggly.handlers.HTTPSHandler",
                "formatter": "json",
                "url": f"https://logs-01.loggly.com/inputs/{log_token}/tag/python",
            },
        },
        "loggers": {
            "nightshift": {"handlers": handlers, "level": "DEBUG", "propagate": True}
        },
    }
    return logging_config
