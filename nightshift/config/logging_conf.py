# -*- coding: utf-8 -*-

"""
Configures app logging
"""
import os


def get_handlers() -> list:
    """
    Retrieves from env variables logging handlers to be used and
    returns them as a list

    Returns:
        handlers

    Raises:
        EnvironmentError
    """
    handlers = os.getenv("LOG_HANDLERS")
    if handlers is None:
        raise EnvironmentError("Missing logger handlers in environment variables.")
    else:
        return handlers.split(",")


def get_token() -> str:
    """
    Retrieves from env variables loggly customer token

    Returns:
        token
    """
    token = os.getenv("LOGGLY_TOKEN")
    if token is None:
        raise EnvironmentError("Missing loggly token in environment variables.")
    else:
        return token


def log_conf() -> dict:
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
                "format": '{"app":"%(name)s", "asciTime":"%(asctime)s", "fileName":"%(filename)s", "lineNo":"%(lineno)d", "levelName":"%(levelname)s", "message":"%(message)s"}'
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
            "nightshift": {
                "handlers": handlers,
                "level": "DEBUG",
                "propagate": True,
            }
        },
    }
    return logging_config
