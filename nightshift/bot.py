"""
Launches NightShift application
"""
import argparse
import logging
import logging.config
import loggly.handlers
import os
import sys

from sqlalchemy.exc import IntegrityError
import yaml

from nightshift import datastore_transactions, manager
from nightshift.config.logging_conf import log_conf


def config_local_env_variables(
    config_file: str = "nightshift/config/config.yaml",
) -> None:
    """
    Sets up environment variables to run NighShift locally.
    Requires a valid config.yaml file in the nighshift/config directory

    Args:
        config_file:            path to config file that includes environmental
                                variables
    """
    with open(config_file, "r") as f:
        data = yaml.safe_load(f)
        for k, v in data.items():
            os.environ[k] = v


def configure_database(env: str = "prod") -> None:
    """
    Sets up proper database tables and populates them with constant data

    Args:
        env:                    environment to set up database
    """
    if env == "local":
        config_local_env_variables()
    try:
        datastore_transactions.init_db()
        print(f"NightShift {env} database successfully set up.")
    except IntegrityError as exc:
        print(
            "NightShift database appears to be already set up. "
            f"Operation raised following error: {exc}"
        )
    except ValueError:
        print(f"Environmental variables are not configured properly.")


def run(env: str = "prod") -> None:
    """
    Launches processing of new and older resources and performs
    database maintenance. This is the main NightShift process.

    In production environment it is assumed environment variables
    are independently set up before NighShift is launched.

    In a local installation environment variables are read from a
    file and set up at runtime.

    Args:
        env:                    application environment: 'local' or 'prod'
    """

    if env == "local":
        config_local_env_variables()

    conf = log_conf()
    logging.config.dictConfig(conf)
    logger = logging.getLogger("nightshift")

    logger.info(f"Launching {env} NightShift...")

    manager.process_resources()
    logger.info("Processing resources completed.")

    manager.perform_db_maintenance()
    logger.info("Database maintenance completed.")


def main(args: list) -> None:
    """
    Parses command-line arguments used to configure and run NightShift

    Args:
        args:                   list of arguments
    """
    parser = argparse.ArgumentParser(
        prog="NightShift", description="NighShift launcher"
    )

    parser.add_argument(
        "action",
        help="'init' sets up database ; 'run' launches processing records and db maintenance",
        type=str,
        choices=["init", "run"],
    )
    parser.add_argument(
        "environment",
        help="selects environment to run the program",
        type=str,
        choices=["prod", "local"],
    )

    pargs = parser.parse_args(args)

    if pargs.action == "run":
        run(env=pargs.environment)

    elif pargs.action == "init":
        configure_database(env=pargs.environment)


if __name__ == "__main__":
    main(sys.argv[1:])  # pragma: no cover
