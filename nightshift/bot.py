"""
Launches NightShift application
"""
import argparse
import logging
import logging.config
import loggly.handlers
import os

from sqlalchemy.exc import IntegrityError
import yaml

from nightshift.config.logging_conf import log_conf
from nightshift.datastore_transactions import init_db
from nightshift.manager import process_resources, perform_db_maintenance


def config_local_env_variables(
    config_file: str = "nightshift/config/config.yaml",
) -> None:
    """
    Sets up environment variables to run NighShift locally.
    Requires a valid config.yaml file in nighshift/config directory

    Args:
        config_file:            path to config file that includes environmental
                                variables
    """
    with open(config_file, "r") as f:
        data = yaml.safe_load(f)
        for k, v in data.items():
            os.environ[k] = v


def configure_database(env: str = "prod"):
    """
    Sets up proper database tables and populates them with constant data

    Args:
        env:                    environment to set up database
    """
    if env == "local":
        config_local_env_variables()
    try:
        init_db()
        print("NightShift database successfully set up.")
    except IntegrityError as exc:
        print(
            "NightShift database appears to be already set up. "
            f"Operation raised following error: {exc}"
        )
    except ValueError:
        print(f"Environmental variables are not configured properly.")


def run(env: str = "prod") -> None:
    """
    In production environment it is assumed environmental variables
    are independently set up before NighShift is launched.
    """

    if env == "local":
        config_local_env_variables()

    conf = log_conf()
    logging.config.dictConfig(conf)
    logger = logging.getLogger("nightshift")

    logger.info(f"Initiating {env} NightShift...")

    process_resources()
    logger.info("Processing resources completed.")

    perform_db_maintenance()
    logger.info("Performing database maintenance completed.")


if __name__ == "__main__":
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

    args = parser.parse_args()

    if args.action == "run":
        if args.environment is None:
            env = "prod"
        else:
            env = args.environment

        run(env=env)

    elif args.action == "init":
        if args.environment is None:
            env = "prod"
        else:
            env = args.environment

        configure_database(env=env)
