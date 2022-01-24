"""
Tests bot.py module
"""
from contextlib import nullcontext as does_not_raise
import os

import pytest
from sqlalchemy import create_engine
import yaml

from nightshift.bot import config_local_env_variables, configure_database
from nightshift.datastore import Base


def test_config_local_env_variables():
    test_config_file = "nightshift/config/config.yaml.example"
    config_local_env_variables(config_file=test_config_file)

    with open(test_config_file, "r") as f:
        data = yaml.safe_load(f)

    for k, v in data.items():
        assert os.getenv(k) == v


@pytest.mark.local
def test_config_local_env_variables_default_file():
    config_local_env_variables()

    with open("nightshift/config/config.yaml", "r") as f:
        data = yaml.safe_load(f)

    for k, v in data.items():
        assert os.getenv(k) == v


def test_configure_database_local_success(
    mock_init_db, mock_config_local_env_variables
):
    with does_not_raise():
        configure_database(env="local")
