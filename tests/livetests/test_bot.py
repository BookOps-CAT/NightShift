"""
Tests bot.py module
"""

import os

import pytest
import yaml

from nightshift.bot import config_local_env_variables


@pytest.mark.livetest
def test_config_local_env_variables_default_file():
    config_local_env_variables()

    with open("nightshift/config/config.yaml", "r") as f:
        data = yaml.safe_load(f)

    for k, v in data.items():
        assert os.environ[k] == v

    # clean up, otherwise vars will linger and mess up other tests
    for k in data.keys():
        del os.environ[k]
