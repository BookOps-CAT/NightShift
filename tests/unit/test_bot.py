"""
Tests bot.py module
"""
from contextlib import nullcontext as does_not_raise
import os
import logging

import pytest
import yaml

from nightshift.bot import config_local_env_variables, configure_database, main, run


def test_config_local_env_variables():
    test_config_file = "nightshift/config/config.yaml.example"
    config_local_env_variables(config_file=test_config_file)

    with open(test_config_file, "r") as f:
        data = yaml.safe_load(f)

    for k, v in data.items():
        assert os.getenv(k) == v

    # clean up, otherwise vars will linger and mess up other tests
    for k in data.keys():
        del os.environ[k]


@pytest.mark.local
def test_config_local_env_variables_default_file():
    config_local_env_variables()

    with open("nightshift/config/config.yaml", "r") as f:
        data = yaml.safe_load(f)

    for k, v in data.items():
        assert os.getenv(k) == v

    # clean up, otherwise vars will linger and mess up other tests
    for k in data.keys():
        del os.environ[k]


def test_configure_database_local_success(
    patch_init_db, patch_config_local_env_variables, capfd
):
    with does_not_raise():
        configure_database(env="local")
        captured = capfd.readouterr()
        assert captured.out == "NightShift local database successfully set up.\n"


def test_configure_database_again(
    patch_config_local_env_variables, mock_init_db_integrity_error, capfd
):
    configure_database()
    captured = capfd.readouterr()
    assert (
        "NightShift database appears to be already set up. Operation raised following error: "
        in captured.out
    )


def test_configure_database_without_env_variables(capfd, mock_init_db_value_error):
    configure_database()
    captured = capfd.readouterr()
    assert "Environmental variables are not configured properly." in captured.out


def test_configure_database_with_improper_structure(
    capfd, mock_init_db_invalid_structure
):
    configure_database()
    captured = capfd.readouterr()
    assert "Created database has invalid structure. Error: Foo Error." in captured.out


def test_run_local(
    caplog,
    patch_config_local_env_variables,
    mock_log_env,
    patch_process_resources,
    patch_perform_db_maintenance,
):
    with caplog.at_level(logging.INFO):
        run(env="local")

    assert "Launching local NightShift..." in caplog.text
    assert "Processing resources completed." in caplog.text
    assert "Database maintenance completed." in caplog.text


@pytest.mark.parametrize("arg", ["local", "prod"])
def test_main_init_arg(arg, patch_init_db, patch_config_local_env_variables, capfd):
    with does_not_raise():
        main(["init", f"{arg}"])
        captured = capfd.readouterr()

    assert f"NightShift {arg} database successfully set up." in captured.out


@pytest.mark.parametrize("arg", ["local", "prod"])
def test_main_run_arg(
    arg,
    patch_config_local_env_variables,
    patch_process_resources,
    patch_perform_db_maintenance,
    mock_log_env,
    caplog,
):
    with caplog.at_level(logging.INFO):
        main(["run", f"{arg}"])

    assert f"Launching {arg} NightShift..." in caplog.text
