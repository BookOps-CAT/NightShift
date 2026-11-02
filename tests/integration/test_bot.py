"""
Tests bot.py module
"""

import os

import pytest

from nightshift import constants
from nightshift.bot import config_local_env_variables, configure_database, main, run


def test_config_local_env_variables(mock_envar_yaml):
    test_config_file = "nightshift/config/config.yaml.example"
    config_local_env_variables(config_file=test_config_file)
    assert os.environ["POSTGRES_DB"] == "nightshift_test"


def test_configure_database_local_success(mock_init_db, caplog):
    configure_database(env="local")
    assert "NightShift local database successfully set up.\n" in caplog.text


def test_configure_database_integrity_error(mock_db_integrity_error, caplog):
    configure_database()
    assert (
        "NightShift database appears to be already set up. Operation raised following error: "
        in caplog.text
    )


def test_configure_database_db_initialization_error(caplog, mock_db_env, monkeypatch):
    monkeypatch.setattr(constants, "LIBRARIES", {"NYP": {"nid": 1}})
    configure_database()
    assert "Created database has invalid structure. " in caplog.text


def test_configure_database_value_error(caplog, mock_db_env, monkeypatch):
    monkeypatch.delenv("POSTGRES_DB")
    configure_database()
    assert "Environmental variables are not configured properly." in caplog.text


@pytest.mark.parametrize("arg", ["local", "prod"])
def test_run(caplog, mock_db_env, mock_manager, arg):
    run(env=arg)

    assert f"Launching {arg} NightShift..." in caplog.text
    assert "Processing resources completed." in caplog.text
    assert "Database maintenance completed." in caplog.text


@pytest.mark.parametrize(
    "arg,output",
    [
        ("init", "NightShift local database successfully set up."),
        ("run", "Launching local NightShift..."),
    ],
)
def test_main(mock_init_db, mock_manager, arg, output, caplog):
    main([f"{arg}", "local"])

    assert output in caplog.text
