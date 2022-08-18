"""
Tests logging_config.py module
"""

import pytest

from nightshift.config.logging_conf import get_handlers, get_token, log_conf


def test_get_handlers(mock_log_env):
    assert get_handlers() == ["console", "file", "loggly"]


def test_get_handlers_missing_env_variable():
    with pytest.raises(EnvironmentError) as exc:
        get_handlers()

    assert "Missing logger handlers in environment variables." in str(exc.value)


def test_get_token(mock_log_env):
    assert get_token() == "ns_token_here"


def test_get_token_missing_env_variable():
    with pytest.raises(EnvironmentError) as exc:
        get_token()

    assert "Missing loggly token in environment variables." in str(exc.value)


def test_log_conf(mock_log_env):
    conf = log_conf()
    assert sorted(conf.keys()) == [
        "disable_existing_loggers",
        "formatters",
        "handlers",
        "loggers",
        "version",
    ]
    assert (
        conf["handlers"]["loggly"]["url"]
        == "https://logs-01.loggly.com/inputs/ns_token_here/tag/python"
    )
    assert conf["loggers"]["nightshift"]["handlers"] == ["console", "file", "loggly"]
