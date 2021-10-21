"""
Tests logging_config.py module
"""

from nightshift.logging_conf import get_handlers, get_token, log_config


def test_get_handlers(mock_log_env):
    assert get_handlers() == ["console", "file", "loggly"]


def test_get_token(mock_log_env):
    assert get_token() == "ns_token_here"


def test_log_config(mock_log_env):
    conf = log_config()
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
