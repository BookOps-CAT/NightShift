"""
Tests logging_config.py module
"""

from nightshift.config.logging_conf import log_conf


def test_log_conf():
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
