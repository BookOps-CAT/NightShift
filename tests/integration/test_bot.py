"""
Top level tests.
"""

import logging

import pytest


from nightshift.bot import run


@pytest.mark.local
def test_logger_on_run(env_var, caplog, test_data):

    with caplog.at_level(logging.INFO):
        run(env="local")
    assert "Launching local NightShift..." in caplog.text
    assert "Processing resources completed." in caplog.text
    assert "Performing database maintenance completed." in caplog.text
