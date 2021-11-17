"""
This probably be more of a functional test eventually. Move out of unit tests when expanded.
"""

import logging


LOGGER = logging.getLogger(__name__)


def test_logger_init_nightshift(
    test_log, caplog, test_connection, test_session, test_data_core
):
    from nightshift.bot import run

    with caplog.at_level(logging.INFO):
        run()
    assert "Initiating NightShift..." in caplog.text
    assert "Processing resources completed." in caplog.text
