import logging


LOGGER = logging.getLogger(__name__)


def test_logger_init_nightshift(
    test_log, caplog, test_connection, test_session, test_data
):
    from nightshift.bot import run

    with caplog.at_level(logging.INFO):
        run()
    assert "Initiating NightShift..." in caplog.text
    assert "Processing resources completed." in caplog.text
