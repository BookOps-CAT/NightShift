from nightshift import __title__, __version__


def test_version():
    assert __version__ == "0.4.0"


def test_title():
    assert __title__ == "NightShift"
