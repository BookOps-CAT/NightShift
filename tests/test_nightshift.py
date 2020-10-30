from nightshift import __version__, __title__


def test_version():
    assert __version__ == "0.1.0"


def test_title():
    assert __title__ == "NightShift"
