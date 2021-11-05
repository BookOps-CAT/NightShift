# -*- coding: utf-8 -*-
import os

import pytest
import yaml


@pytest.fixture
def live_sftp_env(monkeypatch):
    if not os.getenv("TRAVIS"):
        with open("tests/envar.yaml", "r") as f:
            data = yaml.safe_load(f)
            monkeypatch.setenv("SFTP_HOST", data["SFTP_HOST"])
            monkeypatch.setenv("SFTP_USER", data["SFTP_USER"])
            monkeypatch.setenv("SFTP_PASSW", data["SFTP_PASSW"])
            monkeypatch.setenv("SFTP_NS_SRC", data["SFTP_NS_SRC"])
            monkeypatch.setenv("SFTP_NS_DST", data["SFTP_NS_DST"])
