# -*- coding: utf-8 -*-

from io import BytesIO
import os

import pytest

import nightshift


@pytest.fixture
def env_var(monkeypatch, local_test_config):
    if not os.getenv("TRAVIS"):
        for k, v in local_test_config.items():
            monkeypatch.setenv(k, v)


@pytest.fixture
def test_data(test_session, test_data_core, stub_resource):
    test_session.add(stub_resource)
    test_session.commit()


@pytest.fixture
def mock_drive_unprocessed_files(monkeypatch):
    def _files(*args):
        return ["NYP-bar.pout"]

    monkeypatch.setattr(nightshift.tasks, "isolate_unprocessed_files", _files)


@pytest.fixture
def mock_drive_fetch_file(monkeypatch):
    def _fetch(*args):
        with open("tests/nyp-ebook-sample.mrc", "rb") as test_file:
            data = test_file.read()
            marc_target = BytesIO(data)
            return marc_target

    monkeypatch.setattr(nightshift.comms.storage.Drive, "fetch_file", _fetch)
