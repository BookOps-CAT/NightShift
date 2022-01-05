# -*- coding: utf-8 -*-
import os

import pytest


@pytest.fixture
def env_var(monkeypatch, local_test_config):
    if not os.getenv("TRAVIS"):
        for k, v in local_test_config.items():
            monkeypatch.setenv(k, v)


@pytest.fixture
def test_data(test_session, test_data_core, stub_resource):
    test_session.add(stub_resource)
    test_session.commit()
