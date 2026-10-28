import os

import pytest
import yaml


@pytest.fixture
def live_env_vars(monkeypatch):
    if os.getenv("GITHUB_ACTIONS"):
        data = dict(
            POSTGRES_HOST="127.0.0.1",
            POSTGRES_USER="postgres",
            POSTGRES_PASSWORD="postgres",
            POSTGRES_PORT="5432",
            POSTGRES_DB="ns_db",
        )
    else:
        # local and firewalled tests
        with open("tests/envar.yaml", "r") as f:
            data = yaml.safe_load(f)

    for k, v in data.items():
        monkeypatch.setenv(k, v)
