from contextlib import nullcontext as does_not_raise
from datetime import datetime

import pytest


from nightshift.manager import process_resources


@pytest.mark.firewalled
def test_process_resources(env_var, test_data):
    with does_not_raise():
        process_resources()
