from contextlib import nullcontext as does_not_raise
from datetime import datetime

import pytest
from pytest_sftpserver.sftp.server import SFTPServer

from nightshift.manager import process_resources


@pytest.mark.firewalled
def test_process_resources(env_var, test_data):
    with does_not_raise():
        process_resources()


# def calls_counter(func):
#     def wrapper(*args, **kwargs):
#         wrapper.count += 1
#         return func(*args, **kwargs)

#     wrapper.count = 0
#     return wrapper


# @calls_counter
# def sftpserver_multi_call(sftpserver):
#     print(f"sftpserver call #: {sftpserver_multi_call.count}")
#     return sftpserver.serve_content({})
