# -*- coding: utf-8 -*-

"""
This module handles communication with network drive accessible via SFTP where Sierra
dumps daily files for processing and where CAT staff can access them
"""
import logging
import os


from paramiko.transport import Transport
from paramiko.sftp_client import SFTPClient


logger = logging.getLogger("nightshift")


def get_credentials():
    """
    Retrieves SFTP credentials from environmental variables.

    Returns:
        credentials
    """
    return (
        os.getenv("SFTP_HOST"),
        os.getenv("SFTP_USER"),
        os.getenv("SFTP_PASSW"),
        os.getenv("SFTP_NS_HOME"),
    )


class Drive:
    def __init__(self, host, user, password, home_directory):
        """
        Opens communication channel via SFTP to networked drive

        Args:
            host:                       SFTP host
            user:                       SFTP user name
            password:                   SFTP user password
            home_directory:             NighShift directory on the drive
        """
        self.sftp = self._sftp(host, user, password, home_directory)

    def list_directory(self):
        return self.sftp.listdir()

    def _sftp(self, host, user, password, home_directory):
        transport = Transport((host))
        transport.connect(None, user, password)
        sftp = SFTPClient.from_transport(transport)
        sftp.chdir(home_directory)  # wrap in an exception
        return sftp

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """
        Closes SFTP session and underlying channel.
        """
        if self.sftp:
            self.sftp.close()
