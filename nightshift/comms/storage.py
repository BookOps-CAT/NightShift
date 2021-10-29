# -*- coding: utf-8 -*-

"""
This module handles communication with network drive accessible via SFTP where Sierra
dumps daily files for processing and where CAT staff can access them
"""
import os

import paramiko

paramiko.util.log_to_file("nightshift.log")


class DriveSFTP:
    def __init__(self):
        host, user, password, self.home_dir = self._get_credentials()
        transport = paramiko.Transport((host))
        transport.connect(None, user, password)
        self.sftp = paramiko.SFTPClient.from_transport(transport)

    def __enter__(self):
        return self.sftp

    def __exit__(self):
        if self.sftp:
            self.sftp.close()

    def close(self):
        self.sftp.close()

    def _get_credentials(self) -> dict:
        """
        Retrieves SFTP credentials from environmental variables.

        Returns:
            credentials
        """
        return (
            os.getenv("SFTP_HOST"),
            os.getenv("SFTP_USER"),
            os.getenv("SFTP_PASSW"),
            os.getenv("SFTP_DIR"),
        )
