# -*- coding: utf-8 -*-

"""
This module handles communication with network drive accessible via SFTP where Sierra
dumps daily files for processing and where CAT staff can access produces MARC files.
"""
from io import BytesIO
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
        os.getenv("SFTP_PORT"),
        os.getenv("SFTP_USER"),
        os.getenv("SFTP_PASSW"),
        os.getenv("SFTP_NS_SRC"),
        os.getenv("SFTP_NS_DST"),
    )


class Drive:
    def __init__(self, host, port, user, password, src_dir, dst_dir):
        """
        Opens communication channel via SFTP to networked drive

        Args:
            host:                       SFTP host
            port:                       SFTP port
            user:                       SFTP user name
            password:                   SFTP user password
            home_directory:             NighShift directory on the drive
        """
        self.sftp = self._sftp(host, port, user, password)
        self.src_dir = src_dir
        self.dst_dir = dst_dir

    def list_src_directory(self) -> list[str]:
        """
        Returns a list of files found in SFTP/Drive Sierra dumps directory
        """
        return self.sftp.listdir(path=self.src_dir)

    def fetch_file(self, path: str) -> BytesIO:
        """
        Retrieves file of the given path

        Args:
            path:                       path to file on the SFTP server

        Returns:
            bytes stream

        """
        logging.info(f"Fetching {path} file from the SFTP.")
        with self.sftp.file(path, mode="r") as file:
            file_size = file.stat().st_size
            file.prefetch(file_size)
            file.set_pipelined()
            return BytesIO(file.read(file_size))

    def output_file(self, local_fh: str) -> None:
        """
        Appends stream to a file in SFTP/Drive load directory

        Args:
            path:                   path to file on the SFTP server to append to

        """
        drive_fh = self._determine_drive_file_handle(local_fh)
        try:
            self.sftp.put(local_fh, drive_fh)
            logger.info(f"Successfully created {drive_fh} on the SFTP.")
        except IOError:
            logger.error(f"IOError. Unable to create {drive_fh} on the SFTP.")
            raise

    def _determine_drive_file_handle(self, local_fh: str) -> str:
        """
        Creates file handle on the destination SFTP server.

        Args:
            local_fh:                path to a local file

        Returns:
            drive_fh
        """
        file_name = os.path.basename(local_fh)
        return f"{self.dst_dir}/{file_name}"

    def _sftp(self, host, port, user, password):
        logging.debug(f"Opening a secure channel to {host}.")
        if port:
            transport = Transport((host, int(port)))
        else:
            transport = Transport((host))
        transport.connect(None, user, password)
        sftp = SFTPClient.from_transport(transport)
        logging.debug("Successfully connected to the SFTP.")
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
            logging.debug("Connection to the SFTP closed.")
