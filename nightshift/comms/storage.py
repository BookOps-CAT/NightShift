# -*- coding: utf-8 -*-

"""
This module handles communication with network drive accessible via SFTP where Sierra
dumps daily files for processing and where CAT staff can access produces MARC files.
"""
from io import BytesIO
import logging
import os
from typing import Optional


from paramiko.transport import Transport
from paramiko.sftp_client import SFTPClient
from paramiko.ssh_exception import SSHException

from ..ns_exceptions import DriveError


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
        os.getenv("SFTP_NS_SRC"),
        os.getenv("SFTP_NS_DST"),
        os.getenv("SFTP_PORT"),
    )


class Drive:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        src_dir: str,
        dst_dir: str,
        port: Optional[str] = None,
    ) -> None:
        """
        Opens a secure communication channel via SFTP to a networked drive

        Args:
            host:                       SFTP host
            port:                       SFTP port (optional)
            user:                       SFTP user name
            password:                   SFTP user password
            home_directory:             NighShift directory on the drive
        """
        sock = self._sock(host, port)
        self.sftp = self._sftp(sock, user, password)
        self.src_dir = src_dir
        self.dst_dir = dst_dir

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        self.close()

    def check_file_exists(self, remote_file_path: str) -> bool:
        """
        Checks if file exists in 'load' directory of the SFTP drive

        Args:
            remote_file_path:           file path to check on the SFTP

        Returns:
            bool

        Raises:
            DriveError
        """
        if self.sftp:
            try:
                self.sftp.stat(remote_file_path)
                logger.debug(f"'{remote_file_path}' found on SFTP.")
                return True
            except FileNotFoundError:
                logger.debug(f"'{remote_file_path}' not found on SFTP.")
                return False
        else:
            logger.error("Attempted operation on a closed SFTP session.")
            raise DriveError

    def fetch_file(self, src_fh: str) -> Optional[BytesIO]:
        """
        Retrieves file of the given path

        Args:
            src_fh:                     file handle of file in the
                                        'sierra_dump' directory

        Returns:
            bytes stream

        Raises:
            DriveError
        """
        src_file_path = self._construct_src_file_path(src_fh)
        logger.info(f"Fetching {src_file_path} file from the SFTP.")
        if self.sftp:
            try:
                with self.sftp.file(src_file_path, mode="r") as file:
                    file_size = file.stat().st_size
                    file.prefetch(file_size)
                    return BytesIO(file.read(file_size))
            except IOError as exc:
                logger.error(
                    f"Unable to fetch file {src_file_path} from the SFTP. {exc}."
                )
                raise DriveError
        else:
            logger.error("Attempted operation on a closed SFTP session.")
            raise DriveError

    def list_src_directory(self) -> list[str]:
        """
        Returns a list of files found in SFTP/Drive sierra_dumps directory
        """
        if self.sftp is None:
            logger.error("Attempted an operation on a closed SFTP session.")
            raise DriveError
        else:
            try:
                return self.sftp.listdir(path=self.src_dir)
            except IOError as exc:
                logger.error(f"Unable to reach {self.src_dir} on the SFTP. {exc}")
                raise DriveError

    def output_file(self, local_file_path: str, remote_file_name_base: str) -> str:
        """
        Appends stream to a file in SFTP/Drive load directory

        Args:
            local_file_path:            path to a local file to be transfered to SFTP
            remote_file_name_base:      file name base for the file on SFTP

        Returns:
            remote file handle

        Raises:
            DriveError
        """
        if self.sftp:
            try:

                remote_file_path = self._construct_dst_file_path(remote_file_name_base)
                self.sftp.put(local_file_path, remote_file_path)
                logger.info(f"Successfully created {remote_file_path} on the SFTP.")
                return os.path.basename(remote_file_path)

            except IOError as exc:
                logger.error(
                    f"IOError. Unable to output {local_file_path} to "
                    f"{remote_file_path} on the SFTP. {exc}"
                )
                raise DriveError
        else:
            logger.error(
                f"SFTP session closed. Unable to output {local_file_path} "
                f"to {remote_file_name_base} on the drive."
            )
            raise DriveError

    def close(self):
        """
        Closes SFTP session and underlying channel.
        """
        if self.sftp:
            self.sftp.close()
            logging.debug("SFTP client session closed.")
        if (
            self.transport
        ):  # necessary since paramiko keeps open threads hanging occasionally
            self.transport.close()
            logging.debug("Secure channels closed.")

    def _construct_dst_handle(self, base_name: str, n: int) -> str:
        """
        Creates full file handle with given numeration and MARC21 extension

        Args:
            base_name:              base name of the file
            n:                      sequence number to be added to the file name

        Returns:
            file handle
        """
        return f"{base_name}-{str(n).zfill(2)}.mrc"

    def _construct_dst_file_path(self, file_base_name: str) -> str:
        """
        Creates a file path to the destination folder (load) on the SFTP server.
        If such a file already exists, appends a consecutive number to
        avoid overwriting file on the SFTP.

        Args:
            file_base_name:         base name of the file

        Returns:
            file_path
        """
        n = 1
        remote_handle = self._construct_dst_handle(file_base_name, n)
        remote_file_path = f"{self.dst_dir}/{remote_handle}"
        while self.check_file_exists(remote_file_path):
            logger.debug(
                f"'{remote_file_path}' is already taken. "
                "Checking next consecutive number."
            )
            n += 1
            remote_handle = self._construct_dst_handle(file_base_name, n)
            remote_file_path = f"{self.dst_dir}/{remote_handle}"

        logger.debug(f"'{remote_file_path}' is available.")
        return remote_file_path

    def _construct_src_file_path(self, src_fh: str) -> str:
        """
        Creates a file path to the source folder (sierra_dumps) on the SFTP server.

        Args:
            src_fh:                 sierra export file handle

        Returns:
            file_path
        """
        return f"{self.src_dir}/{src_fh}"

    def _sftp(self, sock: str, user: str, password: str) -> Optional[SFTPClient]:
        """
        Establishes a secure channel to SFTP server and returns a client/session for
        communication.

        Args:
            host:                   SFTP server host
            port:                   SFTP server port (optional)
            user:                   SFTP account user
            password:               SFTP account password

        Returns:
            `paramiko.sftp_client.SFTPClient` instance
        """
        logging.debug(f"Opening a secure channel to {sock}.")
        try:
            self.transport = Transport(sock)
            self.transport.connect(None, user, password)
            sftp = SFTPClient.from_transport(self.transport)
        except SSHException as exc:
            logger.critical(
                f"Unable to establish a secure channel and open SFTP session. {exc}"
            )
            raise DriveError
        else:
            logging.debug("Successfully connected to the SFTP.")
            return sftp

    def _sock(self, host: str, port: Optional[str] = None) -> str:
        if port:
            return f"{host}:{port}"
        else:
            return host
