# -*- coding: utf-8 -*-
from contextlib import nullcontext as does_not_raise
from io import BytesIO
import logging

import pytest

from nightshift.comms.storage import get_credentials, Drive
from nightshift.ns_exceptions import DriveError


@pytest.mark.firewalled
class TestDriveLive:
    def test_list_src_directory(self, env_var):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert sorted(drive.list_src_directory()) == [
                "BPLeres210701-pout.mrc",
                "NYP-eres-pout.20220811020001",
                "NYPeres210701-pout.mrc",
            ]

    def test_fetch_file(self, env_var):
        creds = get_credentials()
        with Drive(*creds) as drive:
            with does_not_raise():
                file = drive.fetch_file("NYPeres210701-pout.mrc")
            assert isinstance(file, BytesIO)

    def test_construct_dst_file_path(self, env_var):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert (
                drive._construct_dst_file_path("foo") == "/NSDROP/TEST/load/foo-01.mrc"
            )

    def test_construct_src_file_path(self, env_var):
        creds = get_credentials()
        with Drive(*creds) as drive:
            assert (
                drive._construct_src_file_path("foo.mrc")
                == "/NSDROP/TEST/sierra_dumps/nightshift/foo.mrc"
            )

    def test_list_src_directory_io_error(self, caplog, env_var):
        """
        this may happen when sierra_dumps/nightshift directory is accidentally deleted
        """
        creds = get_credentials()
        with caplog.at_level(logging.ERROR):
            with Drive(*creds) as drive:
                drive.src_dir = "/NSDROP/TEST/FOO"  # non-existing dir
                with pytest.raises(DriveError):
                    drive.list_src_directory()

        assert "Unable to reach /NSDROP/TEST/FOO on the SFTP" in caplog.text

    def test_output_file_success(self, env_var):
        creds = get_credentials()
        with Drive(*creds) as drive:
            local_fh = "tests/nyp-ebook-sample.mrc"

            remote_handle = drive.output_file(local_fh, "foo")

            assert remote_handle == "foo-01.mrc"
            assert "foo-01.mrc" in drive.sftp.listdir(drive.dst_dir)

            # cleanup
            drive.sftp.remove(drive.dst_dir + "/foo-01.mrc")

    def test_output_file_io_error(self, caplog, env_var, mock_io_error):
        creds = get_credentials()
        with Drive(*creds) as drive:
            with caplog.at_level(logging.ERROR):
                with pytest.raises(DriveError):
                    drive.output_file("tests/nyp-ebook-sample.mrc", "foo")
        assert (
            "IOError. Unable to output tests/nyp-ebook-sample.mrc to /NSDROP/TEST/load/foo-01.mrc on the SFTP."
            in caplog.text
        )
