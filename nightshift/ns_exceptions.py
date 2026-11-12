"""Exceptions used within NightShift application."""


class DriveError(Exception):
    """Exception raised when SFTP/Drive error is encountered."""


class SierraSearchPlatformError(Exception):
    """Exception raised during sessions with NYPL Platform and BPL Solr."""


class DatabaseInitializationError(Exception):
    """Raised when the database fails to initialize with required schema or data."""
