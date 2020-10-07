class NightShiftError(Exception):
    """Base class for exceptions in this module."""

    pass


class SierraExportReaderError(NightShiftError):
    """
    Exception raised when Sierra export file reader encounters a problem
    """

    pass
