class ZfsError(Exception):
    """Base OS Level Exception for ZFS problems

    Used for command execution issues with underlying ZFS commands, including
    entities existing or not existing when expected.
    """
    pass

class ZfsUnknownError(ZfsError):
    """An unknown error occurred running a zfs command"""
    pass

class ZfsOSError(OSError):
    """Special subclass of OSError for our class"""
    pass

class ZfsCommandNotFoundError(ZfsError):
    """The zfs command is not available"""
    pass

class ZpoolCommandNotFoundError(ZfsError):
    """The zpool command is not available"""
    pass

class ZfsNoDatasetError(ZfsError):
    """The dataset, filesystem, or snapshot does not exist"""
    pass

class ZfsDatasetExistsError(ZfsError):
    """The dataset, filesystem, or snapshot already exists"""
    pass

class ZfsInvalidPropertyError(ValueError):
    """The specified property does not exist"""
    pass

class ZfsNoPoolError(ZfsError):
    """The zpool does not exist"""
    pass

class ZfsPoolExistsError(ZfsError):
    """The zpool already exists"""
    pass

class ZfsPermissionError(ZfsOSError):
    """The command could not be completed due to a permissions issue.

    This is typically raised due to a lack of permissions for /dev/zfs (Linux) or
    RBAC privileges (Solaris).
    """
    pass

class ZfsArgumentError(ValueError):
    """Zfs-specfic errors for parameters."""
    pass

class ZfsBadFsName(ZfsArgumentError):
    """The filesystem or dataset name is malformed"""
    pass
