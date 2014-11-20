class ZfsOSError(OSError):
    """Base OS Level Exception for ZFS problems

    Used for command execution issues with underlying ZFS commands, including
    entities existing or not existing when expected.
    """
    pass

class ZfsNoDatasetError(ZfsOSError):
    """The dataset, filesystem, or snapshot does not exist"""
    pass

class ZfsDatasetExistsError(ZfsOSError):
    """The dataset, filesystem, or snapshot already exists"""
    pass

class ZfsNoPoolError(ZfsOSError):
    """The zpool does not exist"""
    pass

class ZfsPoolExistsError(ZfsOSError):
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
