class ZfsError(Exception):
    pass

class ZfsNoDatasetError(ZfsError):
    pass

class ZfsNoPoolError(ZfsError): pass

class ZfsPermissionError(ZfsError):
    pass
