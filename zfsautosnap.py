#!/usr/bin/env python
import logging
import sys
from zfs import *
from zfs.snapshot import RollingSnapshotter


logging.basicConfig(level=logging.DEBUG)
# ---------------- MAIN ---------------
if __name__ == "__main__":
    snapper=RollingSnapshotter('daily', 30)
    try:
        snapper.take_snapshot('//')
    except ZfsDatasetExistsError as e:
        logging.critical(e)
