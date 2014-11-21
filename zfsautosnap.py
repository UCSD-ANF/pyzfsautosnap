#!/usr/bin/env python
import logging
import sys
from zfs import *
from zfs.snapshot import AutoSnapshotter


logging.basicConfig(level=logging.DEBUG)
# ---------------- MAIN ---------------
if __name__ == "__main__":
    snapper=AutoSnapshotter('daily', 30)
    try:
        snapper.take_snapshot('//')
    except ZfsDatasetExistsError as e:
        logging.critical(e)
