#!/usr/bin/env python
import logging
import sys
from zfs import *
from zfs.snapshot import take_snapshot


logging.basicConfig(level=logging.DEBUG)
# ---------------- MAIN ---------------
if __name__ == "__main__":
    take_snapshot('//')


