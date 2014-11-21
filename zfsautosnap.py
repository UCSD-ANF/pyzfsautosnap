#!/usr/bin/env python
import logging
import sys
import subprocess
import csv
import re
import datetime
from zfs import *
from zfs.util import zfs_list, is_syncing, zfs_destroy
from zfs.snapshot import take_snapshot


logging.basicConfig(level=logging.DEBUG)
# ---------------- MAIN ---------------
if __name__ == "__main__":
    take_snapshot('//')


