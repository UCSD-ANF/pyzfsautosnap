#!/usr/bin/env python
"""Script to automatically snapshot ZFS filesystems"""

import logging
import sys
from optparse import OptionParser
from zfs import *
from zfs.snapshot import RollingSnapshotter

class App(object):
    """The ZFS automatic snapshotter application"""

    def __init__(self, options):
        self.options=options
        """Initialize the App"""
        if options.verbose:
            level=logging.DEBUG
        else:
            level=logging.INFO

        logging.basicConfig(level=level)

    def run(self):
        """Run this application"""

        ret = 0

        snapper=RollingSnapshotter(self.options.label, self.options.keep)
        try:
            snapper.take_snapshot('//')
        except ZfsDatasetExistsError as e:
            logging.critical(e)
            ret=1

        return ret

def main(args=None):
    if args is None:
        args = sys.argv

    op = OptionParser(usage='usage: %prog [options] label keep')
    op.add_option('-v', '--verbose', dest='verbose', action='store_true')
    (options,args) = op.parse_args(args[1:])
    if len(args) != 2:
        op.error('Not enough arguments provided')
    (options.label,options.keep)=args
    if not options.label:
        op.error('label not provided')
    if not options.keep:
        op.error('number of snapshots to keep not provided')
    if options.keep != 'all':
        try:
            options.keep=int(options.keep)
        except ValueError:
            op.error('Keep must be either a number or "all"')

    app=App(options)
    return app.run()

# ---------------- MAIN ---------------
if __name__ == "__main__":
    exit(main())

