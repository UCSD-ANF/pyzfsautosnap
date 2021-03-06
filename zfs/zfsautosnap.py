#!/usr/bin/env python
"""Automatically snapshot ZFS filesystems"""

import logging
import sys
from optparse import OptionParser
from zfs import *
from zfs.snapshot import RollingSnapshotter, validate_keep

class App(object):
    """The ZFS automatic snapshotter application

    Usage:

        myapp = App(options)
        app.run()
    """

    def __init__(self, options):
        """Initialize the App

        "options" is an object that has at least the following properties
        defined:
            label - name snapshots with this string, e.g. 'hourly' or 'daily'
            keep  - number of total snapshots to retain, including the one we
                    have just created.

        Additionally, if options.dataset is defined, it will be used instead
        of the default value of '//' (which means check the user properties for
        which filesystems to snapshot).

        "options" is implemented as a generic object with properties so that
        the output of an OptionParser can be passed directly to the app.
        """
        self.options=options
        if options.verbose:
            level=logging.DEBUG
        else:
            level=logging.INFO

        logging.basicConfig(level=level)

        if not hasattr(self.options, 'dataset'):
            self.options.dataset='//'

    def run(self):
        """Run this application

        Returns: a result code suitable for passing to exit()
        """

        ret = 0

        snapper=RollingSnapshotter(self.options.label, self.options.keep)
        try:
            snapper.take_snapshot(self.options.dataset)
        except ZfsDatasetExistsError as e:
            logging.critical(e)
            ret=1

        return ret

def main(args=None):
    """Main function for zfsautosnap

    This function parses and validates command line arguments, constructs
    an options object, and instanciates an instance of App.

    It is as simple to use as:
        exit(main())

    However, the args parameter can be defined in order to construct your own
    command line. This is useful for testing.
    """

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
    try:
        options.keep=validate_keep(options.keep)
    except ValueError:
        op.error('Keep must be either a number or "all"')

    app=App(options)
    return app.run()

# ---------------- MAIN ---------------
if __name__ == "__main__":
    exit(main())

