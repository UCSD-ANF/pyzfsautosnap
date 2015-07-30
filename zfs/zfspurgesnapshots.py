#!/usr/bin/env python
"""Purge old ZFS backups"""

import logging
import sys
from optparse import OptionParser
from zfs import *
from zfs.snapshot import SnapshotPurger, validate_keep

class App(object):
    """The ZFS snapshot purger application

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

        purger=SnapshotPurger(label=self.options.label,
                               keep=self.options.keep,
                               baseds=self.options.dataset)
        try:
            purger.run()
        except ZfsDatasetExistsError as e:
            logging.critical(e)
            ret=1

        return ret

def main(args=None):
    """Main function for zfspurgebackups

    This function parses and validates command line arguments, constructs
    an options object, and instanciates an instance of App.

    It is as simple to use as:
        exit(main())

    However, the args parameter can be defined in order to construct your own
    command line. This is useful for testing.
    """

    if args is None:
        args = sys.argv

    op = OptionParser(usage='usage: %prog [options] basedataset label keep')
    op.add_option('-v', '--verbose', dest='verbose', action='store_true')
    (options,args) = op.parse_args(args[1:])
    if len(args) != 3:
        op.error('wrong number of arguments provided')
    (options.dataset,options.label,options.keep)=args
    if not options.dataset:
        op.error('base dataset not provided')
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

