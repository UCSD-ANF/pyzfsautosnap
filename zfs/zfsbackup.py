#!/usr/bin/env python
"""Backup ZFS filesystems to a remote host"""

import logging
import sys
from optparse import OptionParser
from zfs import *
from zfs.backup import MbufferedSSHBackup

class App(object):
    """The ZFS backup application

    Usage:

        myapp = App(options)
        app.run()
    """

    def __init__(self, options):
        """Initialize the App

        "options" is an object that has at least the following properties
        defined:
            * label - back up snapshots with this string, e.g. 'hourly' or 'daily'
            * targethost - hostname of remote system
            * targetuser - username on remote system with privileges to run ZFS
            commands
            * targetdataset - the base dataset on the remote system under which
            all backups will be stored. Usually this is a dedicated zpool.

        "options" is implemented as a generic object with properties so that
        the output of an OptionParser can be passed directly to the app.
        """
        self.options=options
        if options.verbose:
            level=logging.DEBUG
        else:
            level=logging.INFO

        logging.basicConfig(level=level)

    def run(self):
        """Run this application

        Returns: a result code suitable for passing to exit()
        """

        ret = 0

        backerupper=MbufferedSSHBackup(
            label=self.options.label,
            backup_host=self.options.targethost,
            backup_dataset=self.options.targetdataset,
            backup_user=self.options.targetuser)
        try:
            backerupper.take_backup('//')
        except ZfsDatasetExistsError as e:
            logging.critical(e)
            ret=1

        return ret

def main(args=None):
    """Main function for zfsbackup

    This function parses and validates command line arguments, constructs
    an options object, and instanciates an instance of App.

    It is as simple to use as:
        exit(main())

    However, the args parameter can be defined in order to construct your own
    command line. This is useful for testing.
    """

    if args is None:
        args = sys.argv

    op = OptionParser(usage='usage: %prog [options] label targethost targetusername targetdataset')
    op.add_option('-v', '--verbose', dest='verbose', action='store_true')
    (options,args) = op.parse_args(args[1:])
    if len(args) != 4:
        op.error('Not enough arguments provided')
    (options.label,options.targethost,options.targetuser,
     options.targetdataset)=args
    if not options.label:
        op.error('label not provided')
    if not options.targethost:
        op.error('target hostname not provided')
    if not options.targetuser:
        op.error('target username not provided')
    if not options.targetdataset:
        op.error('target dataset not provided')

    app=App(options)
    return app.run()

# ---------------- MAIN ---------------
if __name__ == "__main__":
    exit(main())

