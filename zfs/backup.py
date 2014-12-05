import logging
import os
import sys
import datetime
from snapshot import PREFIX, USERPROP_NAME, get_userpropdatasets
from util import get_pool_from_fsname, get_pool_guid

class Backup():
    def __init__(self, label, prefix=PREFIX, userprop_name=USERPROP_NAME ):
        self.label         = label
        self.prefix        = prefix
        self.userprop_name = userprop_name

    def take_backup(self, fsnames,snapchildren=False):
        """backup the requested fsnames

        Given a list of fsnames, backup the filesystem

        This is a skeleton method that should be overriden by a child class
        """
        pass

class MbufferedSSHBackup(Backup):

    def __init__(self, label, backup_host, backup_zpool, backup_user,
                 prefix=PREFIX, userprop_name=USERPROP_NAME):
        super(MbufferedSSHBackup, self).__init__(label, prefix, userprop_name)
        self.backup_host  = backup_host
        self.backup_zpool = backup_zpool
        self.backup_user  = backup_user

    def take_backup(self, fsnames, snapchildren=False):
        """Back up a filesystem using the mbuffered SSH method

        This is a continual incremental setup. A full is only taken if the
        receiving system doesn't contain any snapshots that match our
        incremental window
        """

        if isisntance(fsnames, basestring) and fsnames == '//':
            single_list,recursive_list = get_userprop_datasets(
                label = self.label, userprop_name=self.userprop_name)

            logging.info("Taking non-recursive backups of: %s" %\
                         ', '.join(single_list))
            single_state = self.take_backup(single_list, snap_children = False)

            logging.info("Taking recursizve backups of: %s" %\
                         ', '.join(recurive_list))
            recursive_state = self.take_backup(
                recursive_list, snap_children = True)

            return

        if isinstance(fsnames, basestring):
            fsnames = [ fsnames ]

        for fs in fsnames:
            logging.info("Looking for %s snapsnots of %s" % (
                "recursive" if snap_children else "non-recursive",
                fs))

            pool = get_pool_from_fsname(fs)
            guid = get_pool_guid(pool)

            remote_backup_path="%s/%s/%s" % ( self.backup_zpool, guid, fs )

            # check and create remote datasets


