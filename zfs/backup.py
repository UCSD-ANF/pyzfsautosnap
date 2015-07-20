import logging
import os
import sys
import datetime
import paramiko
from snapshot import PREFIX, USERPROP_NAME, get_userprop_datasets
from util import get_pool_from_fsname, get_pool_guid

class Backup(object):
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
    def __init__(self, backup_host, backup_zpool, backup_user, **kwds):
        super(MbufferedSSHBackup, self).__init__(**kwds)
        self.backup_host  = backup_host
        self.backup_zpool = backup_zpool
        self.backup_user  = backup_user
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=self.backup_host,
                         username=self.backup_user,
                         look_for_keys=False)
        print 'self.label is :'
        print self.label
        print '\n'
        print 'self.userprop_name is '+self.userprop_name+'\n'

    def take_backup(self, fsnames, snapchildren=False):
        """Back up a filesystem using the mbuffered SSH method

        This is a continual incremental setup. A full is only taken if the
        receiving system doesn't contain any snapshots that match our
        incremental window
        """

        if isinstance(fsnames, basestring) and fsnames == '//':
            single_list,recursive_list = get_userprop_datasets(
                label = self.label, userprop_name=self.userprop_name)

            logging.info("Taking non-recursive backups of: %s" %\
                         ', '.join(single_list))
            single_state = self.take_backup(single_list, snap_children = False)

            logging.info("Taking recursive backups of: %s" %\
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


