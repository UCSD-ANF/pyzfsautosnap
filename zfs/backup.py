import logging
import paramiko
from snapshot import PREFIX, USERPROP_NAME, get_userprop_datasets
from util import get_pool_from_fsname, get_pool_guid, SSHZfsCommandRunner

class Backup(object):
    def __init__(self, label, prefix=PREFIX, userprop_name=USERPROP_NAME ):
        self.label         = label
        self.prefix        = prefix
        self.userprop_name = userprop_name

    def take_backup(self, fsnames,snapchildren=False):
        """backup the requested fsnames

        This is a skeleton method that should be overriden by a child class
        :param fsnames: the filesystem or filesystems to back up
        :type fsnames: str or list
        :param bool snapchildren: snapshot the child filesystems
        :raises NotImplementedError: this method MUST be overridden by sub an
        implementing subclass
        """
        raise NotImplementedError

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
        self.runner=SSHZfsCommandRunner(self.ssh)
        print 'self.label is :'
        print self.label
        print '\n'
        print 'self.userprop_name is '+self.userprop_name+'\n'

    def take_backup(self, filesystems, snapchildren=False):
        """Back up a filesystem using the mbuffered SSH method

        This is a continual incremental setup. A full is only taken if the
        receiving system doesn't contain any snapshots that match our
        incremental window

        See :py:method:`Backup.take_backup` for details on the expected
        parameters.
        """

        if isinstance(filesystems, basestring) and filesystems == '//':
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

        if isinstance(filesystems, basestring):
            filesystems = [ filesystems ]

        for fs in filesystems:
            logging.info("Looking for %s snapsnots of %s" % (
                "recursive" if snap_children else "non-recursive",
                fs))

            # Get the current snapshots of the local fs
            local_snaps=[ s[0] for s in zfs.util.zfs_list(fs,
                                                          types=['snapshot'],
                                                          depth=1,
                                                          properties=['name'])
                        ]

            # Don't process this filesystem if it doesn't have any snapshots
            if len(local_snaps) == 0:
                logging.error('The filesystem %s fs does not have any snapshots.')
                continue

            pool = get_pool_from_fsname(fs)
            guid = get_pool_guid(pool)

            remote_base_path   = os.path.join( self.backup_zpool, guid)
            remote_backup_path = os.path.join( remote_base_path, fs )

            # check and create remote dataset
            runner.zfs_create(remote_fs, create_parents=True)

            # Get the current snapshots of the remote_fs
            remote_snaps=[ s[0].lstrip(remote_base_path) for s in
                          runner.zfs_list(remote_fs, types=['snapshot'],
                                          depth=1) ]

            want_remote_snapshot_purge = False
            if len(remote_snaps) == 0:
                backup_type = 'full'
            elif len(local_snaps) == 1:
                backup_type = 'full'
            elif remote_snaps[-1] not in local_snaps:
                backup_type = 'full'
                want_remote_snapshot_purge = True
            else:
                backup_type = 'incremental'
                # We only want the snapshot name itself
                incremental_source = remote_snaps[-1].split('@', maxsplit=1)

            newest_local_snap = local_snaps[-1]

            # Now we're ready to send the backup to the remote system
            send_backup(snapshot=newest_local_snap,
                        incremental_source=incremental_source,
                        remote_backup_path=remote_backup_path)

    def send_backup(self, snapshot, remote_backup_path, incremental_source=None):
        """Send a backup to the remote_backup_path on self.backup_host

        :param incremental_source: optional name of the source snapshot to use for an incremental backup
        :type incremental_source: str or None
        :param snapshot: the source filesystem@snapshot
        """
        if incremental_source:
            logging.info(
                "Sending incremental backup %s %s to %s on remote host %s" % (
                    incremental_source, snapshot, remote_backup_path,
                    self.backup_host )
            )
        else:
            logging.info("Sending full backup %s to %s on remote host %s" %
                         (snapshot, remote_backup_path, self.backup_host))
        pass
