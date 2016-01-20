import logging
import paramiko
import snapshot
import util
import os

class Backup(object):
    def __init__(self, label, prefix=snapshot.PREFIX,
                 userprop_name=snapshot.USERPROP_NAME ):
        self.label         = label
        self.prefix        = prefix
        self.userprop_name = userprop_name

    def take_backup(self, fsnames, snap_children=False):
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
    def __init__(self, backup_host, backup_dataset, backup_user, *args, **kwargs):
        super(MbufferedSSHBackup, self).__init__(*args,**kwargs)
        self.backup_host  = backup_host
        self.backup_dataset = backup_dataset
        self.backup_user  = backup_user
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=self.backup_host,
                         username=self.backup_user,
                         look_for_keys=True)
        self.runner=util.SSHZfsCommandRunner(self.ssh, command_prefix=util.SUDO_CMD)

    def __del__(self):
        if self.runner:
            self.runner=None
        if self.ssh:
            self.ssh.close()
            self.ssh=None

    def take_backup(self, filesystems, snap_children=False):
        """Back up a filesystem using the mbuffered SSH method

        This is a continual incremental setup. A full is only taken if the
        receiving system doesn't contain any snapshots that match our
        incremental window

        See :py:method:`Backup.take_backup` for details on the expected
        parameters.
        """

        if isinstance(filesystems, basestring) and filesystems == '//':
            single_list,recursive_list = snapshot.get_userprop_datasets(
                label = self.label, userprop_name=self.userprop_name)

            logging.info("Taking non-recursive backups of: %s" %\
                         ', '.join(single_list))
            single_state = self.take_backup(single_list, snap_children = False)

            logging.info("Taking recursive backups of: %s" %\
                         ', '.join(recursive_list))
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
            local_snaps=[ s[0] for s in util.zfs_list(fs,
                                                          types=['snapshot'],
                                                          depth=1,
                                                          properties=['name'])
                        ]

            # Don't process this filesystem if it doesn't have any snapshots
            if len(local_snaps) == 0:
                logging.error('The filesystem %s fs does not have any snapshots.')
                continue

            pool = util.get_pool_from_fsname(fs)
            guid = util.get_pool_guid(pool)

            remote_base_path   = os.path.join( self.backup_dataset, guid)
            remote_backup_path = os.path.join( remote_base_path, fs )

            # check and create remote dataset
            self.runner.zfs_create(remote_backup_path, create_parents=True)

            # Get the current snapshots of the remote_fs
            remote_snaps=[ s[0].lstrip(remote_base_path) for s in
                          self.runner.zfs_list(remote_backup_path, types=['snapshot'],
                                          depth=1) ]

            want_remote_snapshot_purge = False
            incremental_source = None
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
            self.send_backup(snapshot=newest_local_snap,
                        incremental_source=incremental_source,
                        remote_backup_path=remote_backup_path)

    def send_backup(self, snapshot, remote_backup_path, incremental_source=None):
        """Send a backup to the remote_backup_path on self.backup_host

        ZFS backups are performed using the `zfs send` command, which requires
        a single snapshot (for full backups), or two snapshots (for incremental
        snapshots).

        Behavior of this function differs if the optional `incremental_source`
        parameter is specified. If `incremental_source` is None, a full backup
        is sent using the value of `snapshot` as the source of the backup.
        Otherwise, `incremental_source` is assumed to be the older of the pair
        of snapshots used to generate the incremental backup.

        :param incremental_source: optional name of the source snapshot to use
        for an incremental backup. If specified, this can either be a bare
        snapshot name like "@snap" or "filesystem@snap". If the latter format
        is used, the filesystem must be the same filesystem as the one used in
        the `snapshot` parameter.
        :type incremental_source: str or None
        :param str snapshot: the primary source filesystem@snapshot.
        """
        # First, validate our params
        if '@' not in snapshot:
            raise ValueError('The "snapshot" parameter does not contain a ' +
                             'valid snapshot name ("%s")' % snapshot)

        sfs,ssnap = snapshot.split('@', 2)
        if sfs == '':
            raise ValueError('The "snapshot" parameter does not contain a ' +
                             'valid snapshot name ("%s")' % snapshot)

        if incremental_source:
            if '@' not in incremental_source:
                raise ValueError('The "incremental_source" parameter does ' +
                                 'not contain a valid snapshot name ("%s")' %
                                 incremental_source)
            ifs,isnap = incremental_source.split('@', 2)
            if ifs != '' and ifs != sfs:
                raise ValueError('The filesystem specified in the ' +
                                 '"incremental_source" parameter (%s) does ' +
                                 'not match the filesystem in the "snapshot" ' +
                                 'parameter (%s)' % (ifs,sfs))


        # output a log message
        if incremental_source:
            logging.info(
                "Sending incremental backup %s %s to %s on remote host %s" % (
                    incremental_source, snapshot, remote_backup_path,
                    self.backup_host )
            )
        else:
            logging.info("Sending full backup %s to %s on remote host %s" %
                         (snapshot, remote_backup_path, self.backup_host))

        # TODO: Now actually send the snapshot
        pass
