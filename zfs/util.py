import logging
import subprocess
import re
import csv
import errno
import socket
from . import *
from StringIO import StringIO

#logging.basicConfig(level=logging.DEBUG)

ZFS_ENV={'LC_ALL': 'C', 'PATH': '/usr/sbin:/sbin:/usr/bin:/bin'}
ZFS_ERROR_STRINGS={
    'permerr': "Unable to open /dev/zfs: Permission denied.\n",
    'nosnaplinux': "could not find any snapshots to destroy; check snapshot names.\n",
}
ZFS_CMDS=['zpool', 'zfs']
_FS_COMP='[a-zA-Z0-9\-_.]+'

def get_pool_from_fsname(fsname):
    """Return the ZFS pool containing fsname

    Given a ZFS filesystem, return which pool contains it. This is usually
    the part of the filesystem before the first forward slash.

    :param str fsname: name of a ZFS filesystem
    :raises ZfsBadFsName: if the fsname is malformed
    :return: the name of the zpool containing fsname
    :rtype: str
    """
    _validate_fsname(fsname)
    pool = fsname.split('/')[0]
    return pool

def get_pool_guid(pool):
    """Get the GUID of a zpool

    :param str pool: the name of the Zpool
    :return: the GUID of the pool
    :rtype: str
    :raises ZfsBadPoolName: if the pool name is malformed
    :raises ZfsNoPoolError: if the pool does not exist
    """
    _validate_poolname(pool)
    r = zpool_list(pools=pool, properties=['name','guid'])
    s = r.next()
    return s[1]

class ZfsCommandRunner(object):
    """Base class for running a Zfs command, either locally or on a remote
    system
    """

    def __init__(self, command_prefix=None):
        """Initialize a new ZfsCommandRunner

        :param command_prefix: Optional command to prepend to any call to a zfs
        or zpool command. Useful for injecting calls to `sudo` or `pfexec`
        :type command_prefix: str, list, or None
        """
        self.command_prefix=command_prefix

    def run_cmd(self, cmd, args, errorclass=None):
        """Base method to run a command ZFS command.
        This must be overridden by subclasses

        The idea is that this command sets up whatever environment is necessary
        to run a zfs or zpool command, including setting up the environment,
        running sudo, etc

        It should return a tuple with three components:
            * out - the output of the command as a string
            * err - the stderr of the command as a string
            * rc  - the result code of the command as a number

        A subclass implementation should use ZfsCommandRunner.process_cmd_args
        to validate the cmd and args parameters.

        The instance property `command_prefix` is useful for setting up a wrapper command with `cmd`

        :param str cmd: The zfs command to run
        :param args: Arguments to the zfs command
        :param errorclass: the exeception that should be raised if the command
        is not found
        :type errorclass: Execption or None
        :raises NotImplementedError: this method must be overridden
        :return: A tuple containing the output, error output, and result code
        :rtype: tuple
        """
        raise NotImplementedError

    def run_zpool(self, zpoolargs):
        """run zpool with the args specified

        :param list zpoolargs: The arguments to the zpool command
        :raises ZpoolCommandNotFoundError: if it can't find the zpool command
        :return: A tuple containing the outpet, error output, and result code
        :rtype: tuple
        """
        return self.run_cmd('zpool', zpoolargs, ZpoolCommandNotFoundError)

    def run_zfs(self, zfsargs):
        """run zfs with the args specified

        :param list zpoolargs: The arguments to the zpool command
        :raises ZfsCommandNotFoundError: if it can't find the zfs command
        :return: A tuple containing the outpet, error output, and result code
        :rtype: tuple
        """
        cmd='zfs'
        out,err,rc = self.run_cmd(cmd, zfsargs, ZfsCommandNotFoundError)
        if rc > 0:
            _check_perm_err(err)

        return out,err,rc

    def zpool_list(self, pools=None, properties=None):
        """List the specified properties about a pool or pools

        Run the zpool list command, optionally retrieving only the specified
        properties. If no pool is provided, the zpool list default behavior of
        listing all zpools is used.

        If no properties are provided, the zpool default columns are used. This
        varies by platform and implementation, but typically looks like: `name`,
        `size`, `alloc`, `free`, `cap`, `dedup`, `health`, `altroot`

        Note that support for listing the individual vdev properties under the
        zpool (the -v option to zpool list) is not supported at this time.

        :param pools: name of pool or pools to check
        :type pools: list or str
        :param list properties: the properties to retrieve
        :return: `iterable` of `list`s with each requested property occupying
        one field of the list. This is performed under the hood by relying on
        the -H option to output a tab-delimited field of properties, and
        calling the csv.reader to read them. This occurs even if only out
        output field was requested, so be sure to expect something like ['foo']
        instead of 'foo'
        :rtype: iterable
        """
        args=['list', '-H' ]
        if properties is not None:
            if isinstance(properties, basestring):
                cmd_columns=properties
            else:
                cmd_columns=','.join(properties)
            args.append('-o')
            args.append(cmd_columns)

        if pools is not None:
            if isinstance(pools,basestring):
                args.append(pools)
            else:
                args.extend(pools)

        out,err,rc = self.run_zpool(args)

        if rc > 0:
            _check_perm_err(err)
            _check_prop_err(err)
            if "no such pool" in err:
                raise ZfsNoPoolError(err)
            else:
                raise ZfsUnknownError(err)

        r=csv.reader(StringIO(out), delimiter="\t")

        return r


    def zfs_list(self, datasets=None, types=['filesystem','volume'],
                 properties=None, sort=None, sortorder='asc', recursive=False,
                 depth=None):
        """List the specified properties about a ZFS dataset or datasets

        Run the zfs list command, optionally retrieving only the specified
        properties. If no dataset is provided, the zfs list default behavior of
        recursively listing all filesystems and snapshots is used.

        :param datasets: the dataset or datasets to check
        :type datasets: str, list, or None
        :param list types: dataset types to display, valid choices are
        `filesystem`, `snapshot`, `snap`, `volume`, `bookmark`, or `all`
        :param properties:
        :param sort: A property for sorting the output by column based on the
        value of the property. The `sortorder` param is used to determine which
        order to use when sorting.
        :type sort: str or None
        :param str sortorder: either "asc" or "desc" for ascending or
        descending order
        :param bool recursive: if true, list details about the children of the
        `datasets`
        :param depth: if specified, limits the recursion level of `dataset`
        listing. A value of 1 limits the listing to just the specified
        `datasets` and their immediate children. Note: this parameter may not
        be supported on all platforms.
        :depth type: int or None
        :return: an `iterable` of `list`s with each of the specified `field`
        entries occupying one field of the list. This is performed under the
        hood by relying on the `zfs list -H` option to output a tab-delimited
        field of properties, and calling `csv.reader` to read them. This occurs
        even if only one output field was requested, so be sure to expect
        something like ['foo'] instead of 'foo'
        :rtype: iter
        :raise TypeError: if a parameter is of the wrong type
        :raise ValueError: if a parameter has an unexpected value
        :raise ZfsNoDatasetError: if the remote dataset doesn't exist
        """
        SORTORDERS={'asc': '-s',
                    'desc': '-S',
                   }
        args=[ 'list', '-H' ]

        if not isinstance(recursive, bool):
            raise TypeError('recursive must be a boolean')

        # Can't specify -d and -r at the same time, as -d implies -r
        if depth != None:
            args.append('-d')
            args.append(str(depth))
        else:
            if recursive == True:
                args.append('-r')

        if types is not None:
            cmd_types=','.join(types)
            args.append('-t')
            args.append(cmd_types)

        if sort is not None:
            if sortorder not in SORTORDERS.keys():
                raise ValueError('sort order must be one of: %s' %
                                 SORTORDERS.keys())
            args.append(SORTORDERS[sortorder])
            args.append(sort)

        if properties is not None:
            cmd_columns=','.join(properties)
            args.append('-o')
            args.append(cmd_columns)

        if datasets is not None:
            if isinstance(datasets, basestring):
                args.append(datasets)
            else:
                args.extend(datasets)

        out,err,rc = self.run_zfs(args)

        r=csv.reader(StringIO(out), delimiter="\t")
        if rc > 0:
            _check_prop_err(err)
            if "dataset does not exist" in err:
                raise ZfsNoDatasetError(err)
            else:
                raise ZfsUnknownError(err)
        return r

    def zfs_destroy(self, datasets, recursive=False):
        """Destroy datasets or snapshots

        Calls the zfs destroy command, optionally recursively removing
        snapshots of child filesystems with the same snapshot name.

        Note that the underlying `zfs destroy` command can only handle a single
        snapshot at a time.
        :param datasets: the name of the dataset(s) to remove
        :type datasets: list or str
        :param bool recursive: recursively remove snapshots of child
        filesystems
        :raises TypeError: if the dataset type is wrong
        :raises ValueError: if the dataset name is empty or invalid
        :raises ZfsNoDatasetError: if the dataset does not exist
        :raises ZfsPermissionError: if we couldn't execute the command
        :raises ZfsUnknownError: if an unknown ZFS-related error occured
        running the command
        """
        args = ['destroy']

        if recursive:
            args.append('-r')

        if datasets == '':
            raise ValueError('dataset cannot be empty')
        if isinstance(datasets, basestring):
            datasets = [datasets]
        args.extend(datasets)

        out,err,rc=self.run_zfs(args)

        if rc > 0:
            _check_perm_err(err)
            if (err == ZFS_ERROR_STRINGS['nosnaplinux']
                or 'dataset does not exist' in err):
                raise ZfsNoDatasetError(errno.ENOENT, err, ','.join(datasets))
            else:
                raise ZfsUnknownError(err)

        pass

    def zfs_create(self, filesystem, props=None, create_parents=True):
        """Creates a new ZFS file system.

        The file system is automatically mounted according to the mountpoint
        property inherited from the parent.

        :param str filesystem: the name of the dataset to create

        :param bool create_parents: Creates all the non-existing parent
        datasets. Datasets created in this manner are automatically mounted
        according to the mountpoint  property  inherited from their parent. Any
        `properties` are ignored. If the target filesystem already exists, the
        operation completes successfully.

        :param props: key/value dictionary of properties and values. Sets
        the specified properties as if the command zfs set property=value was
        invoked at the same time the dataset was created. Any editable ZFS
        property can also be set at creation time. An error results if the same
        property is specified multiple times.
        :value props: dict or None

        :raises ZfsDatasetExistsError: if the dataset already exists and
        `create_parents` is false

        :raises ZfsNoDatasetError: if the parent dataset to `filesystem` does
        not exist and `create_parents` is false
        """
        args = ['filesys']

        if create_parents:
            args.append('-p')

        for k,v in props:
            args.append('-o')
            args.append(':'.join(k,v))

        args.append(filesys)

        out,err,rc=self.run_zfs(args)

        if rc > 0:
            if 'dataset already exists' in err:
                raise ZfsDatasetExistsError(errno.EEXIST, err, fullsnapname)
            elif 'dataset does not exist' in err:
                raise ZfsNoDatasetError(errno.ENOENT, err, dataset)
            elif 'permission denied' in err:
                raise ZfsPermissionError(errno.EPERM, err, dataset)
            else:
                raise ZfsUnknownError(err)
        pass

    def zfs_snapshot(self, dataset, snapname, recursive=False):
        """Snapshot a ZFS filesystem

        :param str dataset: the name of the dataset to snapshot
        :param str snapname: the name to use for the snapshot
        :param bool recursive: if true, recursively snapshot child filesystems
        using the same `snapname`
        :raises TypeError: if an argument doesn't match it's expected type
        :raises ZfsDatasetExistsError: if the snapshot already exists
        :raises ZfsNoDatasetError: if the `dataset` does not exist
        :raises ZfsPermissionError: if we couldn't run the command
        :raises ZfsUnknownError: if an undetermined Zfs-related error occurred
        """
        args = ['snapshot']

        if recursive==True:
            args.append('-r')

        if not isinstance(dataset, basestring):
            raise TypeError(
                'not sure how to handle filesys with type %s' % type(filesys))
        if not isinstance(snapname, basestring):
            raise TypeError(
                'not sure how to handle snapname with type %s' % type(snapname))

        fullsnapname="%s@%s" % (dataset, snapname)
        _validate_snapname(fullsnapname)
        args.append(fullsnapname)

        out,err,rc=self.run_zfs(args)

        if rc > 0:
            if 'dataset already exists' in err:
                raise ZfsDatasetExistsError(errno.EEXIST, err, fullsnapname)
            elif 'dataset does not exist' in err:
                raise ZfsNoDatasetError(errno.ENOENT, err, dataset)
            elif 'permission denied' in err:
                raise ZfsPermissionError(errno.EPERM, err, dataset)
            else:
                raise ZfsUnknownError(err)
        pass

    def is_syncing(self, pool):
        """Check if the named pool is currently scrubbing or resilvering

        :param str pool: pool to check
        :return: true if pool is syncing, false otherwise
        :rtype: bool
        """
        s = self.zpool_status(pool)
        if " in progress" in s:
            return True
        return False

    def zpool_status(self, pools=None):
        """Call zpool status to check the status of a pool or pools

        :param pools: name of pool or pools to check
        :type pools: list, str, or None
        :return: the raw text output of the zpool status command.  It
        might be worth the time to attempt to parse the big blob of text into the
        various fields in the status statement, and return them as a list of
        ZpoolStatus objects.
        :rtype: str
        """
        args=[ 'zpool', 'status', '-v' ]

        if pools is not None:
            if isinstance(pools,basestring):
                args.append(pools)
            else:
                args.extend(pools)

        out,err,rc=self.run_zpool(args)

        if rc > 0:
            _check_perm_err(err)
            if "no such pool" in err:
                raise ZfsNoPoolError(err)
            else:
                raise ZfsUnknownError(err)

        return out

    def process_cmd_args(self, cmd, args):
        """Process the cmd and args, returning a cmdargs array

        Utility function for implementing a run method in a subclass. If cmd is
        also duplicated in args, it's stripped out.

        :param str cmd: The zfs command to run, either zfs or zpool
        :param list args: Arguments to the zfs command, in list form.
        :return: the final command and it's arguments, suitable for execution
        :rtype: list
        :raises TypeError: if args is not a list
        :raises TypeError: if cmd is not a string
        :raises ValueError: if cmd is not one of the expected Zfs commands
        """
        if isinstance(args, basestring):
            raise TypeError("args must be a list.")

        if cmd not in ZFS_CMDS:
            raise ValueError('cmd must be one of: %s' % ZFS_CMDS)

        if self.command_prefix:
            if isinstance(basestring, self.command_prefix):
                cmd.append(self.command_prefix)
            else:
                cmd.extend(self.command_prefix)

        cmdargs=[cmd]
        if args[0] == cmd:
            cmdargs.extend(args[1:])
        else:
            cmdargs.extend(args)

        return cmdargs

class SSHZfsCommandRunner(ZfsCommandRunner):
    """Run ZFS commands on a remote system

    Uses paramiko to run a Zfs command on a remote system via SSH
    """

    RECV_BUF_SZ=4096

    def __init__(self, ssh_client, *args, **kwargs):
        """Initialize a new SSHZfsCommandRunner

        :param paramiko.SSHClient sshclient: the sshclient instance to use for
        remote commands
        """
        self.ssh = ssh_client
        super(SSHZfsCommandRunner, self).__init__(*args, **kwargs)

    def run_cmd(self, cmd, args, errorclass):
        """run a command on a remote system in a new SSH channel

        See :py:func:`ZfsCommandRunner.run_cmd` for a description of the
        parameters and the return values
        """
        cmdargs = self.process_cmd_args(cmd, args)

        # paramiko doesn't take a list, convert it to a shell compatible string
        command = subprocess.list2cmdline(cmdargs)

        transport=self.ssh.get_transport()
        chan=transport.open_session()
        chan.exec_command(command)

        out=''
        err=''
        done={
            'out' : False,
            'err' : False,
        }
        # Simulate the behavior of subprocess.Popen.communicate
        # Alternate reads between recv and recv_error until we've got all of
        # the data.
        # Note that paramiko channels will return a zero-length string to
        # indicate that the channel stream has closed
        while True:
            if done['out']==False:
                try:
                    t_out=chan.recv(self.RECV_BUF_SZ)
                    if t_out=='':
                        # done receiving from out
                        done['out']=True
                    else:
                        out += t_out
                except socket.timeout:
                    pass
            if done['err']==False:
                try:
                    t_err=chan.recv_stderr(self.RECV_BUF_SZ)
                    if t_err=='':
                        # done receiving from err
                        done['err']=True
                    else:
                        err += t_err
                except socket.timeout:
                    pass
            if not False in done.values():
                # We're done reading from both channels
                break

        rc=chan.recv_exit_status()

        logging.debug('out: ' + out)
        logging.debug('err: ' + err)
        logging.debug('rc:  ' + str(rc))
        return (out,err,rc)

class LocalZfsCommandRunner(ZfsCommandRunner):
    """Run ZFS commands on the local system"""

    def run_cmd(self, cmd, args, errorclass):
        """wrap subprocess.Popen with the ZFS environment

        instanciates a subprocess object, and raises the specified errorclass if
        the subprocess call raises an OSError with errno of 2 (ENOENT)

        See :py:func:`ZfsCommandRunner.run_cmd` for a description of the
        parameters and the return values
        """
        cmdargs = self.process_cmd_args(cmd, args)
        try:
            p=subprocess.Popen(
                cmdargs,
                env=ZFS_ENV,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as e:
            if errorclass != None and e.errno == errno.ENOENT:
                raise errorclass()
            else:
                raise e

        #return p
        out,err=p.communicate()
        rc=p.returncode
        logging.debug('command %s returned result code %d' % (str([cmdargs]),rc))
        return (out,err,rc)

# The local command runner object, used for the functional methods
_LCR=LocalZfsCommandRunner()

def zpool_list(*args, **kwargs):
    """List the specified properties about Zpools

    See :py:func:`ZfsCommandRunner.zpool_list` for details.
    """
    return _LCR.zpool_list(*args, **kwargs)


def zfs_list(*args, **kwargs):
    """List the specified properties about Zfs datasets

    See :py:func:`ZfsCommandRunner.zfs_list` for details.

    """
    return _LCR.zfs_list(*args, **kwargs)

def zfs_create(*args, **kwargs):
    """Creates a new ZFS file system.

    See :py:func:`ZfsCommandRunner.zfs_create` for details.
    """
    return _LCR.zfs_create(*args, **kwargs)

def zfs_destroy(*args, **kwargs):
    """Destroy a dataset or snapshot

    See :py:func:`ZfsCommandRunner.zfs_destroy` for details.
    """
    return _LCR.zfs_destroy(*args, **kwargs)

def zfs_snapshot(*args, **kwargs):
    """Snapshot a ZFS filesystem

    See :py:func:`ZfsCommandRunner.zfs_snapshot` for details.
    """
    return _LCR.zfs_snapshot(*args, **kwargs)

def is_syncing(*args, **kwargs):
    """Check if the named pool is currently scrubbing or resilvering

    See :py:func:`ZfsCommandRunner.is_syncing` for details.
    """
    return _LCR.is_syncing(*args, **kwargs)

def zpool_status(*args, **kwargs):
    """Call zpool status to check the status of a zpool

    See :py:func:`ZfsCommandRunner.zpool_status` for details.
    """
    return _LCR.zpool_status(*args, **kwargs)

def _check_perm_err(errstring):
    """Check if the error string is a /dev/zfs permission error

    This error only seems to occur on zfsonlinux (not sure about FreeBSD).
    Solaris has fine-grained delegated auth for ZFS so errors will be specific
    to each call
    """
    if errstring == ZFS_ERROR_STRINGS['permerr']:
        raise ZfsPermissionError(errno.EPERM, errstring, '/dev/zfs')

    if "Failed to load ZFS module stack." in errstring:
        raise ZfsPermissionError(errno.EPERM, errstring, 'Kernel module zfs.ko')

    if ': permission denied' in errstring:
        raise ZfsPermissionError(errno.EPERM, errstring)

def _check_prop_err(errstring):
    """Check if the errstring is a zfs invalid property error"""
    if 'bad property list: invalid property' in errstring:
        raise ZfsInvalidPropertyError(errstring)

def _validate_fsname(fsname):
    """Verify that the given fsname is a valid ZFS filesystem name

    Raises a ZfsBadFsName if it's invalid
    """
    r = re.match('^' + _FS_COMP + '(/' + _FS_COMP + ')*$', fsname)
    if r == None:
        raise ZfsBadFsName(fsname)

def _validate_poolname(poolname):
    """Verify that the given poolname is a valid ZFS Zpool name

    Raises a ZfsBadFsName if it's invalid
    """
    r = re.match('^'+_FS_COMP+'$', poolname)
    if r == None:
        raise ZfsBadFsName(poolname)

def _validate_snapname(snapname):
    """Verify that the given snapshot name is valid

    Raises a ZfsBadFsName if it's invalid
    """
    r = re.match('^' + _FS_COMP + '(/' + _FS_COMP + ')*@' + _FS_COMP + '$',
                 snapname)
    if r == None:
        raise ZfsBadFsName(snapname)
