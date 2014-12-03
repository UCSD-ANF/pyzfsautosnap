import logging
import os
import sys
import subprocess
import re
import datetime
import csv
import collections
import errno
from . import *
from StringIO import StringIO

#logging.basicConfig(level=logging.DEBUG)

ZFS_ENV={'LC_ALL': 'C', 'PATH': '/usr/sbin:/sbin:/usr/bin:/bin'}
ZFS_ERROR_STRINGS={
    'permerr': "Unable to open /dev/zfs: Permission denied.\n",
    'nosnaplinux': "could not find any snapshots to destroy; check snapshot names.\n",
}
_FS_COMP='[a-zA-Z0-0\-_.]+'

def get_pool_from_fsname(fsname):
    """Return the ZFS pool containing fsname

    Given a ZFS filesystem, return which pool contains it. This is usually
    the part of the filesystem before the first forward slash.

    If the fsname is malformed, raise a ZfsBadFsName exeception
    """
    _validate_fsname(fsname)
    pool = fsname.split('/')[0]
    return pool

def get_zpool_guid(pool):
    """Get the GUID of a zpool

    Convenience function to retrieve the GUID of a single zpool.
    """
    _validate_poolname(pool)
    r = zpool_list(pools=pool, properties=['name','guid'])
    s = r.next()
    return s[1]

def zpool_list(pools=None, properties=None):
    """List the specified properties about Zpools

    Run the zpool list command, optionally retrieving only the specified
    properties. If no pool is provided, the zpool list default behavior of
    listing all zpools is used.

    If no properties are provided, the zpool default columns are used. This
    varies by platform and implementation, but typically looks like:
        NAME, SIZE, ALLOC, FREE, CAP, DEDUP, HEALTH, ALTROOT

    Note that support for listing the individual vdev properties under the
    zpool (the -v option to zpool list) is not supported at this time.

    Returns an iterable of lists with each requested property occupying one
    field of the list. This is performed under the hood by relying on the -H
    option to output a tab-delimited field of properties, and calling the
    csv.reader to read them. This occurs even if only out output field was
    requested, so be sure to expect something like ['foo'] instead of 'foo'
    """
    cmd=['zpool', 'list', '-H' ]
    if properties is not None:
        cmd_columns=','.join(properties)
        cmd.append('-o')
        cmd.append(cmd_columns)

    if pools is not None:
        if isinstance(pools,basestring):
            cmd.append(pools)
        else:
            cmd.extend(pools)

    p=subprocess.Popen(cmd, env=ZFS_ENV, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('command %s returned result code %d' % (str(cmd),rc))

    if rc > 0:
        _check_perm_err(err)
        if "no such pool" in err:
            raise ZfsNoPoolError(err)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    r=csv.reader(StringIO(out), delimiter="	")

    return r


def zfs_list(types=['filesystem','volume'], sort=None, properties=None,
             ds=None, recursive=False):
    """List the specified properties about Zfs datasets

    Run the zfs list command, optionally retrieving only the specified
    properties. If no ds is provided, the zfs list default behavior of
    recursively listing all filesystems and snapshots is used.

    Returns an iterable of lists with each field occupying one field of the
    list. This is performed under the hood by relying on the -H option to
    output a tab-delimited field of properties, and calling the csv.reader to
    read them. This occurs even if only one output field was requested, so be
    sure to expect something like ['foo'] instead of 'foo'
    """
    cmd=[ 'zfs', 'list', '-H' ]

    if recursive == True:
        cmd.append('-r')

    if types is not None:
        cmd_types=','.join(types)
        cmd.append('-t')
        cmd.append(cmd_types)

    if sort is not None:
        cmd.append('-s')
        cmd.append(sort)

    if properties is not None:
        cmd_columns=','.join(properties)
        cmd.append('-o')
        cmd.append(cmd_columns)

    if ds is not None:
        cmd.append(ds)

    p=subprocess.Popen(cmd, env=ZFS_ENV, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('command %s returned result code %d' % (str(cmd),rc))

    if rc > 0:
        _check_perm_err(err)
        if "dataset does not exist" in err:
            raise ZfsNoDatasetError(err)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    r=csv.reader(StringIO(out), delimiter="	")

    return r

def zfs_destroy(dataset, recursive=False):
    """Destroy a dataset or snapshot

    Calls the zfs destroy command, optionally recursively removing snapshots of
    child filesystems with the same name Note that the underlying command can
    only handle a single snapshot at a time.
    """
    cmd = ['zfs', 'destroy']

    if recursive==True:
        cmd.append('-r')

    if dataset is None or dataset == '':
        raise ZfsArgumentError('dataset cannot be empty')
    if isinstance(dataset, basestring):
        cmd.append(dataset)
    else:
        raise ZfsArgumentError(
            'not sure how to handle dataset with %s' % type(dataset))
    p=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=ZFS_ENV)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('command %s returned result code %d' % (str(cmd),rc))

    if rc > 0:
        _check_perm_err(err)
        if (err == ZFS_ERROR_STRINGS['nosnaplinux']
            or 'dataset does not exist' in err):
            raise ZfsNoDatasetError(errno.ENOENT, err, dataset)
        elif 'permission denied' in err:
            raise ZfsPermissionError(errno.EPERM, err, dataset)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    pass

def _check_perm_err(errstring):
    """Check if the error string is a /dev/zfs permission error

    This error only seems to occur on zfsonlinux (not sure about FreeBSD).
    Solaris has fine-grained delegated auth for ZFS so errors will be specific
    to each call
    """
    if errstring == ZFS_ERROR_STRINGS['permerr']:
        raise ZfsPermissionError(errno.EPERM, errstring, '/dev/zfs')

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

def zfs_snapshot(filesys, snapname, recursive=False):
    """Snapshot a ZFS filesystem
    """
    cmd = ['zfs', 'snapshot']

    if recursive==True:
        cmd.append('-r')

    if not isinstance(filesys,basestring):
        raise ZfsArgumentError(
            'not sure how to handle filesys with %s' % type(filesys))
    if not isinstance(snapname,basestring):
        raise ZfsArgumentError(
            'not sure how to handle snapname with %s' % type(snapname))

    fullsnapname="%s@%s" % (filesys, snapname)
    cmd.append(fullsnapname)

    p=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=ZFS_ENV)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('command %s returned result code %d' % (str(cmd),rc))

    if rc > 0:
        _check_perm_err(err)
        if 'dataset already exists' in err:
            raise ZfsDatasetExistsError(errno.EEXIST, err, fullsnapname)
        elif 'dataset does not exist' in err:
            raise ZfsNoDatasetError(errno.ENOENT, err, filesys)
        elif 'permission denied' in err:
            raise ZfsPermissionError(errno.EPERM, err, filesys)
        else:
            raise subprocess.CalledProcessError(rc, cmd)
    pass

def is_syncing(pool):
    """Check if the named pool is currently scrubbing or resilvering
    """
    s = zpool_status(pool)
    if " in progress" in s:
        return True
    return False

def zpool_status(pools=None):
    """Call zpool status to check the status of a zpool

    Currently just returns the raw text output of the zpool status command.  It
    might be worth the time to attempt to parse the big blob of text into the
    various fields in the status statement, and return them as a list of
    ZpoolStatus objects.
    """
    cmd=[ 'zpool', 'status', '-v' ]

    if pools is not None:
        if isinstance(pools,basestring):
            cmd.append(pools)
        else:
            cmd.extend(pools)

    p=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=ZFS_ENV)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('command %s returned result code %d' % (str(cmd),rc))

    if rc > 0:
        _check_perm_err(err)
        if "no such pool" in err:
            raise ZfsNoPoolError(err)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    return out
