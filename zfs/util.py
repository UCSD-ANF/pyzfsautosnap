import logging
import os
import sys
import subprocess
import re
import datetime
import csv
from cStringIO import StringIO
from . import ZfsError, ZfsNoDatasetError, ZfsPermissionError, ZfsNoPoolError

#logging.basicConfig(level=logging.DEBUG)

zfs_env={'LC_ALL': 'C', 'PATH': '/usr/sbin:/sbin:/usr/bin:/bin'}

def zfs_list(types=['filesystem','volume'], sort=None, properties=None,
             ds=None, recursive=False):
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

    p=subprocess.Popen(cmd, env=zfs_env, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('process returned result code %d' % rc)

    if rc > 0:
        if err == "Unable to open /dev/zfs: Permission denied.\n":
            raise ZfsPermissionError(err)
        elif "dataset does not exist" in err:
            raise ZfsNoDatasetError(err)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    r=csv.reader(StringIO(out), delimiter="	")

    return r

def zfs_destroy(items, recursive):
    """Scaffold destroy function"""
    print("SCAFFOLD: zfs_destroy would remove %s" % items)
    pass

def is_syncing(pool):
    """
    Check if the named pool is currently scrubbing or resilvering
    """
    s = zpool_status(pool)
    if " in progress" in s:
        return True
    return False

def zpool_status(pools=None):
    cmd=[ 'zpool', 'status', '-v' ]

    if pools is not None:
        if isinstance(pools,basestring):
            cmd.append(pools)
        else:
            cmd.extend(pools)

    p=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=zfs_env)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('process returned result code %d' % rc)

    if rc > 0:
        if err == "Unable to open /dev/zfs: Permission denied.\n":
            raise ZfsPermissionError(err)
        elif "no such pool" in err:
            raise ZfsNoPoolError(err)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    return out
