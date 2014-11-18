import logging
import sys
import subprocess
import re
import datetime
import csv
from cStringIO import StringIO
from . import ZfsError, ZfsNoDatasetError

#logging.basicConfig(level=logging.DEBUG)

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

    p=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err=p.communicate()
    rc=p.returncode
    logging.debug('process returned result code %d' % rc)

    if rc > 0:
        if "dataset does not exist" in err:
            raise ZfsNoDatasetError(err)
        else:
            raise subprocess.CalledProcessError(rc, cmd)

    r=csv.reader(StringIO(out), delimiter="	")

    return r
