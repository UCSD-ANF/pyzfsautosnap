#!/usr/bin/env python
import logging
import sys
import subprocess
import csv
import re
import datetime
from zfs import *
from zfs.util import zfs_list


logging.basicConfig(level=logging.DEBUG)
PREFIX="zfs-auto-snap"
USERPROP_NAME='com.sun:auto-snapshot'
SEP=":"

def take_snapshot(label="daily", snap_children=False, avoidscrub=False, *args):
    """ Take a snapshot of all eligable filesystems given in *args
    If *args is the special value '//', use the com.sun:auto-snapshot or the
    com.sun:auto-snapshot:#{event} property to determine which filesystems to
    snapshot"""

    today = datetime.datetime.now()
    snapdate=today.strftime('%F-%H%M')
    logging.debug("Snapdate is: %s"% snapdate)

    pass

def can_recursive_snapshot(ds, excludes):
    """ Given a list of datasets to exclude, check if the dataset named ds
    has a child filesystem that is in the exclude list. If so, ds cannot be
    recursively snapshotted. """
    sds=ds.split('/')
    for exc in excludes:
        sexc=exc.split('/')
        if len(sds) > len(sexc):
            next
        if sds == sexc[:len(sds)]:
            return False
    return True

def narrow_recursive_filesystems(recursive_list):
    final_list=[]
    for ds in recursive_list:
        sds=ds.split('/')
        found=False
        logging.debug("checking if %s has a parent in %s" % (ds,recursive_list))
        for tmp in recursive_list:
            logging.debug("comparing %s to %s" % (tmp,ds))
            if tmp!=ds:
                stmp=tmp.split('/')
                if stmp == sds[:len(stmp)]:
                    found=True
                    next
        if found==False:
            final_list.append(ds)
    return final_list

def get_userprop_datasets(label="daily"):
    """ This builds two lists of datasets - RECURSIVE_LIST and SINGLE_LIST
    based on the value of ZFS user properties com.sun:auto-snapshot and
    com.sun:auto-snapshot:${label}
    RECURSIVE_LIST is a list of datasets that can be snapshotted with -r
    SINGLE_LIST is a list of datasets to snapshot individually.
    """

    props=['name',
           USERPROP_NAME,
           SEP.join([USERPROP_NAME,label]) ]

    r=zfs_list(sort='name', properties=props)
    save=[]
    exclude=[]
    for row in r:
        save.append(row)
        if row[2] == 'false' or (row[1] == 'false' and row[2] == '-') or \
           (row[1] == '-' and row[2] == '-'):
            exclude.append(row[0])

    recursive_list=[]
    single_list=[]

    # find recursive snapshot sets
    for row in save:
        ds=row[0]
        logging.debug("checking %s" % ds)
        if can_recursive_snapshot(ds,exclude):
            logging.debug("OK to recursive snapshot %s" % ds)
            recursive_list.append(ds)
        elif ds not in exclude:
            logging.debug("OK to snapshot sole dataset %s" % ds)
            single_list.append(ds)
        else:
            logging.debug("NOT OK to snapshot %s" % ds)

    logging.debug("Pre-Narrowed recursive list is %s" % recursive_list)
    final_recursive_list=narrow_recursive_filesystems(recursive_list)
    logging.debug("Final recursive list is %s" % final_recursive_list)
    return (single_list,final_recursive_list)

# ---------------- MAIN ---------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    try:
        get_userprop_datasets(label="daily")
    except (OSError) as e:
        logging.error("Cannot find zfs executable")
        exit()
    except (subprocess.CalledProcessError) as e:
        logging.error("The ZFS executable failed")
        exit()

    take_snapshot()


