#!/usr/bin/env python
import logging
import sys
import subprocess
import csv
import re
import datetime

PREFIX="zfs-auto-snap"
USERPROP_NAME='com.sun:auto-snapshot'
SEP=":"

logging.basicConfig(level=logging.DEBUG)

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
    for exc in excludes:
        if re.match(ds,exc):
            return False
    return True

def narrow_recursive_filesystems(recursive_list):
    final_list=[]
    for ds in recursive_list:
        found=False
        logging.debug("checking if %s has a parent in %s" % (ds,recursive_list))
        for tmp in recursive_list:
            logging.debug("comparing %s to %s" % (tmp,ds))
            if re.match(tmp,ds) and tmp!=ds:
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

    cols=','.join(['name',
                   USERPROP_NAME,
                   SEP.join([USERPROP_NAME,label])
                  ])

    cmd=[ 'zfs', 'list', '-H', '-t', 'filesystem,volume',
         '-s', 'name', '-o',
         cols ]
    p=subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    r=csv.reader(p.stdout, delimiter="	")
    save=[]
    exclude=[]
    for row in r:
        save.append(row)
        if row[2] == 'false' or (row[1] == 'false' and row[2] == '-') or \
           (row[1] == '-' and row[2] == '-'):
            exclude.append(row[0])

    errmsg=p.communicate()[1]
    rc = p.returncode
    logging.debug("process returned result code %d" % rc)

    if rc > 0:
        logging.error("zfs returned code %d. Error message: \n%s" % (rc,errmsg))
        raise subprocess.CalledProcessError(rc, cmd)

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
    try:
        get_userprop_datasets(label="daily")
    except (OSError) as e:
        logging.error("Cannot find zfs executable")
        exit()
    except (subprocess.CalledProcessError) as e:
        logging.error("The ZFS executable failed")
        exit()

    take_snapshot()


