import logging
import sys
import subprocess
import csv
import re
import datetime
from . import *
from util import (zfs_list, is_syncing, zfs_destroy, zfs_snapshot,
                  get_pool_from_fsname)
from itertools import chain

PREFIX="zfs-auto-snap"
USERPROP_NAME='com.sun:auto-snapshot'
SEP=":"
KEEP={'hourly': 24, 'daily': 30, '__default__': 10}

def validate_keep(keep):
    """validates the value of the keep parameter

    If it's not coercable to an int or equal to the special string values,
    raise a ValueError. Otherwise, return keep.
    """
    if keep != 'all':
        keep=int(keep)
    return keep

class RollingSnapshotter():
    """Automatically snapshot ZFS filesystems

    This class will manage automatic snapshots for ZFS
    filesystems. Snapshots will be named according to the
    pattern #{prefix}_#{label}-#{date}.

    Older snapshots for the given label value will be
    automatically purged, with the most recent items
    retained per the keep attribute. If keep is the special
    value 'all', all older snapshots are retained.

    Attributes:
        label           The label for this set of snapshots
        keep            Number of older snapshots to keep, or 'all'
        avoidsync       Avoid fs on zpools in scrub/resilver state
        prefix          First part of snapshot name
        userprop_name   name of the ZFS user property to check
    """
    def __init__(
        self,
        label,
        keep='all',
        avoidsync=False,
        prefix=PREFIX,
        userprop_name=USERPROP_NAME
    ):
        """Create new RollingSnapshotter instance
        """
        validate_keep(keep)

        self.keep          = keep
        self.label         = label
        self.avoidsync     = avoidsync
        self.prefix        = prefix
        self.userprop_name = userprop_name

    def take_snapshot(self, fsnames, snap_children=False):
        """Take a snapshot of all eligible filesystems given in fsnames

        If fsnames is the special value '//', use the #{self.userprop_name} or
        the #{self.userprop_name}:#{self.label} property to determine which
        filesystems to snapshot
        """

        today = datetime.datetime.now()
        snapdate=today.strftime('%F-%H%M')
        logging.debug("Snapdate is: %s"% snapdate)
        snapname="%s_%s-%s" % (self.prefix, self.label, snapdate)

        # the '//' filesystem is special. We use it as a keyword to determine
        # whether to poll the ZFS user properties.
        # Determine what these are, call ourselves again, then return.
        if isinstance(fsnames, basestring) and fsnames == '//':
            single_list,recursive_list = get_userprop_datasets(
                label=self.label, userprop_name=self.userprop_name)

            logging.info("Taking non-recursive snapshots of: %s" %\
                           ', '.join(single_list))
            single_state = self.take_snapshot(single_list, snap_children=False)

            logging.info("Taking recursive snapshots of: %s" %\
                           ', '.join(recursive_list))
            recursive_state = self.take_snapshot(
                recursive_list, snap_children=True)

            return

        if isinstance(fsnames, basestring):
            fsnames = [ fsnames ]

        if self.avoidsync == True:
            fsnames=filter_syncing_pools(fsnames)

        keep=validate_keep(self.keep)
        # Since we are about to take a new snapshot, get rid of 1 extra
        if keep != 'all':
            keep = keep - 1

        for fs in fsnames:
            # Ok, now say cheese!
            logging.info("Taking %s snapshot %s@%s" % (
                "recursive" if snap_children else "non-recursive",
                fs,
                snapname))
            zfs_snapshot(fs,snapname,snap_children)

            # If we're taking recursive snapshots,
            # walk through the children, destroying old ones if required.
            destroy_older_snapshots(fs, keep, self.label,
                                    self.prefix, snap_children)
        pass

class SnapshotPurger(object):
    """Recursively purge old snapshot

    Given a starting dataset, purge the oldest snapshots of east child dataset,
    keeping the specified newest snapshots around. This is useful on the zfs
    snapshot receiver as a way to purge old snapshots. On client/source
    systems, use the RollingSnapshotter class instead.
    """

    def __init__(self, label='daily', keep=KEEP['daily'], prefix=PREFIX,
                 baseds='zfsbackups'):
        validate_keep(keep)

        self.keep  = keep
        self.label  = label
        self.prefix = prefix
        self.baseds = baseds

    def run(self):
        datasets=get_child_datasets(self.baseds)
        removed = [ destroy_older_snapshots( filesys=ds,
                                            keep=self.keep,
                                            label=self.label,
                                            prefix=self.prefix,
                                           ) for ds in datasets ]
        nremoved = len([chain.from_iterable(removed)])
        logging.info('Removed %d snapshots' % nremoved)
        return 0

def get_child_datasets(ds):
    """get child datasets of the specified ds"""
    return zfs_list(types=['filesystem'], properties=['NAME'], ds=baseds,
                    recursive=True)[1:]

def destroy_older_snapshots(filesys, keep, label, prefix=PREFIX,
                            recursive=False):
    """Destroy old snapshots, keeping 'keep' newest around.

    Given a filesystem name, the number of snapshots we want to keep, along
    with the label for this set of snapshots, we destroy all older
    snapshots of this filesystem whose names being with the text
    prefix_label-

    If keep is set to the special value 'all', no older snapshots are
    removed.

    Note that unlike the original ksh function, we actually keep around the
    requested number of snapshots, rather than "keep - 1".

    Returns the number of snapshots removed.
    """

    if keep == 'all':
        return 0

    snappre="%s@%s_%s-" % (filesys, prefix, label)
    r = zfs_list(types=['snapshot'], sort='creation', properties=['name'],
                 ds=filesys, recursive=True)

    logging.debug("Subsetting for snapshots starting with %s" % snappre)
    # Remove all snapshots for child filesystems and those that aren't for
    # our given label
    rs = [x[0] for x in r if x[0][:len(snappre)] == snappre]

    to_remove=rs[keep:]
    removed=0
    logging.debug(
        "Should remove %d of %d snapshots for filesys %s (keep=%d)" % (
        len(to_remove), len(rs), filesys, keep))
    for snapshot in to_remove:
        try:
            zfs_destroy(snapshot, recursive=recursive)
        # Not catching ZfsArgumentErrors because those are programming problems
        except (ZfsOSError) as e:
            logger.warning('Unable to destroy %s' % snapshot)
        else:
            removed+=1

    return removed

def filter_syncing_pools(fsnames):
    """filter out filesys on pools that are scrubbing/resilvering

    Given a list of fsnames, filter out the filesystems that are on pools
    in the middle of a sync operating (scrub or resilver).

    Sync operations would be interrupted/restarted by a snapshot.

    This function attempts to cache calls to zfs.utils.is_syncing. There is a
    risk that a scrub/resilver will be started just after this check completes,
    and also the risk that a running scrub will complete just after this check.
    """

    poolcache={}
    nosyncfilesys=[]

    for fs in fsnames:
        pool=get_pool_from_fsname(fs)

        if pool in poolcache.keys():
            if poolcache[pool]==True:
                sync=True
            else:
                sync=False
        else:
            sync = is_syncing(pool)
            poolcache[pool]=sync

        if sync==True:
            logging.info("The pool containing %s is being scrubbed/resilvered." % fs)
            logging.info("Not taking snapshots for %s." % fs)
        else:
            nosyncfilesys.append(fs)
    return nosyncfilesys

def can_recursive_snapshot(ds, excludes):
    """Check if a dataset can be recursively snapshotted

    Given a list of datasets to exclude, check if the dataset named ds
    has a child filesystem that is in the exclude list. If so, ds cannot be
    recursively snapshotted.
    """
    sds=ds.split('/')
    for exc in excludes:
        sexc=exc.split('/')
        if len(sds) > len(sexc):
            next
        if sds == sexc[:len(sds)]:
            return False
    return True

def narrow_recursive_filesystems(recursive_list):
    """Get minimum list of recursive filesystems

    Return a list of only the filesystems that are not a child of any other
    filesystem in the input recursive_list. For example, given the input
    ['foo','baz','foo/bar'], this function will return ['foo','baz']
    """

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

def get_userprop_datasets(label="daily", userprop_name=USERPROP_NAME):
    """ This builds two lists of datasets - RECURSIVE_LIST and SINGLE_LIST
    based on the value of ZFS user properties com.sun:auto-snapshot and
    com.sun:auto-snapshot:${label}
    RECURSIVE_LIST is a list of datasets that can be snapshotted with -r
    SINGLE_LIST is a list of datasets to snapshot individually.
    """

    props=['name',
           userprop_name,
           SEP.join([userprop_name,label]) ]

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


