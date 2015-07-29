from zfs import *
import zfs.snapshot as zfssnapshot
from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal

import logging
logging.basicConfig(level=logging.DEBUG)

TESTEXCLUDES=['tank/nodaily','tank/snapnorecurse','chile/rt']
TESTREADEROUTPUT=[
    ['tank', '-', '-'],
    ['tank/crap with spaces', '-', '-'],
    ['tank/nodaily', '-', 'false'],
    ['tank/snapnorecurse', 'true', '-'],
    ['tank/snapnorecurse/child1', 'true', 'false'],
    ['tank/snapnorecurse/child2', 'true', '-'],
    ['tank/snaprecurse', 'true', '-'],
    ['tank/snaprecurse/child1', 'true', '-'],
    ['tank/snaprecurse/child2', 'true', '-'],
]

def test_autosnapshotter():
    """Test instanciation of the Autosnapshotter object"""
    mocksnap=flexmock(zfssnapshot,
                      get_userprop_datasets=(
                          ['tank/snapnorecurse'],
                          ['tank/snaprecurse','tank/snapnorecurse/child2']),
                      filter_syncing_pools=lambda x: x,
                      destroy_older_snapshots=5,
                     )
    mocksnap.should_receive('zfs_snapshot').and_return()
    snapper=mocksnap.RollingSnapshotter(label="daily", keep=24)
    snapper.take_snapshot('//')

def test_can_recursive_snapshot():
    """ test can_recursive snapshot

    Tests multiple aspects of can_recursive_snapshot, including substrings
    """
    # Parent of an excluded ds should be false
    r = zfssnapshot.can_recursive_snapshot('chile',TESTEXCLUDES)
    assert_equal(r,False)

    # Non-excluded child ds of an excluded ds should be True
    r = zfssnapshot.can_recursive_snapshot('chile/rt/chile',TESTEXCLUDES)
    assert_equal(r,True)

    # Parent ds with excluded children should be False
    r = zfssnapshot.can_recursive_snapshot('tank',TESTEXCLUDES)
    assert_equal(r,False)

    # name that contains a non-recursive ds name
    r = zfssnapshot.can_recursive_snapshot('tankety',TESTEXCLUDES)
    assert_equal(r,True)

    # name that's a substring of an excluded dataset
    r = zfssnapshot.can_recursive_snapshot('tan',TESTEXCLUDES)
    assert_equal(r,True)

    # name that's equal to a component of an excluded dataset
    r = zfssnapshot.can_recursive_snapshot('rt',TESTEXCLUDES)
    assert_equal(r,True)

    # excluded dataset should do something. Right now it's false,
    # but maybe it should raise an error?
    r = zfssnapshot.can_recursive_snapshot('tank/nodaily',TESTEXCLUDES)
    assert_equal(r,False)

def test_narrow_recursive_filesystems():
    """ test narrow_recursive_filesystems

    should remove child paths of tank/foo, and not trip on bar
    """
    r = zfssnapshot.narrow_recursive_filesystems([
        'tank/foo',
        'tank/foo/foo',
        'tank/foo/bar/foo',
        'tank/bar'])
    assert_equal(r,['tank/foo','tank/bar'])

def test_get_userprop_datasets():
    """ test get_userprop_datasets with no args

    Defaults to label == "daily"
    """
    myzfssnapshot=flexmock(zfssnapshot)
    myzfssnapshot.should_receive('zfs_list').with_args(
        sort='name',
        properties=['name',
                    zfssnapshot.USERPROP_NAME,
                    zfssnapshot.SEP.join([zfssnapshot.USERPROP_NAME,'daily'])]
    ).and_return(iter(TESTREADEROUTPUT))
    r = myzfssnapshot.get_userprop_datasets()
    single_list = ['tank/snapnorecurse']
    recursive_list = ['tank/snapnorecurse/child2', 'tank/snaprecurse']
    assert r
    assert r[0]==single_list
    assert r[1]==recursive_list

def test_get_userprop_datasets_hourly():
    """test get_userprop_datasets with label hourly

    Note that we don't actually change the test output of the third column, so
    there isn't any change to the expected single and recursive lists"""
    myzfssnapshot=flexmock(zfssnapshot)
    myzfssnapshot.should_receive('zfs_list').with_args(
        sort='name',
        properties=['name',
                    zfssnapshot.USERPROP_NAME,
                    zfssnapshot.SEP.join([zfssnapshot.USERPROP_NAME,'hourly'])]
    ).and_return(iter(TESTREADEROUTPUT))
    r = myzfssnapshot.get_userprop_datasets(label='hourly')
    assert r

def test_filter_syncing_pools():
    """test filter_syncing_pools
    """

    myzfssnapshot=flexmock(zfssnapshot)
    myzfssnapshot.should_receive('is_syncing').and_return(
        False, True, False).one_by_one()
    r = myzfssnapshot.filter_syncing_pools(['tank/foo',
                                            'tank/bar',
                                            'deadweight/foo',
                                            'deadweight/bar',
                                            'tan/deadweight'])
    assert_equal(r, ['tank/foo','tank/bar','tan/deadweight'])

@raises(ZfsBadFsName)
def test_filter_syncing_pools_badname():
    """ test filter_syncing_pools with an invalid fs name """

    r = zfssnapshot.filter_syncing_pools(['/invalid'])

def test_destroy_older_snapshots():
    """test destroy_older_snapshots"""

    # Test output below has 7 hourly snapshots of tank/foo and
    # tank/foo/bar, one daily snapshot each of tank/foo and tank/foo/bar,
    # and one manual snapshot.
    p=[
        ['tank/foo@zfs-auto-snap_hourly-2014-11-20-0500'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-20-0500'],
        ['tank/foo@zfs-auto-snap_hourly-2014-11-20-0400'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-20-0400'],
        ['tank/foo@zfs-auto-snap_hourly-2014-11-20-0300'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-20-0300'],
        ['tank/foo@zfs-auto-snap_hourly-2014-11-20-0200'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-20-0200'],
        ['tank/foo@zfs-auto-snap_hourly-2014-11-20-0100'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-20-0100'],
        ['tank/foo@manual-snapshot'],
        ['tank/foo@zfs-auto-snap_hourly-2014-11-20-0000'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-20-0000'],
        ['tank/foo@zfs-auto-snap_daily-2014-11-19-0003'],
        ['tank/foo/bar@zfs-auto-snap_daily-2014-11-19-0003'],
        ['tank/foo@zfs-auto-snap_hourly-2014-11-19-2300'],
        ['tank/foo/bar@zfs-auto-snap_hourly-2014-11-19-2300'],
    ]
    myzfssnapshot=flexmock(zfssnapshot)
    myzfssnapshot.should_receive('zfs_list').with_args(
        types=['snapshot'], sort='creation', properties=['name'],
        ds='tank/foo', recursive=True
    ).and_return(iter(p))

    myzfssnapshot.should_receive('zfs_destroy').and_return()

    r=myzfssnapshot.destroy_older_snapshots(
        filesys='tank/foo', keep=3, label='hourly', recursive=False)
    assert_equal(r, 4)
