from zfs import *
import zfs.util
import zfsautosnap
from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal

import logging
logging.basicConfig(level=logging.DEBUG)

testexcludes=['tank/nodaily','tank/snapnorecurse','chile/rt']
testreaderoutput=[
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

def test_can_recursive_snapshot():
    # Parent of an excluded ds should be false
    r = zfsautosnap.can_recursive_snapshot('chile',testexcludes)
    assert_equal(r,False)

    # Non-excluded child ds of an excluded ds should be True
    r = zfsautosnap.can_recursive_snapshot('chile/rt/chile',testexcludes)
    assert_equal(r,True)

    # Parent ds with excluded children should be False
    r = zfsautosnap.can_recursive_snapshot('tank',testexcludes)
    assert_equal(r,False)

    # name that contains a non-recursive ds name
    r = zfsautosnap.can_recursive_snapshot('tankety',testexcludes)
    assert_equal(r,True)

    # name that's a substring of an excluded dataset
    r = zfsautosnap.can_recursive_snapshot('tan',testexcludes)
    assert_equal(r,True)

    # name that's equal to a component of an excluded dataset
    r = zfsautosnap.can_recursive_snapshot('rt',testexcludes)
    assert_equal(r,True)

    # excluded dataset should do something. Right now it's false,
    # but maybe it should raise an error?
    r = zfsautosnap.can_recursive_snapshot('tank/nodaily',testexcludes)
    assert_equal(r,False)

def test_narrow_recursive_filesystems():
    r = zfsautosnap.narrow_recursive_filesystems([
        'tank/foo',
        'tank/foo/foo',
        'tank/foo/bar/foo',
        'tank/bar'])
    assert_equal(r,['tank/foo','tank/bar'])

def test_get_userprop_datasets():
    """ with no args (uses label == "daily") """
    myzfsautosnap=flexmock(zfsautosnap)
    myzfsautosnap.should_receive('zfs_list').with_args(
        sort='name',
        properties=['name',
                    zfsautosnap.USERPROP_NAME,
                    zfsautosnap.SEP.join([zfsautosnap.USERPROP_NAME,'daily'])]
    ).and_return(iter(testreaderoutput))
    r = myzfsautosnap.get_userprop_datasets()
    single_list = ['tank/snapnorecurse']
    recursive_list = ['tank/snapnorecurse/child2', 'tank/snaprecurse']
    assert r
    assert r[0]==single_list
    assert r[1]==recursive_list

def test_get_userprop_datasets_hourly():
    """Use label hourly

    Note that we don't actually change the test output of the third column, so
    there isn't any change to the expected single and recursive lists"""
    myzfsautosnap=flexmock(zfsautosnap)
    myzfsautosnap.should_receive('zfs_list').with_args(
        sort='name',
        properties=['name',
                    zfsautosnap.USERPROP_NAME,
                    zfsautosnap.SEP.join([zfsautosnap.USERPROP_NAME,'hourly'])]
    ).and_return(iter(testreaderoutput))
    r = myzfsautosnap.get_userprop_datasets(label='hourly')
    assert r

