from zfs import *
from zfs.util import zfs_list
import zfsautosnap
from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal

testexcludes=['tank/nodaily','tank/snapnorecurse','chile/rt']

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
