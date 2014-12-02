from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal
from optparse import Values

from zfsautosnap import App, main
from zfs.snapshot import RollingSnapshotter

def test_app():
    """Test instantiating an App object directly

    Bypasses the main function for the module
    """
    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    options=Values()
    options.verbose=True
    options.label='stinkily'
    options.keep=5
    a=App(options)
    r=a.run()
    assert_equal(r,0)

@raises(SystemExit)
def test_main_bad_args():
    """test main with no args

    should die due to lack of options passed to main
    """
    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    args=['testapp']
    r=main(args)
    assert(r)

@raises(SystemExit)
def test_main_bad_keep():
    """test main with a bad keep option

    Should choke because args[2] is not parsable as a number or 'all'
    """

    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    args=['testapp','stinkily','poorly']
    r=main(args)
    assert(r)

def test_main_enough_args():
    """test main with the proper number of arguments
    """

    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    args=['testapp', 'stinkily', '5']
    r=main(args)
    assert_equal(r, 0)
