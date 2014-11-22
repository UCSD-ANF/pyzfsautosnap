from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal
from optparse import Values

from zfsautosnap import App, main
from zfs.snapshot import RollingSnapshotter

def test_app():
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
    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    args=['testapp']
    r=main(args)
    assert(r)

@raises(SystemExit)
def test_main_bad_keep():
    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    args=['testapp','stinkily','poorly']
    r=main(args)
    assert(r)

def test_main_enough_args():
    fakesnapper=flexmock(RollingSnapshotter)
    fakesnapper.should_receive('take_snapshot').and_return()
    args=['testapp', 'stinkily', '5']
    r=main(args)
    assert_equal(r, 0)
