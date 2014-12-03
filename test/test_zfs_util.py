import logging
import sys
import subprocess
from subprocess import Popen, PIPE, CalledProcessError
import zfs.util
from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal
from zfs import *
import StringIO

class Test:
    """
    Test zfs.util

    Makes heavy use of subprocess mocking calls to simulate various calls to the
    zfs command line utility.
    http://stackoverflow.com/questions/5166851/intercepting-subprocess-popen-call-in-python
    """

    mockederr=''
    mockedoutdaily='''tank	-	-
tank/crap with spaces	-	-
tank/nodaily	-	false
tank/snapnorecurse	true	-
tank/snapnorecurse/child1	true	false
tank/snapnorecurse/child2	true	-
tank/snaprecurse	true	-
tank/snaprecurse/child1	true	-
tank/snaprecurse/child2	true	-'''
    mockedoutnoargs='''tank	3.91G	3.56T	3.91G	/tank
tank/crap with spaces	30K	3.56T	30K	/tank/crap with spaces
tank/nodaily	30K	3.56T	30K	/tank/nodaily
tank/snapnorecurse	92K	3.56T	32K	/tank/snapnorecurse
tank/snapnorecurse/child1	30K	3.56T	30K	/tank/snapnorecurse/child1
tank/snapnorecurse/child2	30K	3.56T	30K	/tank/snapnorecurse/child2
tank/snaprecurse	92K	3.56T	32K	/tank/snaprecurse
tank/snaprecurse/child1	30K	3.56T	30K	/tank/snaprecurse/child1
tank/snaprecurse/child2	30K	3.56T	30K	/tank/snaprecurse/child2
'''
    mockedoutnoargstank="tank	3.91G	3.56T	3.91G	/tank\n"

    @raises(ZfsPermissionError)
    def test_zpool_list_with_bad_permission(self):
        """
        Test zfs.util.zpool_list with invalid permissions for the zfs executable
        """
        fake_p=flexmock(
            communicate = lambda: (
                '', 'Unable to open /dev/zfs: Permission denied.\n'),
            returncode  = 1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').and_return(fake_p)
        r = util.zpool_list()

    def test_zpool_list_with_properties(self):
        """
        Test zfs.util.zpool_list with the properties argument
        """

        fake_p=flexmock(
            communicate = lambda: ("tank\t3.62T",self.mockederr),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zpool', 'list', '-H',
             '-o','name,size'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return( fake_p)

        r = util.zpool_list(properties=[
            'name',
            'size'])
        assert r
        line = r.next()
        assert line[0] == 'tank'
        assert len(line) == 2

    def test_zpool_list_with_no_args(self):
        """
        test zfs_list with no arguments
        """
        fake_p=flexmock(
            communicate=lambda: ("tank\t3.62T\t3.91G\t0%\t1.00x\tONLINE\t-",
                                 self.mockederr),
            returncode=0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zpool', 'list', '-H'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)
        r = util.zpool_list()
        assert r
        line = r.next()
        assert_equal(line[0], 'tank')
        assert_equal(len(line),7)

    def test_zpool_list_with_existing_ds(self):
        """
        test zpool_list with a dataset that exists
        """
        fake_p=flexmock(
            communicate=lambda: ("tank\t3.62T\t3.91G\t0%\t1.00x\tONLINE\t-\n",
                                   self.mockederr),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zpool', 'list', '-H', 'tank'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)
        r = util.zpool_list(pools='tank')
        assert r
        line = r.next()
        assert_equal(line[0],'tank')
        assert_equal(len(line), 7)
        r = util.zpool_list(pools=['tank'])
        assert r
        line = r.next()
        assert_equal(line[0],'tank')
        assert_equal(len(line), 7)


    @raises(ZfsNoPoolError)
    def test_zpool_list_with_nonexistent_pool(self):
        """
        test zpool_list with a non existent pool name
        """
        fake_p=flexmock(
            communicate = lambda: ('',
                                   "cannot open 'failboat': no such pool\n"),
            returncode  = 1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zpool', 'list', '-H', 'failboat'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zpool_list(pools='failboat')

    @raises(ZfsPermissionError)
    def test_zfs_list_with_bad_permission(self):
        """
        Test zfs.util.zfs_list with invalid permissions for the zfs executable
        """
        fake_p=flexmock(
            communicate = lambda: (
                '', 'Unable to open /dev/zfs: Permission denied.\n'),
            returncode  = 1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').and_return(fake_p)
        r = util.zfs_list()

    def test_zfs_list_with_sort_and_properties(self):
        """
        Test zfs.util.zfs_list with the sort and properties arguments
        """

        fake_p=flexmock(
            communicate = lambda: (self.mockedoutdaily,self.mockederr),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume','-s','name',
             '-o','name,com.sun:auto-snapshot,com.sun:auto-snapshot:daily'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return( fake_p)

        r = util.zfs_list(sort='name', properties=[
            'name',
            'com.sun:auto-snapshot',
            'com.sun:auto-snapshot:daily'])
        assert r
        line = r.next()
        assert line[0] == 'tank'
        assert len(line) == 3

    def test_zfs_list_with_no_args(self):
        """
        test zfs_list with no arguments
        """
        fake_p=flexmock(
            communicate=lambda: (self.mockedoutnoargs,self.mockederr),
            returncode=0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)
        r = util.zfs_list()
        assert r
        line = r.next()
        assert line[0] == 'tank'
        assert len(line) == 5

    def test_zfs_list_with_existing_ds(self):
        """
        test zfs_list with a dataset that exists
        """
        fake_p=flexmock(
            communicate = lambda: (self.mockedoutnoargstank,
                                   self.mockederr),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume', 'tank'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)
        r = util.zfs_list(ds='tank')
        assert r
        line = r.next()
        assert_equal(line[0],'tank')
        assert_equal(len(line), 5)

    def test_zfs_list_with_existing_ds_recursive(self):
        """
        test zfs_list with a dataset that exists and recurse
        """
        fake_p=flexmock(
            communicate = lambda: (self.mockedoutnoargs,
                                   self.mockederr),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-r', '-t', 'filesystem,volume', 'tank'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)
        r = util.zfs_list(ds='tank',recursive=True)
        assert r
        line = r.next()
        assert_equal(line[0],'tank')
        assert_equal(len(line), 5)


    @raises(ZfsNoDatasetError)
    def test_zfs_list_with_nonexistent_ds(self):
        """
        test zfs_list with a non-existent dataset
        """
        fake_p=flexmock(
            communicate = lambda: (
                '', "cannot open 'failboat': dataset does not exist\n"),
            returncode  = 1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume', 'failboat'],
            env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)

        r = util.zfs_list(ds='failboat')

    @raises(ZfsNoPoolError)
    def test_zpool_status_with_nonexistent_pool(self):
        """
        test zpool_status with a non existent pool name
        """
        fake_p=flexmock(
            communicate = lambda: ('',
                                   "cannot open 'failboat': no such pool\n"),
            returncode  = 1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zpool', 'status', '-v', 'failboat'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zpool_status(pools='failboat')

    def test_zpool_status_with_existing_pool(self):
        """
        test zpool_status with an existing pool name
        """
        fake_p=flexmock(
            communicate = lambda: ( 'fake it till you make it',''),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zpool', 'status', '-v', 'pool1'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zpool_status(pools='pool1')
        assert r

    def test_zpool_status_with_existing_pools(self):
        """ test zpool_status with multiple existing pool names """
        fake_p=flexmock(
            communicate = lambda: ('dummy',''),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zpool', 'status', '-v', 'pool1', 'pool2'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zpool_status(pools=['pool1','pool2'])
        assert r

    def test_zfs_destroy_one_existing_item(self):
        """test zfs_destroy with one existing snapshot"""
        fake_p=flexmock(
            communicate = lambda: ('',''),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy('tank@foo')
        assert_equal(r, None)

        # Now, with the recursive flag
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', '-r', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy('tank@foo', True)
        assert_equal(r, None)

    @raises(ZfsArgumentError)
    def test_zfs_destroy_one_existing_item_by_list(self):
        """test zfs_destroy with one existing snapshot passed as a list"""
        fake_p=flexmock(
            communicate = lambda: ('',''),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy(['tank@foo'])
        assert_equal(r, None)

    @raises(ZfsArgumentError)
    def test_zfs_destroy_multiple_existing_items(self):
        """test zfs_destroy with one existing snapshot passed as a list"""
        fake_p=flexmock(
            communicate = lambda: ('',''),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo', 'tank2@bar', 'cox@home'],
            env=util.ZFS_ENV, stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy(['tank@foo', 'tank2@bar', 'cox@home'])
        assert_equal(r, None)



    @raises(ZfsPermissionError)
    def test_zfs_destroy_bad_perms_linux(self):
        """test zfs_destroy with bad permissions on /dev/zfs

        This is typical for a ZFSOnLinux host, possilby for FreeBSD"""
        fake_p=flexmock(
            communicate = lambda: ( '', util.ZFS_ERROR_STRINGS['permerr']),
            returncode  = 1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy('tank@foo')

    @raises(ZfsPermissionError)
    def test_zfs_destroy_bad_perms_granular(self):
        """test zfs_destroy permission error using granular permission failure

        This is more typical for Solaris where the permissions are granular.
        """
        fake_p=flexmock(
            communicate=lambda: (
                '', "cannot destroy 'tank@foo': permission denied\n"),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy('tank@foo')

    @raises(ZfsNoDatasetError)
    def test_zfs_destroy_no_exist_linux(self):
        """ test zfs_destroy when the specified snapshot doesn't exist on Linux

        ZfsOnLinux returns a different error message than Solaris does when a
        snapshot doesn't exist"""
        fake_p=flexmock(
            communicate=lambda: (
                '', util.ZFS_ERROR_STRINGS['nosnaplinux']),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy('tank@foo')

    @raises(ZfsNoDatasetError)
    def test_zfs_destroy_no_exist_solaris(self):
        """ test zfs_destroy when the specified snapshot doesn't exist on Solaris

        ZfsOnLinux returns a different error message than Solaris does when a
        snapshot doesn't exist"""
        fake_p=flexmock(
            communicate=lambda: (
                '', "cannot open 'tank@foo': dataset does not exist\n"),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'destroy', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_destroy('tank@foo')

    @raises(ZfsNoDatasetError)
    def test_zfs_snapshot_filesys_no_exist(self):
        """ test zfs_snapshot when the specified filesystem doesn't exist
        """
        fake_p=flexmock(
            communicate=lambda: (
                '', """cannot open 'tank': dataset does not exist
usage:
	snapshot|snap [-r] [-o property=value] ... <filesystem@snapname|volume@snapname> ...

For the property list, run: zfs set|get

For the delegated permission list, run: zfs allow|unallow
"""
            ),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'snapshot', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_snapshot('tank', 'foo')

    @raises(ZfsDatasetExistsError)
    def test_zfs_snapshot_already_exists(self):
        """ test zfs_snapshot when the specified snapshot already exists
        """
        fake_p=flexmock(
            communicate=lambda: (
                '', "cannot create snapshot 'tank@exists': dataset already exists\n"),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'snapshot', 'tank@exists'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_snapshot('tank', 'exists')

    def test_zfs_snapshot(self):
        """test zfs_snapshot of a new snapshot"""
        fake_p=flexmock(
            communicate = lambda: ('',''),
            returncode  = 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'snapshot', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_snapshot('tank', 'foo')
        assert_equal(r, None)

        # Now, with the recursive flag
        mysubprocess.should_receive('Popen').with_args(
            ['zfs', 'snapshot', '-r', 'tank@foo'], env=util.ZFS_ENV,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zfs_snapshot('tank', 'foo', True)
        assert_equal(r, None)

    def test_get_pool_from_fsname(self):
        """Test ability to get zpool name from fsname
        """

        r = zfs.util.get_pool_from_fsname('foo')
        assert_equal(r, 'foo')

        r = zfs.util.get_pool_from_fsname('foo/bar/baz')
        assert_equal(r, 'foo')

        r = zfs.util.get_pool_from_fsname('foo/foo/foo')
        assert_equal(r, 'foo')

        # This should definitely raise a ZfsBadFsName
        assert_raises(ZfsBadFsName, zfs.util.get_pool_from_fsname, 'foo//foo')

        # This should definitely raise a ZfsBadFsName
        assert_raises(ZfsBadFsName, zfs.util.get_pool_from_fsname, '/foo')

    def test_get_zpool_guid(self):
        guid='16263632456085043332'
        myzfsutil=flexmock(zfs.util)
        myzfsutil.should_receive('zpool_list').with_args(
            properties=['name','guid'], pools='tank'
        ).and_return(iter([['tank', guid]]))
        r = myzfsutil.get_zpool_guid('tank')
        assert_equal(r, guid)



if __name__ == '__main__':
    import nose
    #nose.runmodule(argv=[__file__,'-vvs','-x','--pdb', 'pdb-failure'],
    #               exit=False)
    nose.runmodule(argv=[__file__,'-vvs','-x','--pdb'],exit=False)
