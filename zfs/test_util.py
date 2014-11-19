import logging
import sys
import subprocess
from subprocess import Popen, PIPE, CalledProcessError
import util
from flexmock import flexmock
from nose.tools import raises, assert_raises, assert_equal
from . import ZfsError, ZfsNoDatasetError, ZfsPermissionError,ZfsNoPoolError

class Test:
    """
    Test zfs.utils

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
    def test_zfs_list_with_bad_permission(self):
        """
        Test zfs.util.zfs_list with invalid permissions for the zfs executable
        """
        fake_p=flexmock(
            communicate=lambda: (
                '',
                'Unable to open /dev/zfs: Permission denied.\n'),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').and_return(fake_p)
        r = util.zfs_list()

    def test_zfs_list_with_sort_and_properties(self):
        """
        Test zfs.util.zfs_list with the sort and properties arguments
        """

        fake_p=flexmock(
            communicate=lambda: (self.mockedoutdaily,self.mockederr),
            returncode= 0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume','-s','name',
             '-o','name,com.sun:auto-snapshot,com.sun:auto-snapshot:daily'],
            env=util.zfs_env,
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
            env=util.zfs_env,
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
            communicate=lambda: (self.mockedoutnoargstank,
                                 self.mockederr),
            returncode=0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume', 'tank'],
            env=util.zfs_env,
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
            communicate=lambda: (self.mockedoutnoargs,
                                 self.mockederr),
            returncode=0)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-r', '-t', 'filesystem,volume', 'tank'],
            env=util.zfs_env,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)
        r = util.zfs_list(ds='tank',recursive=True)
        assert r
        line = r.next()
        assert_equal(line[0],'tank')
        assert_equal(len(line), 5)


    @raises(ZfsNoDatasetError)
    def test_zfs_list_with_nonexistant_ds(self):
        """
        test zfs_list with a non-existant dataset
        """
        fake_p=flexmock(
            communicate=lambda: (
                '', "cannot open 'failboat': dataset does not exist\n"),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            [ 'zfs', 'list', '-H', '-t', 'filesystem,volume', 'failboat'],
            env=util.zfs_env,
            stdout=PIPE, stderr=PIPE).and_return(fake_p)

        r = util.zfs_list(ds='failboat')

    @raises(ZfsNoPoolError)
    def test_zpool_status_with_nonexistant_pool(self):
        """
        test zpool_status with a nonexistant pool name
        """
        fake_p=flexmock(
            communicate=lambda: (
                '', "cannot open 'failboat': no such pool\n"),
            returncode=1)
        mysubprocess=flexmock(subprocess)
        mysubprocess.should_receive('Popen').with_args(
            ['zpool', 'status', '-v', 'failboat'], env=util.zfs_env,
            stdout=PIPE, stderr=PIPE
        ).and_return(fake_p)

        r = util.zpool_status(pools='failboat')



if __name__ == '__main__':
    import nose
    #nose.runmodule(argv=[__file__,'-vvs','-x','--pdb', 'pdb-failure'],
    #               exit=False)
    nose.runmodule(argv=[__file__,'-vvs','-x','--pdb'],exit=False)
