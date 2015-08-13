import os
import shutil
import unittest
import mock
import tempfile

from testfixtures import LogCapture, log_capture
# https://pythonhosted.org/testfixtures/logging.html
# LogCapture and log_capture are used in different ways to achieve the same
# results

from rsempipeline.utils import misc


class MiscTestCase(unittest.TestCase):
    @mock.patch('rsempipeline.utils.misc.os')
    def test_mkdir(self, mock_os):
        misc.mkdir('temp_dir')
        mock_os.mkdir.assert_called_once_with('temp_dir')

    @mock.patch('rsempipeline.utils.misc.os')
    def test_mkdir_with_OSError(self, mock_os):
        mock_os.mkdir.side_effect = OSError()
        misc.mkdir('temp_dir')
        mock_os.mkdir.assert_called_once_with('temp_dir')


    def test_decorator(self):
        def decf1(func):
            def decorated_f():
                func()
            return decorated_f

        @decf1
        def f():
            return True

        self.assertEqual(f.func_name, 'decorated_f')

        @misc.decorator
        def decf2(func):
            def decorated_f():
                func()
            return decorated_f

        @decf2
        def f():
            return True

        self.assertEqual(f.func_name, 'f')

    @mock.patch('rsempipeline.utils.misc.glob')
    def test_get_lockers(self, mock_glob):
        pattern = 'some_locker_path/some_locker_prefix*.locker'
        misc.get_lockers(pattern)
        mock_glob.called_once_with(pattern)

    @mock.patch('rsempipeline.utils.misc.touch')
    def test_create_locker(self, mock_touch):
        locker = 'some_locker_path/some_locker_prefix.15-01-01_01:01:01.locker'
        misc.create_locker(locker)
        mock_touch.called_once_with(locker)

    @mock.patch('rsempipeline.utils.misc.os')
    def test_remove_locker(self, mock_os):
        locker = 'some_locker_path/some_locker_prefix.15-01-01_01:01:01.locker'
        misc.remove_locker(locker)
        mock_os.remove.called_once_with(locker)

    @mock.patch('rsempipeline.utils.misc.get_lockers')
    def test_lockit_with_one_existent_locker(self, mock_get_lockers):
        mock_get_lockers.return_value = ['existent_locker']
        @misc.lockit('some_pattern')
        def f():
            return 1

        with LogCapture() as L:
            self.assertIsNone(f())
        log_msg = (
"""Nothing is done because the previous run of f hasn\'t completed yet with the following locker(s) found:
    existent_locker""")
        L.check(('rsempipeline.utils.misc', 'INFO', log_msg),)

    @mock.patch('rsempipeline.utils.misc.get_lockers')
    def test_lockit_with_two_existent_lockers(self, mock_get_lockers):
        mock_get_lockers.return_value = ['existent_locker1', 'existent_locker2']
        @misc.lockit('some_locker_path/some_locker_prefix')
        def f():
            return 1

        with LogCapture() as L:
            self.assertIsNone(f())

        log_msg = (
"""Nothing is done because the previous run of f hasn\'t completed yet with the following locker(s) found:
    existent_locker1
    existent_locker2""")

        L.check(('rsempipeline.utils.misc', 'INFO', log_msg),)

    @mock.patch('rsempipeline.utils.misc.datetime')
    @mock.patch('rsempipeline.utils.misc.remove_locker')
    @mock.patch('rsempipeline.utils.misc.create_locker')
    @mock.patch('rsempipeline.utils.misc.get_lockers')
    def test_lockit_with_no_existent_locker(self, mock_get_lockers, mock_create_locker,
                                            mock_remove_locker, mock_datetime):
        mock_get_lockers.return_value = []
        mock_datetime.now().strftime.return_value = '15-01-01_01:01:01'
        @misc.lockit('some_locker_path/some_locker_prefix')
        def f():
            return 1
        self.assertEqual(f(), 1)
        mock_create_locker.called_once_with('some_locker_path/some_locker_prefix.15-01-01_01:01:01.locker')
        mock_remove_locker.called_once_with('some_locker_path/some_locker_prefix.15-01-01_01:01:01.locker')

    @mock.patch('rsempipeline.utils.misc.datetime')
    @mock.patch('rsempipeline.utils.misc.remove_locker')
    @mock.patch('rsempipeline.utils.misc.create_locker')
    @mock.patch('rsempipeline.utils.misc.get_lockers')
    def test_lockit_with_no_existent_locker_but_lockited_function_raise_an_exception(
            self, mock_get_lockers, mock_create_locker, mock_remove_locker, mock_datetime):
        mock_get_lockers.return_value = []
        mock_datetime.now().strftime.return_value = '15-01-01_01:01:01'
        @misc.lockit('some_locker_path/some_locker_prefix')
        def f():
            raise Exception('something is wrong')

        with LogCapture() as L:
            f()
        L.check(('rsempipeline.utils.misc', 'ERROR', 'something is wrong'),)
        mock_create_locker.called_once_with('some_locker_path/some_locker_prefix.15-01-01_01:01:01.locker')
        mock_remove_locker.called_once_with('some_locker_path/some_locker_prefix.15-01-01_01:01:01.locker')

    @mock.patch('rsempipeline.utils.misc.time')
    def test_timeit(self, mock_time):
        mock_time.time.return_value = 1
        @misc.timeit
        def f():
            return
        with LogCapture() as L:
            f()
        L.check(('rsempipeline.utils.misc', 'INFO', 'time spent on f: 0.00s'),)

    @mock.patch('rsempipeline.utils.misc.os.rename')
    @mock.patch('rsempipeline.utils.misc.os.path.exists')
    def test_backup_file(self, mock_exists, mock_rename):
        # Need to mock multiple return values for mock_exists because the file
        # to backup has to exist, but the backuped file with new name cannnot
        # exist
        # http://stackoverflow.com/questions/24897145/python-mock-multiple-return-values
        mock_exists.side_effect = [True, False]
        with LogCapture() as L:
            misc.backup_file('path/file')
        L.check(('rsempipeline.utils.misc', 'INFO', 'Backing up path/file to path/#file.1#'),)
        mock_rename.called_once_with('path/file', 'path/#file.1#')

    @mock.patch('rsempipeline.utils.misc.os.rename')
    @mock.patch('rsempipeline.utils.misc.os.path.exists')
    def test_backup_file_with_previously_backuped_file(self, mock_exists, mock_rename):
        mock_exists.side_effect = [True, True, False]
        with LogCapture() as L:
            misc.backup_file('path/file')
        L.check(('rsempipeline.utils.misc', 'INFO', 'Backing up path/file to path/#file.2#'),)
        mock_rename.called_once_with('path/file', 'path/#file.2#')

    @mock.patch('rsempipeline.utils.misc.os')
    def test_backup_file_with_nonexistent_file(self, mock_os):
        mock_os.path.exists.return_value = False
        with LogCapture() as L:
            misc.backup_file('some_nonexistent_file')
        L.check(('rsempipeline.utils.misc', 'WARNING', 'some_nonexistent_file doesn\'t exist'),)

    @mock.patch('rsempipeline.utils.misc.os.utime')
    def test_touch(self, mock_utime):
        f = 'test_to_be_touched'
        self.assertFalse(os.path.exists(f))
        misc.touch(f)
        self.assertTrue(os.path.exists(f))
        os.remove(f)
        self.assertFalse(os.path.exists(f))
        mock_utime.called_once_with(f, None)

    def test_gen_completion_stamp(self):
        self.assertEqual(misc.gen_completion_stamp('some_key', 'some_dir'),
                         'some_dir/some_key.COMPLETE')

    def test_get_config(self):
        with mock.patch('rsempipeline.utils.misc.open',
                        mock.mock_open(read_data='a: b')):
            self.assertEqual(misc.get_config('config.yaml'), {'a': 'b'})

    @log_capture()
    def test_get_config_config_file_not_exist(self, L):
        self.assertRaises(IOError, misc.get_config, 'non_existent_config.yaml')
        L.check(('rsempipeline.utils.misc', 'ERROR',
                 'configuration file: non_existent_config.yaml not found'),)

    @log_capture()
    def test_get_config_invalid_format(self, L):
        # from yaml import YAMLError
        import yaml
        with mock.patch('rsempipeline.utils.misc.open',
                        mock.mock_open(read_data='a\nb:')):
            # If the YAML parser encounters an error condition, it raises an
            # exception which is an instance of YAMLError or of its subclass,
            # so both the following two lines can capture the exception.
            self.assertRaises(yaml.scanner.ScannerError, misc.get_config, 'invalid_yaml.yaml')
            # self.assertRaises(yaml.YAMLError, misc.get_config, 'invalid_yaml.yaml')
            L.check(('rsempipeline.utils.misc', 'ERROR',
                     'potentially invalid yaml format in invalid_yaml.yaml'),)


    def test_pretty_usage(self):
        self.assertEqual(misc.pretty_usage(1000), '1000.0 bytes')
        self.assertEqual(misc.pretty_usage(1023), '1023.0 bytes')
        self.assertEqual(misc.pretty_usage(1024), '1.0 KB')
        self.assertEqual(misc.pretty_usage(1025), '1.0 KB')
        self.assertEqual(misc.pretty_usage(-1000), '-1000.0 bytes')
        self.assertEqual(misc.pretty_usage(-1023), '-1023.0 bytes')
        self.assertEqual(misc.pretty_usage(-1024), '-1.0 KB')
        self.assertEqual(misc.pretty_usage(-1025), '-1.0 KB')
        self.assertEqual(misc.pretty_usage(1024 ** 3), '1.0 GB')
        self.assertEqual(misc.pretty_usage(1.5 * 1024 ** 3), '1.5 GB')
        self.assertEqual(misc.pretty_usage(1.59 * 1024 ** 3), '1.6 GB')
        self.assertEqual(misc.pretty_usage(1.69 * 1024 ** 4), '1.7 TB')
        self.assertEqual(misc.pretty_usage(1.69 * 1024 ** 5), '1.7 PB')

    def test_ugly_usage(self):
        self.assertEqual(misc.ugly_usage('1024.0 bytes'), 1024)
        self.assertEqual(misc.ugly_usage('1      kb'), 1024)
        self.assertEqual(misc.ugly_usage('1 kb'), 1024)
        self.assertEqual(misc.ugly_usage('1kb'), 1024)
        self.assertEqual(misc.ugly_usage('1.5MB'), 1.5 * 1024 ** 2)
        self.assertEqual(misc.ugly_usage('1GB'), 1024 ** 3)
        self.assertEqual(misc.ugly_usage('1.5 TB'), 1.5 * 1024 ** 4)
        self.assertEqual(misc.ugly_usage('1.5 PB'), 1.5 * 1024 ** 5)
        self.assertRaises(ValueError, misc.ugly_usage, 'invalid size')
        # exabyte is not handled yet
        self.assertRaises(ValueError, misc.ugly_usage, '1.5 EB')

    def test_disk_used(self):
        fake_dir = tempfile.mkdtemp(suffix='_rsem_testing')
        # Good to know: that the size of a directory can be very different on
        # different systems
        # print size_fake_dir # 4096 or maybe 6 (on travis)
        size_fake_dir = os.path.getsize(fake_dir)
        with tempfile.NamedTemporaryFile(dir=fake_dir, delete=False) as fake_f1:
            pass                # no content
        size_fake_f1 = os.path.getsize(fake_f1.name)
        with tempfile.NamedTemporaryFile(dir=fake_dir, delete=False) as fake_f2:
            fake_f2.write('1')  # one character
        size_fake_f2 = os.path.getsize(fake_f2.name)
        with tempfile.NamedTemporaryFile(dir=fake_dir, delete=False) as fake_f3:
            fake_f3.write('123')  # three character
        size_fake_f3 = os.path.getsize(fake_f3.name)
        self.assertEqual(size_fake_f1, 0)
        self.assertEqual(size_fake_f2, 1)
        self.assertEqual(size_fake_f3, 3)
        # NOTE: the size of directory is not included!
        self.assertEqual(misc.disk_used(fake_dir), 4)
        shutil.rmtree(fake_dir)
        self.assertFalse(os.path.exists(fake_dir))


    @mock.patch('rsempipeline.utils.misc.subprocess')
    def test_get_local_free_disk_space(self, mock_subprocess):
        # -k make sure the unit is KB
        fake_df_cmd = 'some_command like "df -k -P /path/to/dir"'
        mock_subprocess.Popen().communicate.return_value = (
            'Filesystem     1024-blocks       Used  Available Capacity Mounted on\nisaac:/btl2    11111111111 2222222222 3333333333      13% /projects/btl2\n',
            None)
        self.assertEqual(misc.disk_free(fake_df_cmd), 3333333333 * 1024)

    def test_calc_free_space_to_use(self):
        # The following 3 assertions correspond to 3 cases where max_usage
        # could point to
        self.assertEqual(misc.calc_free_space_to_use(10, 90, 50, 5), 0)
        self.assertEqual(misc.calc_free_space_to_use(10, 90, 50, 20), 10)
        self.assertEqual(misc.calc_free_space_to_use(10, 90, 50, 90), 40)
        # when free < min_free
        self.assertEqual(misc.calc_free_space_to_use(10, 90, 100, 200), 0)

    def test_is_empty_dir(self):
        self.assertFalse(misc.is_empty_dir('/p', ['/p', '/p/a.txt']))
        self.assertTrue(misc.is_empty_dir('/p', ['/p', '/s', '/s/a.txt']))

    @mock.patch('rsempipeline.utils.misc.paramiko')
    def test_sshexec(self, mock_paramiko):
        mock_paramiko.Transport().open_session().makefile().readlines.return_value = ['some_output\n']
        self.assertEqual(misc.sshexec('cmd', 'host', 'username'), ['some_output\n'])
        # interestingly, Transpost is called twice while in the code, it's
        # explicitly called only once
        self.assertEqual(mock_paramiko.Transport.call_args_list,
                         [mock.call(), mock.call(('host', 22))])

    @mock.patch('rsempipeline.utils.misc.paramiko')
    def test_sshexec_execution_failed_remotely(self, mock_paramiko):
        mock_paramiko.Transport().open_session().makefile().readlines.return_value = None
        self.assertIsNone(misc.sshexec('cmd', 'host', 'username'))
        self.assertEqual(mock_paramiko.Transport.call_args_list,
                         [mock.call(), mock.call(('host', 22))])


if __name__ == "__main__":
    unittest.main()
