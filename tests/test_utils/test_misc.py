import os
import shutil
import unittest
import logging
import logging.config
import tempfile

import mock
from testfixtures import LogCapture, log_capture
# https://pythonhosted.org/testfixtures/logging.html
# LogCapture and log_capture are used in different ways to achieve the same
# results

from rsempipeline.utils import misc
# can be any of the logging config file as long as logger for utils is included.
# from rsempipeline.conf.settings import RP_RUN_LOGGING_CONFIG
# logging.config.fileConfig(RP_RUN_LOGGING_CONFIG)


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




if __name__ == "__main__":
    unittest.main()
