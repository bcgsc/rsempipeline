import os
import shutil
import unittest
import logging
import logging.config
import tempfile

import mock
from testfixtures import log_capture
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

if __name__ == "__main__":
    unittest.main()
