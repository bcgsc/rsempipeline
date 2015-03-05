import os
import re
import unittest
import types
import logging
import logging.config

from testfixtures import LogCapture, log_capture
# https://pythonhosted.org/testfixtures/logging.html
# LogCapture and log_capture are used in different ways to achieve the same
# results

from rsempipeline.preprocess import utils
from rsempipeline.conf.settings import SHARE_DIR, RP_PREP_LOGGING_CONFIG
logging.config.fileConfig(RP_PREP_LOGGING_CONFIG)

from utils import remove


class UtilsProcessTestCase(unittest.TestCase):
    def setUp(self):
        self.k1, self.row1 = 1, ['GSE0']
        self.k2, self.row2 = 2, ['GSE0', 'GSM1; GSM2']
        self.k3, self.row3 = 3, ['GSE0', 'GSM1; GSM2', 'some 3rd row']
        self.k4, self.row4 = 4, ['invalid_GSE_name', 'GSM1; GSM2']
        self.k5, self.row5 = 5, ['GSE0', 'invalid_GSM1_name; GSM2']
        self.k6, self.row6 = 5, ['GSE0', 'GSM1; GSM1']

    def test_process_row_with_1_columns(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k1, self.row1))
            L.check(('rsempipeline.preprocess.utils', 'WARNING', 'row 1 is not of len 2'))

    def test_process_row_with_2_columns(self):
        self.assertEqual(('GSE0', ['GSM1', 'GSM2']),
                          utils.process(self.k2, self.row2))

    def test_process_row_with_3_columns(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k3, self.row3))
            L.check(('rsempipeline.preprocess.utils', 'WARNING', 'row 3 is not of len 2'))

    def test_process_row_with_invalid_GSE_name(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k4, self.row4))
            L.check(('rsempipeline.preprocess.utils', 'WARNING', 'row 4: invalid GSE name: invalid_GSE_name'))

    def test_process_row_with_invalid_GSM_name(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k5, self.row5))
            s = str(L)
            self.assertIn('rsempipeline.preprocess.utils', s)
            self.assertIn('WARNING', s)
            self.assertIn('row 5: Not all GSMs are of valid names', s)

    def test_process_row_with_duplicated_GSMs(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k6, self.row6))
            L.check(('rsempipeline.preprocess.utils', 'WARNING',
                     'Duplicated GSMs found in GSE0. of 2 GSMs, only 1 are unique'))


class UtilsReadTestCase(unittest.TestCase):
    def setUp(self):
        self.valid_input1 = '___valid1.csv'
        self.valid_input2 = '___valid2.csv'
        self.valid_input3 = '___valid3.csv'
        self.invalid_input = '___invalid_GSE_GSM.csv'
        with open(self.valid_input1, 'wb') as opf1:
            opf1.write(
"""
GSE1,GSM10; GSM11
GSE2,GSM20; GSM21; GSM22
""")
        with open(self.valid_input2, 'wb') as opf2:
            opf2.write(
"""
GSE1,GSM10; GSM11
GSE2,GSM20; GSM21; GSM22;
""")
        with open(self.valid_input3, 'wb') as opf3:
            opf3.write(
"""
GSE1,GSM10; GSM11
# GSE2,GSM20; GSM21; GSM22;
""")

        with open(self.invalid_input, 'wb') as opf3:
            opf3.write(
"""
xGSE1,GSM10; GSM11
GSE2,GSM20; GSM21; GSM22;
""")

        self.RE_GSE = re.compile('^GSE\d+')
        self.RE_GSM = re.compile('^GSM\d+')

    def tearDown(self):
        map(remove, [self.valid_input1, self.valid_input2, self.valid_input3,
                     self.invalid_input])

    def test_valid_input(self):
        self.assertTrue(utils.is_valid(self.valid_input1))

    def test_valid_input_with_appending_semicolon(self):    
        self.assertTrue(utils.is_valid(self.valid_input2))

    def test_valid_input_with_commented_line(self):
        self.assertTrue(utils.is_valid(self.valid_input3))

    def test_invalid_input(self):
        self.assertFalse(utils.is_valid(self.invalid_input))

    def check_yield_result(self, item):
        self.assertEqual(2, len(item))
        self.assertTrue(self.RE_GSE.match(item[0]))
        self.assertTrue(self.RE_GSM.match(item[1]))

    def test_yield_gse_gsm_from_valid_input(self):
        for __ in utils.yield_gse_gsm(self.valid_input1):
            self.check_yield_result(__)

    def test_yield_gse_gsm_from_valid_input_with_appending_semicolon(self):
        for __ in utils.read(self.valid_input2):
            self.check_yield_result(__)

    def test_yield_gse_gsm_from_valid_input_with_commented_line(self):
        for __ in utils.read(self.valid_input3):
            self.check_yield_result(__)

    def test_read_valid_input(self):
        self.assertIsInstance(utils.read(self.valid_input1), types.GeneratorType)

    def test_read_valid_input_with_appending_semicolon(self):
        self.assertIsInstance(utils.read(self.valid_input2), types.GeneratorType)

    def test_read_valid_input_with_commented_line(self):
        self.assertIsInstance(utils.read(self.valid_input3), types.GeneratorType)

    @log_capture(level=logging.ERROR)
    def test_read_invalid_input(self, L):
        # cm: context_manager
        # ref: http://stackoverflow.com/questions/15672151/is-it-possible-for-a-unit-test-to-assert-that-a-method-calls-sys-exit
        # with LogCapture() as L:
        with self.assertRaises(SystemExit) as cm:
            utils.read(self.invalid_input)
            self.assertEqual(cm.exception.code, 1)
        L.check(
            ('rsempipeline.preprocess.utils', 'ERROR',
             'Please correct the invalid entries in {0}'.format(self.invalid_input)),
            ('rsempipeline.preprocess.utils', 'ERROR',
             'If unsure of the correct format, check {0}'.format(os.path.join(SHARE_DIR, 'GSE_GSM.example.csv'))),
        )

if __name__ == "__main__":
    unittest.main()
