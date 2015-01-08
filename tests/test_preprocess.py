import unittest
import logging.config

from testfixtures import LogCapture

from rsempipeline.preprocess import utils
from rsempipeline.conf.settings import RP_PREP_LOGGING_CONFIG
logging.config.fileConfig(RP_PREP_LOGGING_CONFIG)


class UtilsProcessTestCase(unittest.TestCase):
    def setUp(self):
        self.k1, self.row1 = 1, ['GSE0']
        self.k2, self.row2 = 2, ['GSE0', 'GSM1; GSM2']
        self.k3, self.row3 = 3, ['GSE0', 'GSM1; GSM2', 'some 3rd row']
        self.k4, self.row4 = 4, ['invalid_GSE_name', 'GSM1; GSM2']
        self.k5, self.row5 = 5, ['GSE0', 'invalid_GSM1_name; GSM2']

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
            # self.assertIn('WARNING', str(l))
            L.check(('rsempipeline.preprocess.utils', 'WARNING', 'row 3 is not of len 2'))

    def test_process_row_with_invalid_GSE_name(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k4, self.row4))
            # self.assertIn('WARNING', str(l))
            L.check(('rsempipeline.preprocess.utils', 'WARNING', 'row 4: invalid GSE name: invalid_GSE_name'))

    def test_process_row_with_invalid_GSM_name(self):
        with LogCapture() as L:
            self.assertIsNone(utils.process(self.k5, self.row5))
            s = str(L)
            self.assertIn('rsempipeline.preprocess.utils', s)
            self.assertIn('WARNING', s)
            self.assertIn('row 5: Not all GSMs are of valid names', s)

        
if __name__ == "__main__":
    unittest.main()
