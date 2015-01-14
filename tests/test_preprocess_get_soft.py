import os
import shutil
import unittest
import logging
import logging.config
import tempfile

from testfixtures import log_capture
# https://pythonhosted.org/testfixtures/logging.html
# LogCapture and log_capture are used in different ways to achieve the same
# results

from rsempipeline.preprocess import rp_prep, get_soft
from rsempipeline.conf.settings import SOFT_OUTDIR_BASENAME
from rsempipeline.conf.settings import RP_PREP_LOGGING_CONFIG
logging.config.fileConfig(RP_PREP_LOGGING_CONFIG)

class GenCsvTestCase(unittest.TestCase):
    def setUp(self):
        """setUp and tearDown are run for teach test"""
        self.input_csv = '___valid_input.csv'
        with open(self.input_csv, 'wb') as opf1:
            opf1.write(
"""
GSE59813,GSM1446812;
GSE61491,GSM1506106; GSM1506107;
""")
        parser = rp_prep.get_parser()
        self.temp_outdir = tempfile.mkdtemp() # mkdtemp returns abspath
        self.options1 = parser.parse_args(['get-soft', '-f', self.input_csv])
        self.options2 = parser.parse_args(['get-soft', '-f', self.input_csv,
                                           '--outdir', self.temp_outdir])
        self.gse = 'GSE38003'
        self.gsm = 'GSM931711'
        
    def tearDown(self):
        os.remove(self.input_csv)
        shutil.rmtree(self.temp_outdir)

    def test_gen_soft_outdir(self):
        d = os.path.join(self.temp_outdir, SOFT_OUTDIR_BASENAME)
        self.assertEqual(d, get_soft.gen_soft_outdir(self.temp_outdir))
        self.assertTrue(os.path.exists(d))

if __name__ == "__main__":
    unittest.main()
