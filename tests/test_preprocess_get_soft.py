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

class GetSoftTestCase(unittest.TestCase):
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


class SOFTDownloaderTestCase(unittest.TestCase):
    def setUp(self):
        # der: downloader
        self.der = get_soft.SOFTDownloader()
        self.gse1 = 'GSE45284'    # a real one
        self.gse2 = 'GSE12345678' # a fake one
        self.outdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.outdir)

    def test_base(self):
        self.der.base = 'ftp://ftp.ncbi.nlm.nih.gov'

    def test_get_soft_gz_basename(self):
        self.assertEqual(self.der.get_soft_gz_basename(self.gse1),
                         'GSE45284_family.soft.gz')

    def test_get_gse_mask(self):
        self.assertEqual(self.der.get_gse_mask(self.gse1), 'GSE45nnn')
        self.assertEqual(self.der.get_gse_mask(self.gse2), 'GSE12345nnn')

    def test_get_path(self):
        self.assertEqual(self.der.get_path(self.gse1),
                         '/geo/series/GSE45nnn/GSE45284/soft')

    def test_get_url(self):
        self.assertEqual(self.der.get_url(self.gse1),
                         'ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE45nnn/GSE45284/soft/GSE45284_family.soft.gz')

    def test_retrieve(self):
        path = self.der.get_path(self.gse1)
        soft_gz = self.der.get_soft_gz_basename(self.gse1)
        out = os.path.join(self.outdir, soft_gz)
        self.assertFalse(os.path.exists(out))
        self.der.retrieve(path, soft_gz, out)
        self.assertTrue(os.path.exists(out))

    @log_capture()
    def test_download_soft_gz(self, L):
        soft_gz = self.der.get_soft_gz_basename(self.gse1)
        out = os.path.join(self.outdir, soft_gz)
        self.assertFalse(os.path.exists(out))
        self.der.download_soft_gz(self.gse1, self.outdir)
        self.assertTrue(os.path.exists(out))
        L.check(('rsempipeline.preprocess.get_soft', 'INFO',
                 'downloading GSE45284_family.soft.gz from ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE45nnn/GSE45284/soft/GSE45284_family.soft.gz to {0}'.format(out)))

    # def test_gen_soft


if __name__ == "__main__":
    unittest.main()
