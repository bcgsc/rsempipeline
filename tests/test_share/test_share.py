import os
import unittest

from rsempipeline.conf.settings import SHARE_DIR


class ShareTestCase(unittest.TestCase):
    def test_GSE_GSM_example_csv_exits(self):
        self.assertTrue(os.path.exists(os.path.join(SHARE_DIR,  'GSE_GSM.example.csv')))

    def test_rp_config_example_yml_exists(self):
        self.assertTrue(os.path.exists(os.path.join(SHARE_DIR, 'rp_config.example.yml')))


if __name__ == "__main__":
    unittest.main()
