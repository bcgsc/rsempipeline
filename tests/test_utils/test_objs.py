import unittest

import mock

from rsempipeline.utils.objs import Series, Sample

class SeriesTestCase(unittest.TestCase):
    def setUp(self):
        self.series = Series('GSE123456', 'GSE123456_family.soft.subset')

    def test___init__(self):
        self.assertEqual(self.series.name, 'GSE123456')
        self.assertEqual(self.series.passed_samples, [])
        self.assertEqual(self.series.samples, [])
        self.assertEqual(self.series.soft_file, 'GSE123456_family.soft.subset')
        self.assertEqual(self.series.num_passed_samples(), 0)
        self.assertEqual(self.series.num_samples(), 0)

    def test_add_sample(self):
        sample = Sample('GSM1', self.series)
        self.series.add_sample(sample)
        self.assertEqual(self.series.num_samples(), 1)
        self.assertEqual(self.series.num_passed_samples(), 0)

    def test_add_passed_sample(self):
        sample1 = Sample('GSM1', self.series)
        sample2 = Sample('GSM2', self.series)
        self.series.add_sample(sample1)
        self.series.add_passed_sample(sample2)
        self.assertEqual(self.series.num_samples(), 2)
        self.assertEqual(self.series.num_passed_samples(), 1)

    def test___str__(self):
        self.assertEqual(str(self.series), 'GSE123456 (passed: 0/0)')

    def test___repr__(self):
        self.assertEqual(repr(self.series), 'GSE123456 (passed: 0/0)')



class SampleTestCase(unittest.TestCase):
    def setUp(self):
        self.series = Series('GSE123456', 'GSE123456_family.soft.subset')        
        self.sample = Sample('GSM1', self.series)

    def test_is_info_complete(self):
        self.assertFalse(self.sample.is_info_complete())
        self.sample.organism = 'Mus musculus'
        self.assertFalse(self.sample.is_info_complete())
        self.sample.url = 'ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX000/SRX000000'
        self.assertTrue(self.sample.is_info_complete())

    @mock.patch('rsempipeline.utils.objs.Sample.is_info_complete')
    def test_gen_outdir(self, mock_is_info_complete):
        self.sample.organism = 'Mus musculus'
        self.sample.url = 'ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX000/SRX000000'
        self.assertEqual(self.sample.gen_outdir('some_outdir'),
                         'some_outdir/GSE123456/mus_musculus/GSM1')
        mock_is_info_complete.return_value = False
        self.assertRaisesRegexp(ValueError, 'not information complete', 
                                self.sample.gen_outdir, 'some_outdir')

    def test___str__(self):
        # 0/0, not indexed
        self.assertEqual(str(self.sample), '<GSM1 (0/0) of GSE123456>')
        
    def test___repr__(self):
        # 0/0, not indexed
        self.assertEqual(repr(self.sample), '<GSM1 (0/0) of GSE123456>')
