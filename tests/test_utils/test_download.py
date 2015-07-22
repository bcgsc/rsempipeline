import mock
import unittest

from rsempipeline.utils import download
from rsempipeline.utils.objs import Series, Sample


SRA_INFO_YAML_SINGLE_SRA = """- SRX685892/SRR1557065/SRR1557065.sra:
    readable_size: 2.4 GB
    size: 2546696608"""


SRA_INFO_YAML_MULTIPLE_SRAS = """- SRX135160/SRR453140/SRR453140.sra:
    readable_size: 3.2 GB
    size: 3458148669
- SRX135160/SRR453141/SRR453141.sra:
    readable_size: 3.2 GB
    size: 3460637077
- SRX135160/SRR453142/SRR453142.sra:
    readable_size: 3.8 GB
    size: 4106298857
- SRX135160/SRR453143/SRR453143.sra:
    readable_size: 3.3 GB
    size: 3569561477"""


class UtilsDownloadTestCase(unittest.TestCase):
    def test_gen_orig_params_per_with_a_single_sra(self):
        # mock a series and sample
        series = Series('GSE123456', 'GSE123456_family.soft.subset')
        sample = Sample('GSM1', series)
        sample.outdir = 'some_outdir/GSE123456/some_species/GSM1'
        series.add_passed_sample(sample)

        with mock.patch('rsempipeline.utils.download.open',
                        mock.mock_open(read_data=SRA_INFO_YAML_SINGLE_SRA),
                        create=True):
            vals = download.gen_orig_params_per(sample)
        self.assertEqual(vals, [
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX685892/SRR1557065/SRR1557065.sra',
                    'some_outdir/GSE123456/some_species/GSM1/SRR1557065.sra.download.COMPLETE'], sample]])

    
    def test_gen_orig_params_per_with_multiple_sras(self):
        # mock a series and sample
        series = Series('GSE123456', 'GSE123456_family.soft.subset')
        sample = Sample('GSM1', series)
        sample.outdir = 'some_outdir/GSE123456/some_species/GSM1'
        series.add_passed_sample(sample)

        with mock.patch('rsempipeline.utils.download.open',
                        mock.mock_open(read_data=SRA_INFO_YAML_MULTIPLE_SRAS),
                        create=True):
            vals = download.gen_orig_params_per(sample)
        self.assertEqual(vals, [
            # in the format of input, outputs, other params
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453140/SRR453140.sra', 'some_outdir/GSE123456/some_species/GSM1/SRR453140.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453141/SRR453141.sra', 'some_outdir/GSE123456/some_species/GSM1/SRR453141.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453142/SRR453142.sra', 'some_outdir/GSE123456/some_species/GSM1/SRR453142.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453143/SRR453143.sra', 'some_outdir/GSE123456/some_species/GSM1/SRR453143.sra.download.COMPLETE'], sample]
        ])

    @mock.patch('rsempipeline.utils.download.gen_orig_params_per')
    def test_gen_orig_params(self, mock_gen):
        mock_gen.return_value = [None, ['path/to/sra', 'path/to/sra.download.COMPLETE'], 'mock_sample']
        mock_samples = ['mock_sample'] * 2
        self.assertEqual(download.gen_orig_params(mock_samples),
                         [None, ['path/to/sra', 'path/to/sra.download.COMPLETE'], 'mock_sample',
                          None, ['path/to/sra', 'path/to/sra.download.COMPLETE'], 'mock_sample'])
