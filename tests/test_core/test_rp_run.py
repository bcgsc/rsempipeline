# -*- coding: utf-8 -*

import unittest
import mock
import types

from testfixtures import log_capture

from rsempipeline.core import rp_run
from rsempipeline.utils.objs import Series, Sample

class RPRunTestCase(unittest.TestCase):
    @mock.patch('rsempipeline.core.rp_run.samples', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.gen_orig_params', autospec=True)
    def test_originate_params(self, mock_gen, mock_samples):
        series = Series('GSE123456', 'GSE123456_family.soft.subset')
        sample = Sample('GSM1', series)
        return_val  = [
            # in the format of input, outputs, other params
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453140/SRR453140.sra',
                    'some_outdir/GSE123456/some_species/GSM1/SRR453140.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453141/SRR453141.sra',
                    'some_outdir/GSE123456/some_species/GSM1/SRR453141.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453142/SRR453142.sra',
                    'some_outdir/GSE123456/some_species/GSM1/SRR453142.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453143/SRR453143.sra',
                    'some_outdir/GSE123456/some_species/GSM1/SRR453143.sra.download.COMPLETE'], sample]
        ]
        mock_gen.return_value = return_val
        res = rp_run.originate_params()
        self.assertIsInstance(res, types.GeneratorType)
        self.assertEqual(list(res), return_val)
