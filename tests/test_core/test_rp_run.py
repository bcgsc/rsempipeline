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
            [None, ['some_outdir/rsem_output/GSE123456/some_species/GSM1/SRX135160/SRR453140/SRR453140.sra',
                    'some_outdir/rsem_output/GSE123456/some_species/GSM1/SRR453140.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/rsem_output/GSE123456/some_species/GSM1/SRX135160/SRR453141/SRR453141.sra',
                    'some_outdir/rsem_output/GSE123456/some_species/GSM1/SRR453141.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/rsem_output/GSE123456/some_species/GSM1/SRX135160/SRR453142/SRR453142.sra',
                    'some_outdir/rsem_output/GSE123456/some_species/GSM1/SRR453142.sra.download.COMPLETE'], sample],
            [None, ['some_outdir/rsem_output/GSE123456/some_species/GSM1/SRX135160/SRR453143/SRR453143.sra',
                    'some_outdir/rsem_output/GSE123456/some_species/GSM1/SRR453143.sra.download.COMPLETE'], sample]
        ]
        mock_gen.return_value = return_val
        res = rp_run.originate_params()
        self.assertIsInstance(res, types.GeneratorType)
        self.assertEqual(list(res), return_val)

    @mock.patch('rsempipeline.core.rp_run.options', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.config', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.os', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.misc.execute', autospec=True)
    def test_download(self, mock_execute, mock_os, mock_config, mock_options):
        series = Series('GSE31555', 'GSE31555_family.soft.subset')
        sample = Sample('GSM783253', series, url='ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX093/SRX093321')
        outputs = ['some_outdir/rsem_output/GSE31555/some_species/GSM783253/SRX093321/SRR333831/SRR333831.sra',
                   'some_outdir/rsem_output/GSE31555/some_species/GSM1/SRR333831.sra.download.COMPLETE']
        mock_os.path.exists.return_value = False
        cmd = '''/path/to/ascp 
-i /path/to/.aspera/connect/etc/asperaweb_id_dsa.putty 
--ignore-host-key 
-QT 
-L some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453140/
-k2 
-l 300m 
anonftp@ftp-trace.ncbi.nlm.nih.gov:/sra/sra-instant/reads/ByExp/sra/SRX/SRX093/SRX093321 some_outdir/rsem_output/GSE31555/some_species/GSM783253/SRX093321/SRR333831''',
        mock_config.__getitem__().format.return_value = cmd
        mock_options.debug = False
        mock_execute.return_value = 0
        rp_run.download(None, outputs, sample)
        mock_execute.assert_called_once_with(
            cmd,
            '<GSM783253 (0/0/0) of GSE31555 at None>',
            'some_outdir/rsem_output/GSE31555/some_species/GSM1/SRR333831.sra.download.COMPLETE',
            False)

    @mock.patch('rsempipeline.core.rp_run.options', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.config', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.os', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.misc.execute', autospec=True)
    def test_download_ascp_failed_use_cmd_instead(self, mock_execute, mock_os, mock_config, mock_options):
        series = Series('GSE31555', 'GSE31555_family.soft.subset')
        sample = Sample('GSM783253', series, url='ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX093/SRX093321')
        outputs = ['some_outdir/rsem_output/GSE31555/some_species/GSM783253/SRX093321/SRR333831/SRR333831.sra',
                   'some_outdir/rsem_output/GSE31555/some_species/GSM1/SRR333831.sra.download.COMPLETE']
        mock_os.path.exists.return_value = False
        ascp_cmd = '''/path/to/ascp 
-i /path/to/.aspera/connect/etc/asperaweb_id_dsa.putty 
--ignore-host-key 
-QT 
-L some_outdir/GSE123456/some_species/GSM1/SRX135160/SRR453140/
-k2 
-l 300m 
anonftp@ftp-trace.ncbi.nlm.nih.gov:/sra/sra-instant/reads/ByExp/sra/SRX/SRX093/SRX093321 some_outdir/rsem_output/GSE31555/some_species/GSM783253/SRX093321/SRR333831''',
        wget_cmd = '''wget ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX093/SRX093321
-P some_outdir/rsem_output/GSE31555/some_species/GSM783253/SRX093321/SRR333831 -N'''
        mock_config.__getitem__().format.side_effect = [ascp_cmd, wget_cmd]
        mock_options.debug = False
        # assume ascp fails with 1, wget succeeds with 0
        mock_execute.side_effect = [1, 0]
        rp_run.download(None, outputs, sample)
        mock_execute.assert_has_calls([
            mock.call(ascp_cmd,
                      '<GSM783253 (0/0/0) of GSE31555 at None>',
                      'some_outdir/rsem_output/GSE31555/some_species/GSM1/SRR333831.sra.download.COMPLETE',
                      False),
            mock.call(wget_cmd,
                      '<GSM783253 (0/0/0) of GSE31555 at None>',
                      'some_outdir/rsem_output/GSE31555/some_species/GSM1/SRR333831.sra.download.COMPLETE',
                      False)])

    @mock.patch('rsempipeline.core.rp_run.options', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.config', autospec=True)
    @mock.patch('rsempipeline.core.rp_run.misc.execute_log_stdout_stderr', autospec=True)
    def test_sra2_fastq(self, mock_execute, mock_config, mock_options):
        cmd = '''fastq-dump 
--minReadLen 25 --gzip --split-files --outdir some_outdir/rsem_output/GSE99999/some_species/GSM999999 
some_outdir/rsem_output/GSE99999/some_species/GSM999999/SRX999999/SRR999999/SRR999999.sra'''
        flag_file = 'some_outdir/rsem_output/GSE99999/some_species/GSM999999/SRX999999/SRR999999/SRR999999.sra.sra2fastq.COMPLETE'
        mock_config.__getitem__().format.return_value = cmd
        mock_options.debug = False
        rp_run.sra2fastq(['some_outdir/rsem_output/GSE99999/some_species/GSM999999/SRX999999/SRR999999/SRR999999.sra',
                          'some_outdir/rsem_output/GSE99999/some_species/GSM999999/SRR999999.sra.download.COMPLETE'],
                         [flag_file])
        mock_execute.assert_called_once_with(cmd, flag_file=flag_file, debug=False)
