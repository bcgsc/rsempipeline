import os
import unittest
import mock
import tempfile


from rsempipeline.utils import pre_pipeline_run as ppr


SRA_INFO_YAML_SINGLE_SRA = """- SRX685892/SRR1557065/SRR1557065.sra:
    readable_size: 2.4 GB
    size: 2546696608"""

PARSED_SRA_INFO_YAML_SINGLE_SRA = [
    {
        'SRX685892/SRR1557065/SRR1557065.sra': {
            'readable_size': '2.4 GB',
            'size': 2546696608
        }
    }
]


class PrePipelineRunTestCase(unittest.TestCase):
    



    def test_sanity_check(self):
        self.assertRaises(ValueError, ppr.sanity_check, 1000, 1010)
        self.assertIsNone(ppr.sanity_check(1000, 1000))

    def test_get_rsem_outdir(self):
        self.assertEqual(ppr.get_rsem_outdir('some_outdir'), 'some_outdir/rsem_output')

    @mock.patch('rsempipeline.utils.pre_pipeline_run.os')
    def test_init_sample_outdirs(self, mock_os):
        mock_os.path.exists.return_value = False
        fake_samples = [mock.Mock(), mock.Mock()]
        ppr.init_sample_outdirs(fake_samples, 'some_top_outdir')
        self.assertEqual(mock_os.makedirs.call_count, 2)


    def test_calc_free_space_to_use(self):
        max_usage = 10
        current_usage = 2
        free_space = 20
        min_free = 1
        self.assertEqual(ppr.calc_free_space_to_use(max_usage, current_usage, free_space, min_free), 7)
        free_space = 6
        self.assertEqual(ppr.calc_free_space_to_use(max_usage, current_usage, free_space, min_free), 5)


    @mock.patch('rsempipeline.utils.pre_pipeline_run.is_processed')
    def test_find_gsms_to_process_all_processed(self, mock_is_processed):
        mock_is_processed.return_value = True
        samples = [mock.Mock(), mock.Mock()]
        self.assertEqual(ppr.find_gsms_to_process(samples, 1024 ** 3, False), [])

    @mock.patch('rsempipeline.utils.pre_pipeline_run.is_processed')
    def test_find_gsms_to_process_ignore_disk_usage(self, mock_is_processed):
        mock_is_processed.return_value = False
        samples = [mock.Mock(), mock.Mock()]
        self.assertEqual(ppr.find_gsms_to_process(samples, 1024 ** 3, True), samples)

    @mock.patch('rsempipeline.utils.pre_pipeline_run.estimate_proc_usage')
    @mock.patch('rsempipeline.utils.pre_pipeline_run.is_processed')
    def test_find_gsms_to_process_fit_disk_usage(self, mock_is_processed, mock_estimate_proc_usage):
        mock_is_processed.return_value = False
        mock_estimate_proc_usage.return_value = 513
        samples = [mock.Mock(), mock.Mock()]
        self.assertEqual(ppr.find_gsms_to_process(samples, 1024, False), [samples[0]])

    @mock.patch('rsempipeline.utils.pre_pipeline_run.os')
    def test_is_gen_qsub_script_complete(self, mock_os):
        mock_os.path.exists.return_value = True
        self.assertTrue(ppr.is_gen_qsub_script_complete('some_gsm_dir'))
        mock_os.path.exists.return_value = False
        self.assertFalse(ppr.is_gen_qsub_script_complete('some_gsm_dir'))
        
    @mock.patch('rsempipeline.utils.pre_pipeline_run.os')
    def test_get_sras_info(self, _):
        with mock.patch('rsempipeline.utils.pre_pipeline_run.open',
                        mock.mock_open(read_data=SRA_INFO_YAML_SINGLE_SRA),
                        create=True):
            res = ppr.get_sras_info('some_dir')
        self.assertEqual(res, PARSED_SRA_INFO_YAML_SINGLE_SRA)


    @mock.patch('rsempipeline.utils.pre_pipeline_run.get_sras_info')
    @mock.patch('rsempipeline.utils.pre_pipeline_run.SRA2FASTQ_SIZE_RATIO')
    def test_estimate_proc_usage(self, mock_ratio, mock_get_sras_info):
        mock_ratio = 2
        mock_get_sras_info.return_value = PARSED_SRA_INFO_YAML_SINGLE_SRA
        self.assertEqual(ppr.estimate_proc_usage('some_gsm_dir'), 2546696608 * mock_ratio)

    @mock.patch('rsempipeline.utils.pre_pipeline_run.get_sras_info')
    @mock.patch('rsempipeline.utils.pre_pipeline_run.is_gen_qsub_script_complete')
    @mock.patch('rsempipeline.utils.pre_pipeline_run.is_sra2fastq_complete')
    @mock.patch('rsempipeline.utils.pre_pipeline_run.is_download_complete')
    def test_is_process(self, mock_g, mock_s, mock_d, mock_get_sras_info):
        mock_get_sras_info
        mock_d.return_value = False
        self.assertFalse(ppr.is_processed('some_gsm_dir'))
        mock_d.return_value = True
        mock_s.return_value = False
        self.assertFalse(ppr.is_processed('some_gsm_dir'))
        mock_d.return_value = True
        mock_s.return_value = True
        mock_g.return_value = False
        self.assertFalse(ppr.is_processed('some_gsm_dir'))
        mock_d.return_value = True
        mock_s.return_value = True
        mock_g.return_value = True
        self.assertTrue(ppr.is_processed('some_gsm_dir'))
