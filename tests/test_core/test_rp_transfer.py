import datetime

import unittest
import mock

from testfixtures import log_capture

from rsempipeline.core import rp_transfer as RP_T
from rsempipeline.utils.objs import Series, Sample


class RPRunTestCase(unittest.TestCase):
    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec', autospec=True)
    def test_get_remote_free_disk(self, mock_sshexec):
        mock_sshexec.return_value = [
            'Filesystem         1024-blocks      Used Available Capacity Mounted on\n',
            '/dev/analysis        16106127360 13106127360 3000000000      82% /extscratch\n']
        cmd = 'df -k -P target_dir'
        res = RP_T.get_remote_free_disk_space('remote', 'username', cmd)
        self.assertEqual(res, 3072e9)

    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec', autospec=True)
    def test_fetch_remote_file_list(self, mock_sshexec):
        mock_sshexec.return_value = [
                             '/path/to/rsemoutput\n',
                             '/path/to/rsemoutput/GSE1\n',
                             '/path/to/rsemoutput/GSE1/homo_sapiens\n',
                             '/path/to/rsemoutput/GSE1/homo_sapiens/GSM1\n',
                             '/path/to/rsemoutput/GSE2\n',
                             '/path/to/rsemoutput/GSE2/homo_sapiens\n',
                             '/path/to/rsemoutput/GSE2/homo_sapiens/GSM2\n']
        self.assertEqual(RP_T.fetch_remote_file_list('remote', 'username', 'r_dir'),
                         [
                            '/path/to/rsemoutput',
                             '/path/to/rsemoutput/GSE1',
                             '/path/to/rsemoutput/GSE1/homo_sapiens',
                             '/path/to/rsemoutput/GSE1/homo_sapiens/GSM1',
                             '/path/to/rsemoutput/GSE2',
                             '/path/to/rsemoutput/GSE2/homo_sapiens',
                             '/path/to/rsemoutput/GSE2/homo_sapiens/GSM2'])

    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec', autospec=True)
    def test_fetch_remote_file_list_remote_down(self, mock_sshexec):
        mock_sshexec.return_value = None
        self.assertRaisesRegexp(
            ValueError,
            'cannot estimate current usage on remote host. Please check r_dir exists on remote, or remote may be down',
            RP_T.fetch_remote_file_list, 'remote', 'username', 'r_dir')

    @mock.patch('rsempipeline.core.rp_transfer.estimate_rsem_usage', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.fetch_remote_file_list', autospec=True)
    def test_estimate_current_remote_usage(self, mock_fetch, mock_est):
        mock_fetch.return_value = [
            '/path/to/rsemoutput',
            '/path/to/rsemoutput/GSE1',
            '/path/to/rsemoutput/GSE1/homo_sapiens', 
            '/path/to/rsemoutput/GSE1/homo_sapiens/GSM1', # completed
            '/path/to/rsemoutput/GSE1/homo_sapiens/GSM1/rsem.COMPLETE',
            '/path/to/rsemoutput/GSE2',
            '/path/to/rsemoutput/GSE2/homo_sapiens',
            '/path/to/rsemoutput/GSE2/homo_sapiens/GSM2', # empty dirempty 
            '/path/to/rsemoutput/GSE3',
            '/path/to/rsemoutput/GSE3/homo_sapiens',
            '/path/to/rsemoutput/GSE3/homo_sapiens/GSM3',
            '/path/to/rsemoutput/GSE3/homo_sapiens/GSM3/some.fq.gz'
        ]
        mock_est.return_value = 1e3
        self.assertEqual(RP_T.estimate_current_remote_usage('remote', 'username', '/path/to', '/l_path/to', 5),
                         1e3)

    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec', autospec=True)
    def test_get_real_current_usage(self, mock_sshexec):
        mock_sshexec.return_value = ['3096\t/path/to/top_outdir\n'] # in KB
        self.assertEqual(RP_T.get_real_current_usage('remote', 'username', 'r_dir'),
                         3170304) # in byte

    @mock.patch('rsempipeline.core.rp_transfer.PPR.estimate_sra2fastq_usage', autospec=True)
    def test_estimate_rsem_usage(self, mock_estimate_sra2fastq_usage):
        mock_estimate_sra2fastq_usage.return_value = 1e5
        self.assertEqual(RP_T.estimate_rsem_usage('gsm_dir', 5), 5e5)

    def test_get_gsms_transferred_record_file_not_exist(self):
        self.assertEqual(RP_T.get_gsms_transferred('nonexistent_transferred_GSMs.txt'), [])

    @mock.patch('rsempipeline.core.rp_transfer.os.path.exists', autospec=True)
    def test_get_gsms_transferred(self, mock_exists):
        mock_exists.return_value = True
        # known issue, mock_open doesn't implement iteration,
        # http://stackoverflow.com/questions/24779893/customizing-unittest-mock-mock-open-for-iteration
        # cannot successfully implement what is suggested in this thread, but
        # my such hack works
        m = mock.mock_open()
        m.return_value.__iter__.return_value = [
            '# 15-07-17 09:04:38',
            'rsem_output/GSE34736/homo_sapiens/GSM854343',
            'rsem_output/GSE34736/homo_sapiens/GSM854344'
        ]
        with mock.patch('rsempipeline.core.rp_transfer.open', m):
            self.assertEqual(RP_T.get_gsms_transferred('transferred_GSMs.txt'),
                             ['rsem_output/GSE34736/homo_sapiens/GSM854343',
                              'rsem_output/GSE34736/homo_sapiens/GSM854344'])

    @mock.patch('rsempipeline.core.rp_transfer.datetime', autospec=True)
    def test_append_transfer_record(self, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2015, 1, 1, 1, 1, 1)
        m = mock.mock_open()
        records = ['rsem_output/GSE1/homo_sapiens/GSM1',
                   'rsem_output/GSE2/homo_sapiens/GSM2']
        with mock.patch('rsempipeline.core.rp_transfer.open', m):
            RP_T.append_transfer_record(records, 'transferred_GSMs.txt')
            self.assertEqual(m().write.call_count, 3)


    # such mocking works, too
    @mock.patch('rsempipeline.core.rp_transfer.datetime.datetime', autospec=True)
    def test_append_transfer_record2(self, mock_datetime):
        mock_datetime.now().strftime.return_value = '15-01-01 01:01:01'
        m = mock.mock_open()
        records = ['rsem_output/GSE1/homo_sapiens/GSM1',
                   'rsem_output/GSE2/homo_sapiens/GSM2']
        with mock.patch('rsempipeline.core.rp_transfer.open', m):
            RP_T.append_transfer_record(records, 'transferred_GSMs.txt')
            m.assert_called_once_with('transferred_GSMs.txt', 'ab')
            expected = [mock.call('# 15-01-01 01:01:01\n'),
                        mock.call('rsem_output/GSE1/homo_sapiens/GSM1\n'),
                        mock.call('rsem_output/GSE2/homo_sapiens/GSM2\n')]
            self.assertEqual(expected, m().write.call_args_list)

    # # Interestingly, such mock doesn't work because of the following error
    # # E           TypeError: can't set attributes of built-in/extension type 'datetime.datetime'
    # # http://stackoverflow.com/questions/4481954/python-trying-to-mock-datetime-date-today-but-not-working
    # @mock.patch('rsempipeline.core.rp_transfer.datetime.datetime.now')
    # def test_append_transfer_record3(self, mock_now):
    #     mock_now.return_value = datetime.datetime(2015, 1, 1, 1, 1, 1)
    #     m = mock.mock_open()
    #     records = ['rsem_output/GSE1/homo_sapiens/GSM1',
    #                'rsem_output/GSE2/homo_sapiens/GSM2']
    #     with mock.patch('rsempipeline.core.rp_transfer.open', m):
    #         RP_T.append_transfer_record(records, 'transferred_GSMs.txt')
    #         self.assertEqual(m().write.call_count, 3)


    @mock.patch('rsempipeline.core.rp_transfer.PPR.is_processed', autospec=True)
    def test_select_gsms_to_transfer_all_processed(self, mock_is_processed):
        mock_is_processed.return_value = False
        m1 = mock.Mock()
        m1.outdir = 'l_top_outdir/rsemoutput/GSE1/homo_sapiens/GSM1'
        m1.name = 'GSM1'
        m2 = mock.Mock()
        m2.outdir = 'l_top_outdir/rsemoutput/GSE2/homo_sapiens/GSM2'
        m2.name = 'GSM2'
        all_gsms = [m1, m2]
        transferred_gsms = ['GSM1']
        self.assertEqual(
            RP_T.select_gsms_to_transfer(
                all_gsms, transferred_gsms, 'l_top_outdir', 1e6, 5), [])

    @mock.patch('rsempipeline.core.rp_transfer.estimate_rsem_usage', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.is_processed', autospec=True)
    def test_select_gsms_to_transfer_with_one_GSM_already_transferred(self, mock_is_processed,
                                                                    mock_estimate_rsem_usage):
        mock_is_processed.return_value = True
        mock_estimate_rsem_usage.return_value = 7e5
        m1 = mock.Mock()
        m1.outdir = 'l_top_outdir/rsemoutput/GSE1/homo_sapiens/GSM1'
        m1.name = 'GSM1'
        m2 = mock.Mock()
        m2.outdir = 'l_top_outdir/rsemoutput/GSE2/homo_sapiens/GSM2'
        m2.name = 'GSM2'
        all_gsms = [m1, m2]
        transferred_gsms = ['GSM1']
        self.assertEqual(
            RP_T.select_gsms_to_transfer(
                all_gsms, transferred_gsms, 'l_top_outdir', 1e6, 5), [m2])


    @mock.patch('rsempipeline.core.rp_transfer.estimate_rsem_usage', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.is_processed', autospec=True)
    def test_select_gsms_to_transfer_with_no_GSM_already_transferred(self, mock_is_processed,
                                                                   mock_estimate_rsem_usage):
        mock_is_processed.return_value = True
        mock_estimate_rsem_usage.return_value = 7e5
        m1 = mock.Mock()
        m1.outdir = 'l_top_outdir/rsemoutput/GSE1/homo_sapiens/GSM1'
        m1.name = 'GSM1'
        m2 = mock.Mock()
        m2.outdir = 'l_top_outdir/rsemoutput/GSE2/homo_sapiens/GSM2'
        m2.name = 'GSM2'
        all_gsms = [m1, m2]
        transferred_gsms = []
        self.assertEqual(
            RP_T.select_gsms_to_transfer(
                all_gsms, transferred_gsms, 'l_top_outdir', 1e6, 5), [m1])


    @mock.patch.object(RP_T.os, 'mkdir', autospec=True)
    @mock.patch.object(RP_T.os.path, 'exists', autospec=True)
    def test_create_transfer_sh_dir(self, mock_exists, mock_mkdir):
        mock_exists.return_value = False
        self.assertEqual(RP_T.create_transfer_sh_dir('l_top_outdir'), 'l_top_outdir/transfer_scripts')
        mock_mkdir.assert_called_once_with('l_top_outdir/transfer_scripts')

        mock_exists.reset_mock()
        mock_mkdir.reset_mock()
        mock_exists.return_value = True
        self.assertEqual(RP_T.create_transfer_sh_dir('l_top_outdir'), 'l_top_outdir/transfer_scripts')
        self.assertFalse(mock_mkdir.called)

    @mock.patch('rsempipeline.core.rp_transfer.create_transfer_sh_dir', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.datetime', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.write', autospec=True)
    def test_write_transfer_sh(self, mock_write, mock_datetime, mock_create):
        mock_create.return_value = 'l_top_outdir/transfer_scripts'
        mock_datetime.datetime.now.return_value = datetime.datetime(2015, 1, 1, 1, 1, 1)
        self.assertEqual(RP_T.write_transfer_sh(
            ['rsem_output/GSE56743/rattus_norvegicus/GSM1367849',
             'rsem_output/GSE56743/rattus_norvegicus/GSM1367850'],
            'rsync_template',
            'l_top_outdir',
            'r_username', 'r_host', 'r_top_outdir'),
                         'l_top_outdir/transfer_scripts/transfer.15-01-01_01:01:01.sh')

    @mock.patch('rsempipeline.core.rp_transfer.get_real_current_usage', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.estimate_current_remote_usage', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.get_remote_free_disk_space', autospec=True)
    def test_calc_remote_free_space_to_use(
            self, mock_get_real, mock_estimate_current, mock_get_remote_free):
        """numbers are intentionally made small for convenience, in real scenario,
        just image multiplying them by a constant factor"""
        mock_get_real.return_value = 1234
        mock_estimate_current.return_value = 10
        mock_get_remote_free.return_value = 90
        r_max_usage = 50
        r_min_free = 20
        res = RP_T.calc_remote_free_space_to_use(
            'r_host', 'r_username', 'r_top_outdir',
            'l_top_outdir', 'r_cmd_df', r_max_usage, r_min_free, 5)
        self.assertEqual(res, 40)

    @mock.patch('rsempipeline.core.rp_transfer.os', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.append_transfer_record', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.misc.execute_log_stdout_stderr', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.write_transfer_sh', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.select_gsms_to_transfer', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.get_gsms_transferred', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.init_sample_outdirs', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.gen_all_samples_from_soft_and_isamp', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.calc_remote_free_space_to_use', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.misc.get_config', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.parse_args_for_rp_transfer', autospec=True)
    def test_main(self, mock_parse, mock_get_config, mock_calc, mock_gen, mock_init,
                  mock_get_gsms_transferred, mock_find_gsms, mock_write_transfer_script,
                  mock_execute, mock_append, mock_os):
        mock_get_config.return_value = {
            'LOCAL_TOP_OUTDIR': 'l_top_outdir',
            'REMOTE_TOP_OUTDIR': 'r_top_outdir',
            'REMOTE_HOST': 'remote',
            'USERNAME': 'username',
            'REMOTE_CMD_DF': 'df -k -P target_dir',
            'REMOTE_MAX_USAGE': '50 GB',
            'REMOTE_MIN_FREE': '20 GB',
            'FASTQ2RSEM_RATIO': 5,
        }
        mock_calc.return_value = 40
        m1 = mock.Mock()
        m1.outdir = 'l_top_outdir/rsemoutput/GSE1/homo_sapiens/GSM1'
        m1.name = 'GSM1'
        m2 = mock.Mock()
        m2.outdir = 'l_top_outdir/rsemoutput/GSE2/homo_sapiens/GSM2'
        m2.name = 'GSM2'
        mock_find_gsms.return_value = [m1, m2]
        mock_execute.return_value = 0
        RP_T.main()
        self.assertTrue(mock_execute.called)
        self.assertTrue(mock_append.called)

    @mock.patch('rsempipeline.core.rp_transfer.os', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.append_transfer_record', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.misc.execute_log_stdout_stderr', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.write_transfer_sh', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.select_gsms_to_transfer', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.get_gsms_transferred', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.init_sample_outdirs', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.gen_all_samples_from_soft_and_isamp', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.calc_remote_free_space_to_use', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.misc.get_config', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.parse_args_for_rp_transfer', autospec=True)
    def test_main_no_GSM_found_for_transfer(
            self, mock_parse, mock_get_config, mock_calc, mock_gen, mock_init,
            mock_get_gsms_transferred, mock_find_gsms, mock_write_transfer_script,
            mock_execute, mock_append, mock_os):
        mock_get_config.return_value = {
            'LOCAL_TOP_OUTDIR': 'l_top_outdir',
            'REMOTE_TOP_OUTDIR': 'r_top_outdir',
            'REMOTE_HOST': 'remote',
            'USERNAME': 'username',
            'REMOTE_CMD_DF': 'df -k -P target_dir',
            'REMOTE_MAX_USAGE': '50 GB',
            'REMOTE_MIN_FREE': '20 GB',
            'FASTQ2RSEM_RATIO': 5,
        }
        mock_calc.return_value = 40
        mock_find_gsms.return_value = []
        mock_execute.return_value = 0
        RP_T.main()
        self.assertFalse(mock_write_transfer_script.called)
        self.assertFalse(mock_execute.called)

    @mock.patch('rsempipeline.core.rp_transfer.os', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.append_transfer_record', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.misc.execute_log_stdout_stderr', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.write_transfer_sh', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.select_gsms_to_transfer', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.get_gsms_transferred', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.init_sample_outdirs', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.PPR.gen_all_samples_from_soft_and_isamp', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.calc_remote_free_space_to_use', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.misc.get_config', autospec=True)
    @mock.patch('rsempipeline.core.rp_transfer.parse_args_for_rp_transfer', autospec=True)
    def test_main_transfer_unsuccessfull(
            self, mock_parse, mock_get_config, mock_calc, mock_gen, mock_init,
            mock_get_gsms_transferred, mock_find_gsms, mock_write_transfer_script,
            mock_execute, mock_append, mock_os):
        mock_get_config.return_value = {
            'LOCAL_TOP_OUTDIR': 'l_top_outdir',
            'REMOTE_TOP_OUTDIR': 'r_top_outdir',
            'REMOTE_HOST': 'remote',
            'USERNAME': 'username',
            'REMOTE_CMD_DF': 'df -k -P target_dir',
            'REMOTE_MAX_USAGE': '50 GB',
            'REMOTE_MIN_FREE': '20 GB',
            'FASTQ2RSEM_RATIO': 5,
        }
        mock_calc.return_value = 40
        m1 = mock.Mock()
        m1.outdir = 'l_top_outdir/rsemoutput/GSE1/homo_sapiens/GSM1'
        m1.name = 'GSM1'
        m2 = mock.Mock()
        m2.outdir = 'l_top_outdir/rsemoutput/GSE2/homo_sapiens/GSM2'
        m2.name = 'GSM2'
        mock_find_gsms.return_value = [m1, m2]
        mock_execute.return_value = 1
        RP_T.main()
        self.assertTrue(mock_execute.called)
        self.assertFalse(mock_append.called)
