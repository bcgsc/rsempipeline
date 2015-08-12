import unittest
import mock

from testfixtures import log_capture

from rsempipeline.core import rp_transfer as RP_T
from rsempipeline.utils.objs import Series, Sample


class RPRunTestCase(unittest.TestCase):
    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec')
    def test_get_remote_free_disk(self, mock_sshexec):
        mock_sshexec.return_value = [
            'Filesystem         1024-blocks      Used Available Capacity Mounted on\n',
            '/dev/analysis        16106127360 13106127360 3000000000      82% /extscratch\n']
        res = RP_T.get_remote_free_disk_space('df -k -P target_dir', 'remote', 'username')
        self.assertEqual(res, 3072e9)

    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec')
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

    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec')
    def test_fetch_remote_file_list_remote_down(self, mock_sshexec):
        mock_sshexec.return_value = None
        self.assertRaisesRegexp(
            ValueError,
            'cannot estimate current usage on remote host. Please check r_dir exists on remote, or remote may be down',
            RP_T.fetch_remote_file_list, 'remote', 'username', 'r_dir')

    @mock.patch('rsempipeline.core.rp_transfer.estimate_rsem_usage')
    @mock.patch('rsempipeline.core.rp_transfer.fetch_remote_file_list')
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
        self.assertEqual(RP_T.estimate_current_remote_usage('remote', 'username', '/path/to', '/l_path/to'),
                         1e3)

    @mock.patch('rsempipeline.core.rp_transfer.misc.sshexec')
    def test_get_real_current_usage(self, mock_sshexec):
        mock_sshexec.return_value = ['3096\t/path/to/top_outdir\n'] # in KB
        self.assertEqual(RP_T.get_real_current_usage('remote', 'username', 'r_dir'),
                         3170304) # in byte

    @mock.patch('rsempipeline.core.rp_transfer.PPR.estimate_sra2fastq_usage')
    def test_estimate_rsem_usage(self, mock_estimate_sra2fastq_usage):
        mock_estimate_sra2fastq_usage.return_value = 1e5
        self.assertEqual(RP_T.estimate_rsem_usage('gsm_dir', 5), 5e5)

    def test_get_gsms_transferred_record_file_not_exist(self):
        self.assertEqual(RP_T.get_gsms_transferred('nonexistent_transferred_GSMs.txt'), [])

    @mock.patch('rsempipeline.core.rp_transfer.os.path.exists')
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
