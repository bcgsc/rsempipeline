import unittest

from rsempipeline.utils import rsem


class UtilsRsemTestCase(unittest.TestCase):
    def test_gen_fastq_gz_input(self):
        self.assertEqual(rsem.gen_fastq_gz_input(
            [
                'path/to/SRR000000_1.fastq.gz',
                'path/to/SRR000000_2.fastq.gz'
            ]), '--paired-end <(/bin/zcat path/to/SRR000000_1.fastq.gz) <(/bin/zcat path/to/SRR000000_2.fastq.gz)')

        self.assertEqual(rsem.gen_fastq_gz_input(
            [
                'path/to/SRR000000_1.fastq.gz',
                'path/to/SRR111111_1.fastq.gz',
                'path/to/SRR000000_2.fastq.gz',
                'path/to/SRR111111_2.fastq.gz'
            ]), ('--paired-end '
                 '<(/bin/zcat path/to/SRR000000_1.fastq.gz path/to/SRR111111_1.fastq.gz) '
                 '<(/bin/zcat path/to/SRR000000_2.fastq.gz path/to/SRR111111_2.fastq.gz)'))

        self.assertEqual(rsem.gen_fastq_gz_input(['path/to/SRR000000_1.fastq.gz']),
                         '<(/bin/zcat path/to/SRR000000_1.fastq.gz)')

        self.assertEqual(rsem.gen_fastq_gz_input(['path/to/SRR000000_2.fastq.gz']),
                         '<(/bin/zcat path/to/SRR000000_2.fastq.gz)')

        self.assertIsNone(rsem.gen_fastq_gz_input(['invalid_fastq_gz_name']))
