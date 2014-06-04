import re

def gen_fastq_gz_input(fastq_gzs):
    gz1_RE = re.compile('SRR\d+\_1\.fastq\.gz')
    gz2_RE = re.compile('SRR\d+\_2\.fastq\.gz')
    fastq_gz1 = sorted([_ for _ in fastq_gzs if gz1_RE.search(_)])
    fastq_gz2 = sorted([_ for _ in fastq_gzs if gz2_RE.search(_)])
    
    if fastq_gz1:
        if fastq_gz2:
            fastq_gz_input = (
                "--paired-end <(/bin/zcat {0}) <(/bin/zcat {1})".format(
                    ' '.join(fastq_gz1), ' '.join(fastq_gz2)))
        else:
            fastq_gz_input = "<(/bin/zcat {0})".format(' '.join(fastq_gz1))
    else:
        fastq_gz_input = None
    return fastq_gz_input
