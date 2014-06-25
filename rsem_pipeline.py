#!/usr/bin/env python
# -*- coding: utf-8 -*

"""
This script parse soft files, download sra, convert sra to fastq and run rsem
on them, either on local system or on remote cluster.
"""

import os
import re
import logging.config

import urlparse
import yaml

import ruffus as R

import utils as U

import utils_main as UM
from utils_download import gen_orig_params
from utils_rsem import gen_fastq_gz_input

PATH_RE = r'(.*)/(?P<species>\S+)/(?P<GSE>GSE\d+)/(?P<GSM>GSM\d+)'

# because of ruffus, have to use some global variables
# global variables: options, config, samples, logger, logger_mutex
options = UM.parse_args()
with open(options.config_file) as inf:
    config = yaml.load(inf.read())
logging.config.fileConfig(options.logging_config)

samples = UM.gen_samples_from_soft_and_isamples(
    options.soft_files, UM.get_isamples(options.isamples), config)

logger, logger_mutex = R.proxy_logger.make_shared_logger_and_proxy(
    R.proxy_logger.setup_std_shared_logger,
    "rsem_pipeline",
    {"config_file": options.logging_config})

UM.init_sample_outdirs(samples, UM.get_rsem_outdir(options))

##################################end of main##################################

def originate_params():
    """
    Generate a list of sras to download for each sample

    This function gets called twice, once before entering the queue, once after 
    """
    logger.info('preparing originate_params '
                'for {0} samples'.format(len(samples)))
    orig_params_sets = gen_orig_params(samples, options.use_pickle)
    logger.info(
        '{0} sets of orig_params generated'.format(len(orig_params_sets)))
    for _ in orig_params_sets:
        yield _


@R.files(originate_params)
def download(inputs, outputs, sample):
    """inputs is None""" 
    msg_id = U.gen_sample_msg_id(sample)
    # e.g. sra
    # test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/SRX029242/SRR070177/SRR070177.sra
    sra, flag_file = outputs    # the others are sra files
    sra_outdir = os.path.dirname(sra)
    if not os.path.exists(sra_outdir):
        os.makedirs(sra_outdir)
    # e.g. url_path:
    # /sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
    url_path = urlparse.urlparse(sample.url).path
    sra_url_path = os.path.join(url_path, *sra.split('/')[-2:])
    cmd = (
        "/home/kmnip/bin/ascp "
        "-i /home/kmnip/.aspera/connect/etc/asperaweb_id_dsa.putty "
        "--ignore-host-key "
        "-QT "                 # -Q Fair transfer policy, -T Disable encryption
        "-L {0} "              # log dir
        "-k2 "
        "-l 300m "
        "anonftp@ftp-trace.ncbi.nlm.nih.gov:{1} {0}".format(
            sra_outdir, sra_url_path))
    returncode = U.execute(cmd, msg_id, flag_file, options.debug)
    if returncode != 0 or returncode is None:
        # try wget
        cmd = "wget ftp://ftp-trace.ncbi.nlm.nih.gov{0} -P {1} -N".format(
            sra_url_path, sra_outdir)
        U.execute(cmd, msg_id, flag_file, options.debug)

               
@R.subdivide(
    download,
    R.formatter(r'{0}/(?P<SRX>SRX\d+)/(?P<SRR>SRR\d+)/(.*)\.sra'.format(PATH_RE)),
    ['{subpath[0][2]}/{SRR[0]}_[12].fastq.gz',
     '{subpath[0][2]}/{SRR[0]}.sra.sra2fastq.COMPLETE'])
def sra2fastq(inputs, outputs):
    sra, _ = inputs             # ignore the flag file from previous task
    flag_file = outputs[-1]
    outdir = os.path.dirname(os.path.dirname(os.path.dirname(sra)))
    cmd = ('fastq-dump --minReadLen 25 --gzip --split-files '
           '--outdir {0} {1}'.format(outdir, sra))
    U.execute(cmd, flag_file=flag_file, debug=options.debug)


@R.collate(
    sra2fastq,
    # the commented R.formatter line is for reference only, use the next one
    # because thus ruffus can guarantee that all the required
    # sra2fastq.COMPLETE files do exist before starting rsem, this is
    # inconvenient when it comes to multiple samples because a single missing
    # sra2fastq.COMPLETE will make the analysis for all samples crash, but
    # generally you run one sample at a time when it comes to sra2fastq and
    # rsem, so it should be fine most of the time

    # R.formatter(r'{0}\/(?P<SRR>SRR\d+)\_[12]\.fastq.gz'.format(PATH_RE)),
    R.formatter(PATH_RE),
    ['{subpath[0][0]}/{GSM[0]}.genes.results',
     '{subpath[0][0]}/{GSM[0]}.isoforms.results',
     '{subpath[0][0]}/{GSM[0]}.stat/{GSM[0]}.cnt',
     '{subpath[0][0]}/{GSM[0]}.stat/{GSM[0]}.model',
     '{subpath[0][0]}/{GSM[0]}.stat/{GSM[0]}.theta',
     '{subpath[0][0]}/align.stats',
     '{subpath[0][0]}/rsem.log',
     '{subpath[0][0]}/rsem.COMPLETE'])

# example of outputs:
# ├── GSM629265
# │── ├── GSM629265.genes.results
# │── ├── GSM629265.isoforms.results
# │── ├── align.stats
# │── ├── rsem.log
# │── ├── rsem.COMPLETE
# │── └── GSM629265.stat
# │──     ├── GSM629265.cnt
# │──     ├── GSM629265.model
# │──     └── GSM629265.theta
def rsem(inputs, outputs):
    """
    :params inputs: a tuple of 1 or 2 fastq.gz files, e.g.
    ('/path/to/rsem_output/homo_sapiens/GSE50599/GSM1224499/SRR968078_1.fastq.gz',
     '/path/to/rsem_output/homo_sapiens/GSE50599/GSM1224499/SRR968078_2.fastq.gz')
    """
    inputs = [_ for _ in inputs if not _.endswith('.sra2fastq.COMPLETE')]
    # this is equivalent to the sample.outdir or GSM dir 
    outdir = os.path.dirname(inputs[0])
    fastq_gz_input = gen_fastq_gz_input(inputs)

    res = re.search(PATH_RE, outdir)
    species = res.group('species')
    gsm = res.group('GSM')

    # following rsem naming convention
    reference_name = config['REFERENCE_NAMES'][species]
    sample_name = '{outdir}/{gsm}'.format(**locals())

    flag_file = outputs[-1]

    cmd = ' '.join([
        'rsem-calculate-expression', # 1.2.5
        '-p {0}'.format(options.jobs),   # not the best way to determine num jobs,
                                     # but consistent with the number of sra
                                     # files
        '--time',
        '--no-bam-output',
        '--bowtie-chunkmbs 256',
        # could also be found in the PATH
        # '--bowtie-path', '/home/zxue/Downloads/rchiu_Downloads/bowtie-1.0.0',
        fastq_gz_input,
        reference_name,
        sample_name,
        '1>{0}/rsem.log'.format(outdir),
        '2>{0}/align.stats'.format(outdir)])
    U.execute(cmd, flag_file=flag_file, debug=options.debug)

if __name__ == "__main__":
    UM.act(options, samples)
