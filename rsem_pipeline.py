#!/usr/bin/env python
# -*- coding: utf-8 -*

"""
This script parse soft files, download sra, convert sra to fastq and run rsem
on them, either on local system or on remote cluster.
"""

import os
import sys
import glob
import re
import argparse
import logging.config
import urlparse
import csv
import pickle
import yaml

import ruffus as R

from soft_parser import parse
import utils as U
import settings as S

from utils_download import gen_originate_files
from utils_rsem import get_reference_name, gen_fastq_gz_input

# to match path of GSM, e.g.
# /projects/btl2/batch7/rsem_pipeline2/sample_data/rsem_output/mouse/GSE35213/GSM863770
logging.config.dictConfig(S.LOGGING_CONFIG)
LOGGER = logging.getLogger('rsem_pipeline')

with open('rsem_pipeline_config.yaml') as inf:
    CONFIG = yaml.load(inf.read())
PATH_RE = r'(.*)/(?P<species>\S+)/(?P<GSE>GSE\d+)/(?P<GSM>GSM\d+)'

def gen_samples(soft_files, input_csv):
    """
    @param input_csv: e.g. GSE_GSM_species.csv
    """
    # output_dir is decided by convention over configuration!
    input_csv_dir = os.path.dirname(input_csv)
    output_dir = os.path.join(input_csv_dir, 'rsem_output')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    input_csv_data = U.gen_input_csv_data(input_csv, key='gse')
    samples = []
    for soft_file in soft_files:
        series = parse(soft_file, CONFIG['SPECIES_INFO'].keys())
        # samples that are interested by the collaborator 
        used_samples = input_csv_data[series.name]
        # intersection among GSMs found in the soft file and input_csv
        samples.extend([_ for _ in series.passed_samples
                        if _.name in used_samples])
    LOGGER.info(
        'After intersection among GSMs found in the {0} and '
        '{1}, {2} samples remained'.format(
            soft_file, input_csv, len(samples)))

    for sample in samples:
        sample.gen_outdir(output_dir, CONFIG['SPECIES_INFO'])
        if not os.path.exists(sample.outdir):
            os.makedirs(sample.outdir)
    return samples


def originate_files():
    """
    Generate a list of sras to download for each sample

    This function gets called twice, once before entering the queue, once after 
    """
    global samples, top_output_dir, input_csv, soft_files
    LOGGER.info('preparing originate_files '
                'for {0} samples'.format(len(samples)))
    cache_file = os.path.join(top_output_dir, 'originate_files.pickle')
    if U.cache_usable(cache_file, input_csv, *soft_files):
        with open(cache_file) as inf:
            outputs = pickle.load(inf)
    else:
        LOGGER.info('generating originate files from FTP')
        outputs = gen_originate_files(samples)
        LOGGER.info('generating cache file: {0}'.format(cache_file))
        with open(cache_file, 'wb') as opf:
            pickle.dump(outputs, opf)
    LOGGER.info('{0} sets of parameters generated '
                'in originate files'.format(len(outputs)))
    for _ in outputs:
        yield _

@R.files(originate_files)
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
    U.execute(cmd, msg_id, flag_file)


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
    U.execute(cmd, flag_file=flag_file)


@R.collate(
    sra2fastq,
    R.formatter(r'{0}\/(?P<SRR>SRR\d+)\_[12]\.fastq.gz'.format(PATH_RE)),
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
    ('/path/to/rsem_output/human/GSE50599/GSM1224499/SRR968078_1.fastq.gz',
     '/path/to/rsem_output/human/GSE50599/GSM1224499/SRR968078_2.fastq.gz')
    """
    # this is equivalent to the sample.outdir or GSM dir 
    outdir = os.path.dirname(inputs[0])
    fastq_gz_input = gen_fastq_gz_input(inputs)

    res = re.search(PATH_RE, outdir)
    species = res.group('species')
    gsm = res.group('GSM')

    # following rsem naming convention
    reference_name = get_reference_name(species, CONFIG['SPECIES_INFO'])
    sample_name = '{outdir}/{gsm}'.format(**locals())

    flag_file = outputs[-1]
    cmd = ' '.join([
        'rsem-calculate-expression', # 1.2.5
        '-p 2',
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
    print cmd
    # U.execute(cmd, flag_file=flag_file)




if __name__ == "__main__":
    input_csv = 'sample_data/sample_GSE_GSM_species.csv'
    top_output_dir = os.path.dirname(input_csv)
    soft_files = ['sample_data/soft/GSE24455_family.soft.subset',
                  'sample_data/soft/GSE35213_family.soft.subset',
                  'sample_data/soft/GSE50599_family.soft.subset']
    samples = gen_samples(soft_files, input_csv)
    R.pipeline_run([rsem], multiprocess=1)
    # R.pipeline_run([download, sra2fastq, rsem], multiprocess=7)
