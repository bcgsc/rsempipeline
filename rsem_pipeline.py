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


import utils as U
import settings as S

from soft_parser import parse
from sample_data_parser import gen_sample_data_from_csv_file
from utils_download import gen_originate_files
from utils_rsem import gen_fastq_gz_input

# to match path of GSM, e.g.
# /projects/btl2/batch7/rsem_pipeline2/sample_data/rsem_output/mouse/GSE35213/GSM863770
logging.config.dictConfig(S.LOGGING_CONFIG)
logger = logging.getLogger('rsem_pipeline')

PATH_RE = r'(.*)/(?P<species>\S+)/(?P<GSE>GSE\d+)/(?P<GSM>GSM\d+)'


def gen_samples_from_soft_and_data_file(
        soft_files, data_file, top_outdir=None):
    """
    :param input_csv: e.g. GSE_GSM_species.csv
    """
    # Nomenclature:
    #     soft_files: soft_files downloaded with tools/download_soft.py
    #     samples: a list of Sample instances
    #     sample_data: data from the sample_data_file stored in a dict
    #     data_file: the file with sample_data stored (e.g. GSE_GSM_species.csv)
    #     series: a series instance constructed from information in a soft file

    if os.path.splitext(data_file)[-1] == '.csv':
        sample_data = gen_sample_data_from_csv_file(data_file)
    else:
        raise ValueError(
            "uncognized file type of {0} as samples_input_file".format(data_file))

    samples = []
    for soft_file in soft_files:
        global config
        series = parse(soft_file, config['INTERESTED_ORGANISMS'])
        # samples that are interested by the collaborator 
        interested_samples = sample_data[series.name]
        # intersection among GSMs found in the soft file and
        # sample_data_file
        samples.extend([_ for _ in series.passed_samples
                        if _.name in interested_samples])
    logger.info(
        'After intersection among GSMs found in the {0} and '
        '{1}, {2} samples remained'.format(soft_file, data_file, len(samples)))

    outdir = get_outdir(data_file, top_outdir)
    logger.info('initializing outdirs of samples...')
    init_sample_outdirs(samples, outdir)
    return samples


def get_outdir(data_file, top_outdir=None):
    """
    get the proper output directory based on top_outdir (higher priority) or
    sample_data_file
    """
    if top_outdir is None:
        top_outdir = os.path.dirname(data_file)
    outdir =  os.path.join(top_outdir, 'rsem_output')
    return outdir


def init_sample_outdirs(samples, outdir):
    for sample in samples:
        sample.gen_outdir(outdir)
        if not os.path.exists(sample.outdir):
            logger.info('creating directory: {0}'.format(sample.outdir))
            os.makedirs(sample.outdir)


def originate_files():
    """
    Generate a list of sras to download for each sample

    This function gets called twice, once before entering the queue, once after 
    """
    global samples, top_outdir, input_csv, soft_files

    logger.info('preparing originate_files '
                'for {0} samples'.format(len(samples)))
    cache_file = os.path.join(top_outdir, 'originate_files.pickle')
    if U.cache_usable(cache_file, input_csv, *soft_files):
        with open(cache_file) as inf:
            outputs = pickle.load(inf)
    else:
        logger.info('generating originate files from FTP')
        outputs = gen_originate_files(samples)
        logger.info('generating cache file: {0}'.format(cache_file))
        with open(cache_file, 'wb') as opf:
            pickle.dump(outputs, opf)
    logger.info('{0} sets of parameters generated '
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
    print cmd
    # U.execute(cmd, msg_id, flag_file)


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
    print cmd
    # U.execute(cmd, flag_file=flag_file)


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
    ('/path/to/rsem_output/homo_sapiens/GSE50599/GSM1224499/SRR968078_1.fastq.gz',
     '/path/to/rsem_output/homo_sapiens/GSE50599/GSM1224499/SRR968078_2.fastq.gz')
    """
    # this is equivalent to the sample.outdir or GSM dir 
    outdir = os.path.dirname(inputs[0])
    fastq_gz_input = gen_fastq_gz_input(inputs)

    res = re.search(PATH_RE, outdir)
    species = res.group('species')
    gsm = res.group('GSM')

    for _ in outputs:
        print _, os.path.exists(_)
    print gsm

    # following rsem naming convention
    global config
    reference_name = config['REFERENCE_NAMES'][species]
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

def parse_args():
    parser = argparse.ArgumentParser(
        description='Run rsem pepline on a local system',
        usage='python2.7.x {0} -s soft_file [-o some_outdir]')
    parser.add_argument(
        '-s', '--soft-files', nargs='+', required=True,
        help='a list of soft files')
    parser.add_argument(
        '-f', '--data-file', required=True,
        help=('e.g. GSE_GSM_species.csv '
              '(based on the xlsx/csv file provided by the collaborator) '
              'for intersection with GSMs available in the soft files'))
    parser.add_argument(
        '--host-to-run', dest='host_to_run', required =True,
        choices=['local', 'genesis'], 
        help=('choose a host to run, if it is not local, '
              'a corresponding template of submission script '
              'is expected to be found in the templates folder'))
    parser.add_argument(
        '-o', '--top_outdir', 
        help=('top output directory, default to the dirname of '
              'the value of --data-file'))
    parser.add_argument(
        '--tasks', nargs='+', dest='tasks', choices=['download', 'sra2fastq', 'rsem'],
        help=('Specify the tasks to run, e.g. on genesis, you can only do '
              'sra2fastq and rsem; on apollo, you may want do download only '
              'and then transfer all sra files to genesis'))
    parser.add_argument(
        '-n', '--num-threads', type=int, dest='num_threads', default=1,
        help='number of threads used in Ruffus.pipeline_run')
    parser.add_argument(
        '--config-file', default='rsem_pipeline_config.yaml', 
        help='a YAML configuration file')

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    try:
        with open(args.config_file) as inf:
            config = yaml.load(inf.read())
    except:
        IOError('configuration file: {0} NOT found'.format(args.config_file))

    samples = gen_samples_from_soft_and_data_file(
        args.soft_files, args.data_file, args.top_outdir)
    print samples

    # R.pipeline_run([rsem], multiprocess=1)
    # R.pipeline_run([download, sra2fastq, rsem], multiprocess=7)
