#!/usr/bin/env python

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
import Queue
import threading
import csv
import pickle
import yaml
with open('rsem_pipeline_config.yaml') as inf:
    CONFIG = yaml.load(inf.read())

import ruffus as R

from soft_parser import parse
import utils as U
import settings as S
logging.config.dictConfig(S.LOGGING_CONFIG)
logger = logging.getLogger('rsem_pipeline')

from utils_download import gen_originate_files

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
    logger.info(
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
    logger.info('preparing originate_files '
                'for {0} samples'.format(len(samples)))
    cache_file = os.path.join(top_output_dir, 'originate_files.pickle')
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
    flag_file = outputs[-1]     # the others are sra files
    msg_id = U.gen_sample_msg_id(sample)
    url_path = urlparse.urlparse(sample.url).path
    cmd = (
        "/home/kmnip/bin/ascp "
        "-i /home/kmnip/.aspera/connect/etc/asperaweb_id_dsa.putty "
        "--ignore-host-key "
        "-QT "                 # -Q Fair transfer policy, -T Disable encryption
        "-L {0} "              # log dir
        "-k2 "
        "-l 300m "
        "anonftp@ftp-trace.ncbi.nlm.nih.gov:{1} {0}".format(
            sample.outdir, url_path))
    U.execute(cmd, msg_id, flag_file)

# @R.split(download, ['*_[12].fastq.gz', 'sra2fastq.COMPLETE'])
# @R.transform(download, R.suffix('.sra'), ['_1.fastq', '_2.fastq'])
@R.subdivide(download, R.regex('(.*)\.sra'),
             [r'\1_[12].fastq.gz', r'sra2fastq.COMPLETE'])
def sra2fastq(inputs, outputs):
    inputs.pop()                # remove flag file from previous task
    flag_file = outputs[-1]
    sub_flag_files = []
    # inputs contains a list of sra files for each Sample
    for sra in inputs:
        outdir = os.path.dirname(os.path.dirname(os.path.dirname(sra)))
        sub_flag_file = os.path.join(
            outdir, '{0}.sra2fastq.COMPLETE'.format(os.path.splitext(sra)[0]))
        sub_flag_files.append(sub_flag_file)
        cmd = ('fastq-dump --minReadLen 25 --gzip --split-files '
               '--outdir {0} {1}'.format(outdir, sra))
        U.execute(cmd, flag_file=sub_flag_file)
    if all(os.path.exists(_) for _ in sub_flag_files):
        U.touch(flag_file)

    
if __name__ == "__main__":
    input_csv = 'sample_data/sample_GSE_GSM_species.csv'
    top_output_dir = os.path.dirname(input_csv)
    soft_files = ['sample_data/soft/GSE24455_family.soft.subset',
                  'sample_data/soft/GSE35213_family.soft.subset',
                  'sample_data/soft/GSE50599_family.soft.subset']
    samples = gen_samples(soft_files, input_csv)
    R.pipeline_run([download], multiprocess=8)
