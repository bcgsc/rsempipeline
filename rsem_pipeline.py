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
    cmd = U.gen_download_cmd(sample.outdir, url_path)
    U.execute(cmd, msg_id, flag_file)

if __name__ == "__main__":
    input_csv = 'test_data_downloaded_for_genesis/test_GSE_GSM_species_on_genesis.csv'
    top_output_dir = os.path.dirname(input_csv)
    soft_files = ['test_data_downloaded_for_genesis/soft/GSE24455_family.soft.subset',
                  'test_data_downloaded_for_genesis/soft/GSE35213_family.soft.subset']
    samples = gen_samples(soft_files, input_csv)
    R.pipeline_run([download], multiprocess=4)
