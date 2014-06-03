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
import yaml
with open('rsem_pipeline_config.yaml') as inf:
    CONFIG = yaml.load(inf.read())
from ftplib import FTP
import pickle

import ruffus as R

from soft_parser import parse
import utils as U
import settings as S
logging.config.dictConfig(S.LOGGING_CONFIG)
logger = logging.getLogger('rsem_pipeline')

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
    cache_file = os.path.join(top_output_dir, 'originate_files.pickle')
    if cache_usable(cache_file, input_csv, *soft_files):
        with open(cache_file) as inf:
            outputs = pickle.load(inf)
    else:
        logger.info('generating originate files from FTP')
        outputs = gen_originate_files(samples)
        logger.info('generating cache file: {0}'.format(cache_file))
        with open(cache_file, 'wb') as opf:
            pickle.dump(outputs, opf)
    for job_parameters in outputs:
        yield job_parameters


def cache_usable(cache_file, *ref_files):
    f_cache_usable = True
    if os.path.exists(cache_file):
        logger.info('{0} exists'.format(cache_file))
        if cache_up_to_date(cache_file, *ref_files):
            logger.info('{0} is up to date. '
                        'reading outputs from cache'.format(cache_file))
        else:
            logger.info('{0} is outdated'.format(cache_file))
            f_cache_usable = False
    else:
        logger.info('{0} doesn\'t exist'.format(cache_file))
        f_cache_usable = False
    return f_cache_usable

def cache_up_to_date(cache_file, *ref_files):
    for _ in ref_files:
        if os.path.getmtime(cache_file) < os.path.getmtime(_):
            return False
    return True


def gen_originate_files(samples):
    """
    Example of outputs:
    [[None,
      ['test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/SRX029242/SRR070177/SRR070177.sra',
       'test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/download.COMPLETE'],
      <GSM602557 (1/20) of GSE24455>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602558/SRX029243/SRR070178/SRR070178.sra',
       'test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602558/download.COMPLETE'],
      <GSM602558 (2/20) of GSE24455>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRX116910/SRR401053/SRR401053.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRX116910/SRR401054/SRR401054.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/download.COMPLETE'],
      <GSM863770 (1/8) of GSE35213>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401055/SRR401055.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401056/SRR401056.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/download.COMPLETE'],
      <GSM863771 (2/8) of GSE35213>]]
    """
    outputs = []
    for sample in samples:
        # e.g. of sample.url
        # ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
        url_obj = urlparse.urlparse(sample.url)
        ftp_handler = FTP(url_obj.hostname)
        ftp_handler.login()
        # one level above SRX123456
        before_srx_dir = os.path.dirname(url_obj.path)
        ftp_handler.cwd(before_srx_dir)
        srx = os.path.basename(url_obj.path)
        srrs =  ftp_handler.nlst(srx)
        # cool trick for flatten 2D list:
        # http://stackoverflow.com/questions/2961983/convert-multi-dimensional-list-to-a-1d-list-in-python
        sras = [_ for srr in srrs for _ in ftp_handler.nlst(srr)]
        sras = [os.path.join(sample.outdir, _) for _ in sras]
        ftp_handler.quit()
    
        flag_file = U.gen_completion_stamp('download', sample.outdir)
        outputs.append([None, sras + [flag_file], sample])
    return outputs

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

