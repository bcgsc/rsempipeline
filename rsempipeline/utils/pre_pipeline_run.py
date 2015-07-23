# -*- coding: utf-8 -*

"""
This module contains utilities functions used before the pipeline actually
gets run, e.g. Generating a list of Sample instances based on inputs,
initiating directories for all samples, downloading sras_info.yaml files and
selecting a subset of Sample instances for further process based on free-space
availabilities (a combined rule based on availale free space on the file system
and parameters from rsempipeline_config.yaml
"""

import os
import sys
import re
import yaml
import urlparse
import subprocess
from ftplib import FTP
import logging
logger = logging.getLogger(__name__)

from rsempipeline.parsers.soft_parser import parse
from rsempipeline.parsers.isamp_parser import get_isamp
from rsempipeline.utils.misc import pretty_usage, ugly_usage, disk_used, disk_free
from rsempipeline.conf.settings import (
    SRA_INFO_FILE_BASENAME, QSUB_SUBMIT_SCRIPT_BASENAME, SRA2FASTQ_SIZE_RATIO,
    RSEM_OUTPUT_BASENAME)


def calc_num_isamp(isamp):
    """
    Calculate the  number of isamples in isamp

    :param isamp: a dict with key and value as listing of strings, not Sample
    instances
    """
    return sum(len(val) for val in isamp.values())


def log_isamp(isamp_file_or_str, isamp):
    """just do some logging"""
    num = calc_num_isamp(isamp)
    if os.path.exists(isamp_file_or_str): # then it's a file
        logger.info(
            '{0} samples found in {1}'.format(num, isamp_file_or_str))
    else:
        logger.info('{0} samples found from -i/--isamp input'.format(num))


# about generating samples from soft and isamp inputs
def gen_all_samples_from_soft_and_isamp(soft_files, isamp_file_or_str, config):
    """
    :param isamp: e.g. mannually prepared interested sample file
    (e.g. GSE_species_GSM.csv) or isamp_str as specified on the command

    :type isamp: a dict with key and value as listing of strings, not Sample
    instances
    """
    # IMPORTANT NOTE: for historical reason, soft files parsed does not return
    # dict as get_isamp
    isamp = get_isamp(isamp_file_or_str)
    log_isamp(isamp_file_or_str, isamp)

    # a list, of Sample instances resultant of intersection
    intersected_samples = []
    for soft_file in soft_files:
        soft_samples = analyze_one(soft_file, isamp, config['INTERESTED_ORGANISMS'])
        if soft_samples:
            intersected_samples.extend(soft_samples)
    num_inter_samp = len(intersected_samples)
    sanity_check(calc_num_isamp(isamp), num_inter_samp)
    return intersected_samples


def analyze_one(soft_file, isamp, interested_organisms):
    """analyze a single soft_file"""
    if not filename_check(soft_file):
        return

    s_series = parse(soft_file, interested_organisms)
    # s_series should be in the list of series that are interested by the
    # collaborator
    if not s_series.name in isamp.keys():
        logger.warning('{0} from {1} does not appear in '
                       'isamp'.format(s_series.name, soft_file))
        return

    return intersect(s_series, isamp)


def intersect(series, isamp):
    """
    :param series: a series instance
    """
    i_samps = isamp[series.name]
    s_samps = series.passed_samples
    intersected = [_ for _ in s_samps if _.name in i_samps]
    if len(i_samps) != len(intersected):
        logger.error('Discrepancy for {0}: {1} GSMs in soft, {2} GSMs in isamp, '
                     'and only {3} left after intersection.'.format(
                         series.name, len(s_samps), len(i_samps), len(intersected)))
    return intersected


def filename_check(soft_file):
    res = re.search(r'(GSE\d+)\_family\.soft\.subset', soft_file)
    if not res:
        logger.error(
            'invalid soft file because of no GSE information found '
            'in its filename: {0}'.format(soft_file))
    return res


def sanity_check(num_isamp, num_res_samp):
    """
    sanity check to see if the number of isamples equal the number of samples
    that are to be processed
    """
    if num_isamp != num_res_samp:
        raise ValueError(
            'Unmatched numbers of samples interested ({0}) and to be processed '
            '({1}) after intersection. Please check ERROR in log '
            'for details.'.format(num_isamp, num_res_samp))
    else:
        logger.info('No discrepancies detected after intersection, '
                    'all {0} samples will be processed '.format(num_isamp))


def get_rsem_outdir(top_outdir):
    """
    get the output directory for rsem, it's top_outdir/rsem_output by default.
    """
    return os.path.join(top_outdir, RSEM_OUTPUT_BASENAME)


# about init sample outdirs
def init_sample_outdirs(samples, top_outdir):
    """
    Initiate the output directories for samples.
    """
    outdir = get_rsem_outdir(top_outdir)
    for sample in samples:
        sample.gen_outdir(outdir)
        if not os.path.exists(sample.outdir):
            logger.info('creating directory: {0}'.format(sample.outdir))
            os.makedirs(sample.outdir)


def get_ftp_handler(sample_url):
    """Get a FTP hander from sample url"""
    urlparsed = urlparse.urlparse(sample_url)
    logger.info('connecting to {scheme}://{netloc}'.format(
        scheme=urlparsed.scheme, netloc=urlparsed.netloc))
    ftp_handler = FTP(urlparsed.hostname)
    ftp_handler.login()
    return ftp_handler


# about fetch sras info
def fetch_sras_info(samples, flag_recreate_sras_info):
    """
    Fetch information (name & size) for sra files to be downloaded and save
    them to a sras_info.yaml file under the output dir of each sample.
    """
    # e.g. of sample.url
    # ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
    num_samples = len(samples)
    ftp_handler = get_ftp_handler(samples[0].url)
    for k, sample in enumerate(samples):
        sras_info_yml = os.path.join(sample.outdir, SRA_INFO_FILE_BASENAME)
        if os.path.exists(sras_info_yml) and not flag_recreate_sras_info:
            continue
        logger.info('({0}/{1}), fetching sras info from FTP for {2}, saving '
                    'to {3}'.format(k+1, num_samples, sample, sras_info_yml))
        sras_info = fetch_sras_info_per(sample, ftp_handler)
        if sras_info:       # could be None due to Network problem
            write(sras_info, sras_info_yml)
    ftp_handler.quit()


def write(sras_info, output_yml):
    with open(output_yml, 'wb') as opf:
        yaml.dump(sras_info, stream=opf, default_flow_style=False)


def fetch_sras_info_per(sample, ftp_handler):
    """
    fetch information of sra files for one sample.
    """
    url_obj = urlparse.urlparse(sample.url)
    # one level above SRX123456
    # e.g. before_srx_dir: /sra/sra-instant/reads/ByExp/sra/SRX/SRX573
    before_srx_dir = os.path.dirname(url_obj.path)
    ftp_handler.cwd(before_srx_dir)
    # e.g. srx: SRX573027
    srx = os.path.basename(url_obj.path)
    try:
        srrs = ftp_handler.nlst(srx)
        # cool trick for flatten 2D list:
        # http://stackoverflow.com/questions/2961983/convert-multi-dimensional-list-to-a-1d-list-in-python
        sras = [_ for srr in srrs for _ in ftp_handler.nlst(srr)]

        # to get size,
        # http://stackoverflow.com/questions/3231910/python-ftplib-cant-get-size-of-file-before-download
        ftp_handler.sendcmd('TYPE i')
        # sizes returned are in unit of byte
        sizes = [ftp_handler.size(_) for _ in sras]

        sras_info = [{i: {'size': j, 'readable_size': pretty_usage(j)}}
                     for (i, j) in zip(sras, sizes)]
        # e.g. [{sra1: {'size': 123}}), {sra2: {'size': 456}}, ...]
        return sras_info
    except Exception, err:
        logger.exception(err)


def calc_free_space_to_use(max_usage, current_usage, free_space, min_free):
    return min(max_usage - current_usage - min_free,
               free_space - min_free)


# about filtering samples based on their sizes
def select_samples_to_process(samples, config, options):
    """
    Select a subset of samples based on a combined rule based on availale free
    space on the file system and parameters from rsempipeline_config.yaml
    """
    P, G = pretty_usage, ugly_usage
    top_outdir = config['LOCATOP_OUTDIR']
    free_space = disk_free(config['LOCACMD_DF'])
    logger.info(
        'local free space avaialbe: {0}'.format(P(free_space)))
    current_usage = disk_used(top_outdir)
    logger.info('local current usage by {0}: {1}'.format(
        top_outdir, P(current_usage)))
    max_usage = G(config['LOCAL_MAX_USAGE'])
    logger.info('maximum usage: {0}'.format(P(max_usage)))
    min_free = G(config['LOCAMIN_FREE'])
    logger.info('min_free: {0}'.format(P(min_free)))
    free_to_use = calc_free_space_to_use(max_usage, current_usage,
                                         free_space, min_free)
    logger.info('free to use: {0}'.format(P(free_to_use)))

    # gsms are a list of Sample instances
    gsms = find_gsms_to_process(samples, free_to_use, options.ignore_disk_usage_rule)
    return gsms


def find_gsms_to_process(samples, l_free_to_use, ignore_disk_usage):
    """
    Find samples that are to be processed, the selecting rule is implemented
    here
    """
    gsms_to_process = []
    P = pretty_usage
    for gsm in samples:
        if is_processed(gsm.outdir):
            logger.debug('{0} has already been processed successfully, pass)'.format(gsm))
            continue

        if ignore_disk_usage:
            gsms_to_process.append(gsm)
            continue

        usage = estimate_proc_usage(gsm.outdir)
        if usage > l_free_to_use:
            logger.debug('{0} ({1}) doesn\'t fit current local free_to_use '
                         '({2})'.format(gsm, P(usage), P(l_free_to_use)))
            continue

        logger.info('{0} ({1}) fits local free_to_use '
                    '({2})'.format(gsm, P(usage), P(l_free_to_use)))
        l_free_to_use -= usage
        gsms_to_process.append(gsm)
    return gsms_to_process


def get_sras_info(gsm_dir):
    info_file = os.path.join(gsm_dir, SRA_INFO_FILE_BASENAME)
    with open(info_file) as inf:
        return yaml.load(inf.read())


def estimate_proc_usage(gsm_dir):
    """
    Estimated the disk usage needed for processing a sample based on the size
    of sra files, the information of which is contained in the info_file 
    """
    ratio = float(SRA2FASTQ_SIZE_RATIO)
    sras_info = get_sras_info(gsm_dir)
    usage = sum(d[k]['size'] for d in sras_info for k in d.keys())
    usage = (1 + ratio) * usage
    return usage


def is_processed(gsm_dir):
    """
    Checking the processing status, whether completed or not based the
    existence of COMPLETE flags
    """
    sras_info = get_sras_info(gsm_dir)
    # e.g. SRXxxxxxx/SRRxxxxxxx/SRRxxxxxxx.sra
    sra_files = [i for j in sras_info for i in j.keys()]
    # e.g. /path/to/rsem_output/GSExxxxx/homo_sapiens/GSMxxxxxx
    sra_files = [os.path.join(gsm_dir, _) for _ in sra_files]
    res = False
    if is_download_complete(gsm_dir, sra_files):
        if is_sra2fastq_complete(gsm_dir, sra_files):
            if is_gen_qsub_script_complete(gsm_dir):
                res = True
            else:
                logger.debug('{0}: gen_qsub_script incomplete'.format(gsm_dir))
        else:
            logger.debug('{0}: sra2fastq incomplete'.format(gsm_dir))
    else:
        logger.debug('{0}: download incomplete'.format(gsm_dir))
    return res

    
def is_download_complete(gsm_dir, sra_files):
    flags = [os.path.join(gsm_dir, '{0}.download.COMPLETE'.format(_))
             for _ in map(os.path.basename, sra_files)]
    return all(map(os.path.exists, flags))
    

def is_sra2fastq_complete(gsm_dir, sra_files):
    flags = [os.path.join(gsm_dir, '{0}.sra2fastq.COMPLETE'.format(_))
             for _ in map(os.path.basename, sra_files)]
    return all(map(os.path.exists, flags))


def is_gen_qsub_script_complete(gsm_dir):
    return os.path.exists(os.path.join(gsm_dir, QSUB_SUBMIT_SCRIPT_BASENAME))


# def get_recorded_gsms(record_file):
#     """
#     fetch the list of GSMs that have already been recorded in the record_file
#     (e.g. transferred_GSMs.txt, sra2fastqed_GSMs.txt)
#     """
#     if not os.path.exists(record_file):
#         return []
#     else:
#         with open(record_file) as inf:
#             return [_.strip() for _ in inf if not _.strip().startswith('#')]
