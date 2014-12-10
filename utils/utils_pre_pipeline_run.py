# -*- coding: utf-8 -*

"""
This module contains utilities functions used before the pipeline actually
gets run, e.g. Generating a list of Sample instances based on inputs,
initiating directories for all samples, downloading sras_info.yaml files and
selecting a subset of Sample instances for further process based on free-space
availabilities (a combined rule based on availale free space on the file system
and parameters from rsem_pipeline_config.yaml
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

from soft_parser import parse
from isamp_parser import get_isamp

from utils import pretty_usage, ugly_usage

# about generating samples from soft and isamp inputs
def gen_samples_from_soft_and_isamp(soft_files, isamp_file_or_str, config):
    """
    :param isamp: e.g. mannually prepared interested sample file
    (e.g. GSE_species_GSM.csv) or isamp_str
    :type isamp: dict
    """
    # IMPORTANT NOTE: for historical reason, soft files parsed does not return
    # dict as get_isamp

    # a dict with key and value as str, not Sample instances
    isamp = get_isamp(isamp_file_or_str)
    num_isamp = sum(len(val) for val in isamp.values())
    if os.path.exists(isamp_file_or_str): # then it's a file
        logger.info(
            '{0} samples found in {1}'.format(num_isamp, isamp_file_or_str))
    else:
        logger.info('{0} samples found from -i/--isamp input'.format(num_isamp))

    # a list, of Sample instances resultant of intersection
    res_samp = []
    for soft_file in soft_files:
        # e.g. soft_file: GSE51008_family.soft.subset
        gse = re.search(r'(GSE\d+)\_family\.soft\.subset', soft_file)
        if not gse:
            logger.error(
                'unrecognized soft file: {0} '
                '(not GSE information in its file name'.format(soft_file))
        else:
            if gse.group(1) in isamp:
                series = parse(soft_file, config['INTERESTED_ORGANISMS'])
                # samples that are interested by the collaborator
                if not series.name in isamp:
                    continue
                isamp_gse = isamp[series.name]
                ssamp_gse = series.passed_samples
                # samples after intersection
                res_samp_gse = [_ for _ in ssamp_gse if _.name in isamp_gse]
                if len(isamp_gse) != len(res_samp_gse):
                    logger.error(
                        'Discrepancy for {0:12s}: '
                        '{1:4d} GSMs in isamp, '
                        'but {2:4d} left after '
                        'intersection. (# GSMs in soft: {3})'.format(
                            series.name, len(isamp_gse), len(ssamp_gse),
                            len(res_samp_gse)))
                res_samp.extend(res_samp_gse)
    num_res_samp = len(res_samp)
    sanity_check(num_isamp, num_res_samp)
    return res_samp


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
    return os.path.join(top_outdir, 'rsem_output')


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


# about fetch sras info
def fetch_sras_info(samples, flag_recreate_sras_info):
    """
    Fetch information (name & size) for sra files to be downloaded and save
    them to a sras_info.yaml file under the output dir of each sample.
    """
    # e.g. of sample.url
    # ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
    ftp_handler = None
    num_samples = len(samples)
    for k, sample in enumerate(samples):
        yaml_file = os.path.join(sample.outdir, 'sras_info.yaml')
        if not os.path.exists(yaml_file) or flag_recreate_sras_info:
            if ftp_handler is None:
                ftp_handler = get_ftp_handler(samples[0])
            logger.info('({0}/{1}), fetching sras info from FTP '
                        'for {2}, saving to {3}'.format(k+1, num_samples,
                                                        sample, yaml_file))
            sras_info = fetch_sras_info_per(sample, ftp_handler)
            if sras_info:            # could be None due to Network problem
                with open(yaml_file, 'wb') as opf:
                    yaml.dump(sras_info, stream=opf, default_flow_style=False)
    if ftp_handler is not None:
        ftp_handler.quit()


def fetch_sras_info_per(sample, ftp_handler=None):
    """
    fetch information of sra files for one sample.
    """
    if ftp_handler is None:
        ftp_handler = get_ftp_handler(sample)
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


def get_ftp_handler(sample):
    """Get a FTP hander from sample url"""
    hostname = urlparse.urlparse(sample.url).hostname
    logger.info('connecting to ftp://{0}'.format(hostname))
    ftp_handler = FTP(hostname)
    ftp_handler.login()
    return ftp_handler


# about filtering samples based on their sizes
def select_samples_to_process(samples, config, options):
    """
    Select a subset of samples based on a combined rule based on availale free
    space on the file system and parameters from rsem_pipeline_config.yaml
    """

    l_top_outdir = config['LOCAL_TOP_OUTDIR']
    l_free_space = get_local_free_disk_space(config['LOCAL_CMD_DF'])
    logger.info(
        'local free space avaialbe: {0}'.format(pretty_usage(l_free_space)))
    l_current_usage = get_current_local_usage(l_top_outdir)
    logger.info('local current usage by {0}: {1}'.format(
        l_top_outdir, pretty_usage(l_current_usage)))
    l_max_usage = min(ugly_usage(config['LOCAL_MAX_USAGE']), l_free_space)
    logger.info('l_max_usage: {0}'.format(pretty_usage(l_max_usage)))
    l_min_free = ugly_usage(config['LOCAL_MIN_FREE'])
    logger.info('l_min_free: {0}'.format(pretty_usage(l_min_free)))
    l_free_to_use = min(l_max_usage - l_current_usage,
                        l_free_space - l_min_free)
    logger.info('free to use: {0}'.format(pretty_usage(l_free_to_use)))

    # gsms are a list of Sample instances
    gsms = find_gsms_to_process(samples, l_top_outdir,
                                l_free_to_use, options.ignore_disk_usage_rule)
    return gsms


def find_gsms_to_process(samples, l_top_outdir, l_free_to_use,
                         flag_ignore_disk_usage_rule):
    """
    Find samples that are to be processed, the selecting rule is implemented
    here
    """
    gsms_to_process = []
    for gsm in samples:
        gsm_id = os.path.relpath(gsm.outdir, l_top_outdir)
        if processed(gsm.outdir):
            logger.debug('{0} has already been processed successfully, '
                         'pass'.format(gsm_id))
            continue

        if flag_ignore_disk_usage_rule:
            gsms_to_process.append(gsm)
            continue

        usage = est_proc_usage(gsm.outdir)
        if usage > l_free_to_use:
            logger.debug(
                '{0} ({1}) doesn\'t fit current local free_to_use ({2})'.format(
                    gsm_id, pretty_usage(usage), pretty_usage(l_free_to_use)))
            continue

        logger.info(
            '{0} ({1}) fits local free_to_use ({2})'.format(
                gsm_id, pretty_usage(usage), pretty_usage(l_free_to_use)))
        l_free_to_use -= usage
        gsms_to_process.append(gsm)

    return gsms_to_process


def get_sras_info(gsm_dir):
    info_file = os.path.join(gsm_dir, 'sras_info.yaml')
    with open(info_file) as inf:
        return yaml.load(inf.read())


def est_proc_usage(gsm_dir):
    """
    Estimated the disk usage needed for processing a sample based on the size
    of sra files, the information of which is contained in the info_file 
    """
    sra2fastq_size_ratio = 1.5  # rough estimate, based on statistics
    sras_info = get_sras_info(gsm_dir)
    usage = sum(d[k]['size'] for d in sras_info for k in d.keys())
    usage = usage * (sra2fastq_size_ratio + 1)
    return usage


def processed(gsm_dir):
    """
    Checking the processing status, whether completed or not based the
    existence of COMPLETE flags
    """
    sras_info = get_sras_info(gsm_dir)
    # e.g. SRXxxxxxx/SRRxxxxxxx/SRRxxxxxxx.sra
    sra_files = [i for j in sras_info for i in j.keys()]
    # e.g. /path/to/rsem_output/GSExxxxx/homo_sapiens/GSMxxxxxx
    sra_files = [os.path.join(gsm_dir, _) for _ in sra_files]
    if is_download_complete(gsm_dir, sra_files):
        if is_sra2fastq_complete(gsm_dir, sra_files):
            if is_gen_qsub_script_complete(gsm_dir):
                return True
            else:
                logger.debug('{0}: gen_qsub_script incomplete'.format(gsm_dir))
        else:
            logger.debug('{0}: sra2fastq incomplete'.format(gsm_dir))
    else:
        logger.debug('{0}: download incomplete'.format(gsm_dir))

    
def is_download_complete(gsm_dir, sra_files):
    flags = [os.path.join(gsm_dir, '{0}.download.COMPLETE'.format(_))
             for _ in map(os.path.basename, sra_files)]
    return all(map(os.path.exists, flags))
    

def is_sra2fastq_complete(gsm_dir, sra_files):
    flags = [os.path.join(gsm_dir, '{0}.sra2fastq.COMPLETE'.format(_))
             for _ in map(os.path.basename, sra_files)]
    return all(map(os.path.exists, flags))


def is_gen_qsub_script_complete(gsm_dir):
    return os.path.exists(os.path.join(gsm_dir, '0_submit.sh'))


def get_recorded_gsms(record_file):
    """
    fetch the list of GSMs that have already been recorded in the record_file
    (e.g. transferred_GSMs.txt, sra2fastqed_GSMs.txt)
    """
    if not os.path.exists(record_file):
        return []
    else:
        with open(record_file) as inf:
            return [_.strip() for _ in inf if not _.strip().startswith('#')]


def get_current_local_usage(l_top_outdir):
    """Get the real local usage, equivalent to du -s l_top_outdir"""
    # proc = subprocess.Popen(
    #     'du -s {0}'.format(l_top_outdir), stdout=subprocess.PIPE, shell=True)
    # output = proc.communicate()[0]
    # return int(output[0].split('\t')[0]) * 1024 # in KB => byte
    # surprisingly, os.walk is of similar speed to du
    total_size = 0
    for dirpath, _, filenames in os.walk(l_top_outdir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def get_local_free_disk_space(cmd_df):
    """
    Get the local free disk space with cmd_df specified in the
    rsem_pipeline_config.yaml
    """
    proc = subprocess.Popen(cmd_df, stdout=subprocess.PIPE, shell=True)
    output = proc.communicate()[0]
    # e.g. output:
    # 'Filesystem     1024-blocks       Used  Available Capacity Mounted on\nisaac:/btl2    10200547328 1267127584 8933419744      13% /projects/btl2\n'
    return int(output.split(os.linesep)[1].split()[3]) * 1024
