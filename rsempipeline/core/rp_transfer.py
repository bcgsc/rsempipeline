#!/usr/bin/env python
# -*- coding: utf-8 -*

"""
This script will find out if enough space is available on remote cluster, if
there is, then it will find out the suitable GSMs, of which their sizes fit the
available space remotely and transfer them to remote by templating an rsync.sh
and execute it
"""

import os
import sys
import re
import stat
import yaml
import datetime
import logging.config

from jinja2 import Template

from rsempipeline.utils import pre_pipeline_run as PPR
from rsempipeline.utils import misc
from rsempipeline.conf.settings import RP_TRANSFER_LOGGING_CONFIG
from rsempipeline.parsers.args_parser import parse_args_for_rp_transfer

sys.stdout.flush()              #flush print outputs to screen

# global variables: options, config, logger
options = parse_args_for_rp_transfer()
try:
    with open(options.config_file) as inf:
        config = yaml.load(inf.read())
except IOError, _:
    print 'configuration file: {0} not found'.format(options.config_file)
    sys.exit(1)

logging.config.fileConfig(RP_TRANSFER_LOGGING_CONFIG)

logger = logging.getLogger('rp_transfer')


def get_remote_free_disk_space(df_cmd, remote, username):
    """
    find the free disk space on remote host.

    :param df_cmd: should be in the form of df -k -P target_dir
    """
    output = misc.sshexec(df_cmd, remote, username)
    # e.g. output:
    # ['Filesystem         1024-blocks      Used Available Capacity Mounted on\n',
    #  '/dev/analysis        16106127360 12607690752 3498436608      79% /extscratch\n']
    return int(output[1].split()[3]) * 1024


def est_current_remote_usage(remote, username, r_dir, l_dir):
    """
    estimate the space that has already been or will be consumed by rsem_output
    by walking through each GSM and computing the sum of their estimated usage,
    if rsem.COMPLETE exists for a GSM, then ignore that GSM

    mechanism: fetch the list of files in r_dir, and find the
    fastq.gz for each GSM, then find the corresponding fastq.gz in
    l_dir, and estimate sizes based on them

    :param find_cmd: should be in the form of find {remote_dir}
    :param r_dir: remote rsem output directory
    :param l_dir: local rsem output directory

    """
    find_cmd = 'find {0}'.format(r_dir)
    output = misc.sshexec(find_cmd, remote, username)
    if output is None:
        raise ValueError(
            'cannot estimate current usage on remote host. please check '
            '{0} exists on {1}'.format(r_dir, remote))
    output = [_.strip() for _ in output] # remote trailing '\n'

    usage = 0
    for dir_ in sorted(output):
        match = re.search(r'(GSM\d+$)', os.path.basename(dir_))
        if match:
            rsem_comp = os.path.join(dir_, 'rsem.COMPLETE')
            if (not rsem_comp in output) and (not misc.is_empty_dir(dir_, output)):
                # only count the disk spaces used by those GSMs that are
                # finished or processed successfully
                gsm_dir = dir_.replace(r_dir, l_dir)
                usage += est_rsem_usage(gsm_dir)
    return usage


def get_real_current_usage(remote, username, r_dir):
    """this will return real space consumed currently by rsem analysis"""
    output = misc.sshexec('du -s {0}'.format(r_dir), remote, username)
    # e.g. output:
    # ['3096\t/path/to/top_outdir\n']
    usage = int(output[0].split('\t')[0]) * 1024 # in KB => byte
    return usage


def est_rsem_usage(gsm_dir):
    """
    estimate the maximum disk space that is gonna be consumed by rsem analysis
    on one GSM based on a list of fq_gzs

    :param fq_gz_size: a number reprsenting the total size of fastq.gz files
                       for the corresponding GSM
    """
    # Based on observation of smaller fastq.gz file by gunzip -l
    # compressed        uncompressed  ratio uncompressed_name
    # 266348960          1384762028  80.8% rsem_output/GSE42735/homo_sapiens/GSM1048945/SRR628721_1.fastq
    # 241971266          1255233364  80.7% rsem_output/GSE42735/homo_sapiens/GSM1048946/SRR628722_1.fastq

    # would be easier just incorporate this value into FASTQ2USAGE_RATIO, or
    # ignore it based on the observation of the size between fastq.gz and
    # *.temp
    # gzip_compression_ratio = 0.8
    fastq2usage_ratio = config['FASTQ2USAGE_RATIO']

    # estimate the size of uncompressed fastq
    # res = fq_gz_size / (1 - gzip_compression_ratio)
    res = PPR.est_proc_usage(gsm_dir)
    # overestimate
    res = res * fastq2usage_ratio
    return res


def get_gsms_transferred(record_file):
    """
    fetch the list of GSMs that have already been transferred from record_file
    """
    if not os.path.exists(record_file):
        return []
    else:
        with open(record_file) as inf:
            return [_.strip() for _ in inf if not _.strip().startswith('#')]


def append_transfer_record(gsm_to_transfer, record_file):
    """
    append the GSMs that have just beened transferred successfully to
    record_file
    """
    with open(record_file, 'ab') as opf:
        now = datetime.datetime.now()
        opf.write('# {0}\n'.format(now.strftime('%y-%m-%d %H:%M:%S')))
        for _ in gsm_to_transfer:
            opf.write('{0}\n'.format(_))


def select_samples_to_transfer(samples, l_top_outdir, r_top_outdir,
                               r_host, r_username):
    """
    select samples to transfer (different from select_samples_to_process in
    utils_pre_pipeline.py, which are to process)
    """
    # r_: means relevant to remote host, l_: to local host
    r_free_space = get_remote_free_disk_space(
        config['REMOTE_CMD_DF'], r_host, r_username)
    logger.info(
        'r_free_space: {0}: {1}'.format(r_host, misc.pretty_usage(r_free_space)))

    # r_real_current_usage is just for giving an idea of real usage on remote,
    # this variable is not utilized by following calculations, but the
    # corresponding current local usage is always real since there's no point
    # to estimate because only one process would be writing to the disk
    # simultaneously.
    r_real_current_usage = get_real_current_usage(
        r_host, r_username, r_top_outdir)
    logger.info('real current usage on {0} by {1}: {2}'.format(
        r_host, r_top_outdir, misc.pretty_usage(r_real_current_usage)))

    r_est_current_usage = est_current_remote_usage(
        r_host, r_username, r_top_outdir, l_top_outdir)
    logger.info('estimated current usage (excluding samples with '
                'rsem.COMPLETE) on {0} by {1}: {2}'.format(
                    r_host, r_top_outdir, misc.pretty_usage(r_est_current_usage)))
    r_max_usage = min(misc.ugly_usage(config['REMOTE_MAX_USAGE']), r_free_space)
    logger.info('r_max_usage: {0}'.format(misc.pretty_usage(r_max_usage)))
    r_min_free = misc.ugly_usage(config['REMOTE_MIN_FREE'])
    logger.info('r_min_free: {0}'.format(misc.pretty_usage(r_min_free)))
    r_free_to_use = min(r_max_usage - r_est_current_usage,
                        r_free_space - r_min_free)
    logger.info('r_free_to_use: {0}'.format(misc.pretty_usage(r_free_to_use)))

    gsms = find_gsms_to_transfer(samples, l_top_outdir, r_free_to_use)
    return gsms


def find_gsms_to_transfer(samples, l_top_outdir, r_free_to_use):
    """
    Walk through local top outdir, and for each GSMs, estimate its usage, and
    if it fits free_to_use space on remote host, count it as an element
    gsms_to_transfer
    """
    gsms_to_transfer = []
    for gsm in samples:
        gsm_id = os.path.relpath(gsm.outdir, l_top_outdir)

        comp_flag = os.path.join(gsm.outdir, 'transfer.COMPLETE')
        if os.path.exists(comp_flag):
            logger.debug('{0}: already transferred'.format(gsm_id))
            continue

        if not PPR.processed(gsm.outdir):
            # debug info will be logged by PPR.processed
            continue

        rsem_usage = est_rsem_usage(gsm.outdir)

        if rsem_usage > r_free_to_use:
            logger.debug(
                '{0} ({1}) doesn\'t fit current remote free_to_use ({2})'.format(
                    gsm_id, misc.pretty_usage(rsem_usage), misc.pretty_usage(r_free_to_use)))
            continue

        logger.info('{0} ({1}) fit remote free_to_use ({2})'.format(
            gsm_id, misc.pretty_usage(rsem_usage), misc.pretty_usage(r_free_to_use)))
        r_free_to_use -= rsem_usage
        gsms_to_transfer.append(gsm)

    return gsms_to_transfer


def create_transfer_sh_dir(l_top_outdir):
    d = os.path.join(l_top_outdir, 'transfer_scripts')
    if not os.path.exists(d):
        os.mkdir(d)
    return d


def write_transfer_sh(gsms_tf_ids, rsync_template, l_top_outdir,
                      r_username, r_host, r_top_outdir):
    now = datetime.datetime.now()
    job_name = 'transfer.{0}'.format(now.strftime('%y-%m-%d_%H:%M:%S'))
    tf_dir = create_transfer_sh_dir(l_top_outdir) # tf: transfer
    tf_script = os.path.join(tf_dir, '{0}.sh'.format(job_name))

    # tf_ids: transfer, e.g. rsem_output/GSExxxxx/homo_sapiens/GSMxxxxxx
    write(tf_script, rsync_template,
          job_name=job_name,
          username=r_username,
          hostname=r_host,
          gsms_to_transfer=gsms_tf_ids,
          local_top_outdir=l_top_outdir,
          remote_top_outdir=r_top_outdir)
    return tf_script
    

def write(transfer_script, template, **params):
    """
    template the qsub_rsync (for qsub e.g. on apollo thosts.q queue) or rsync
    (for execution directly (e.g. westgrid) script
    """
    # needs improvment to make it configurable
    input_file = os.path.join(template)

    with open(input_file) as inf:
        template = Template(inf.read())

    with open(transfer_script, 'wb') as opf:
        opf.write(template.render(**params))


@misc.lockit(os.path.join(config['LOCAL_TOP_OUTDIR'], '.rp-transfer'))
def main():
    """the main function"""
    l_top_outdir = config['LOCAL_TOP_OUTDIR']
    r_top_outdir = config['REMOTE_TOP_OUTDIR']
    r_host, r_username = config['REMOTE_HOST'], config['USERNAME']
    # different from processing in rsempipeline.py, where the completion is
    # marked by .COMPLETE flags, but by writting the completed GSMs to
    # gsms_transfer_record
    gsms_transfer_record = os.path.join(l_top_outdir, 'transferred_GSMs.txt')
    gsms_transferred = get_gsms_transferred(gsms_transfer_record)
    samples = PPR.gen_all_samples_from_soft_and_isamp(
        options.soft_files, options.isamp, config)
    PPR.init_sample_outdirs(samples, config['LOCAL_TOP_OUTDIR'])
    samples = [_ for _  in samples
               if _.name not in map(os.path.basename, gsms_transferred)]

    gsms = select_samples_to_transfer(
        samples, l_top_outdir, r_top_outdir, r_host, r_username)
    if not gsms:
        logger.info('Cannot find a GSM that fits the current disk usage rule')
        return

    logger.info('GSMs to transfer:')
    for k, gsm in enumerate(gsms):
        logger.info('\t{0:3d} {1:30s} {2}'.format(k+1, gsm, gsm.outdir))

    gsms_tf_ids = [os.path.relpath(_.outdir, l_top_outdir) for _ in gsms]
    tf_script = write_transfer_sh(
        gsms_tf_ids, options.rsync_template, l_top_outdir,
        r_username, r_host, r_top_outdir)

    os.chmod(tf_script, stat.S_IRUSR | stat.S_IWUSR| stat.S_IXUSR)
    rcode = misc.execute_log_stdout_stderr(tf_script)

    if rcode == 0:
        append_transfer_record(gsms_tf_ids, gsms_transfer_record)


if __name__ == "__main__":
    main()
