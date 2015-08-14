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
sys.stdout.flush()              # flush print outputs to screen
import re
import stat
import datetime
import logging.config

from jinja2 import Template

from rsempipeline.utils import misc
misc.mkdir('log')
from rsempipeline.utils import pre_pipeline_run as PPR
from rsempipeline.parsers.args_parser import parse_args_for_rp_transfer
from rsempipeline.conf.settings import (RP_TRANSFER_LOGGING_CONFIG,
                                        TRANSFER_SCRIPTS_DIR_BASENAME)


logging.config.fileConfig(RP_TRANSFER_LOGGING_CONFIG)
logger = logging.getLogger('rp_transfer')


def get_remote_free_disk_space(remote, username, df_cmd):
    """
    find the free disk space on remote host.

    :param df_cmd: should be in the form of df -k -P target_dir
    """
    output = misc.sshexec(df_cmd, remote, username)
    # e.g. output:
    # ['Filesystem         1024-blocks      Used Available Capacity Mounted on\n',
    #  '/dev/analysis        16106127360 12607690752 3498436608      79% /extscratch\n']
    return int(output[1].split()[3]) * 1024


def fetch_remote_file_list(remote, username, r_dir):
    find_cmd = 'find {0}'.format(r_dir)
    output = misc.sshexec(find_cmd, remote, username)
    if output is None:
        raise ValueError(
            'cannot estimate current usage on remote host. Please check '
            '{0} exists on {1}, or {1} may be down'.format(r_dir, remote))
    output = [_.strip() for _ in output] # remote trailing '\n'
    return output


def estimate_current_remote_usage(remote, username, r_dir, l_dir):
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
    files = fetch_remote_file_list(remote, username, r_dir)
    usage = 0
    for dir_ in sorted(files):
        match = re.search(r'(GSM\d+$)', os.path.basename(dir_))
        if match:
            rsem_comp = os.path.join(dir_, 'rsem.COMPLETE')
            if (not rsem_comp in files) and (not misc.is_empty_dir(dir_, files)):
                # only count the disk spaces used by those GSMs that are
                # being processed
                gsm_dir = dir_.replace(r_dir, l_dir)
                usage += estimate_rsem_usage(gsm_dir)
    return usage


def get_real_current_usage(remote, username, r_dir):
    """this will return real space consumed currently by rsem analysis"""
    output = misc.sshexec('du -s {0}'.format(r_dir), remote, username)
    # e.g. output:
    # ['3096\t/path/to/top_outdir\n']
    usage = int(output[0].split('\t')[0]) * 1024 # in KB => byte
    return usage


def estimate_rsem_usage(gsm_dir, fastq2rsem_ratio):
    """
    estimate the maximum disk space that is gonna be consumed by rsem analysis
    on one GSM based on a list of fq_gzs

    :param fq_gz_size: a number reprsenting the total size of fastq.gz files
                       for the corresponding GSM
    """
    fastq_usage = PPR.estimate_sra2fastq_usage(gsm_dir)
    return fastq_usage * fastq2rsem_ratio


def get_gsms_transferred(record_file):
    """
    fetch the list of GSMs that have already been transferred from record_file
    """
    if not os.path.exists(record_file):
        return []
    with open(record_file) as inf:
        return [_.strip() for _ in inf if not _.strip().startswith('#')]


def append_transfer_record(gsms_to_transfer, record_file):
    """
    append the GSMs that have just beened transferred successfully to
    record_file
    :param gsms_to_transfer: a list of strings representing the GSMs to be transferred, e.g.
     ['rsem_output/GSE34736/homo_sapiens/GSM854343',
      'rsem_output/GSE34736/homo_sapiens/GSM854344']
    """
    with open(record_file, 'ab') as opf:
        now = datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S')
        opf.write('# {0}\n'.format(now))
        for _ in gsms_to_transfer:
            opf.write('{0}\n'.format(_))


def find_gsms_to_transfer(all_gsms, transferred_gsms,
                          l_top_outdir, r_free_to_use, fastq2rsem_ratio):
    """
    select samples to transfer (different from select_samples_to_process in
    utils_pre_pipeline.py, which are to process)

    Walk through local top outdir, and for each GSMs, estimate its usage, and
    if it fits free_to_use space on remote host, count it as an element
    gsms_to_transfer

    :param all_gsms: a list of Sample instances representing all GSMs
    :param transferred_gsms: a list of string with GSM ids. e.g. [GSM1, GSM2]
    """
    # not yet transferred GSMs
    non_tf_gsms = [_ for _  in all_gsms if _.name not in transferred_gsms]
    gsms_to_transfer = []
    for gsm in non_tf_gsms:
        gsm_id = os.path.relpath(gsm.outdir, l_top_outdir)

        if not PPR.is_processed(gsm.outdir):
            # debug info will be logged by PPR.processed
            continue

        rsem_usage = estimate_rsem_usage(gsm.outdir, fastq2rsem_ratio)

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
    d = os.path.join(l_top_outdir, TRANSFER_SCRIPTS_DIR_BASENAME)
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


def calc_remote_free_space_to_use(r_host, r_username, r_top_outdir, l_top_outdir,
                                  r_cmd_df, r_max_usage, r_min_free):
    # r_real_current_usage is just for giving an idea of real usage on remote,
    # and it's not used for calculating free space to use
    P = misc.pretty_usage
    r_real = get_real_current_usage(r_host, r_username, r_top_outdir)
    r_real_pretty = P(r_real)
    logger.info('real current usage on {r_host} by {r_top_outdir}: '
                '{r_real_pretty}'.format(**locals()))

    r_estimated_current_usage = estimate_current_remote_usage(
        r_host, r_username, r_top_outdir, l_top_outdir)
    r_estimated_current_usage_pretty = P(r_estimated_current_usage)
    logger.info('free space on {r_host}: {r_estimated_current_usage_pretty}'.format(**locals()))

    r_free_space = get_remote_free_disk_space(r_host, r_username, r_cmd_df)
    r_free_space_pretty = P(r_free_space)
    logger.info('free space on {r_host}: {r_free_space_pretty}'.format(**locals()))

    r_max_usage_pretty = P(r_max_usage)
    logger.info('r_max_usage: {0}'.format(r_max_usage_pretty))

    r_min_free_pretty = P(r_min_free)
    logger.info('r_min_free: {0}'.format(r_min_free_pretty))

    r_free_to_use = misc.calc_free_space_to_use(
        r_estimated_current_usage, r_free_space, r_min_free, r_max_usage)
    return r_free_to_use


@misc.lockit(os.path.expanduser('~/.rp-transfer'))
def main():
    options = parse_args_for_rp_transfer()
    config = misc.get_config(options.config_file)

    # r_: means relevant to remote host, l_: to local host
    l_top_outdir = config['LOCAL_TOP_OUTDIR']
    r_top_outdir = config['REMOTE_TOP_OUTDIR']

    G = PPR.gen_all_samples_from_soft_and_isamp
    all_gsms = G(options.soft_files, options.isamp, config)
    PPR.init_sample_outdirs(all_gsms, l_top_outdir)

    r_host, r_username = config['REMOTE_HOST'], config['USERNAME']
    r_cmd_df = config['REMOTE_CMD_DF']
    r_max_usage = misc.ugly_usage(config['REMOTE_MAX_USAGE'])
    r_min_free = misc.ugly_usage(config['REMOTE_MIN_FREE'])
    r_free_to_use  = calc_remote_free_space_to_use(
        r_host, r_username, r_top_outdir, l_top_outdir,
        r_cmd_df, r_max_usage, r_min_free)

    # tf: transfer/transferred
    tf_record = os.path.join(l_top_outdir, 'transferred_GSMs.txt')
    tf_gsms = get_gsms_transferred(tf_record)
    tf_gsms_bn = map(os.path.basename, tf_gsms)

    # Find GSMs to transfer based on disk usage rule
    gsms_to_tf = find_gsms_to_transfer(
        all_gsms, tf_gsms_bn, l_top_outdir, r_free_to_use, config['FASTQ2RSEM_RATIO'])

    if not gsms_to_tf:
        logger.info('Cannot find a GSM that fits the current disk usage rule')
        return

    logger.info('GSMs to transfer:')
    for k, gsm in enumerate(gsms_to_tf):
        logger.info('\t{0:3d} {1:30s} {2}'.format(k+1, gsm, gsm.outdir))

    gsms_to_tf_ids = [os.path.relpath(_.outdir, l_top_outdir)
                      for _ in gsms_to_tf]
    tf_script = write_transfer_sh(
        gsms_to_tf_ids, options.rsync_template, l_top_outdir,
        r_username, r_host, r_top_outdir)

    os.chmod(tf_script, stat.S_IRUSR | stat.S_IWUSR| stat.S_IXUSR)
    rcode = misc.execute_log_stdout_stderr(tf_script)

    if rcode == 0:
        # different from processing in rsempipeline.py, where the completion is
        # marked by .COMPLETE flags, but by writting the completed GSMs to
        # gsms_transfer_record
        append_transfer_record(gsms_to_tf_ids, tf_record)


if __name__ == "__main__":
    main()
