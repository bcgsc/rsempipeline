import os
import glob
import logging
import logging.config
import re
import subprocess
import csv
import json

import settings as S
logger = logging.getLogger('utils')

def backup_file(f):
    if os.path.exists(f):
        dirname = os.path.dirname(f)
        basename = os.path.basename(f)
        count = 1
        rn_to = os.path.join(
            dirname, '#' + basename + '.{0}#'.format(count))
        while os.path.exists(rn_to):
            count += 1
            rn_to = os.path.join(
                dirname, '#' + basename + '.{0}#'.format(count))
        logger.info("BACKING UP {0} to {1}".format(f, rn_to))
        os.rename(f, rn_to)
        return rn_to
        logger.info("BACKUP FINISHED")


def touch(fname, times=None):
    with file(fname, 'a'):
        os.utime(fname, times)

    
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
        if (os.path.getmtime(cache_file) < os.path.getmtime(_) or
            # ctime: e.g. when renaming test.bk to test changes information in
            # inode
            os.path.getctime(cache_file) < os.path.getctime(_)):
            return False
    return True


def gen_sample_msg_id(sample):
    """
    used as an id to identify a particular sample for each logging message
    """
    return '{0} ({2}/{3}) of {1}'.format(
        sample.name, sample.series.name, 
        sample.index, sample.series.num_passed_samples())


def gen_sra_msg_id(sra):
    sample = sra.sample
    series = sample.series
    return '{0} ({1}/{2}) of {3} ({4}/{5}) of {6}'.format(
        sra.name, sra.index, sample.num_sras(), 
        sample.name, sample.index, series.num_passed_samples(),
        series.name)


def execute(cmd, msg_id='', flag_file=None, debug=False):
    logger.info('executing CMD: {0}'.format(cmd))
    if debug:
        return
    try:
        returncode = subprocess.call(cmd, shell=True, executable="/bin/bash")
        if returncode != 0:
            logger.error(
                '{0}, started, but then failed with returncode: {1}. '
                'CMD "{2}"'.format(msg_id, returncode, cmd))
        else:
            if flag_file is not None:
                touch(flag_file)
    except OSError, err:
        logger.exception(
            '{0}, failed to start, raising OSError {1}. '
            'CMD: "{2}"'.format(msg_id, err, cmd))


def gen_completion_stamp(key, stamp_dir):
    """
    @param key: the key to identify the type of stamp. e.g. FASTQ
    @param stamp_dir: where this stamp is to be created
    """
    return os.path.join(stamp_dir, '{0}.COMPLETE'.format(key))

