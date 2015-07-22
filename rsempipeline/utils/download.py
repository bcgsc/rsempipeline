"""utilities for the download task"""

import os
import logging
logger = logging.getLogger(__name__)

import yaml

from rsempipeline.conf.settings import SRA_INFO_FILE_BASENAME


def gen_orig_params_per(sample):
    """
    construct original parameters per sample, refer to the tests in
    test_download to see what return value looks like
    """
    info_file = SRA_INFO_FILE_BASENAME
    with open(os.path.join(sample.outdir, info_file)) as inf:
        sras_info = yaml.load(inf.read())
        sras = [os.path.join(sample.outdir, i)
                for j in sras_info for i in j.keys()]
    flag_files = [
        os.path.join(sample.outdir,
                     '{0}.download.COMPLETE'.format(os.path.basename(sra)))
        for sra in sras]
    # originate params for one sample
    orig_params = []
    for s, f in zip(sras, flag_files):
        orig_params.append([None, [s, f], sample])
    return orig_params


def gen_orig_params(samples):
    """
    Generate original parameters for the download task
    """
    orig_params_sets = []
    for sample in samples:
        orig_params = gen_orig_params_per(sample)
        orig_params_sets.extend(orig_params)
    return orig_params_sets
