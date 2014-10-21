"""utilities for the download task"""

import os
import logging
logger = logging.getLogger(__name__)

import yaml

def gen_orig_params(samples):
    """
    Generate original parameters for the download task
    """
    orig_params_sets = []
    for sample in samples:
        orig_params = gen_orig_params_per(sample)
        orig_params_sets.extend(orig_params)
    return orig_params_sets


def gen_orig_params_per(sample):
    """
    construct orig_params based on current sample.outdir

    Example of orig_params:
    [[None,
      ['path/to/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401055/SRR401055.sra',
       'path/to/rsem_output/mouse/GSE35213/GSM863771/SRR401056.sra.download.COMPLETE'],
      <GSM863771 (2/8) of GSE35213>],
     [None,
      ['path/to/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401056/SRR401056.sra',
       'path/to/rsem_output/mouse/GSE35213/GSM863771/SRR401055.sra.download.COMPLETE'],
      <GSM863771 (2/8) of GSE35213>],
     ]
    """
    info_file = 'sras_info.yaml'
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
