import os
import re

from soft_parser import parse
from isamp_parser import get_isamp

import logging
logger = logging.getLogger(__name__)


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
        logger.info('{0} samples found in {1}'.format(num_isamp, isamp_file_or_str))
    else:
        logger.info('{0} samples found from -i/--isamp input'.format(num_isamp))

    # a list, of Sample instances resultant of intersection
    res_samp = []
    for soft_file in soft_files:
        # e.g. soft_file: GSE51008_family.soft.subset
        gse = re.search('(GSE\d+)\_family\.soft\.subset', soft_file)
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
    if num_isamp != num_res_samp:
        raise ValueError(
            'Unmatched numbers of samples interested ({0}) and to be processed '
            '({1}) after intersection. Please check ERROR in log '
            'for details.'.format(num_isamp, num_res_samp))
    else:
        logger.info('No discrepancies detected after intersection, '
                    'all {0} samples will be processed '.format(num_isamp))


def get_top_outdir(config, options):
    """
    decides the top output dir, if specified in the configuration file, then
    use the specified one, otherwise, use the directory where
    GSE_species_GSM.csv is located
    """
    top_outdir = config.get('LOCAL_TOP_OUTDIR')
    if top_outdir is not None:
        return top_outdir
    else:
        if os.path.exists(options.isamp):
            top_outdir = os.path.dirname(options.isamp)
        else:
            raise ValueError(
                'input from -i is not a file and '
                'no LOCAL_TOP_OUTDIR parameter found in {0}'.format(
                    options.config_file))
    return top_outdir


def get_rsem_outdir(config, options):
    """get the output directory for rsem, it's top_outdir/rsem_output by default"""
    top_outdir = get_top_outdir(config, options)
    return os.path.join(top_outdir, 'rsem_output')


def init_sample_outdirs(samples, config, options):
    outdir = get_rsem_outdir(config, options)
    for sample in samples:
        sample.gen_outdir(outdir)
        if not os.path.exists(sample.outdir):
            logger.info('creating directory: {0}'.format(sample.outdir))
            os.makedirs(sample.outdir)
