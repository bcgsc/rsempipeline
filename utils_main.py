import os
import re
import subprocess

import ruffus as R

from soft_parser import parse
from isamp_parser import get_isamp
from utils import decide_num_jobs

import logging
logger = logging.getLogger(__name__)

def parse_args():
    parser = R.cmdline.get_argparse(
        description="rsem_pipeline",
        usage='require python2.7.x',
        version='0.1')

    parser.add_argument(
        '-s', '--soft_files', nargs='+', required=True,
        help='a list of soft files')
    parser.add_argument(
        '-i', '--isamp',
        help=('interested samples, could be in a file (e.g. GSE_GSM_species.csv prepared) '
              'based on the xlsx/csv file provided by the collaborator '
              'or a string in the form of '
              '"GSE11111 GSM000001 GSM000002;GSE222222 GSM000001" '
              'data is used for intersection with GSMs available '
              'in the soft files to extract only those are of interest.'
              'The pipeline will check data is a file and exists, or '
              'it assumes it\'s a data string'))
    parser.add_argument(
        '--qsub_template',
        help=('used when tasks is gen_qsub_script, '
              'see a list of templates in templates directory'))
    parser.add_argument(
        '--config_file', default='rsem_pipeline_config.yaml', 
        help='a YAML configuration file')
    parser.add_argument(
        '--debug', action='store_true',
        help='if debug, commands won\'t be executed, just print out per task')
    parser.add_argument(
        '--not_use_pickle', action='store_true',
        help=('if specified, it will recreate a pickle file per sample to store '
              'a list of sra files as cache by fetching them from FTP server), '
              'Rerun without this option is faster, and also useful '
              'when there is no Internet connection.'))
    args = parser.parse_args()
    return args


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


def gen_samples_from_soft_and_isamp(soft_files, isamp_file_or_str, config):
    """
    :param isamp: e.g. mannually prepared interested sample file
    (e.g. GSE_species_GSM.csv) or isamp_str
    :type isamp: dict
    """
    # Nomenclature:
    #     soft_files: soft_files downloaded with tools/download_soft.py
    #     isamp: interested samples extracted from (e.g. GSE_species_GSM.csv)

    #     sample_data: data from the sample_data_file stored in a dict
    #     series: a series instance constructed from information in a soft file

    # for historical reason, soft files parsed does not return dict as
    # get_isamp
    isamp = get_isamp(isamp_file_or_str)
    samp_proc = []                # a list of Sample instances
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
                interested_samples = isamp[series.name]
                # intersection among GSMs found in the soft file and
                # sample_data_file
                samp_proc.extend([_ for _ in series.passed_samples
                                if _.name in interested_samples])
    logger.info('After intersection among soft and data, '
                '{0} samples remained'.format(len(samp_proc)))
    sanity_check(samp_proc, isamp)
    return samp_proc

def sanity_check(samp_proc, isamp):
    # gsm ids of interested samples
    gsms_isamp = ['{0}:{1}'.format(k, v) for k in isamp.keys() for v in isamp[k]]
    # gsm ids of samples after intersection
    gsms_proc = ['{0}:{1}'.format(_.series.name, _.name) for _ in samp_proc]

    def unmatch(x, y):
        raise ValueError('Unmatched numbers of samples interested ({0}) '
                         'and to be processed ({1})'.format(x, y))

    diff1 = sorted(list(set(gsms_isamp) - set(gsms_proc)))
    if diff1:
        logger.error('{0} samples in isamp (-i) but not to be processed:\n'
                     # '{1}'.format(len(diff1), os.linesep.join(diff1)))
                     '{1}'.format(len(diff1), format_gsms_diff(diff1)))
        unmatch(len(gsms_isamp), len(gsms_proc))

    diff2 = sorted(list(set(gsms_proc) - set(gsms_isamp)))
    if diff2:
        logger.error('{0} samples to be processed but not in isamples (-i):\n\t'
                     '{1}'.format(len(diff2), format_gsms_diff(diff2)))
        unmatch(len(gsms_isamp), len(gsms_proc))

def format_gsms_diff(diff):
    """format diff gsms for pretty output to the screen"""
    # the code is horrible, don't look into it, just need to know the input is
    # like:
    # ['GSExxxxx:GSMxxxxxxx', 'GSExxxxx:GSMxxxxxxx', 'GSExxxxx:GSMxxxxxxx']
    # and the output is like
    # GSExxxxx (3): GSMxxxxxxx;GSMxxxxxxx;GSMxxxxxxx
    # GSExxxxx (1): GSMxxxxxxx
    # GSExxxxx (2): GSMxxxxxxx;GSMxxxxxxx
    
    dd = {}
    current_gse = None
    for _ in sorted(diff):
        gse, gsm = _.split(':')
        if current_gse is None or gse != current_gse:
            current_gse = gse
            dd[gse] = [gsm]
        else:
            dd[gse].append(gsm)
    dd2 = {}
    for gse in dd:
        dd2['{0} ({1})'.format(gse, len(dd[gse]))] = dd[gse]
    return os.linesep.join('{0}: {1}'.format(gse, ' '.join(gsms)) for gse, gsms in dd2.items())


def init_sample_outdirs(samples, outdir):
    for sample in samples:
        sample.gen_outdir(outdir)
        if not os.path.exists(sample.outdir):
            logger.info('creating directory: {0}'.format(sample.outdir))
            os.makedirs(sample.outdir)


def act(options, samples):
    R.pipeline_run(
        logger=logger,
        target_tasks=options.target_tasks,
        forcedtorun_tasks=options.forced_tasks,
        multiprocess=options.jobs,
        verbose=options.verbose,
        touch_files_only=options.touch_files_only)
