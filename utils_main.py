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
        '--host_to_run', required =True,
        choices=['local', 'genesis'], 
        help=('choose a host to run, if it is not local, '
              'a corresponding template of submission script '
              'is expected to be found in the templates folder'))
    parser.add_argument(
        '-o', '--top_outdir', 
        help=('top output directory, which defaults to the dirname of '
              'the value of --data if it\'s a file. Otherwise it '
              'defaults to the current directory'))
    parser.add_argument(
        '--config_file', default='rsem_pipeline_config.yaml', 
        help='a YAML configuration file')
    parser.add_argument(
        '--debug', action='store_true',
        help='if debug, commands won\'t be executed, just print out per task')
    parser.add_argument(
        '--use_pickle', action='store_true',
        help=('if true, will create a pickle file per sample to store '
              'a list of sra files as cache, this not only speed up rerun, '
              'but also is useful when there is no Internet connection. '
              'Otherwise, the list will be fetched from FTP server.'))
    args = parser.parse_args()
    return args


def get_top_outdir(options):
    """
    decides the top output dir, if specified in the command line, then use the
    specified one, otherwise, use the directory where GSE_species_GSM.csv is
    located
    """
    if options.top_outdir:
        top_outdir = options.top_outdir
    else:
        if os.path.exists(options.isamp):
            top_outdir = os.path.dirname(options.isamp)
        else:
            top_outdir = os.path.dirname(__file__)
    return top_outdir


def get_rsem_outdir(options):
    """get the output directory for rsem, it's top_outdir/rsem_output by default"""
    top_outdir = get_top_outdir(options)
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
    if options.host_to_run == 'local':
        R.pipeline_run(options.target_tasks, logger=logger,
                       multiprocess=options.jobs, verbose=options.verbose)
    elif options.host_to_run == 'genesis':
        from jinja2 import Environment, PackageLoader
        env = Environment(loader=PackageLoader('rsem_pipeline', 'templates'))
        template = env.get_template('{}.jinja2'.format(options.host_to_run))
        for sample in samples:
            submission_script = os.path.join(sample.outdir, '0_submit.sh')
            render(submission_script, template, sample, options)
            logger.info('preparing submitting {0}'.format(submission_script))
            qsub(submission_script)


def render(submission_script, template, sample, options):
    """
    :param submission_script: target submission_script with path
    """
    num_jobs = decide_num_jobs(sample.outdir)
    top_outdir = get_top_outdir(options)
    with open(submission_script, 'wb') as opf:
        content = template.render(
            sample=sample,
            rsem_pipeline_py=os.path.relpath(
                os.path.join(os.path.dirname(__file__), 'rsem_pipeline.py'),
                sample.outdir),
            soft_file=os.path.relpath(sample.series.soft_file, sample.outdir),
            isamples_str='{0} {1}'.format(sample.series.name, sample.name),
            top_outdir=os.path.relpath(top_outdir, sample.outdir),
            config_file=os.path.relpath(options.config_file, sample.outdir),
            logging_config=os.path.relpath(options.logging_config, sample.outdir),
            num_jobs=num_jobs)
        opf.write(content)


def qsub(submission_script):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(os.path.dirname(submission_script))
    subprocess.call(['qsub', os.path.basename(submission_script)])
    os.chdir(current_dir)
