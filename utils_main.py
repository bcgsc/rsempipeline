import os
import pickle
import subprocess

import ruffus as R

from soft_parser import parse
from isamples_parser import gen_isamples_from_csv, gen_isamples_from_str
from utils import decide_num_jobs

import logging
logger = logging.getLogger('utils_main')

def parse_args():
    parser = R.cmdline.get_argparse(
        description="rsem_pipeline",
        usage='require python2.7.x',
        version='0.1')

    parser.add_argument(
        '-s', '--soft_files', nargs='+', required=True,
        help='a list of soft files')
    parser.add_argument(
        '-i', '--isamples',
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
        '--logging_config', default='logging.config', 
        help='logging configuration file')
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
    if options.top_outdir:
        top_outdir = options.top_outdir
    else:
        if os.path.exists(options.isamples):
            top_outdir = os.path.dirname(options.isamples)
        else:
            top_outdir = os.path.dirname(__file__)
    return top_outdir


def get_rsem_outdir(options):
    top_outdir = get_top_outdir(options)
    return os.path.join(top_outdir, 'rsem_output')


def get_isamples(isamples_file_or_str):
    """
    get the mannually curated data of interested samples (isamples) in a
    particular data structure
    """
    V = isamples_file_or_str
    if os.path.exists(V):     # then it's a file
        if os.path.splitext(V)[-1] == '.csv':
            res = gen_isamples_from_csv(V)
        else:
            raise ValueError(
                "uncognized file type of {0} as input_file for isamples".format(V))
    else:                       # it's a string
        res = gen_isamples_from_str(V)
    return res


def gen_samples_from_soft_and_isamples(soft_files, data, config):
    """
    :param data: e.g. mannually prepared sample data from data_file
    (GSE_GSM_species.csv) or data_str
    """
    # Nomenclature:
    #     soft_files: soft_files downloaded with tools/download_soft.py
    #     samples: a list of Sample instances
    #     sample_data: data from the sample_data_file stored in a dict
    #     data_file: the file with sample_data stored (e.g. GSE_GSM_species.csv)
    #     series: a series instance constructed from information in a soft file

    samples = []
    for soft_file in soft_files:
        series = parse(soft_file, config['INTERESTED_ORGANISMS'])
        # samples that are interested by the collaborator 
        if not series.name in data:
            continue
        interested_samples = data[series.name]
        # intersection among GSMs found in the soft file and
        # sample_data_file
        samples.extend([_ for _ in series.passed_samples
                        if _.name in interested_samples])
    logger.info('After intersection among soft and data, '
                '{0} samples remained'.format(len(samples)))
    # gsm ids of interested samples
    gsm_set1 = [_ for val in data.values() for _ in val]
    # gsm ids of samples after intersection
    gsm_set2 = [_.name for _ in samples]
    diff1 = list(set(gsm_set1) - set(gsm_set2))
    if diff1:
        logger.error('samples in isamples (-i) but not to be processed:\n\t'
                     '{0}'.format(diff1))
    diff2 = list(set(gsm_set2) - set(gsm_set1))
    if diff2:
        logger.error('samples to be processed but not in isamples (-i):\n\t'
                     '{0}'.format(diff2))
    return samples


def init_sample_outdirs(samples, outdir):
    for sample in samples:
        sample.gen_outdir(outdir)
        if not os.path.exists(sample.outdir):
            logger.info('creating directory: {0}'.format(sample.outdir))
            os.makedirs(sample.outdir)


def act(options, samples):
    if options.host_to_run == 'local':
        R.pipeline_run(options.target_tasks, logger=logger,
                       verbose=options.verbose)
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
