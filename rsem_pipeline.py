#!/usr/bin/env python
# -*- coding: utf-8 -*

"""
This script parse soft files, download sra, convert sra to fastq and run rsem
on them, either on local system or on remote cluster.
"""

import os
import sys
import re
import argparse
import logging.config
import urlparse
import yaml

import ruffus as R


import utils as U
import settings as S

from soft_parser import parse
from sample_data_parser import \
    gen_sample_data_from_csv_file, gen_sample_data_from_data_str
from utils_download import gen_orig_params
from utils_rsem import gen_fastq_gz_input

# to match path of GSM, e.g.
# /projects/btl2/batch7/rsem_pipeline2/sample_data/rsem_output/mouse/GSE35213/GSM863770
logging.config.dictConfig(S.LOGGING_CONFIG)
logger = logging.getLogger('rsem_pipeline')

PATH_RE = r'(.*)/(?P<species>\S+)/(?P<GSE>GSE\d+)/(?P<GSM>GSM\d+)'


def gen_samples_from_soft_and_data(soft_files, data):
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
        global config
        series = parse(soft_file, config['INTERESTED_ORGANISMS'])
        # samples that are interested by the collaborator 
        if not series.name in data:
            continue
        interested_samples = data[series.name]
        # intersection among GSMs found in the soft file and
        # sample_data_file
        samples.extend([_ for _ in series.passed_samples
                        if _.name in interested_samples])
    logger.info(
        'After intersection among GSMs found in the {0} and '
        '{1}, {2} samples remained'.format(soft_file, data_file, len(samples)))
    return samples


def originate_params():
    """
    Generate a list of sras to download for each sample

    This function gets called twice, once before entering the queue, once after 
    """
    global samples, args

    logger.info('preparing originate_params '
                'for {0} samples'.format(len(samples)))
    orig_params_sets = gen_orig_params(samples, args.use_pickle)
    logger.info(
        '{0} sets of orig_params generated'.format(len(orig_params_sets)))
    for _ in orig_params_sets:
        yield _


@R.files(originate_params)
def download(inputs, outputs, sample):
    """inputs is None""" 
    msg_id = U.gen_sample_msg_id(sample)
    # e.g. sra
    # test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/SRX029242/SRR070177/SRR070177.sra
    sra, flag_file = outputs    # the others are sra files
    sra_outdir = os.path.dirname(sra)
    if not os.path.exists(sra_outdir):
        os.makedirs(sra_outdir)
    # e.g. url_path:
    # /sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
    url_path = urlparse.urlparse(sample.url).path
    sra_url_path = os.path.join(url_path, *sra.split('/')[-2:])
    cmd = (
        "/home/kmnip/bin/ascp "
        "-i /home/kmnip/.aspera/connect/etc/asperaweb_id_dsa.putty "
        "--ignore-host-key "
        "-QT "                 # -Q Fair transfer policy, -T Disable encryption
        "-L {0} "              # log dir
        "-k2 "
        "-l 300m "
        "anonftp@ftp-trace.ncbi.nlm.nih.gov:{1} {0}".format(
            sra_outdir, sra_url_path))
    global args
    U.execute(cmd, msg_id, flag_file, args.debug)


@R.subdivide(
    download,
    R.formatter(r'{0}/(?P<SRX>SRX\d+)/(?P<SRR>SRR\d+)/(.*)\.sra'.format(PATH_RE)),
    ['{subpath[0][2]}/{SRR[0]}_[12].fastq.gz',
     '{subpath[0][2]}/{SRR[0]}.sra.sra2fastq.COMPLETE'])
def sra2fastq(inputs, outputs):
    sra, _ = inputs             # ignore the flag file from previous task
    flag_file = outputs[-1]
    outdir = os.path.dirname(os.path.dirname(os.path.dirname(sra)))
    cmd = ('fastq-dump --minReadLen 25 --gzip --split-files '
           '--outdir {0} {1}'.format(outdir, sra))
    global args
    U.execute(cmd, flag_file=flag_file, debug=args.debug)


@R.collate(
    sra2fastq,
    R.formatter(r'{0}\/(?P<SRR>SRR\d+)\_[12]\.fastq.gz'.format(PATH_RE)),
    ['{subpath[0][0]}/{GSM[0]}.genes.results',
     '{subpath[0][0]}/{GSM[0]}.isoforms.results',
     '{subpath[0][0]}/{GSM[0]}.stat/{GSM[0]}.cnt',
     '{subpath[0][0]}/{GSM[0]}.stat/{GSM[0]}.model',
     '{subpath[0][0]}/{GSM[0]}.stat/{GSM[0]}.theta',
     '{subpath[0][0]}/align.stats',
     '{subpath[0][0]}/rsem.log',
     '{subpath[0][0]}/rsem.COMPLETE'])

# example of outputs:
# ├── GSM629265
# │── ├── GSM629265.genes.results
# │── ├── GSM629265.isoforms.results
# │── ├── align.stats
# │── ├── rsem.log
# │── ├── rsem.COMPLETE
# │── └── GSM629265.stat
# │──     ├── GSM629265.cnt
# │──     ├── GSM629265.model
# │──     └── GSM629265.theta
def rsem(inputs, outputs):
    """
    :params inputs: a tuple of 1 or 2 fastq.gz files, e.g.
    ('/path/to/rsem_output/homo_sapiens/GSE50599/GSM1224499/SRR968078_1.fastq.gz',
     '/path/to/rsem_output/homo_sapiens/GSE50599/GSM1224499/SRR968078_2.fastq.gz')
    """
    # this is equivalent to the sample.outdir or GSM dir 
    outdir = os.path.dirname(inputs[0])
    fastq_gz_input = gen_fastq_gz_input(inputs)

    res = re.search(PATH_RE, outdir)
    species = res.group('species')
    gsm = res.group('GSM')

    # following rsem naming convention
    global config
    reference_name = config['REFERENCE_NAMES'][species]
    sample_name = '{outdir}/{gsm}'.format(**locals())

    flag_file = outputs[-1]
    cmd = ' '.join([
        'rsem-calculate-expression', # 1.2.5
        '-p 2',
        '--time',
        '--no-bam-output',
        '--bowtie-chunkmbs 256',
        # could also be found in the PATH
        # '--bowtie-path', '/home/zxue/Downloads/rchiu_Downloads/bowtie-1.0.0',
        fastq_gz_input,
        reference_name,
        sample_name,
        '1>{0}/rsem.log'.format(outdir),
        '2>{0}/align.stats'.format(outdir)])
    global args
    U.execute(cmd, flag_file=flag_file, debug=args.debug)

def parse_args():
    parser = argparse.ArgumentParser(
        description='Run rsem pepline on a local system',
        usage='python2.7.x {0} -s soft_file [-o some_outdir]')
    parser.add_argument(
        '-s', '--soft-files', nargs='+', required=True,
        help='a list of soft files')
    parser.add_argument(
        '-f', '--data-file',
        help=('e.g. GSE_GSM_species.csv '
              '(based on the xlsx/csv file provided by the collaborator) '
              'for intersection with GSMs available in the soft files'))
    parser.add_argument(
        '-d', '--data-str',
        help=("Instead of specifying a data file, "
              "should also serve a data string separated by ';'. e.g. "
              "'GSE11111 GSM000001 GSM000002;GSE222222 GSM000001'"))
    parser.add_argument(
        '--host-to-run', required =True,
        choices=['local', 'genesis'], 
        help=('choose a host to run, if it is not local, '
              'a corresponding template of submission script '
              'is expected to be found in the templates folder'))
    parser.add_argument(
        '-o', '--top-outdir', 
        help=('top output directory, default to the dirname of '
              'the value of --data-file, if --data-file is not specified, '
              'default to the current directory'))
    parser.add_argument(
        '--tasks', nargs='+', choices=['download', 'sra2fastq', 'rsem'],
        help=('Specify the tasks to run, e.g. on genesis, you can only do '
              'sra2fastq and rsem; on apollo, you may want do download only '
              'and then transfer all sra files to genesis'))
    parser.add_argument(
        '-n', '--ruffus-num-threads', type=int, default=1,
        help='number of threads used in Ruffus.pipeline_run')
    parser.add_argument(
        '--config-file', default='rsem_pipeline_config.yaml', 
        help='a YAML configuration file')
    parser.add_argument(
        '--debug', action='store_true',
        help='if debug, commands won\'t be executed')
    parser.add_argument(
        '--use-pickle', action='store_true',
        help='if true, pickle file per sample will be used to generate originate files')
    parser.add_argument(
        '--ruffus-verbose', type=int, default=1,
        help='verbosity of ruffus')
    args = parser.parse_args()
    return args


def init_sample_outdirs(samples, outdir):
    for sample in samples:
        sample.gen_outdir(outdir)
        if not os.path.exists(sample.outdir):
            logger.info('creating directory: {0}'.format(sample.outdir))
            os.makedirs(sample.outdir)


if __name__ == "__main__":
    # have to use global variables because of ruffus
    args = parse_args()
    with open(args.config_file) as inf:
        config = yaml.load(inf.read())

    soft_files = args.soft_files
    data_file = args.data_file
    data_str = args.data_str

    if data_file:
        if os.path.splitext(data_file)[-1] == '.csv':
            data = gen_sample_data_from_csv_file(data_file)
        else:
            raise ValueError(
            "uncognized file type of {0} as samples_input_file".format(data_file))
    elif data_str:
        data = gen_sample_data_from_data_str(data_str)
    else:
        raise ValueError(
            "At least one --data-file or a --data-str should be specified")

    samples = gen_samples_from_soft_and_data(soft_files, data)

    if args.top_outdir:
        top_outdir = args.top_outdir
    else:
        if data_file:
            top_outdir = os.path.dirname(args.data_file)
        else:
            top_outdir = os.path.dirname(__file__)

    outdir =  os.path.join(top_outdir, 'rsem_output')
    logger.info('initializing outdirs of samples...')
    init_sample_outdirs(samples, outdir)

    if args.host_to_run == 'local':
        tasks_to_run = [locals()[_] for _ in args.tasks] if args.tasks else []
        R.pipeline_run(tasks_to_run, multiprocess=args.ruffus_num_threads,
                       verbose=args.ruffus_verbose)
    elif args.host_to_run == 'genesis':
        from jinja2 import Environment, PackageLoader
        env = Environment(loader=PackageLoader('rsem_pipeline', 'templates'))
        template = env.get_template('{}.jinja2'.format(args.host_to_run))
        for sample in samples:
            submission_script = os.path.join(sample.outdir, '0_submit.sh')
            with open(submission_script, 'wb') as opf:
                opf.write(template.render(
                    sample=sample,
                    rsem_pipeline_py=os.path.relpath(__file__, sample.outdir),
                    soft_file=os.path.relpath(sample.series.soft_file, sample.outdir),
                    data_str='{0} {1}'.format(sample.series.name, sample.name),
                    top_outdir=os.path.relpath(top_outdir, sample.outdir),
                    config_file=os.path.relpath(args.config_file, sample.outdir),
                    ruffus_num_threads=args.ruffus_num_threads))
            print submission_script
