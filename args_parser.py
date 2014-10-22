"""parser for command line arguments"""

import os
import argparse

import ruffus as R

def add_common_arguments(parser):
    parser.add_argument(
        '-s', '--soft_files', nargs='+', required=True,
        help='a list of soft files')

    parser.add_argument(
        '-i', '--isamp',
        help=('interested samples, could be in a file (e.g. '
              'GSE_species_GSM.csv prepared previously) '
              'or a string in the form of '
              '"GSE11111 GSM000001 GSM000002;GSE222222 GSM000001". '
              'The data is used for intersecting with GSMs available '
              'in the soft files to extract only those are of interest.'
              'The pipeline will check data is a file and exists, or '
              'it assumes it\'s a data string'))


def parse_args_for_rsem_pipeline():
    """parse the command line arguments"""
    parser = R.cmdline.get_argparse(
        description="rsem_pipeline",
        usage='require python-2.7.x',
        version='0.1')

    add_common_arguments(parser)

    base_dir = os.path.abspath(os.path.dirname(__file__))
    parser.add_argument(
        '--j_rsem', type=int,
        help="the number of jobs to run when running rsem (-j)")
    parser.add_argument(
        '-t', '--qsub_template',
        help=('used when -T is gen_qsub_script, '
              'see a list of templates in templates directory'))

    config_examp = os.path.join(base_dir, 'rsem_pipeline_config.yaml.example')
    parser.add_argument(
        '-c', '--config_file', default='rsem_pipeline_config.yaml',
        help=('a YAML configuration file, refer to {0} for an example.'.format(
            config_examp)))

    parser.add_argument(
        '--recreate_sras_info', action='store_true',
        help=('if specified, it will recreate a yaml file per sample to store '
              'a list of sra files as well as their sizes as cache by fetching '
              'them from FTP server). Rerun without this option is faster, '
              'and also useful when there is no Internet connection.'))
    parser.add_argument(
        '--ignore_disk_usage_rule', action='store_true',
        help=('DANGEROUS, when specified, it will ignore the disk usage rule, '
              'and process as many GSMs as available. Better used when an '
              'small number of GSMs are explicitly set via -i/--isamp argument '
              'on the commandline.'))
    parser.add_argument(
        '--debug', action='store_true',
        help='if debug, commands won\'t be executed, just printed out per task')
    args = parser.parse_args()
    return args


def parse_args_for_rsem_transfer():
    """parse command line arguments and return options"""
    parser = argparse.ArgumentParser(
        description='rsem_cron_transfer.py',
        usage='require python-2.7.x',
        version='0.1')

    add_common_arguments(parser)
    base_dir = os.path.abspath(os.path.dirname(__file__))

    default_rsync_template = os.path.join(base_dir, 'templates/rsync.sh')
    parser.add_argument(
        '-t', '--rsync_template', default=default_rsync_template,
        help=('template for transferring GSMs from localhost to remote host, '
              'refer to {0} (default template) for an example.'.format(
                  default_rsync_template)))

    config_examp = os.path.join(base_dir, 'rsem_pipeline_config.yaml.example')
    parser.add_argument(
        '-c', '--config_file', default='rsem_pipeline_config.yaml',
        help=('a YAML configuration file, refer to {0} for an example.'.format(
            config_examp)))

    return parser.parse_args()
