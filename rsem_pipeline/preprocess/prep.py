import os
import argparse

from . import find_duplicated_GSMs, gen_GSE_species_GSM_csv, download_soft

from rsem_pipeline.conf.settings import SHARE_DIR

INPUT_FILE = 'GSE_GSM.csv'
OUTPUT_FILE = 'GSE_species_GSM.csv'

def get_dummy_parser():
    """
    dummy_parser just holds the common arguments shared by different
    sub-parsers of top level parser
    """
    dp = argparse.ArgumentParser(add_help=False) # dp: dummy_parser
    dp.add_argument(
        '-f', '--input_csv', type=str, required=True,
        help='input GSE_GSM.csv, check {0} for its format'.format(
            os.path.join(SHARE_DIR, 'example_GSE_GSM.csv')))
    return dp


def parse_args():
    parser = argparse.ArgumentParser(
        description=('To preprocess {0}, and generate {1} for the use of '
                     'rsem-pipeline run'.format(INPUT_FILE, OUTPUT_FILE)))
    # metavar='': to supress the listing of sub-commands in {}
    subparsers = parser.add_subparsers(metavar='')
    dummy_parser = get_dummy_parser()

    # sp: subparser
    sp_find_dup = subparsers.add_parser(
        'find-dup', parents=[dummy_parser],
        help='Find duplicated GSMs in {0}'.format(INPUT_FILE))
    sp_find_dup.set_defaults(func=find_duplicated_GSMs.main)

    sp_gen_csv = subparsers.add_parser(
        'gen-csv', parents=[dummy_parser],
        help=('Generate {0}, as well as downloading all htmls for all '
              'GSMs'.format(OUTPUT_FILE)))
    sp_gen_csv.add_argument(
        '--nt', type=int, default=1,
        help='number of threads')
    sp_gen_csv.add_argument(
        '--outdir', type=str,
        help=('output directory, default to the location of '
              '{0}.'.format(INPUT_FILE)))
    sp_gen_csv.set_defaults(func=gen_GSE_species_GSM_csv.main)

    sp_get_soft = subparsers.add_parser(
        'get-soft', parents=[dummy_parser],
        help='Download soft files for all GSEs')
    sp_get_soft.add_argument(
        '--outdir', type=str,
        help=('directory for downloaded htmls, default to '
              '{{location of {0}}}/soft, '.format(INPUT_FILE)))
    sp_get_soft.set_defaults(func=download_soft.main)

    options = parser.parse_args()
    return options


def main():
    options = parse_args()
    options.func(options)


if __name__ == "__main__":
    main()
