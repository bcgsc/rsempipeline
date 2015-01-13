import os
import sys
import re
import csv
import logging
logger = logging.getLogger(__name__)

from rsempipeline.conf.settings import SHARE_DIR

    
def read(input_csv):
    """
    Read csv and yield gse and gsm in pair

    :param input_csv: the csv with only information of GSE and GSM with one GSE
    per row, and GSMs are separated by semicolon
    check example_GSE_GSM.txt for example.
    """
    valid = is_valid(input_csv)
    if not valid:
        logger.error('Please correct the invalid entries in '
                    '{0}'.format(input_csv))
        logger.error('If unsure of the correct format, check {0}'.format(
            os.path.join(SHARE_DIR, 'GSE_GSM.example.csv')))
        sys.exit(1)
    else:
        return yield_gse_gsm(input_csv)


def stream(input_csv):
    with open(input_csv, 'rb') as inf:
        csv_reader = csv.reader(inf)
        for k, row in enumerate(csv_reader):
            if row and not row[0].startswith('#'):             # not a blank line
                res = process(k+1, row)
                yield res


def is_valid(input_csv):
    """Check wheter the input_csv is valid or not"""
    for res in stream(input_csv):
        if res is None:
            return False
    return True


def yield_gse_gsm(input_csv):
    for res in stream(input_csv):
        gse, gsms = res
        for gsm in gsms:
            yield gse, gsm


def process(k, row):
    """
    Process each row and return gse, and gsms, including basic sanity
    checking

    :param k: row number. i.e. 1, 2, 3, 4, ...
    :param row: csv row value as a list

    """
    # check if there are only two columns
    if len(row) != 2:
        logger.warning('row {0} is not of len 2'.format(k))
        return

    gse, gsms = row
    gsms = [_.strip() for _ in gsms.strip().rstrip(';').split(';')]

    # check if GSE is properly named
    if not re.search('^GSE\d+', gse):
        logger.warning('row {0}: invalid GSE name: {1}'.format(k, gse))
        return

    # check if GSMs are properly named
    if not all(re.search('^GSM\d+', _) for _ in gsms):
        logger.warning("row {0}: Not all GSMs are of valid names, do you have invalid "
                       "characters at the end of the line by mistake?".format(k))
        return

    num_gsms = len(gsms)
    num_unique_gsms = len(set(gsms))
    if num_gsms > num_unique_gsms:
        logger.warning("Duplicated GSMs found in {0}. of {1} GSMs, only {2} "
                       "are unique".format(gse, num_gsms, num_unique_gsms))
        return
    return gse, gsms
