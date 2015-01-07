#!/usr/bin/env python

"""
This script detects duplicated GSMs within one GSE in GSE_GSM.csv

Please remove duplicates mannually (if practical) and save the file to a new
GSE_GSM.csv file
"""

import sys
import logging
logger = logging.getLogger(__name__)

from rsempipeline.preprocess.utils import read

def main(options):
    input_csv = options.input_csv

    flag = True
    current_gse = None
    for gse, gsm in read(input_csv):
        if current_gse is None or gse != current_gse:
            all_gsm = []
            current_gse = gse
        if gsm not in all_gsm:
            all_gsm.append(gsm)
        else:
            logger.warning('duplicated GSM: {0} from {1}'.format(gsm, gse))
            flag = False
    if flag:
        logger.info('No duplication detected within any GSE. :)')
    else:
        logger.warning('please check the duplicated GSMs. :(')
        sys.exit(1)
