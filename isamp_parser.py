"""parser for GSE_species_GSM.csv, which include isamp (Interested Samples)"""

import re
import os
import csv
import json

from utils import cache_usable

import logging
logger = logging.getLogger('isamp_parser')

def read_csv_gse_as_key(infile):
    """
    "param infile: input file, usually named GSE_species_GSM.csv

    Data structure of sample_data returned:
    sample_data = {
        'gse1': {[gsm1, gsm2, gsm3, ...],
                 [gsm1, gsm2, gsm3, ...],
                 [gsm1, gsm2, gsm3, ...],
                  ...},
        'gse2': {[gsm1, gsm2, gsm3, ...],
                 [gsm1, gsm2, gsm3, ...],
                 [gsm1, gsm2, gsm3, ...],
                  ...},
        'gse3':  [gsm1, gsm2, gsm3, ...],
                 [gsm1, gsm2, gsm3, ...],
                 [gsm1, gsm2, gsm3, ...],
                  ...},
        }
    """
    sample_data = {}
    with open(infile, 'rb') as inf:
        csv_reader = csv.reader(inf)
        for k, row in enumerate(csv_reader):
            if row:
                res = process(k+1, row)
                if res is not None:
                    gse, species, gsm = res
                    if gse in sample_data:
                        sample_data[gse].append(gsm)
                    else:
                        sample_data[gse] = [gsm]
    return sample_data


def process(k, row):
    def err():
        print ('Ignored invalid row ({0}): {1}'.format(k, row))

    # check if there are only two columns
    if len(row) != 3:
        err()
        return

    gse, species, gsm = row
    # check if GSE is properly named
    if not re.search('^GSE\d+$', gse):
        err()
        return
    # check if GSMs are properly named
    if not re.search('^GSM\d+$', gsm):
        err()
        return
    return gse, species, gsm


def gen_isamples_from_csv(input_csv):
    """
    Generate input data with the specified data structure as shown below
    """

    dirname, basename = os.path.split(input_csv)
    cache_file = os.path.join(
        dirname, '.{0}.json'.format(os.path.splitext(basename)[0]))
    if cache_usable(cache_file, input_csv):
        logger.info('reading from cache: {0}'.format(cache_file))
        with open(cache_file, 'rb') as inf:
            sample_data = json.load(inf)
    else:
        sample_data = read_csv_gse_as_key(input_csv)
        with open(cache_file, 'wb') as opf:
            json.dump(sample_data, opf)
    return sample_data

def gen_isamples_from_str(data_str):
    sample_data = {}
    for _ in data_str.split(';'):
        stuffs = _.strip().split()
        gse, gsms = stuffs[0], stuffs[1:]
        if gse not in sample_data:
            sample_data[gse] = gsms
        else:
            sample_data[gse].extend(gsms)
    return sample_data
