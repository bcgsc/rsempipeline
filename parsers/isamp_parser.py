"""parser for GSE_species_GSM.csv, which include isamp (Interested Samples)"""

import re
import os
import csv

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
            # could use # to comment a line
            if row and not row[0].startswith('#'):
                res = process(k+1, row)
                if res is not None:
                    gse, _, gsm = res
                    if gse in sample_data:
                        sample_data[gse].append(gsm)
                    else:
                        sample_data[gse] = [gsm]
    return sample_data


def process(k, row):
    """Process one row of GSE_species_GSM.csv"""
    def err():
        logger.error('Ignored invalid row ({0}): {1}'.format(k, row))

    # check if there are only two columns
    if len(row) != 3:
        err()
        return

    gse, species, gsm = row
    # check if GSE is properly named
    if not re.search(r'^GSE\d+$', gse):
        err()
        return
    # check if GSMs are properly named
    if not re.search(r'^GSM\d+$', gsm):
        err()
        return
    return gse, species, gsm


def gen_isamp_from_csv(input_csv):
    """
    Generate input data from GSE_species_GSM.csv with the specified data
    structure as shown below
    """
    sample_data = read_csv_gse_as_key(input_csv)
    return sample_data


def gen_isamp_from_str(data_str):
    """Generate input data from command line argument"""
    sample_data = {}
    for _ in data_str.split(';'):
        stuffs = _.strip().split()
        gse, gsms = stuffs[0], stuffs[1:]
        if gse not in sample_data:
            sample_data[gse] = gsms
        else:
            sample_data[gse].extend(gsms)
    return sample_data


def get_isamp(isamp_file_or_str):
    """
    get a dict of interested samples from GSE_species_GSM.csv or str

    data structure returned:
    {'GSExxxxx': ['GSMxxxxxxx', 'GSMxxxxxxx', 'GSMxxxxxxx'],
     'GSExxxxx': ['GSMxxxxxxx', 'GSMxxxxxxx', 'GSMxxxxxxx']}
    """
    V = isamp_file_or_str
    if os.path.exists(V):                     # then it's a file
        if os.path.splitext(V)[-1] == '.csv': # then it's a csv file
            res = gen_isamp_from_csv(V)
        else:
            raise ValueError("uncognized file type of {0} as input_file for "
                             "isamples".format(V))
    else:                       # it's a string
        res = gen_isamp_from_str(V)
    return res
