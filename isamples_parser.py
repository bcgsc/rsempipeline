import os
import csv
import json

from utils import cache_usable

def read_csv_gse_as_key(infile):
    """
    "param infile: input file, usually named GSE_GSM_species.csv

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
        csvreader = csv.reader(inf, delimiter='\t')
        for (gse, gsm, species) in csvreader:
            if gse in sample_data:
                sample_data[gse].append(gsm)
            else:
                sample_data[gse] = [gsm]
    return sample_data

def gen_sample_data_from_csv_file(input_csv):
    """
    Generate input data with the specified data structure as shown below
    """

    dirname, basename = os.path.split(input_csv)
    cache_file = os.path.join(
        dirname, '.{0}.json'.format(os.path.splitext(basename)[0]))
    if cache_usable(cache_file, input_csv):
        with open(cache_file, 'rb') as inf:
            sample_data = json.load(inf)
    else:
        sample_data = read_csv_gse_as_key(input_csv)
        with open(cache_file, 'wb') as opf:
            json.dump(sample_data, opf)
    return sample_data

def gen_sample_data_from_data_str(data_str):
    sample_data = {}
    for _ in data_str.split(';'):
        stuffs = _.strip().split()
        gse, gsms = stuffs[0], stuffs[1:]
        sample_data[gse] = gsms
    return sample_data
