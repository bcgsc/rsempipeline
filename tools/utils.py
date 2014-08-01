import re
import csv

def read(input_csv):
    """
    Read csv and yield gse and gsm in pair

    :param input_csv: the csv with only information of GSE and GSM with one GSE
    per row, and GSMs are separated by semicolon
    check example_GSE_GSM.txt for example.
    """
    with open(input_csv, 'rb') as inf:
        csv_reader = csv.reader(inf)
        for k, row in enumerate(csv_reader):
            if row:             # not a blank line
                res = process(k+1, row)
                if res is not None: # could be None in cases of invalid rows
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
    def err():
        print ('Ignored invalid row ({0}): {1}. Please check the format '
               'of example_GSE_GSM.csv'.format(k, row))

    # check if there are only two columns
    if len(row) != 2:
        err()
        return

    gse, gsms = row
    gsms = [_.strip() for _ in gsms.split(';')]
    # check if GSE is properly named
    if not re.search('^GSE\d+', gse):
        err()
        return
    # check if GSMs are properly named
    if not all(re.search('^GSM\d+', _) for _ in gsms):
        err()
        return
    return gse, gsms




def read_csv_species_as_key(infile):
    """
    @param infile: input file, usually named GSE_GSM_species.csv
    """
    input_data = {}
    with open(infile, 'rb') as inf:
        csvreader = csv.reader(inf, delimiter='\t')
        for (gse, gsm, species) in csvreader:
            if species in input_data:
                if gse in input_data[species]:
                    input_data[species][gse].append(gsm)
                else:
                    input_data[species][gse] = [gsm]
            else:
                input_data[species] = {gse:[gsm]}
    return input_data

def gen_data_from_csv(input_csv):
    """
    generate input data with the specified data structure as shown below

    @param key: species or gse
    """
    
    # data structure of input_data with key=species
    # input_data = {
    #     'human': {gse1: [gsm1, gsm2, gsm3, ...],
    #               gse2: [gsm1, gsm2, gsm3, ...],
    #               gse3: [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     'mouse': {gse1: [gsm1, gsm2, gsm3, ...],
    #               gse2: [gsm1, gsm2, gsm3, ...],
    #               gse3: [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     'rat':   {gse1: [gsm1, gsm2, gsm3, ...],
    #               gse2: [gsm1, gsm2, gsm3, ...],
    #               gse3: [gsm1, gsm2, gsm3, ...],
    #               ...},
    #     }

    input_data = read_csv_species_as_key(input_csv)
    return input_data
