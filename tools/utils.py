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
