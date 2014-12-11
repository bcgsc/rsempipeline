import os
import sys
import re
import csv

def read(input_csv):
    """
    Read csv and yield gse and gsm in pair

    :param input_csv: the csv with only information of GSE and GSM with one GSE
    per row, and GSMs are separated by semicolon
    check example_GSE_GSM.txt for example.
    """
    is_valid = True # indicates if all entries in GSE_GSM.csv are of valid format
    with open(input_csv, 'rb') as inf:
        csv_reader = csv.reader(inf)
        for k, row in enumerate(csv_reader):
            if row and not row[0].startswith('#'):             # not a blank line
                res = process(k+1, row)
                if res is None:
                    is_valid = False

    if not is_valid:
        print 'Please correct the format of invalid entries in GSE_GSM.csv'
        print ('Please check {0} for the correct format '
               'and rerun the script'.format(os.path.join(
                   os.path.dirname(__file__), 'example_GSE_GSM.csv')))
        sys.exit(1)

    with open(input_csv, 'rb') as inf:
        csv_reader = csv.reader(inf)
        for k, row in enumerate(csv_reader):
            if row and not row[0].startswith('#'):             # not a blank line
                res = process(k+1, row)
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
        print 'row {0} is not of len 2'.format(k)

    gse, gsms = row
    gsms = [_.strip() for _ in gsms.strip().rstrip(';').split(';')]

    # check if GSE is properly named
    if not re.search('^GSE\d+', gse):
        print 'row {0}: GSE in  is of invalid name'.format(k)
        return

    # check if GSMs are properly named
    if not all(re.search('^GSM\d+', _) for _ in gsms):
        print ("row {0}: Not all GSMs are of valid names, do you have invalid "
               "characters at the end of the line by mistake?".format(k))
        return
    return gse, gsms
