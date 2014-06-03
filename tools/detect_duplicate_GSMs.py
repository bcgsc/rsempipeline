#!/usr/bin/env python

import sys
import csv

"""
This script detects duplicate GSMs within one GSE from the xlsx/csv files
provided by the collaborator

Please remove duplicates mannually (if practical) and save the file to a new
csv file with name like RNAseq_batchn.processed.csv
"""

from objs import GEORecord

def print_header():
    print "{0:15s} {1:20s} {2:20s} {3:20s} {4:20s} {5}".format(
        'Invalid GSE', '# used_samples', '# total_samples',
        '# GSMs', 'set(# GSMs)', 'duplicated GSMs')

def print_summary(batch_csv, has_invalid_GSE, count, set_count):
    print "=" * 79
    if has_invalid_GSE:
        print '!!! Remove invalid GSE(s) in {0} before proceeding'.format(
            batch_csv)
    else:
        print '{0} is clean'.format(batch_csv)
    print "total # GSMs: {0}".format(count)
    print "total set(# GSMs): {0}".format(set_count)
    print "=" * 79

def main(batch_csv):
    has_invalid_GSE = False
    count, set_count = 0, 0
    with open(batch_csv, 'rb') as inf:
        inf.readline()          # skip the headers
        csv_reader = csv.reader(inf)
        for row in csv_reader:
            geo_record = GEORecord(*row)
            GSMs = geo_record.GSMs
            num, set_num = len(GSMs), len(set(GSMs))
            if num != set_num or geo_record.num_used_samples != set_num:
                if not has_invalid_GSE:
                    print_header()
                    has_invalid_GSE = True
                duplicated_GSMs = [_ for _ in GSMs if GSMs.count(_) > 1]
                print "{0:15s} {1:<20d} {2:<20d} {3:<20d} {4:<20d} {5}".format(
                    geo_record.GSE,
                    geo_record.num_used_samples,
                    geo_record.num_used_samples,
                    num, set_num, ' '.join(duplicated_GSMs))
            count += num
            set_count += set_num

    print_summary(batch_csv, has_invalid_GSE, count, set_count)

if __name__ == "__main__":
    try:
        batch_csv = sys.argv[1]
        main(batch_csv)
    except IndexError, _:
        print 'Usage: python2.7.x detect_duplicate_GSMs.py batch.csv (e.g. RNAseq_batchn.csv)'
        sys.exit(1)
