#!/usr/bin/env python

"""
This script download the html file for all GSMs and extract species
information, then generate a GSE_GSM_species.csv file. The GSMs are from the
xlsx/csv files provided by the collaborator after removal of duplicate
GSMs. Duplicate GSMs can be found by detect_duplicate_GSMs.py
"""


import os
import sys
import csv
import threading
import Queue

from bs4 import BeautifulSoup
import requests

from objs import GEORecord
from utils import backup_file

def main(batch_csv):
    output_dir = os.path.dirname(batch_csv)
    html_dir = os.path.join(output_dir, 'html')
    if not os.path.exists(html_dir):
        os.mkdir(html_dir)

    output_csv = os.path.join(output_dir, 'GSE_GSM_species.csv')
    no_species_csv = os.path.join(output_dir, 'GSE_GSM_no_species.csv')
    for _ in [output_csv, no_species_csv]:
        backup_file(_)

    # Sometimes GSM data could be private, so no species information will be
    # extracted. e.g. GSE49366 GSM1198168
    res, res_no_species = [], []
    q = Queue.Queue()
    def worker():
        while True:
            GSE, GSM = q.get()
            species = find_species(GSE, GSM, html_dir)
            row = [GSE, GSM, species]
            if species:
                res.append(row)
            else:
                res_no_species.append(row)
            q.task_done()

    for i in range(16):
        thrd = threading.Thread(target=worker)
        thrd.daemon = True
        thrd.start()

    with open(batch_csv, 'rb') as inf:
        inf.readline()      # skip the headers
        csv_reader = csv.reader(inf)
        for row in csv_reader:
            geo_record = GEORecord(*row)
            for GSM in geo_record.GSMs:
                q.put([geo_record.GSE, GSM])
    q.join()

    write_csv(res, output_csv)
    write_csv(res_no_species, no_species_csv)


def write_csv(rows, output_file):
    with open(output_file, 'wb') as opf:
        csv_writer = csv.writer(opf, delimiter='\t')
        for _ in sorted(rows):
            csv_writer.writerow(_)


def find_species(GSE, GSM, html_dir):
    soup = gen_soup(GSE, GSM, html_dir)
    tds = soup.findAll('td')
    for _ in tds:
        if _.text.strip() == 'Organism':
            species = _.findNextSibling().text.strip()
            return species


def gen_soup(GSE, GSM, html_dir):
    GSE_dir = os.path.join(html_dir, GSE)
    try:
        # because of parallel execution, not atomic
        os.mkdir(GSE_dir)
    except OSError:
        pass
    GSM_html_file = os.path.join(GSE_dir, '{0}.html'.format(GSM))
    if not os.path.exists(GSM_html_file):
        print 'downloading {0}'.format(GSM_html_file)
        soup = download_html(GSM, GSM_html_file)
    with open(GSM_html_file) as inf:
        print '{0} already downloaded'.format(GSM_html_file)
        soup = BeautifulSoup(inf)
    return soup


def download_html(GSM, outputfile):
    url = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={0}".format(GSM)
    response = requests.get(url)
    html = response.text
    soup = BeautifulSoup(html)
    with open(outputfile, 'wb') as opf:
        opf.write(soup.prettify().encode('utf-8'))
    return soup


if __name__ == "__main__":
    try:
        batch_csv = sys.argv[1]
        main(batch_csv)
    except IndexError, _:
        print 'Usage: python2.7.x find_organism.py batch.csv (e.g. RNA_batch7.processed.csv)'
        sys.exit(1)
