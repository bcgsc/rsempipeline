#!/usr/bin/env python

"""
This script download the html file for all GSMs and extract species
information, then generate a GSE_GSM_species.csv file. The GSMs are from the
xlsx/csv files provided by the collaborator after removal of duplicate
GSMs. Duplicate GSMs can be found by detect_duplicate_GSMs.py
"""

import os
import re
import csv
import argparse
import threading
import Queue

from bs4 import BeautifulSoup
import requests

from .utils import read

def mkdir(dir_):
    if not os.path.exists(dir_):
        os.mkdir(dir_)


def write_csv(rows, out_csv):
    with open(out_csv, 'wb') as opf:
        # lineterminator defaults to \r\n, which is odd for linux
        csv_writer = csv.writer(opf, lineterminator='\n')
        for _ in sorted(rows):
            csv_writer.writerow(_)


def find_species(gse, gsm, html_dir):
    """functions calling order: find_sepecies -> gen_soup -> download_html"""
    soup = gen_soup(gse, gsm, html_dir)
    td = soup.find('td', text=re.compile('Organism|Organisms'))
    if td:
        species = td.find_next_sibling().text.strip()
        return species


def gen_soup(gse, gsm, html_dir):
    gse_dir = os.path.join(html_dir, gse)
    try:
        # because of parallel execution, not atomic, often OSError is raised
        os.mkdir(gse_dir)
    except OSError:
        pass
    gsm_html_file = os.path.join(gse_dir, '{0}.html'.format(gsm))

    if not os.path.exists(gsm_html_file):
        print 'downloading {0}'.format(gsm_html_file)
        download_html(gsm, gsm_html_file)
    else:
        print '{0} already downloaded'.format(gsm_html_file)
    with open(gsm_html_file) as inf:
        soup = BeautifulSoup(inf)
    return soup


def download_html(GSM, out_html):
    url = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={0}".format(GSM)
    response = requests.get(url)
    with open(out_html, 'wb') as opf:
        opf.write(response.text.encode('utf-8'))
    return out_html


def backup_file(f):
    if os.path.exists(f):
        dirname = os.path.dirname(f)
        basename = os.path.basename(f)
        count = 1
        rn_to = os.path.join(
            dirname, '#' + basename + '.{0}#'.format(count))
        while os.path.exists(rn_to):
            count += 1
            rn_to = os.path.join(
                dirname, '#' + basename + '.{0}#'.format(count))
        print "BACKING UP {0} to {1}".format(f, rn_to)
        os.rename(f, rn_to)
        return rn_to
        print "BACKUP FINISHED"


def main(options):
    """
    :param n_threads: number of threads to run simultaneously

    :param outdir: the directory where all outputs are to located, default to
    the directory where input_csv is located

    """
    input_csv = options.input_csv
    n_threads = options.nt
    outdir = options.outdir

    if outdir is None:
        outdir = os.path.dirname(input_csv)

    out_html_dir = os.path.join(outdir, 'html')
    mkdir(out_html_dir)

    # Sometimes GSM data could be private, so no species information will be
    # extracted. e.g. GSE49366 GSM1198168
    res, res_no_species = [], []

    # execute in parallel
    queue = Queue.Queue()
    def worker():
        while True:
            GSE, GSM = queue.get()
            species = find_species(GSE, GSM, out_html_dir)
            row = [GSE, species, GSM]
            if species:
                res.append(row)
            else:
                res_no_species.append(row)
            queue.task_done()

    for i in range(n_threads):
        thrd = threading.Thread(target=worker)
        thrd.daemon = True
        thrd.start()

    for gse, gsm in read(input_csv):
        queue.put([gse, gsm])
    queue.join()

    # write output
    out_csv = os.path.join(outdir, 'GSE_species_GSM.csv')
    backup_file(out_csv)
    write_csv(res, out_csv)

    if res_no_species:
        no_species_csv = os.path.join(outdir, 'GSE_no_species_GSM.csv')
        backup_file(no_species_csv)
        write_csv(res_no_species, no_species_csv)

if __name__ == "__main__":
    main()
