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
import threading
import Queue
import logging
logger = logging.getLogger(__name__)

from bs4 import BeautifulSoup
import requests

from rsempipeline.preprocess.utils import read
from rsempipeline.utils.misc import backup_file, mkdir
from rsempipeline.conf.settings import (HTML_OUTDIR_BASENAME,
                                        SPECIES_CSV_BASENAME,
                                        NO_SPECIES_CSV_BASENAME)

def write_csv(rows, out_csv):
    with open(out_csv, 'wb') as opf:
        # lineterminator defaults to \r\n, which is odd for linux
        csv_writer = csv.writer(opf, lineterminator='\n')
        for _ in sorted(rows):
            csv_writer.writerow(_)


def find_species(gse, gsm, outdir):
    """functions calling order: find_sepecies -> gen_soup -> download_html"""
    soup = gen_soup(gse, gsm, outdir)
    td = soup.find('td', text=re.compile('Organism|Organisms'))
    if td:
        species = td.find_next_sibling().text.strip()
        return species


def gen_soup(gse, gsm, outdir):
    gsm_html = gen_gsm_html(outdir, gse, gsm)
    with open(gsm_html) as inf:
        soup = BeautifulSoup(inf)
    return soup


def gen_outdir(options):
    if options.outdir:
        d = options.outdir
        mkdir(d)
    else:
        d = os.path.dirname(options.input_csv)
    return d


def gen_html_outdir(outdir):
    d = os.path.join(outdir, HTML_OUTDIR_BASENAME)
    mkdir(d)
    return d


def gen_gse_dir(outdir, gse):
    html_dir = gen_html_outdir(outdir)
    gse_dir = os.path.join(html_dir, gse)
    mkdir(gse_dir)
    return gse_dir


def gen_gsm_html(outdir, gse, gsm):
    gse_dir = gen_gse_dir(outdir, gse)
    gsm_html = os.path.join(gse_dir, '{0}.html'.format(gsm))
    if not os.path.exists(gsm_html):
        logger.info('downloading {0}'.format(gsm_html))
        download_html(gsm, gsm_html)
    else:
        logger.info('{0} already downloaded'.format(gsm_html))        
    return gsm_html


def download_html(gsm, out_html):
    url = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={0}".format(gsm)
    response = requests.get(url)
    with open(out_html, 'wb') as opf:
        opf.write(response.text.encode('utf-8'))

    
def generate_csv(input_csv, outdir, num_threads):
    # Sometimes GSM data could be private, so no species information will be
    # extracted. e.g. GSE49366 GSM1198168
    res, res_no_species = [], []

    # execute in parallel
    queue = Queue.Queue()
    def worker():
        while True:
            GSE, GSM = queue.get()
            species = find_species(GSE, GSM, outdir)
            row = [GSE, species, GSM]
            if species:
                res.append(row)
            else:
                res_no_species.append(row)
            queue.task_done()

    for i in range(num_threads):
        thrd = threading.Thread(target=worker)
        thrd.daemon = True
        thrd.start()

    for gse, gsm in read(input_csv):
        queue.put([gse, gsm])
    queue.join()

    # write output
    out_csv = os.path.join(outdir, SPECIES_CSV_BASENAME)
    no_species_csv = os.path.join(outdir, NO_SPECIES_CSV_BASENAME)
    backup_file(out_csv)
    write_csv(res, out_csv)

    if res_no_species:
        backup_file(no_species_csv)
        write_csv(res_no_species, no_species_csv)


def main(options):
    """
    :param n_threads: number of threads to run simultaneously

    :param outdir: the directory where all outputs are to located, default to
    the directory where input_csv is located

    """
    input_csv = options.input_csv
    num_threads = options.nt
    outdir = gen_outdir(options)
    generate_csv(input_csv, outdir, num_threads)
