#!/usr/bin/env python

"""
This file download soft files, it automatically creates a new soft dir
where the input csv_file (e.g. GSE_GSM_species.csv) is located
"""

import os
import sys
import urlparse
import gzip
import itertools
# import Queue
# import threading
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s|%(asctime)s|%(name)s:%(message)s')    

from ftplib import FTP

from utils import gen_data_from_csv

class SOFTDownloader(object):
    """
    downloader responsible for downloading soft file and generate soft.subset
    file
    """
    def __init__(self):
        self.ftp_handler = FTP('ftp.ncbi.nlm.nih.gov')
        self.ftp_handler.login()

    def gen_soft_subset(self, gse_id, soft_dir=''):
        """
        @param gse_id: GSE ID, e.g. GSE45284
        @param soft_dir: directory where soft is to be saved
        """
        logger = logging.getLogger(__name__)

        soft_subset = os.path.join(soft_dir,
                                  '{0}_family.soft.subset'.format(gse_id))
        if os.path.exists(soft_subset):
            logger.info('{0} has already existed'.format(soft_subset))
        else:
            soft_gzip = self.download_soft_gz(gse_id, soft_dir)
            if soft_gzip:       # meaning downloading successful
                soft_subset = self.gunzip_and_extract_soft(soft_gzip)
                logger.info('removing {0}'.format(soft_gzip))
                os.remove(soft_gzip)
                return soft_subset

    def download_soft_gz(self, gse_id, soft_dir):
        """download soft.gzip file"""
        logger = logging.getLogger(__name__)

        gse_mask = gse_id[0:-3] + 'nnn'
        base = 'ftp://ftp.ncbi.nlm.nih.gov'
        path = os.path.join('/', 'geo', 'series', gse_mask, gse_id, 'soft')
        soft_gz = '{0}_family.soft.gz'.format(gse_id)

        local_soft_gz = os.path.join(soft_dir, soft_gz)

        if os.path.exists(local_soft_gz):
            logger.info('{0} has already existed'.format(local_soft_gz))
            return local_soft_gz
        else:
            url = urlparse.urljoin(base, os.path.join(path, soft_gz))
            # e.g. ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE45nnn/GSE45284/soft/GSE45284_family.soft.gz
            logger.info(
                'downloading {0} from {1} to {2}'.format(soft_gz, url,
                                                         local_soft_gz))
            self.ftp_handler.cwd(path)
            try:
                with open(local_soft_gz, 'wb') as opf:
                    self.ftp_handler.retrbinary(
                        'RETR {0}'.format(soft_gz), opf.write)
                    return local_soft_gz
            except Exception:
                logger.error(Exception)
                logger.error('error when downloading {0}'.format(url))

    def gunzip_and_extract_soft(self, soft_gz):
        """gunzip soft.gzip and extract its content to generate soft.subset"""
        logger = logging.getLogger(__name__)
        dirname, basename = os.path.split(soft_gz)
        soft_subset = os.path.join(
            dirname, basename.replace('.gz', '') + '.subset')
        logger.info('gunziping and extracting from {0} to {1}'.format(
            soft_gz, soft_subset))
        with open(soft_subset, 'wb') as opf:
            with gzip.open(soft_gz, 'rb') as inf:
                for line in inf:
                    if (line.startswith('^SERIES') or
                        line.startswith('^SAMPLE') or
                        line.startswith('!Series_sample_id') or
                        line.startswith('!Sample_organism_ch') or
                        line.startswith('!Sample_library_strategy') or
                        line.startswith('!Sample_supplementary_file_')):
                        opf.write(line)
        return soft_subset

def main(input_csv):
    """
    @param input_csv: e.g. GSE_GSM_species.csv
    """
    output_dir = os.path.dirname(input_csv)
    soft_dir = os.path.join(output_dir, 'soft')
    if not os.path.exists(soft_dir):
        os.mkdir(soft_dir)

    input_data = gen_data_from_csv(input_csv)
    # merging all GSEs
    gses = set(list(itertools.chain(
        *[input_data[_].keys() for _ in input_data])))

    # Parallelization not successful yet! 2014-04-23, not sure why
    # ftp.gen_soft_subset always points to the same object (via id()), need to
    # come up with a better parallelization design.

    # def worker(queue):
    #     while True:
    #         func, args = queue.get()
    #         func(*args)
    #         queue.task_done()

    # queue = Queue.Queue()
    # for i in range(7):
    #     thrd = threading.Thread(target=worker, args=(queue,))
    #     thrd.daemon = True
    #     thrd.start()

    ftp = SOFTDownloader()
    for _ in gses:
        ftp.gen_soft_subset(_, soft_dir)
    # queue.join()

if __name__ == '__main__':
    try:
        input_csv = sys.argv[1] # e.g. GSE_GSM_SPECIES.csv
        main(input_csv)
    except IndexError:
        print 'Usage: python2.7.x download_soft.py input_csv (e.g. GSE_GSM_species.csv)'

