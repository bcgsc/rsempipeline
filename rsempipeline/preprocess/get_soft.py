#!/usr/bin/env python

"""
This file download soft files, it automatically creates a new soft dir
where the input csv_file (e.g. GSE_GSM_species.csv) is located
"""

import os
import urlparse
import gzip
from ftplib import FTP
import logging
logger = logging.getLogger(__name__)

from rsempipeline.preprocess.utils import read
from rsempipeline.utils.misc import mkdir
from rsempipeline.conf.settings import SOFT_OUTDIR_BASENAME
from rsempipeline.preprocess.gen_csv import gen_outdir

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
                logger.exception('error when downloading {0}'.format(url))

    def gunzip_and_extract_soft(self, soft_gz):
        """gunzip soft.gzip and extract its content to generate soft.subset"""
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
                        line.startswith('!Sample_supplementary_file_') or
                        line.startswith('!Sample_type') or
                        line.startswith('!Sample_library_strategy') or
                        line.startswith('!Sample_instrument_model') or
                        line.startswith('!Sample_library_source')):
                        opf.write(line)
        return soft_subset


def gen_soft_outdir(outdir):
    d = os.path.join(outdir, SOFT_OUTDIR_BASENAME)
    mkdir(d)
    return d


def download_soft(input_csv, soft_outdir):
    ftp = SOFTDownloader()
    current_gse = None
    for gse, gsm in read(input_csv):
        if current_gse is None or gse != current_gse:
            ftp.gen_soft_subset(gse, soft_outdir)
            current_gse = gse

def main(options):
    """
    @param input_csv: e.g. GSE_species_GSM.csv
    """
    input_csv = options.input_csv
    outdir = gen_outdir(options)
    soft_outdir = gen_soft_outdir(outdir)
    download_soft(input_csv, soft_outdir)


if __name__ == '__main__':
    main()
