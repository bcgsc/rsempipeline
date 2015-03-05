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
        domain = 'ftp.ncbi.nlm.nih.gov'
        self.ftp_handler = FTP(domain)
        self.ftp_handler.login()
        self.base = 'ftp://{0}'.format(domain)
        
    def gen_soft(self, gse, outdir):
        """
        @param gse: GSE ID, e.g. GSE45284
        @param soft_dir: directory where soft is to be saved
        """
        soft_subset = self.get_soft_subset(gse, outdir)
        if os.path.exists(soft_subset):
            logger.info('{0} has already existed'.format(soft_subset))
        else:
            soft_gz = self.download_soft_gz(gse, outdir)
            if soft_gz:       # meaning downloading successful
                self.gunzip_and_extract_soft(soft_gz, soft_subset)
                logger.info('removing {0}'.format(soft_gz))
                os.remove(soft_gz)
                return soft_subset

    def download_soft_gz(self, gse, outdir):
        """download soft.gzip file"""
        remote_path = self.get_remote_path(gse)
        soft_gz = self.get_soft_gz_basename(gse)
        local_soft_gz = os.path.join(outdir, soft_gz)
        url = self.get_url(gse) # for logging purpose only
        # e.g. ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE45nnn/GSE45284/soft/GSE45284_family.soft.gz
        logger.info('downloading {0} from {1} to '
                    '{2}'.format(soft_gz, url, local_soft_gz))
        try:
            self.retrieve(remote_path, soft_gz, local_soft_gz)
            return local_soft_gz
        except Exception:
            logger.exception('error when downloading {0}'.format(url))            

    def retrieve(self, path, filename, out):
        self.ftp_handler.cwd(path)
        with open(out, 'wb') as opf:
            cmd = 'RETR {0}'.format(filename)
            self.ftp_handler.retrbinary(cmd, opf.write)

    def get_soft_subset(self, gse, outdir):
        """soft.subset with path"""
        return os.path.join(outdir, '{0}_family.soft.subset'.format(gse))

    def get_soft_gz_basename(self, gse):
        return '{0}_family.soft.gz'.format(gse)

    def get_gse_mask(self, gse):
        return gse[0:-3] + 'nnn'

    def get_remote_path(self, gse):
        mask = self.get_gse_mask(gse)
        return os.path.join('/', 'geo', 'series', mask, gse, 'soft')
    
    def get_url(self, gse):
        path = self.get_remote_path(gse)
        soft_gz = self.get_soft_gz_basename(gse)
        return urlparse.urljoin(self.base, os.path.join(path, soft_gz))

    def gunzip_and_extract_soft(self, soft_gz, soft_subset):
        """gunzip soft.gzip and extract its content to generate soft.subset"""
        logger.info('gunziping and extracting from '
                    '{0} to {1}'.format(soft_gz, soft_subset))
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

def gen_soft_outdir(outdir):
    d = os.path.join(outdir, SOFT_OUTDIR_BASENAME)
    mkdir(d)
    return d


def download_soft(input_csv, soft_outdir): # pragma: no cover
    ftp = SOFTDownloader()
    current_gse = None
    for gse, gsm in read(input_csv):
        if current_gse is None or gse != current_gse:
            ftp.gen_soft(gse, soft_outdir)
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
