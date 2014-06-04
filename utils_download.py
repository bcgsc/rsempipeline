import os
import logging
import urlparse
import pickle

from ftplib import FTP

logger = logging.getLogger('utils_download')

def gen_orig_params(samples):
    """
    Connect to the FTP server and fetch the list of files to download

    Example of originate files as held by the variable outputs:
    [[None,
      ['test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/SRX029242/SRR070177/SRR070177.sra',
       'test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/SRR070177.sra.download.COMPLETE'],
      <GSM602557 (1/20) of GSE24455>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602558/SRX029243/SRR070178/SRR070178.sra',
       'test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602558/SRR070178.sra.download.COMPLETE'],
      <GSM602558 (2/20) of GSE24455>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRX116910/SRR401053/SRR401053.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRX116910/SRR401054/SRR401054.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRR401053.sra.download.COMPLETE',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRR401054.sra.download.COMPLETE'],
      <GSM863770 (1/8) of GSE35213>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401055/SRR401055.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401056/SRR401056.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRR401055.sra.download.COMPLETE',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRR401056.sra.download.COMPLETE'],
      <GSM863771 (2/8) of GSE35213>]]
    """
    # e.g. of sample.url
    # ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
    ftp_handler = get_ftp_handler(samples[0])
    orig_params_sets = []
    for sample in samples:
        orig_params = gen_orig_params_per_sample(sample)
        pickle_file = os.path.join(sample.outdir, 'orig_params.pickle')
        with open(pickle_file, 'wb') as opf:
            pickle.dump(orig_params, opf)
        orig_params_sets.append(orig_params)
    ftp_handler.quit()
    return orig_params_sets


def gen_orig_params_per_sample(sample, ftp_handler=None):
    if ftp_handler is None:
        ftp_handler = get_ftp_handler(sample)
    url_obj = urlparse.urlparse(sample.url)
    # one level above SRX123456
    before_srx_dir = os.path.dirname(url_obj.path)
    ftp_handler.cwd(before_srx_dir)
    srx = os.path.basename(url_obj.path)
    srrs =  ftp_handler.nlst(srx)
    # cool trick for flatten 2D list:
    # http://stackoverflow.com/questions/2961983/convert-multi-dimensional-list-to-a-1d-list-in-python
    sras = [_ for srr in srrs for _ in ftp_handler.nlst(srr)]
    sras = [os.path.join(sample.outdir, _) for _ in sras]

    for sra in sras:
        flag_file = os.path.join(
            sample.outdir, '{0}.download.COMPLETE'.format(
                os.path.basename(sra)))
    # originate params for one sample
    orig_params = [None, [sra, flag_file], sample]
    return orig_params


def get_ftp_handler(sample):
    hostname = urlparse.urlparse(sample.url).hostname
    ftp_handler = FTP(hostname)
    ftp_handler.login()
    return ftp_handler
