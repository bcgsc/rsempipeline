import os
import logging
import urlparse
import pickle

from ftplib import FTP

logger = logging.getLogger('utils_download')

def gen_orig_params(samples, use_pickle):
    """
    Connect to the FTP server and fetch the list of files to download
    """

    # e.g. of sample.url
    # ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
    ftp_handler = None
    orig_params_sets = []
    for sample in samples:
        pickle_file = os.path.join(sample.outdir, 'orig_params.pickle')
        if use_pickle and os.path.exists(pickle_file):
            with open(pickle_file) as inf:
                orig_params = pickle.load(inf)
        else:
            if ftp_handler is None:
                ftp_handler = get_ftp_handler(samples[0])
            logger.info(
                'generating originate params from FTP for {0}'.format(sample))
            orig_params = gen_orig_params_per_sample(sample, ftp_handler)
            if use_pickle:
                with open(pickle_file, 'wb') as opf:
                    pickle.dump(orig_params, opf)
        orig_params_sets.append(orig_params)
    if ftp_handler is not None:
        ftp_handler.quit()
    return orig_params_sets


def gen_orig_params_per_sample(sample, ftp_handler=None):
    """
    Example of orig_params:
    [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401055/SRR401055.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401056/SRR401056.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRR401055.sra.download.COMPLETE',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRR401056.sra.download.COMPLETE'],
      <GSM863771 (2/8) of GSE35213>]
    """

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

    flag_files = [os.path.join(
        sample.outdir, '{0}.download.COMPLETE'.format(os.path.basename(sra)))
                  for sra in sras]
    # originate params for one sample
    orig_params = [None, sras + flag_files, sample]
    return orig_params


def get_ftp_handler(sample):
    hostname = urlparse.urlparse(sample.url).hostname
    logger.info('connecting to ftp://{0}'.format(hostname))
    ftp_handler = FTP(hostname)
    ftp_handler.login()
    return ftp_handler
