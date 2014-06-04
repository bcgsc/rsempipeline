import os
import logging
import urlparse

from ftplib import FTP

logger = logging.getLogger('utils_download')

def gen_originate_files(samples):
    """
    Connect to the FTP server and fetch the list of files to download

    Example of originate files as held by the variable outputs:
    [[None,
      ['test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/SRX029242/SRR070177/SRR070177.sra',
       'test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602557/download.COMPLETE'],
      <GSM602557 (1/20) of GSE24455>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602558/SRX029243/SRR070178/SRR070178.sra',
       'test_data_downloaded_for_genesis/rsem_output/human/GSE24455/GSM602558/download.COMPLETE'],
      <GSM602558 (2/20) of GSE24455>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRX116910/SRR401053/SRR401053.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/SRX116910/SRR401054/SRR401054.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863770/download.COMPLETE'],
      <GSM863770 (1/8) of GSE35213>],
     [None,
      ['test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401055/SRR401055.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/SRX116911/SRR401056/SRR401056.sra',
       'test_data_downloaded_for_genesis/rsem_output/mouse/GSE35213/GSM863771/download.COMPLETE'],
      <GSM863771 (2/8) of GSE35213>]]
    """
    outputs = []
    for sample in samples:
        # e.g. of sample.url
        # ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByExp/sra/SRX/SRX029/SRX029242
        url_obj = urlparse.urlparse(sample.url)
        ftp_handler = FTP(url_obj.hostname)
        ftp_handler.login()
        # one level above SRX123456
        before_srx_dir = os.path.dirname(url_obj.path)
        ftp_handler.cwd(before_srx_dir)
        srx = os.path.basename(url_obj.path)
        srrs =  ftp_handler.nlst(srx)
        # cool trick for flatten 2D list:
        # http://stackoverflow.com/questions/2961983/convert-multi-dimensional-list-to-a-1d-list-in-python
        sras = [_ for srr in srrs for _ in ftp_handler.nlst(srr)]
        sras = [os.path.join(sample.outdir, _) for _ in sras]
        ftp_handler.quit()
    
        for sra in sras:
            flag_file = os.path.join(
                sample.outdir, '{0}.download.COMPLETE'.format(
                    os.path.basename(sra)))
            outputs.append([None, [sra, flag_file], sample])
    return outputs
