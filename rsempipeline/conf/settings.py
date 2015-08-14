import os

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASE_DIR = os.path.join(PROJECT_DIR, 'rsempipeline')
CONF_DIR = os.path.join(BASE_DIR, 'conf')
TEMPLATES_DIR = os.path.join(BASE_DIR, 'templates')
SHARE_DIR = os.path.join(BASE_DIR, 'share')


RP_PREP_LOGGING_CONFIG = os.path.join(CONF_DIR, 'rp-prep.logging.config')
RP_RUN_LOGGING_CONFIG = os.path.join(CONF_DIR, 'rp-run.logging.config')
RP_TRANSFER_LOGGING_CONFIG = os.path.join(CONF_DIR, 'rp-transfer.logging.config')

# the dir/file names used running rp-prep gen-csv
HTML_OUTDIR_BASENAME = 'html'
SPECIES_CSV_BASENAME = 'GSE_species_GSM.csv'
NO_SPECIES_CSV_BASENAME = 'GSE_no_species_GSM.csv'

# the dir/file names used running rp-prep get-soft
SOFT_OUTDIR_BASENAME = 'soft'

# the qsub script name used for submission to remote cluster
QSUB_SUBMIT_SCRIPT_BASENAME = '0_submit.sh'

# the name of the file that stores information about sra files of a GSM
SRA_INFO_FILE_BASENAME = 'sras_info.yaml'

# this is rough estimated ratio when converting sra to fastq files using
# fastq-dump based on statistics
SRA2FASTQ_SIZE_RATIO = 1.5

# where all analysis results go to
RSEM_OUTPUT_BASENAME = 'rsem_output'

# regex that represents the directory hierarchy in rsem_output
RSEM_OUTPUT_DIR_RE = r'(.*)/(?P<GSE>GSE\d+)/(?P<species>\S+)/(?P<GSM>GSM\d+)'

# where scripts are saved for transferring fastq files to remote HPC host
TRANSFER_SCRIPTS_DIR_BASENAME = 'transfer_scripts'

# the file name that records all transferred GSMs
TRANSFERRED_GSMS_RECORD_BASENAME = 'transferred_GSMs.txt'
