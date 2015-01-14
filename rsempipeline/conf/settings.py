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
HTML_DIR_BASENAME = 'html'
SPECIES_CSV_BASENAME = 'GSE_species_GSM.csv'
NO_SPECIES_CSV_BASENAME = 'GSE_no_species_GSM.csv'
