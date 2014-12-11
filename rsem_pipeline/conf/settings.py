import os

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASE_DIR = os.path.join(PROJECT_DIR, 'rsem_pipeline')
CONF_DIR = os.path.join(BASE_DIR, 'conf')
SHARE_DIR = os.path.join(CONF_DIR, 'share')

RP_RUN_LOGGING_CONFIG = os.path.join(CONF_DIR, 'rp-run.logging.config')
RP_TRANSFER_LOGGING_CONFIG = os.path.join(CONF_DIR, 'rp-transfer.logging.config')

