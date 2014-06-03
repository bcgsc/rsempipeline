import os

LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(levelname)s|%(asctime)s|%(name)s:%(message)s',
            },
        'brief' : {
            'format': '%(message)s',
        },
    },

    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
        },

        'file': {
            'class': 'logging.FileHandler',
            'filename': os.path.join(os.path.dirname(__file__), 'rsem_pipeline.log'),
            'encoding': 'utf-8',
            'formatter': 'standard',
            'level': 'DEBUG'
        },
    },

    'loggers': {
        'rsem_pipeline': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            },
        'utils': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            },
    },
}
