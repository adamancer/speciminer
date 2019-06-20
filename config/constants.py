"""Defines paths used elsewhere in the speciminer script"""
import logging.config
import os

import yaml




BASE_DIR = os.path.realpath(os.path.join(__file__, '..', '..'))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'caches')
LOG_DIR = os.path.join(OUTPUT_DIR, 'logs')

LOG_CONFIG_FILE = os.path.join(BASE_DIR, 'config', 'logging.yml')
GDD_CONFIG_FILE = os.path.join(BASE_DIR, 'config.yml')

# Confirm that required directories exist
for path in [INPUT_DIR, OUTPUT_DIR, CACHE_DIR, LOG_DIR]:
    try:
        os.makedirs(path)
    except OSError:
        pass

# Initiate logger and fix file path if script called from root
log_config = yaml.safe_load(open(LOG_CONFIG_FILE, 'r'))
for _, handler in log_config['handlers'].items():
    try:
        fn = handler['filename']
    except KeyError:
        pass
    else:
        if fn.startswith('..'):
            fn = fn[2:].lstrip('/\\')
        fp = os.path.realpath(os.path.join(BASE_DIR, fn.replace('/', os.sep)))
        handler['filename'] = fp
logging.config.dictConfig(log_config)
