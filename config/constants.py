"""Defines paths used elsewhere in the speciminer script"""

import os

BASE_DIR = os.path.realpath(os.path.join(__file__, '..', '..'))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'caches')

LOG_FILE = os.path.join(BASE_DIR, 'config', 'logging.yml')
GDD_CONFIG_FILE = os.path.join(BASE_DIR, 'config.yml')

# Confirm that INPUT_DIR and OUTPUT_DIR exist
for path in [INPUT_DIR, OUTPUT_DIR, CACHE_DIR]:
    try:
        os.makedirs(path)
    except OSError:
        pass
