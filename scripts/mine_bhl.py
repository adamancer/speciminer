"""Defines methods for working with the Biodiveristy Heritage Library service"""
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import glob
import logging
import os
import re
import sys

import requests_cache
import yaml

sys.path.insert('..')
from config.constants import CACHE_DIR, LOG_FILE
from miners.bhl.bhl import extract_items, extract_names




logger = logging.getLogger('speciminer')
logger.info('Running mine_gdd_specimens.py')




if __name__ == '__main__':
    # Install cache
    requests_cache.install_cache(os.path.join(CACHE_DIR, 'bhl'))
    # Mine BHL documents and taxa
    #acronyms = ['NMNH', 'USNH', 'USNM', 'FMNH', 'MCZ', 'YPM']
    #for acronym in acronyms:
    #    extract_items(acronym, aggressive=False)
    extract_names()
    logger.info('Script completed without errors')
