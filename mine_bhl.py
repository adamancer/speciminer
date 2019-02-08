"""Defines methods for working with the Biodiveristy Heritage Library service"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import logging.config
logger = logging.getLogger(__name__)

import csv
import glob
import os
import re

import requests_cache
import yaml

from .miners.bhl.bhl import extract_items, extract_names


requests_cache.install_cache(os.path.join('output', 'names'))

if __name__ == '__main__':
    logging.config.dictConfig(yaml.load(open('logging.yml', 'r')))
    logging.info('Cache path is %s', os.path.abspath(os.path.join('output', 'cache.sqlite')))
    #acronyms = ['NMNH', 'USNH', 'USNM', 'FMNH', 'MCZ', 'YPM']
    #for acronym in acronyms:
    #    extract_items(acronym, aggressive=False)
    extract_names()
    logging.info('Script completed without errors')