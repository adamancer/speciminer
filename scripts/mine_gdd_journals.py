"""Defines methods for working with the GeoDeepDive service"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import glob
import logging
import os
import pprint as pp
import re
import sys
import time

import requests_cache
import yaml
from unidecode import unidecode

sys.path.insert(0, '..')
from config.constants import CACHE_DIR
from miners.gdd.api import get_documents, get_journals
from database.database import DocCount
from database.queries import Query




logger = logging.getLogger('speciminer')
logger.info('Running mine_gdd_journals.py')




def std(val):
    return re.sub('[^A-z0-9]', '', unidecode(val.lower()))


if __name__ == '__main__':
    # Create the journal cache
    requests_cache.install_cache(os.path.join(CACHE_DIR, 'gdd'))
    # Keywords
    keywords = ['geochim', 'geol', 'earth', 'planetary', 'meteorit', 'usgs']
    # Get list of journals
    all_journals = get_journals(all='')
    all_journals.sort(key=lambda j: j['articles'], reverse=True)
    journals = []
    for journal in all_journals:
        jour_pub = journal['journal'] + ': ' + journal['publisher']
        if any([kw in jour_pub.lower() for kw in keywords]):
            journals.append(journal)
    stats = {
        'journals': {},
        'publishers': {}
    }
    publishers = {}
    for journal in journals:
        jrnl = stats['journals'].setdefault(journal['journal'], {})
        pub  = stats['publishers'].setdefault(journal['publisher'], {})
        # Get lists of titles by year
        name = journal['journal']
        print('Finding all articles from {} (n={:,})...'.format(unidecode(name), journal['articles']))
        kwargs = {'pubname': name}
        if journal['articles'] <= 10000:
            kwargs['max'] = journal['articles']
        documents = get_documents(**kwargs)
        print('Counting...')
        for doc in documents:
            key = std(doc['title'])
            year = doc.get('year', None)
            jrnl.setdefault(year, []).append(key)
            pub.setdefault(year, []).append(key)
        #if len(documents) < 10:
        #    break
    # Convert lists to counts
    db = Query(1000)
    db.delete(DocCount)
    for key in stats:
        for name, years in list(stats[key].items()):
            for year in years:
                db.upsert(DocCount,
                          name=name,
                          kind=key.rstrip('s'),
                          year=year,
                          num_articles=len(set(stats[key][name][year])))
    db.commit().close()
