"""Defines methods for working with the GeoDeepDive service"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import logging
import os
import re
import sys

import yaml

sys.path.insert(0, '..')
from config.constants import CACHE_DIR, GDD_CONFIG_FILE, INPUT_DIR
from miners.parser import Parser
from miners.gdd.api import get_snippets
from miners.gdd.documents import GDDDocument
from database.database import Snippet, Document
from database.queries import Query




logger = logging.getLogger('speciminer')
logger.info('Running mine_gdd_specimens.py')




def speciminer(debug=False):
    """Finds mentions of USNM/NMNH in nlp352 docs from GDD"""
    if debug:
        find_snippets()
    # Define acronyms for long search terms
    preferred = {
        'USNMNH': 'US NMNH',
        'US NATIONAL MUSEUM': 'USNM',
        'U S NATIONAL MUSEUM': 'USNM',
        'UNITED STATES NATIONAL MUSEUM': 'USNM'
    }
    # Define classes to query and parse GDD
    db = Query(100)
    parser = Parser()
    # Clean up the list of search terms from config.yml
    terms = yaml.safe_load(open(GDD_CONFIG_FILE, 'r'))['terms']
    terms = [t.upper() for t in terms if not ' ' in t or len(t) > 11]
    terms.sort(key=len, reverse=True)
    # Find USNM specimens in documents in input
    i = 0
    for root, _, filenames in os.walk(INPUT_DIR):
        for fp in [os.path.join(root, fn) for fn in filenames if '.' not in fn]:
            i += 1
            print('{}. Checking {}...'.format(i, os.path.basename(fp)))
            doc_id = 'gdd:{}'.format(os.path.basename(fp).split('_')[0])
            with open(fp, 'r', encoding='utf-8') as f:
                if 'nlp352' in root:
                    data = list(csv.reader(f, delimiter='\t'))
                else:
                    data = f.read()
                # Skip empty or already processed documents
                if not data or db.get_document(doc_id):
                    continue
                # Read the text into a document, then clean up the acronyms
                doc = GDDDocument(data)
                # Create a version of the text with clean acronyms
                for term in terms:
                    if len(term) <= 6:
                        pattern = r'\b' + r'\.? *'.join(term) + r'\b'
                    else:
                        words = term.split(' ')
                        words = [w + r'\.? *' if len(w) == 1 else w + ' '
                                 for w in words]
                        words = ''.join(words).strip()
                        pattern = (r'\b{}\b(?! of\b)'.format(words))
                    pattern = re.compile(pattern, flags=re.I)
                    # Having the prefixes the same length will be useful later
                    repl = preferred.get(term, term)
                    doc.edited = pattern.sub(repl, doc.edited)
                # Find all catalog numbers in the cleaned text
                matches = parser.findall(doc.edited)
                snippets = []
                for verbatim in matches:
                    snippets.extend(doc.snippets(verbatim, num_chars=50))
                # Strip parsed catalog numbers from the text and look for
                # occurrences of the museum codes that were not caught
                for verbatim in matches:
                    doc.edited = doc.edited.replace(verbatim,
                                                    '[CATNUM_REMOVED]')
                for term in terms:
                    snippets.extend(doc.snippets(term, num_chars=50))
                # Add snippets to the database
                db.safe_add(Document,
                            id=doc_id,
                            source='GDD',
                            num_snippets=len(snippets))
                for snippet in snippets:
                    db.safe_add(Snippet, doc_id=doc_id, snippet=snippet)
    db.commit().close()


def find_snippets():
    """Retrieves snippets from the GDD API for testing"""
    # Lazy load so it doesn't interfere when running on GDD
    import requests_cache
    requests_cache.install_cache(os.path.join(CACHE_DIR, 'gdd'))
    try:
        os.makedirs(os.path.join(INPUT_DIR, 'snippets'))
    except OSError:
        pass
    for term in yaml.safe_load(open(GDD_CONFIG_FILE, 'r'))['terms']:
        response = get_snippets(term=term, clean='', fragment_limit=100)
        for doc in response:
            snippets = doc['highlight']
            fn = '{_gddid}_snippets'.format(**doc)
            fp = os.path.join(INPUT_DIR, 'snippets', fn)
            with open(fp, 'w', encoding='utf-8') as f:
                f.write('\n'.join(snippets))


if __name__ == '__main__':
    speciminer(debug=True)
