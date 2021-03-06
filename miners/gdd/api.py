"""Defines methods for using the GeoDeepDive API"""
from __future__ import print_function
from __future__ import unicode_literals

import logging
import time

import requests




logger = logging.getLogger('speciminer')
logger.info('Loading api.py')




def get_documents(**kwargs):
    """Returns metadata about a set of GeoDeepDive documents"""
    url = 'https://geodeepdive.org/api/articles'
    response = requests.get(url, params=kwargs)
    print('Checking {}...'.format(response.url))
    if hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(3)
    if response.status_code == 200:
        return response.json().get('success', {}).get('data', [])
    return []


def get_document(doc_id):
    """Returns metadata about a single GeoDeepDive document"""
    docs = get_documents(id=doc_id)
    return docs[0] if docs else {}


def get_journals(**kwargs):
    """Returns metadata about a set of GeoDeepDive documents"""
    url = 'https://geodeepdive.org/api/journals'
    response = requests.get(url, params=kwargs)
    print('Checking {}...'.format(response.url))
    if hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(1)
    if response.status_code == 200:
        return response.json().get('success', {}).get('data', [])
    return []


def get_snippets(**kwargs):
    url = 'https://geodeepdive.org/api/snippets'
    response = requests.get(url, params=kwargs)
    print('Checking {}...'.format(response.url))
    if hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(1)
    if response.status_code == 200:
        return response.json().get('success', {}).get('data', [])
    return []
