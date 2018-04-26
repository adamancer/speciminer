"""Finds occurrences of USNM specimens in the scientific literature"""

import csv
import glob
import os
import re

import requests


def get_documents(**kwargs):
    """Returns metadata about a set of GeoDeepDive documents"""
    url = 'https://geodeepdive.org/api/articles'
    response = requests.get(url, params=kwargs)
    if  response.status_code == 200:
        return response.json().get('success', {}).get('data', [])
    return []


def get_document(doc_id):
    """Returns metadata about a single GeoDeepDive document"""
    docs = get_documents(id=doc_id)
    return docs[0] if docs else {}


# Preflight
for dirname in ['input', 'output']:
    try:
        os.makedirs(dirname)
    except OSError:
        pass

# Find specimens in documents in input
pattern = r'\b((USNM|NMNH)[\-\s]?[A-Z]?\d+)(-[A-Za-z0-9]+)?\b'
output = []
for fp in glob.iglob(os.path.join('input', '*')):
    with open(fp, 'rb') as f:
        # Find USNM specimens
        matches = re.findall(pattern, f.read())
        specimens = list(set([sorted(m, key=len)[-1].strip() for m in matches]))
        # Get document metadata from the GeoDeepDive website
        doc_id = os.path.splitext(os.path.basename(fp))[0]
        doc = get_document(doc_id)
        for specimen in specimens:
            output.append([doc_id, doc.get('journal'), specimen])

# Write results to file
with open(os.path.join('output', 'cited.csv'), 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(['DocId', 'Journal', 'Specimen'])
    for row in output:
        writer.writerow(row)
