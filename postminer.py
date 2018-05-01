"""Extends and summarizes data mined from GeoDeepDive"""

import csv
import os
import time

import requests
import requests_cache

from minsci.catnums import get_catnums


requests_cache.install_cache()


def get_documents(**kwargs):
    """Returns metadata about a set of GeoDeepDive documents"""
    url = 'https://geodeepdive.org/api/articles'
    response = requests.get(url, params=kwargs)
    if not response.from_cache:
        time.sleep(3)
    if response.status_code == 200:
        return response.json().get('success', {}).get('data', [])
    return []


def get_document(doc_id):
    """Returns metadata about a single GeoDeepDive document"""
    docs = get_documents(id=doc_id)
    return docs[0] if docs else {}


def get_specimens(catnum, **kwargs):
    """Returns specimen metadata from the Smithsonian"""
    url = 'https://geogallery.si.edu/portal'
    headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
    params = {
        'keyword': catnum,
        'format': 'json',
        'schema': 'simpledwr'
    }
    params.update(**kwargs)
    response = requests.get(url, headers=headers, params=params)
    print 'Checking {}...'.format(response.url)
    if not response.from_cache:
        time.sleep(3)
    if response.status_code == 200:
        try:
            content = response.json().get('response', {}).get('content', {})
            records = content.get('SimpleDarwinRecordSet', [])
        except AttributeError:
            return []
        else:
            return [rec['SimpleDarwinRecord'] for rec in records]
    return []


def filter_records(record, refnum):
    """Returns records that match a reference catalog number"""
    matches = []
    for rec in records:
        try:
            catnum = get_catnums(rec['catalogNumber'])[0]
        except (IndexError, KeyError):
            pass
        else:
            # Exclude records with one-character prefixes if the refnum
            # is not prefixed. Other departments appear to have prefixes for
            # internal use (e.g., PAL) that are not (or are not always) given
            # when that specimen is cited in the literature.
            if not refnum.prefix and catnum.prefix and len(catnum.prefix) == 1:
                continue
            # Exclude records that don't have the same base number
            if catnum.number == refnum.number:
                matches.append(rec)
    return [m['occurrenceID'] for m in matches]




if __name__ == '__main__':
    # Fill out the data returned by the data mining script
    output = []
    with open(os.path.join('output', 'cited.csv'), 'rb') as f:
        rows = csv.reader(f)
        keys = next(rows)
        for row in rows:
            data = dict(zip(keys, row))
            # Fetch data about the publication
            doc = get_document(data['DocId'])
            # Normalize catalog numbers and expand ranges
            verbatim = data['VerbatimId']
            catnums = get_catnums(verbatim)
            for catnum in catnums:
                # Check the NMNH data portal for this specimen
                spec_id = str(catnum.set_mask('default'))
                if data['Dept']:
                    records = get_specimens(spec_id, dept=data['Dept'].rstrip('?'))
                else:
                    records = get_specimens(spec_id)
                ezids = filter_records(records, catnum)
                doi = [id_['id'] for id_ in doc.get('identifier', [])
                       if id_['type'] == 'doi']
                output.append({
                    'DocId': data['DocId'],
                    'DOI': doi[0] if doi else '',
                    'Journal': doc.get('journal'),
                    'Year': doc.get('year'),
                    'OccurrenceId': ' | '.join(ezids),
                    'VerbatimId': verbatim,
                    'SpecimenId': str(catnum.set_mask('include_code')),
                    'Dept': data['Dept'],
                    'Snippets': data['Snippets']
                })

    # Create a new output file with the extended data
    keys = ['DocId', 'DOI', 'VerbatimId', 'SpecimenId', 'Dept',
            'Journal', 'Year', 'OccurrenceId', 'Snippets']
    with open(os.path.join('output', 'extended.csv'), 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(keys)
        for row in output:
            writer.writerow([row[key] for key in keys])

    # Create a file summarizing specimen usage by year
    years = {}
    for row in output:
        year = years.setdefault(row['Year'], {'pubs': [], 'specimens': []})
        year['pubs'].append(row['DocId'])
        # Specimens are counted per publication per year
        year['specimens'].append(row['DocId'] + '|' + row['SpecimenId'])
    with open(os.path.join('output', 'summary.csv'), 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(['Year', 'Publications', 'Specimens'])
        for year in sorted(years):
            row = years[year]
            writer.writerow([year,
                             len(set(row['pubs'])),
                             len(set(row['specimens']))])
