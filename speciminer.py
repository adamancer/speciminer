"""Finds occurrences of USNM specimens in the scientific literature"""

import csv
import glob
import os
import re

import yaml

from specimens import Parser
from documents import Document
from database.queries import Query


if __name__ == '__main__':
    # Ensure the input directory exists (output is created in the
    # database file)
    try:
        os.makedirs('input')
    except OSError:
        pass
    # Define
    db = Query()
    parser = Parser()
    # Find USNM specimens in documents in input
    terms = yaml.load(open('config.yaml', 'rb'))['terms']
    pattern = '(' + '|'.join([t.upper() for t in terms]) + ')'
    files = glob.iglob(os.path.join('input', 'nlp352', '*'))
    for i, fp in enumerate([fp for fp in files if '.' not in fp]):
        print '{}. Checking {}...'.format(i + 1, os.path.basename(fp))
        with open(fp, 'rb') as f:
            rows = list(csv.reader(f, delimiter='\t'))
            # Skip empty or already processed documents
            if not rows or db.get_document(rows[0][0]):
                continue
            doc = Document(rows)
            topic = doc.guess_department()
            # Parse specimens
            specimens = {}
            matches = parser.findall(unicode(doc))
            for verbatim in matches:
                snippets = doc.snippets(verbatim, num_chars=50)
                parsed = parser.parse(verbatim)
                for spec_num in parsed:
                    specimens.setdefault(spec_num, []).extend(snippets)
                # Capture failed parses
                if not parsed:
                    specimens.setdefault(None, []).extend(snippets)
            if not matches:
                specimens[None] = doc.snippets(pattern, num_chars=50)
            for spec_num, snippets in specimens.iteritems():
                count = len(specimens) - (1 if None in specimens else 0)
                db.add_citation(doc.doc_id, spec_num, snippets, topic, count)
    db.commit().close()
