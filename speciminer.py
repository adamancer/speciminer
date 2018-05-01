"""Finds occurrences of USNM specimens in the scientific literature"""

import csv
import glob
import os
import re


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

# Write results to file
with open(os.path.join('output', 'cited.csv'), 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(['DocId', 'Journal', 'Specimen'])
    for row in output:
        writer.writerow(row)
