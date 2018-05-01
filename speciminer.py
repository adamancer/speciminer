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

class Sentence(object):
    """Contains methods to parse row data from a GeoDeepDive TSV file"""

    def __init__(self, row):
        row = [row.decode('utf-8') for row in row]
        self.doc_id = row[0]
        self.sent_id = row[1]
        # Words
        token_id = self.parse(row[2])
        word = self.parse(row[3])
        pos = self.parse(row[4])
        ner = self.parse(row[5])
        lemma = self.parse(row[4])
        self.tokens = [Token(*t) for t in zip(token_id, word, pos, ner, lemma)]


    def __str__(self):
        return self.detokenize()


    def __unicode__(self):
        return u''.format(self.detokenize())


    def detokenize(self):
        """Reconstructs a sentence from a list of tokens"""
        sentence = u' '.join([t.joinable() for t in self.tokens]).strip()
        # Delete spaces before
        for punc in ['.', '?', '!', "'", ',', ';', ':', '--', ')', ']', '}']:
            sentence = sentence.replace(' ' + punc, punc)
        # Delete spaces after
        for punc in ['--', '(', '[', '{']:
            sentence = sentence.replace(punc + ' ', punc)
        # Delete double spaces
        while '  ' in sentence:
            sentence = sentence.replace('  ', ' ')
        return sentence


    def snippet(self, val, num_chars=32, highlight=True):
        """Gets the snippet around the given string in the sentence"""
        sentence = unicode(self)
        i = sentence.index(val) - num_chars
        j = i + len(val) + 2 * num_chars
        if i < 0:
            i = 0
        if j > len(sentence):
            j = len(sentence)
        # Construct the snippet
        snippet = []
        if i:
            snippet.append('...')
        snippet.append(sentence[i:j].strip())
        if j < len(sentence):
            snippet.append('...')
        snippet = ''.join(snippet)
        if highlight:
            snippet = snippet.replace(val, '**' + val + '**')
        return snippet


        @staticmethod
        def parse(val):
            """Parses a cell from a row in a GeoDeepDive TSV file"""
            if val.startswith('{') and val.endswith('}'):
                vals = val.strip('{}').split(',')
                return vals
            return val




class Token(object):

    def __init__(self, token_id, word, pos, ner, lemma):
        self.token_id = int(token_id)
        self.word = word.strip('"')
        self.pos = pos
        self.ner = ner
        self.lemma = lemma


    def joinable(self):
        """Returns the original character that produced a token"""
        repl = {
            '-LCB-': '{',
            '-LRB-': '(',
            '-LSB-': '[',
            '-RCB-': '}',
            '-RRB-': ')',
            '-RSB-': ']'
        }
        word = repl.get(self.word, self.word)
        if not word:
            return ''
        return word


