"""Finds occurrences of USNM specimens in the scientific literature"""

import csv
import glob
import os
import re
import string
from collections import namedtuple

import yaml


class Document(object):

    def __init__(self, rows):
        self.sentences = [Sentence(row) for row in rows]


    def __unicode__(self):
        return ' '.join([s.detokenize() for s in self.sentences])


    def guess_department(self):
        """Tries to guess the department based on available text"""
        text = unicode(self).lower()
        keywords = {
            'anthropol': 'an',
            'amphibian': 'hr',
            'archeol': 'an',
            'entomol': 'en',
            'fish': 'fs',
            'fossil': 'pl',
            'icthyo': 'fs',
            'insect': 'en',
            'invertebrate': 'iz',
            'mammal': 'mm',
            'meteorit': 'ms',
            'mineral': 'ms',
            'paleo': 'pl',
            'ornitho': 'br',
            'reptil': 'hr'
        }
        results = {}
        for kw, dept in keywords.iteritems():
            count = text.count(kw)
            if count:
                try:
                    results[dept] += count
                except KeyError:
                    results[dept] = count
        if results:
            dept, count = sorted(results.iteritems(), key=lambda v: v[1])[-1]
            if count < 10:
                dept += '?'
            return dept
        return ''


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
        return u'{}'.format(self.detokenize())


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




if __name__ == '__main__':
    # Preflight
    for dirname in ['input', 'output']:
        try:
            os.makedirs(dirname)
        except OSError:
            pass

    # Find USNM specimens in documents in input
    pattern = r'\b((USNM|NMNH)[\-\s]?(No\.? |# ?|specimens? )?[A-Z]{0,3}\d+[A-Z]?([-/,][A-Z0-9]+)?)\b'
    terms = yaml.load(open('config.yaml', 'rb'))['terms']
    specimens = {}
    depts = {}
    files = glob.iglob(os.path.join('input', 'nlp352', '*'))
    for i, fp in enumerate([fp for fp in files if '.' not in fp]):
        print '{}. Checking {}...'.format(i + 1, os.path.basename(fp))
        with open(fp, 'rb') as f:
            rows = list(csv.reader(f, delimiter='\t'))
            dept = Document(rows).guess_department()
            for row in rows:
                # Confirm that one of the search terms occurs in the current
                # row before checking for catalog numbers
                if [t for t in terms if t in unicode(row).lower()]:
                    sentence = Sentence(row)
                    text = sentence.detokenize()
                    matches = re.findall(pattern, text, flags=re.I)
                    spec_ids = set([sorted(m, key=len)[-1].strip() for m in matches])
                    for spec_id in spec_ids:
                        specimens.setdefault(spec_id, {}) \
                                 .setdefault(sentence.doc_id, []) \
                                 .append(sentence.snippet(spec_id))
                    depts[sentence.doc_id] = dept

    # Create output list from the specimens dictionary
    output = []
    for spec_id, docs in specimens.iteritems():
        for doc_id, snippets in docs.iteritems():
            output.append([doc_id, spec_id, depts.get(doc_id), ' | '.join(snippets)])

    # Write results to file
    print 'Writing results...'
    with open(os.path.join('output', 'cited.csv'), 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(['DocId', 'VerbatimId', 'Dept', 'Snippets'])
        for row in output:
            writer.writerow([val.encode('utf-8') for val in row])
