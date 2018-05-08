"""Defines methods to analyze and search TSV files from GeoDeepDive"""

import os
import re
import time

import requests


# Implement the cache if requests_cache is installed
try:
    import requests_cache
except ImportError:
    pass
else:
    try:
        os.mkdir('output')
    except OSError:
        pass
    requests_cache.install_cache(os.path.join('output', 'cache'))


class Document(object):

    def __init__(self, rows, terms=None):
        # Reintegrate the text from the document
        if terms is None:
            self.sentences = [Sentence(row) for row in rows]
        else:
            indexes = []
            for i, row in enumerate(rows):
                srow = str(row).lower()
                for term in terms:
                    if term.lower() in srow:
                        for x in xrange(0, 5):
                            indexes.extend([i + x, i - x])
                        break
            self.sentences = [Sentence(row) for i, row in enumerate(rows) if i in indexes]
        self.doc_id = self.sentences[0].doc_id if self.sentences else None
        self.text = '. '.join([s.detokenize().rstrip('. ') for s in self.sentences])


    def __unicode__(self):
        return self.text


    def snippets(self, val, num_chars=32, highlight=True):
        """Find all occurrences of a string in the document"""
        doc = unicode(self)
        snippets = []
        for i in [m.start() for m in re.finditer(r'\b' + val + r'\b', doc)]:
            i -= num_chars
            j = i + len(val) + 2 * num_chars
            if i < 0:
                i = 0
            if j > len(doc):
                j = len(doc)
            # Construct the snippet
            snippet = []
            if i:
                snippet.append('...')
            snippet.append(doc[i:j].strip())
            if j < len(doc):
                snippet.append('...')
            snippet = ''.join(snippet)
            if highlight:
                snippet = snippet.replace(val, '**' + val + '**')
            snippets.append(snippet)
        return snippets


    def guess_department(self):
        """Tries to guess the department based on available text"""
        text = unicode(self).lower()
        keywords = {
            'anthropol': 'an',
            'amphibian': 'hr',
            'archeol': 'an',
            'botan': 'bt',
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


def get_documents(**kwargs):
    """Returns metadata about a set of GeoDeepDive documents"""
    url = 'https://geodeepdive.org/api/articles'
    response = requests.get(url, params=kwargs)
    print 'Checking {}...'.format(response.url)
    if hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(1)
    if response.status_code == 200:
        return response.json().get('success', {}).get('data', [])
    return []


def get_document(doc_id):
    """Returns metadata about a single GeoDeepDive document"""
    docs = get_documents(id=doc_id)
    return docs[0] if docs else {}
