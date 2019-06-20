"""Defines methods to analyze and search TSV files from GeoDeepDive"""
from __future__ import absolute_import
from __future__ import unicode_literals

from builtins import zip
from builtins import str
from builtins import range
from builtins import object

import logging
import os
import re
import time

import requests

from .api import get_document, get_documents




logger = logging.getLogger('speciminer')
logger.info('Loading api.py')




class GDDDocument(object):

    def __init__(self, text, terms=None):
        if isinstance(text, list):
            self._from_lines(text, terms)
        else:
            self._from_blob(text, terms)
        self.edited = self.text[:]


    def __str__(self):
        return self.text


    def _from_blob(self, text, terms=None):
        """Populate the document from a blob of text"""
        self.sentences = []
        self.doc_id = None
        self.text = text
        return


    def _from_lines(self, lines, terms=None):
        """Populate the document from list of lines"""
        if terms is None:
            self.sentences = [Sentence(line) for line in lines]
        else:
            indexes = []
            for i, line in enumerate(lines):
                sline = str(line).lower()
                for term in terms:
                    if term.lower() in sline:
                        for x in range(0, 5):
                            indexes.extend([i + x, i - x])
                        break
            self.sentences = [Sentence(line) for i, line
                              in enumerate(lines) if i in indexes]
        self.doc_id = self.sentences[0].doc_id if self.sentences else None
        self.text = '. '.join([s.detokenize().rstrip('. ')
                               for s in self.sentences])


    def __unicode__(self):
        return self.text


    def snippets(self, val, num_chars=32, highlight=True, zap=False):
        """Find all occurrences of a string in the document"""
        doc = str(self.edited)
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
        if zap:
            self.edited = self.edited.replace(val, '')
        return snippets


    def save(self, fp):
        with open(fp, 'w', encoding='utf-8') as f:
            f.write(self.text)


    def get_keywords(self):
        """Looks for taxa and keywords in the document"""
        pass




class Sentence(object):
    """Contains methods to parse row data from a GeoDeepDive TSV file"""

    def __init__(self, row):
        #row = [row.decode('utf-8') for row in row]
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
        sentence = str(self)
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
