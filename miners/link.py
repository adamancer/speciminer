import logging

import re
import time

import requests
from unidecode import unidecode
from nltk.corpus import stopwords

from parser import Parser


class Score(object):
    hints = {}

    def __init__(self):
        self._points = 0
        self._penalties = 0
        self.components = {}


    def __str__(self):
        return str(self._points - self._penalties)


    def __add__(self, val):
        if val < 0:
            self._penalties += val
        else:
            self._points += val


    def __eq__(self, val):
        return self.points() == val


    def __ne__(self, val):
        return self.points() != val


    def __gt__(self, val):
        return self.points() > val


    def __lt__(self, val):
        return self.points() < val


    def __getitem__(self, key):
        try:
            return self.components[key]
        except KeyError:
            return 0


    def points(self):
        return self._points + self._penalties


    def summary(self, general):
        # Convert score to match statement
        matched = []
        for key in ['higherClassification', 'vernacularName', 'scientificName/catalogNumber']:
            if self[key] > 0:
                matched.append('taxa')
                break
        for key in ['group/formation/member']:
            if self[key] > 0:
                matched.append('stratigraphy')
                break
        for key in ['country', 'stateProvince']:
            if self[key] > 0:
                matched.append('country/state')
                break
        for key in ['municipality/island/verbatimLocality']:
            if self[key] > 0:
                matched.append('locality')
                break
        for key in ['collectionCode']:
            if self[key] > 0:
                matched.append('collection')
                break
        for key in ['prefix', 'suffix']:
            if self[key] > 1:
                matched.append('catalog')
                break
        specifics = ''
        if matched:
            specifics = ' (matched {})'.format(', '.join(matched))
        return '{}{}'.format(general, specifics)


    def add(self, keys, val):
        if not isinstance(keys, list):
            keys = [keys]
        key = '/'.join(keys)
        try:
            self.components[key] += val
        except KeyError as e:
            self.components[key] = val
        self += val
        return self


    def score(self, catnum, rec, keys, refwords, multiplier=1, match_all=False, **kwargs):
        if not isinstance(keys, list):
            keys = [keys]
        if kwargs:
            name = kwargs.pop('name', None)
            if name is not None:
                name = '|'.join([name] + list(refwords))
            try:
                refwords = self.hints[name]
            except KeyError:
                refwords = get_keywords(' '.join(refwords), **kwargs)
                if name is not None:
                    self.hints[name] = refwords
        words = [rec[k] for k in keys if rec.get(k) is not None]
        keywords = get_keywords(' '.join(words), **kwargs)
        match = None
        score = 0
        if keywords:
            match = keywords & refwords
            if match:
                logging.debug('Keyword match: %s (%s)', '; '.join(sorted(list(match))), rec['occurrenceID'])
            if match_all and len(match) == len(keywords):
                score = multiplier
            elif not match_all:
                score = multiplier * len(match)
        return self.add(keys, score)


def _get_specimens(catnum=None, **kwargs):
    """Returns specimen metadata from the Smithsonian"""
    #url = 'http://supersite.local:8080/portal'
    url = 'https://geogallery.si.edu/portal'
    headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
    params = {
        'dept': 'any',
        'format': 'json',
        'schema': 'simpledwr',
        'limit': 1000
    }
    if catnum is not None:
        params['sample_id'] = catnum
    params.update(**kwargs)
    response = requests.get(url, headers=headers, params=params)
    #print 'Checking {}...'.format(response.url)
    if not '.local' in url and hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(1)
    return response


def get_specimens(*args, **kwargs):
    """Returns complete SimpleDarwinCore records from the GeoGallery Portal"""
    for i in xrange(1, 13):
        try:
            response = _get_specimens(*args, **kwargs)
        except:
            logging.error('Request failed: %s', args)
            if i > 3:
                raise
        else:
            if response.status_code == 200:
                logging.info('Request succeeded: %s (%s)', response.url, response.status_code)
                try:
                    records = response.json() \
                                      .get('response', {}) \
                                      .get('content', {}) \
                                      .get('SimpleDarwinRecordSet', [])
                except AttributeError:
                    logging.info('No records found')
                    return []
                else:
                    logging.info('%d records found', len(records))
                    return [rec['SimpleDarwinRecord'] for rec in records]
            logging.info('Request failed: %s (%s)', response.url, response.status_code)
        logging.info('Retrying in %d s (retry %d/13)', 2**i, i)
        time.sleep(2**i)


def get_keywords(text, minlen=5, blacklist=None, endings=None, replacements=None):
    if blacklist is None:
        blacklist = [
            'above',
            'along',
            'animalia',
            'beach',
            'boundary',
            'coast',
            'collection',
            'confluence',
            'county',
            'creek',
            'district',
            'early',
            'eastern',
            'family',
            'formation',
            'harbor',
            'indet',
            'island',
            'late',
            'locality',
            'lower',
            'member',
            'middle',
            'mountain',
            'national',
            'north',
            'northern',
            'northeast',
            'northeastern',
            'northwest',
            'northwestern',
            'genus',
            'group',
            'present',
            'province',
            'ridge',
            'river',
            'slide',
            'slope',
            'south',
            'southern',
            'southeast',
            'southeastern',
            'southwest',
            'southwestern',
            'sp',
            'specimen',
            'states',
            'united',
            'unknown',
            'upper',
            'valley',
            'western',
            # COLORS
            'blue',
            'green',
            'red',
            'yellow',
            'white',
            'black'
            ]
        blacklist.extend(stopwords.words('english'))
    keywords = []
    words = unidecode(u'{}'.format(text).lower()).split()
    for word in words:
        word = word.strip('.:;,-!?()')
        if (re.search(r'^[A-Za-z]+$', word)
            and len(word) >= minlen
            and word not in blacklist):
            # Strip endings
            if endings is not None:
                for ending in endings:
                    if word[-len(ending):] == ending:
                        word = word[:-len(ending)]
            # Replacements
            if replacements is not None:
                for find, repl in replacements.iteritems():
                    word = word.replace(find, repl)
            if len(word) > 2:
                keywords.append(word)
    return set([kw for kw in keywords if kw])


def filter_records(records, refnum, keywords=None, dept=None):
    """Returns records that match a reference catalog number"""
    logging.debug('Filtering matches for %s', refnum)
    depts = {
        'an': 'Anthropology',
        'bt': 'Botany',
        'br': 'Vertebrate Zoology: Birds',
        'en': 'Entomology',
        'fs': 'Vertebrate Zoology: Fishes',
        'hr': 'Vertebrate Zoology: Herpetology',
        'iz': 'Invertebrate Zoology',
        'mm': 'Vertebrate Zoology: Mammals',
        'ms': 'Mineral Sciences',
        'pl': 'Paleobiology'
    }
    if dept is not None:
        dept = depts.get(dept.rstrip('*'), dept)
        if dept.rstrip('*') not in depts.values():
            raise ValueError('Bad department: {}'.format(dept))
    parser = Parser()
    try:
        refnum = parser.parse_num(refnum)
    except ValueError:
        return []
    scored = []
    #if len(records) == 1000:
    #    raise ValueError('Too many matches: {}'.format(refnum))
    for rec in records:
        score = Score()
        # Check catalog number
        try:
            catnum = rec['catalogNumber'].upper().split('|')[-1].strip()
            catnum = parser.parse_num(catnum)
        except (IndexError, KeyError, ValueError) as e:
            # Hack to catch ento type numbers
            recnums = [r.strip() for r
                       in rec.get('recordNumber', '').upper().split('|')
                       if r.strip() == str(refnum.number)]
            try:
                catnum = parser.parse_num(recnums[0])
            except IndexError:
                catnum = None
        if catnum is not None:
            # Exclude records with one-character prefixes if the refnum
            # is not prefixed. Other departments appear to have prefixes for
            # internal use (e.g., paleo uses PAL and V) that are not (or are
            # not always) given when that specimen is cited in the literature.
            if not refnum.prefix and catnum.prefix and len(catnum.prefix) == 1:
                score.add('prefix', -1)
            if catnum.prefix != refnum.prefix and catnum.prefix == 'SD':
                score.add('prefix', -1)
            # Exclude records that don't have the same base number
            if catnum.number != refnum.number:
                score.add('number', -100)
            # Bonus point for matching prefix or suffix
            if score >= 0 and refnum.prefix and catnum.prefix == refnum.prefix:
                score.add('prefix', 1)
            if score >= 0 and refnum.suffix and catnum.suffix == refnum.suffix:
                score.add('suffix', 1)
        # Check collectionCode against topic
        if dept is not None:
            if rec.get('collectionCode') == dept.rstrip('*'):
                score.add('collectionCode', 1)
                # Bonus half-point if department assigned contextually
                if dept.endswith('*'):
                    score.add('collectionCode', 0.5)
            else:
                score.add('collectionCode', -100)
        # Check taxa against keywords
        if keywords:
            # Get taxonomy
            endings = [
                'idae',
                'ian',
                'ide',
                'ine',
                'ia',
                'us',
                's',
                'a',
                'e'
            ]
            replacements = {
                'aeo': 'eo',  # archaeo
                'usc': 'usk'  # mollusk
            }
            if rec.get('collectionCode') != 'Mineral Sciences':
                score.score(catnum, rec, 'higherClassification', keywords, multiplier=5, name='hc', endings=endings, replacements=replacements)
                score.score(catnum, rec, 'vernacularName', keywords, multiplier=3, match_all=True)
            else:
                score.score(catnum, rec, ['scientificName', 'catalogNumber'], keywords, multiplier=3, match_all=True, name='vn', endings=['ic', 'y'])
            if rec.get('collectionCode') in ['Mineral Sciences', 'Paleobiology']:
                score.score(catnum, rec, ['group', 'formation', 'member'], keywords, multiplier=3)
            score.score(catnum, rec, 'country', keywords, multiplier=0.51, match_all=True)
            score.score(catnum, rec, 'stateProvince', keywords, multiplier=0.51, match_all=True)
            score.score(catnum, rec, ['municipality', 'island', 'verbatimLocality'], keywords)
        if score.points() > 1:
            scored.append([rec, score])
    if scored:
        max_score = max([s[1] for s in scored])
        return [m for m in scored if m[1] == max_score]
    return []
