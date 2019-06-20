"""Defines methods for determining the subject area of a string"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import object

import glob
import json
import logging
import os
import pprint as pp
import re
import time

from collections import OrderedDict, namedtuple
from lxml import etree
from unidecode import unidecode

try:
    from .rerequests import ReRequest
    requests = ReRequest()
except ImportError:
    import requests




logger = logging.getLogger('speciminer')
logger.info('Loading topic.py')




Mapping = namedtuple('Mapping', ['rank', 'value', 'dept'])

class Topicker(object):
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
    mappings = [
        Mapping('kingdom', 'Plantae', 'bt'),
        Mapping('class', 'Arachnida', 'en'),
        Mapping('class', 'Aves', 'br'),
        Mapping('class', 'Amphibia', 'hr'),
        Mapping('class', 'Insecta', 'en'),
        Mapping('class', 'Mammalia', 'mm'),
        Mapping('class', 'Reptilia', 'hr'),
        Mapping('phylum', 'Chordata', 'fs'),  # assign remaining verts to fs
        Mapping('phylum', '!Chordata', 'iz')  # assign remaining inverts to iz
    ]


    def __init__(self):
        try:
            with open('hints.json', 'r') as f:
                self.hints = json.load(f)
        except IOError:
            self.hints = {}
        self.keywords = self._read_keywords()


    def _read_keywords(self):
        script_dir = os.path.dirname(__file__)
        keywords = {}
        for fp in glob.iglob(os.path.join(script_dir, 'files', '*.txt')):
            dept = os.path.basename(fp)[:-4]
            with open(fp, 'r') as f:
                patterns = [p.strip() for p in f.readlines() if p.strip()]
                keywords[dept] = patterns
        ordered = OrderedDict()
        for dept in ['an', 'pl', 'ms']:
            ordered[dept] = keywords.pop(dept)
        for dept in sorted(keywords):
            ordered[dept] = keywords[dept]
        return ordered


    def get_names(self, text, **kwargs):
        """Returns taxonomic names fouind in the given text"""
        logger.debug(u'Seeking taxonomic names in "{}"'.format(text))
        url = 'http://gnrd.globalnames.org/name_finder.json'
        headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
        params = {'text': unidecode(text[:5000])}
        params.update(**kwargs)
        response = requests.get(url, headers=headers, params=params)
        mask = 'Retrieved {} from server'
        if hasattr(response, 'from_cache') and not response.from_cache:
            mask = 'Retrieved {} from cache'
            time.sleep(3)
        logger.debug(mask.format(response.request.url))
        sci_names = []
        if response.status_code == 200:
            names = response.json().get('names', [])
            sci_names = self.clean_names([n['scientificName'] for n in names])
        logger.debug(u'Found {} names: {}'.format(len(sci_names), ', '.join(sci_names)))
        return sci_names


    def resolve_names(self, names, **kwargs):
        """Resolve taxonomic names"""
        logger.debug(u'Resolving taxonomic names "{}"'.format(names))
        url = 'http://resolver.globalnames.org/name_resolvers.json'
        headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
        params = {
            'names': unidecode('|'.join(names)),
            'with_context': True,
            'with_vernaculars': True
        }
        params.update(**kwargs)
        response = requests.get(url, headers=headers, params=params)
        mask = 'Retrieved {} from server'
        if hasattr(response, 'from_cache') and not response.from_cache:
            mask = 'Retrieved {} from cache'
            time.sleep(3)
        logger.debug(mask.format(response.request.url))
        sci_names = []
        if response.status_code == 200:
            for results in response.json().get('data', []):
                if results['is_known_name']:
                    for row in results['results']:
                        vernaculars = row.get('vernaculars', [])
                        print(vernaculars)
            sci_names = self.clean_names([n['scientificName'] for n in names])
        logger.debug(u'Found {} names: {}'.format(len(sci_names), ', '.join(sci_names)))
        return sci_names


    def get_tsns(self, name, **kwargs):
        """Returns TSNs matching the given name"""
        logger.debug(u'Seeking TSNs for "{}"'.format(name))
        url = 'http://www.itis.gov/ITISWebService/services/ITISService/searchByScientificName'
        headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
        params = {'srchKey': name}
        params.update(**kwargs)
        response = requests.get(url, headers=headers, params=params)
        mask = 'Retrieved {} from server'
        if hasattr(response, 'from_cache') and not response.from_cache:
            mask = 'Retrieved {} from cache'
            time.sleep(3)
        logger.debug(mask.format(response.request.url))
        tsns = []
        if response.status_code == 200:
            root = etree.fromstring(response.text)
            for child in root:
                for tag in child.iter('{*}tsn'):
                    tsns.append(tag.text)
        logger.debug(u'Found {} TSNs'.format(len(tsns)))
        if len(tsns) > 100:
            logger.debug(u'Limited results to first 100 TSNs')
            tsns = tsns[:100]
        return tsns


    def get_hierarchy(self, tsn, **kwargs):
        """Returns the taxonomic hierarchy for a given TSN"""
        logger.debug(u'Retrieving hierarchy for {}'.format(tsn))
        url = 'http://www.itis.gov/ITISWebService/services/ITISService/getFullHierarchyFromTSN'
        headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
        params = {'tsn': tsn}
        params.update(**kwargs)
        response = requests.get(url, headers=headers, params=params)
        mask = 'Retrieved {} from server'
        if hasattr(response, 'from_cache') and not response.from_cache:
            mask = 'Retrieved {} from cache'
            time.sleep(3)
        logger.debug(mask.format(response.request.url))
        if response.status_code == 200:
            root = etree.fromstring(response.text)
            for child in root:
                ranks = OrderedDict()
                for item in child.iter('{*}hierarchyList'):
                    rank = item.findtext('{*}rankName')
                    name = item.findtext('{*}taxonName')
                    if rank is not None:
                        #logger.debug(u'{} == {}'.format(rank.upper(), name))
                        ranks[rank.lower()] = name.lower()
                return ranks


    def map_to_department(self, hierarchy):
        """Maps a taxonomic hierarchy to an NMNH department"""
        # Ensure that the hierarchy has at least one of the keys needed for
        # the comparison to NMNH departments
        avail = set([mp.rank for mp in self.mappings])
        if not [k for k in avail if k in hierarchy]:
            logger.debug(u'Not enough info to place the following taxon:')
            for rank in hierarchy:
                logger.debug(u'{} == {}'.format(rank.upper(), hierarchy[rank]))
            return
        # Assign to a division based on the mapping
        for mp in self.mappings:
            eq = hierarchy.get(mp.rank) == mp.value.strip('!').lower()
            if mp.value.startswith('!'):
                eq = not eq
            if eq:
                logger.debug(u'Mapped to {}'.format(mp.dept))
                return mp.dept
        else:
            # Log failures
            logger.debug(u'Could not classify the following taxon:')
            for rank in hierarchy:
                logger.debug(u'{} == {}'.format(rank.upper(), hierarchy[rank]))


    def match_dept_keywords(self, text, i=None, j=None):
        """Matches a list of keywords"""
        words = [w for w in re.split(r'\W', text.lower())]
        for dept in list(self.keywords.keys())[i:j]:
            for pattern in self.keywords[dept]:
                for word in words:
                    if re.match('^' + pattern + '$', word, flags=re.I):
                        if len(word) < 3:
                            raise ValueError('Bad pattern in {}: {}'.format(dept, pattern))
                        logger.debug(u'Matched {} on keyword {}={}'.format(dept, pattern, word.lower()))
                        return dept, word.lower()
        return None, None


    @staticmethod
    def clean_names(names):
        """Standardizes the formatting of a list of names"""
        cleaned = []
        for name in names:
            cleaned.extend([s.strip() for s in re.split(r'[:\(\)]', name)])
        return sorted(list(set([n for n in cleaned if n])))


    @staticmethod
    def score_match(names, taxon):
        names = [s.lower() for s in names]
        for key in ['class', 'order', 'family']:
            for name in names:
                if name == taxon.get(key, '').lower():
                    logger.debug(u'Scored match at {0:.1f} points'.format(1))
                    return 1
        taxon = [s.lower() for s in list(taxon.values())]
        logger.debug(u'Names: {}'.format('; '.join(names)))
        logger.debug(u'Taxon: {}'.format('; '.join(taxon)))
        score = 0
        for name in names:
            if name in taxon:
                score += 1
            elif any([(name in s) for s in taxon]):
                score += 0.5
        score = score / len(names)
        logger.debug(u'Scored match at {0:.1f} points'.format(score))
        return score


    @staticmethod
    def guess_department(depts):
        counts = {}
        for dept, score in list(depts.items()):
            try:
                counts[dept] += score
            except KeyError:
                counts[dept] = score
        return [dept for dept, count in counts.items()
                if count == max(counts.values())][0]


    def get_department(self, text, **kwargs):
        logger.debug(u'Matching department in "{}"'.format(text))
        # Check keyword lists for non-biological collections, including paleo
        dept, match = self.match_dept_keywords(text, j=3)
        if dept:
            return dept
        # Check mappings and hints for a previously encountered match
        words = [s.lower() for s in re.split(r'\W', text) if s]
        for mapping in self.mappings:
            if mapping.value.lower() in words:
                return mapping.dept
        for key, dept in list(self.hints.items()):
            if key.lower() in [s.lower() for s in words]:
                logger.debug(u'Matched {}={} in hints'.format(key, dept))
                return dept
        # Proceed with the more complex search since no easy match was found
        stop = False
        depts = {}
        names = self.get_names(text, **kwargs)
        for name in [n for n in names if len(n) > 6]:
            score = 0
            tsns = self.get_tsns(name)
            for tsn in self.get_tsns(name):
                hierarchy = self.get_hierarchy(tsn)
                if hierarchy:
                    # One more check against hints
                    for key in set([mp.rank for mp in self.mappings]):
                        try:
                            return self.hints[hierarchy[key]]
                        except KeyError:
                            pass
                    # No match, so try to determine the department
                    dept = self.map_to_department(hierarchy)
                    try:
                        logger.debug(u'Dept: {}'.format(self.depts[dept]))
                    except KeyError:
                        pass
                    else:
                        score = self.score_match(names, hierarchy)
                        try:
                            depts[dept] += score
                        except KeyError:
                            depts[dept] = score
                        msg = 'Cumulative score: {}={}'.format(dept, depts[dept])
                        logger.debug(msg)
                        if (score >= 0.8 or
                            depts[dept] >= 8 or
                            len(depts) == 1 and depts[dept] >= 5):
                                depts = {dept: score}
                                self.add_hint(hierarchy, dept)
                                stop = True
                                break
            if stop:
                break
        if depts:
            dept = self.guess_department(depts)
            logger.debug(u'Matched to {}'.format(self.depts[dept]))
            return dept
        # Check keyword lists for biological collections
        dept, match = self.match_dept_keywords(text, i=3)
        if dept:
            return dept


    def get_department2(self, text, **kwargs):
        logger.debug(u'Matching department in "{}"'.format(text))
        # Check keyword lists for non-biological collections, including paleo
        dept, match = self.match_dept_keywords(text, j=3)
        if dept:
            return dept
        # Check mappings and hints for a previously encountered match
        words = [s.lower() for s in re.split(r'\W', text) if s]
        for mapping in self.mappings:
            if mapping.value.lower() in words:
                return mapping.dept
        for key, dept in list(self.hints.items()):
            if key.lower() in [s.lower() for s in words]:
                logger.debug(u'Matched {}={} in hints'.format(key, dept))
                return dept
        # Proceed with the more complex search since no easy match was found
        stop = False
        depts = {}
        names = self.get_names(text, **kwargs)
        self.resolve_names(names)


    def add_hint(self, hierarchy, dept):
        # Add order and family to hints
        for rank in ['phylum', 'order', 'suborder', 'family']:
            try:
                name = hierarchy[rank].lower()
            except KeyError:
                pass
            else:
                if name not in ['arthropoda', 'chordata']:
                    self.hints[name] = dept
                    with open('hints.json', 'w') as f:
                        json.dump(self.hints, f, indent=4, sort_keys=True)
                    msg = 'Added {}={} to hints'.format(name, dept)
                    logger.debug(msg)
