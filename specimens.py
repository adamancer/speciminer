"""Defines methods to work with USNM catalog numbers and specimen data"""
# FIXME: Allow user to disable short range (e.g., for Mineral Sciences)


import math
import os
import re
import time
from collections import namedtuple

import requests
import yaml
from nltk.corpus import stopwords
from unidecode import unidecode


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

SpecNum = namedtuple('SpecNum', ['code', 'prefix', 'number', 'suffix'])

LOG = open(os.path.join('output', 'match.log'), 'wb')


def epen(fn, *args, **kwargs):
    fn = os.path.basename(fn)
    return open(os.path.join(os.path.dirname(__file__), fn), *args, **kwargs)



class Parser(object):
    regex = yaml.load(epen(os.path.abspath('regex.yaml'), 'rb'))

    def __init__(self):
        # Create regex masks
        if '{prefix}' in self.regex['catnum']:
            self.regex['catnum'] = self.regex['catnum'].format(**self.regex)
            self.mask = re.compile(self.regex['mask'].format(**self.regex))
        else:
            self.mask = re.compile(self.regex['mask'])
        self.discrete = re.compile(self.regex['discrete_mask'].format(**self.regex))
        self.range = re.compile(self.regex['range_mask'].format(**self.regex))
        self.code = ''
        self.metadata = []
        self.expand_short_ranges = True


    def sprint(self, *msg):
        if self.regex['debug']:
            print ' '.join([s if isinstance(s, basestring) else repr(s) for s in msg])


    def findall(self, val):
        """Finds all likely catalog numbers within the given string"""
        matches = []
        for match in set([m[0] for m in self.mask.findall(val)]):
            if re.search('\d', match):
                matches.append(match)
        return sorted(matches)


    def parse(self, val, expand_short_ranges=True):
        """Parses catalog numbers from a string"""
        self.expand_short_ranges = expand_short_ranges
        val = val.decode('utf-8')
        try:
            return self._parse(val)
        except ValueError:
            return []


    def _parse(self, val):
        """Parses catalog numbers from a string"""
        self.sprint('Parsing {}...'.format(val))
        self.code = re.findall(self.regex['code'], val)[0]
        self.metadata = []
        nums = []
        for match in [m[0] for m in self.mask.findall(val)]:
            # The museum code interferes with parsing, so strip it here
            match = self.remove_museum_code(match)
            nums.extend(self.parse_discrete(match))
            nums.extend(self.parse_ranges(match))
        if not nums and self.is_range(val):
            self.sprint('Parsed as simple range:', val)
            nums = self.fill_range(val)
        if not nums:
            self.sprint('Parsed as catalog number:', val)
            nums = [self.parse_num(val)]
        # Are the lengths in the results reasonable?
        if len(nums) > 1:
            minlen = min([len(str(n.number)) for n in nums])
            if minlen < 4:
                maxlen = max([len(str(n.number)) for n in nums])
                nums = [n for n in nums if n.number > 10**(maxlen - 2)]
        nums = [self.stringify(n) for n in nums]
        nums = [n for i, n in enumerate(nums) if n not in nums[:i]]
        return nums


    def stringify(self, spec_num):
        delim_prefix = ' '
        delim_suffix = '-'
        if not spec_num.prefix:
            delim_prefix = ''
        elif len(spec_num.prefix) > 1:
            delim_prefix = ' '
        if ((spec_num.suffix.isalpha() and len(spec_num.suffix) == 1)
            or re.match('[A-Za-z]-\d+', spec_num.suffix)):
            delim_suffix = ''
        return '{} {}{}{}{}{}'.format(spec_num.code,
                                      spec_num.prefix,
                                      delim_prefix,
                                      spec_num.number,
                                      delim_suffix,
                                      spec_num.suffix).rstrip(delim_suffix)


    def parse_discrete(self, val):
        """Returns a list of discrete specimen numbers"""
        val = re.sub(self.regex['filler'], '', val)
        prefix = re.match('(' + self.regex['prefix'] + ')', val)
        prefix = prefix.group() if prefix is not None else ''
        discrete = self.discrete.search(val)
        nums = []
        if discrete is not None:
            val = discrete.group().strip()
            val = self.cluster(self.fix_ocr_errors(val))
            if re.match(self.regex['catnum'] + '$', val):
                # Check if value is actually a single catalog number
                nums.append(self.parse_num(val.replace(' ', '')))
            else:
                # Check if specimen number is actually a range
                for spec_num in re.findall(self.regex['catnum'], val):
                    if not spec_num.startswith(prefix):
                        spec_num = prefix + spec_num
                    if self.is_range(spec_num):
                        nums.extend(self.fill_range(spec_num))
                    elif re.match(self.regex['catnum'] + '$', spec_num.strip()):
                        nums.append(self.parse_num(spec_num.replace(' ', '')))
                    else:
                        nums.append(self.parse_num(spec_num))
        if nums:
            self.sprint('Parsed discrete:', [self.stringify(n) for n in nums])
        return nums


    def parse_ranges(self, val):
        """Returns a list of specimen numbers given in ranges"""
        val = self.cluster(self.fix_ocr_errors(val))
        ranges = self.range.search(val)
        nums = []
        if ranges is not None:
            spec_num = ranges.group().strip()
            if self.is_range(spec_num):
                nums.extend(self.fill_range(spec_num))
            else:
                # Catch legitimate specimen numbers. Short ranges are caught
                # above, so anything that parses should be excluded here.
                try:
                    parsed = self.parse_num(spec_num)
                except ValueError:
                    # Finds ranges joined by something other than a dash
                    try:
                        n1, n2 = re.findall(self.regex['catnum'], spec_num)
                        n1, n2 = [self.parse_num(n) for n in (n1, n2)]
                    except ValueError:
                        pass
                    else:
                        nums.extend(self.fill_range(n1, n2))
        if nums:
            self.sprint('Parsed range:', [self.stringify(n) for n in nums])
        return nums


    def cluster(self, val, minlen=4, maxlen=6, related=None):
        """Clusters related digits to better resemble catalog numbers"""
        if related is None:
            related = []
        orig = val
        # Leave values with range keywords as-is
        if re.search(self.regex['join_range'], val):
            return val
        nums = re.findall(r'\b' + self.regex['number'], val)
        if nums:
            # Are all the numbers four digits or longer?
            if min([len(n) for n in nums]) >= 4:
                return ' '.join(nums)
            # Numbers are a mix of short and long numbers. This may be a
            # spacing issue, so see if we can combine the numbers intelligently.
            related += nums
            if maxlen is None:
                maxlen = max([len(n) for n in related])
            # Can shorter fragments be combined into that length?
            clustered = []
            fragment = ''
            for num in nums:
                fragment += num
                if len(fragment) == maxlen:
                    clustered.append(int(fragment))
                    fragment = ''
                elif len(fragment) > maxlen:
                    break
            else:
                # Combined groups of related numbers
                nums = [int(n) for n in nums if int(n) in clustered]
                for n in clustered:
                    mindiff = min([abs(n - m) for m in nums]) if nums else 0
                    if mindiff > 10:
                        nums = [n for n in nums if len(str(n)) > minlen]
                        break
                else:
                    nums = clustered
        nums = [str(num) for num in nums]
        if orig.replace(' ', '-') == '-'.join(nums):
            return orig
        self.sprint('Clustered:', orig, '=>', nums)
        return ' '.join(nums)


    def remove_museum_code(self, val):
        """Strips the museum code from the beginning of a value"""
        if self.code:
            return val.replace(self.code, '', 1).replace('()', '').strip(' -')
        return re.sub(self.regex['code'], '', val).replace('()', '').strip(' -')


    def parse_num(self, val):
        """Parses a catalog number into prefix, number, and suffix"""
        val = self.remove_museum_code(val.strip())
        val = re.sub(self.regex['filler'], '', val)
        # Identify prefix and number
        try:
            prefix = re.search(ur'\b^[A-Z ]+', val).group()
        except AttributeError:
            prefix = ''
        else:
            prefix = self.fix_ocr_errors(prefix, True)
            if prefix.isnumeric():
                prefix = ''
        # Format number
        number = val[len(prefix):].strip(' -')
        # Identify suffix
        suffix = ''
        for delim in ('--', ' - ', '-', ',', '/', '.'):
            try:
                number, suffix = number.split(delim, 1)
            except ValueError:
                delim = ''
            else:
                break
        # Clean up stray OCR errors in the number now suffix has been removed
        if not number.isdigit():
            number = ''.join([self.fix_ocr_errors(c) for c in number])
        # Identify trailing letters, wacky suffixes, etc.
        if not number.isdigit():
            try:
                trailing = re.search(self.regex['suffix2'], number).group()
            except AttributeError:
                pass
            else:
                suffix = trailing + delim + suffix
                number = number.rstrip(trailing)
        prefix = prefix.strip()
        number = self.fix_ocr_errors(number)
        suffix = self.fix_ocr_errors(suffix.strip(), match=True)
        return SpecNum(self.code, prefix, int(number), suffix.upper())


    @staticmethod
    def fix_ocr_errors(val, match=False):
        pairs = {
            u'I': u'1',
            u'l': u'1',
            u'O': u'0',
            u'S': u'5'
        }
        if match:
            return pairs.get(val, val)
        else:
            for search, repl in pairs.iteritems():
                val = val.replace(search, repl)
            return val


    def fill_range(self, n1, n2=None):
        """Fills a catalog number range"""
        derived_n2 = False
        if n2 is None:
            n1, n2 = self.get_range(n1)
            derived_n2 = True
        if n1.prefix and not n2.prefix:
            n2 = SpecNum(n2.code, n1.prefix, n2.number, n2.suffix)
        if self.is_range(n1, n2):
            return [SpecNum(self.code, n1.prefix, n, '')
                    for n in xrange(n1.number, n2.number + 1)]
        # Range parse failed!
        return [n1, n2] if not derived_n2 else [n1]


    def get_range(self, n1, n2=None):
        """Generates all the catalog numbers in a catalog number range"""
        if n2 is None:
            n1, n2 = self.split_num(n1)
        if not self._is_range(n1, n2):
            n1, n2 = self.short_range(n1, n2)
        return n1, n2


    def split_num(self, val, delim='-'):
        """Splits the catalog number and suffix for range testing"""
        n1, n2 = [n.strip() for n in val.strip().split(delim)]
        n1, n2 = [self.parse_num(n) for n in (n1, n2)]
        if n1.prefix and not n2.prefix:
            n2 = SpecNum(n2.code, n1.prefix, n2.number, n2.suffix)
        return n1, n2


    def is_range(self, n1, n2=None):
        """Tests if a given value is likely to be a range"""
        if n2 is None:
            try:
                n1, n2 = self.split_num(n1)
            except ValueError:
                return False
        is_range = self._is_range(n1, n2)
        if not is_range and self.expand_short_ranges:
            is_range = self._is_range(*self.short_range(n1, n2))
        return is_range


    def _is_range(self, n1, n2=None):
        """Tests if a given pair of numbes are likely to be a range"""
        if n2 is None:
            n1, n2 = self.split_num(n1)
        same_prefix = n1.prefix == n2.prefix
        big_numbers = n1.number > 100 and n2.number > 100
        small_diff = n2.number - n1.number < 30
        no_suffix = not n1.suffix and not n2.suffix
        n2_bigger = n2.number > n1.number
        return bool(same_prefix
                    and (big_numbers or small_diff)
                    and small_diff
                    and no_suffix
                    and n2_bigger)


    def short_range(self, n1, n2):
        """Expands numbers to test for short ranges (e.g., 123456-59)"""
        x = 10.**(len(str(n2.number)))
        num = int(math.floor(n1.number / x) * x) + n2.number
        n2 = SpecNum(n2.code, n2.prefix, num, n2.suffix)
        return n1, n2




def get_specimens(catnum=None, **kwargs):
    """Returns specimen metadata from the Smithsonian"""
    url = 'http://supersite.local/portal'
    #url = 'https://geogallery.si.edu/portal'
    headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
    params = {
        'dept': 'any',
        'format': 'json',
        'schema': 'simpledwr',
        'limit': 1000
    }
    if catnum is not None:
        params['sample_id'] = Parser().remove_museum_code(catnum)
    params.update(**kwargs)
    response = requests.get(url, headers=headers, params=params)
    #print 'Checking {}...'.format(response.url)
    if not '.local' in url and hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(1)
    if response.status_code == 200:
        try:
            content = response.json().get('response', {}).get('content', {})
            records = content.get('SimpleDarwinRecordSet', [])
        except AttributeError:
            return []
        else:
            return [rec['SimpleDarwinRecord'] for rec in records]
    return []


def get_keywords(text, minlen=5, blacklist=None, endings=None, replacements=None):
    if blacklist is None:
        blacklist = [
            'above',
            'along',
            'animalia',
            'beach',
            'boundary',
            'coast',
            'county',
            'creek',
            'district',
            'early',
            'eastern',
            'family',
            'formation',
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
            'genus',
            'group',
            'present',
            'province',
            'ridge',
            'river',
            'south',
            'southern',
            'sp',
            'specimen',
            'united states',
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
        if (re.search('^[A-Za-z]+$', word)
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
        dept = depts[dept]
    parser = Parser()
    try:
        refnum = parser.parse_num(refnum)
    except ValueError:
        return []
    scored = []
    for rec in records:
        score = 0
        # Check catalog number
        try:
            catnum = rec['catalogNumber'].upper().split('|')[-1].strip()
            catnum = parser.parse_num(catnum)
        except (IndexError, KeyError, ValueError):
            continue
        else:
            # Exclude records with one-character prefixes if the refnum
            # is not prefixed. Other departments appear to have prefixes for
            # internal use (e.g., paleo uses PAL and V) that are not (or are
            # not always) given when that specimen is cited in the literature.
            if not refnum.prefix and catnum.prefix and len(catnum.prefix) == 1:
                score -= 1
            if catnum.prefix != refnum.prefix and catnum.prefix == 'SD':
                score -= 1
            # Exclude records that don't have the same base number
            if catnum.number != refnum.number:
                score -= 100
        # Check collectionCode against topic
        if dept is not None:
            if rec.get('collectionCode') == dept:
                score += 1
            else:
                score -= 100
        # Check taxa against keywords from publication title
        if keywords:
            # Get taxonomy
            higher_class = rec.get('higherClassification', '').lower()
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
                'aeo': 'eo'
            }
            score += score_match(catnum, rec, 'higherClassification', keywords, multiplier=5, endings=endings, replacements=replacements)
            score += score_match(catnum, rec, 'vernacularName', keywords, multiplier=3, match_all=True)
            score += score_match(catnum, rec, ['group', 'formation', 'member'], keywords, multiplier=3)
            score += score_match(catnum, rec, 'country', keywords, match_all=True)
            score += score_match(catnum, rec, ['stateProvince', 'municipality', 'verbatimLocality'], keywords)
        if score >= 1:
            scored.append([rec, score])
    if scored:
        max_score = max([s[1] for s in scored]) if scored else 0
        #if refnum.number == 344300:
        #    for m in scored:
        #        print m[0]['occurrenceID'], m[1]
        #    raw_input()
        return [m[0]['occurrenceID'] for m in scored if m[1] == max_score]
    return []


def score_match(catnum, rec, keys, refwords, multiplier=1, match_all=False, **kwargs):
    if not isinstance(keys, list):
        keys = [keys]
    if kwargs:
        refwords = get_keywords(' '.join(refwords), **kwargs)
    words = [rec[k] for k in keys if rec.get(k) is not None]
    keywords = get_keywords(' '.join(words), **kwargs)
    match = None
    score = 0
    if keywords:
        match = keywords & refwords
        if match_all and len(match) == len(keywords):
            score = multiplier
        elif not match_all:
            score = multiplier * len(match)
    # Log information about the match
    if False:
        if hasattr(catnum, 'prefix'):
            catnum = Parser().stringify(catnum)
        log = [
            u'Catalog num:   {}'.format(catnum),
            u'Keys:          {}'.format(keys),
            u'Ref. keywords: {}'.format(list(refwords)),
            u'Rec. words:    {}'.format(words),
            u'Rec. keywords: {}'.format(list(keywords)),
            u'Matches:       {}'.format(match),
            u'Score:         {}'.format(score),
            u'-' * 80
        ]
        LOG.write('\n'.join(log) + '\n')
    return score




RESULTS = []

if __name__ == '__main__':
    # Test filter_records
    if False:
        import pprint as pp
        catnum = u'USNM 147442'
        records = get_specimens(catnum)
        keywords = get_keywords('arthropods')
        pp.pprint(records)
        print filter_records(records, catnum, keywords=keywords)
    # Test the catalog number parser
    if True:
        parser = Parser()
        print '-' * 60
        print 'Testing parser'
        print '-' * 60
        for val in Parser.regex['test']:
            matches = parser.findall(val)
            parsed = []
            for m in matches:
                parsed.extend(parser.parse(m))
            print 'VERBATIM:', val
            print 'MATCHES: ', matches
            print 'PARSED:  ', parsed
            print '-' * 60
            if Parser.regex['troubleshoot']:
                break
