"""Defines methods to work with USNM catalog numbers and specimen data"""

import logging
logger = logging.getLogger(__name__)

import math
import os
import re
import time
from collections import namedtuple

import requests
import yaml
from unidecode import unidecode

from cluster import Cluster, epen


IndexedSnippet = namedtuple('IndexedSnippet', ['text', 'start', 'end'])
SpecNum = namedtuple('SpecNum', ['code', 'prefix', 'number', 'suffix'])


class Parser(object):

    def __init__(self):
        self.regex = yaml.load(epen(os.path.abspath('regex.yml'), 'rb'))
        self.regex['catnum'] = self.regex['catnum'].format(**self.regex)
        self.mask = re.compile(self.regex['mask'].format(**self.regex))
        self.simple = re.compile(self.regex['simple'])
        self.discrete = re.compile(self.regex['discrete_mask'].format(**self.regex))
        self.suf_range = re.compile(r'(([A-z])' + self.regex['join_range'].format(**self.regex) + r'([A-z]))')
        self.range = re.compile(self.regex['range_mask'].format(**self.regex))
        self.code = ''
        self.codes = [s.strip() for s in self.regex['code'].strip('()').split('|')]
        self.metadata = []
        self.expand_short_ranges = True
        self._cluster = Cluster()


    def sprint(self, *msg):
        if self.regex['debug']:
            try:
                print ' '.join([s if isinstance(s, basestring) else repr(s) for s in msg]).encode('cp1252')
            except UnicodeEncodeError:
                pass


    def cluster(self, val):
        return self._cluster.cluster(val)


    def findall(self, text):
        """Finds all likely catalog numbers within the given string"""
        logging.info('Search "%s"', text)
        matches = []
        for match in set([m[0] for m in self.mask.findall(text)]):
            if re.search(r'\d', match):
                matches.append(match)
        matches.sort()
        logging.info(u'Found catalog numbers: %s"', matches)
        return sorted(matches, key=len)


    def snippets(self, text, num_chars=32, highlight=True, pages=None):
        """Find all occurrences of a pattern in text"""
        snippets = {}
        for match in self.mask.finditer(text):
            val = match.group()
            start = match.start()
            i = start - num_chars
            j = i + len(val) + 2 * num_chars
            if i < 0:
                i = 0
            if j > len(text):
                j = len(text)
            # Construct the snippet
            snippet = []
            if i:
                snippet.append('...')
            snippet.append(text[i:j].strip())
            if j < len(text):
                snippet.append('...')
            snippet = ''.join(snippet)
            if highlight:
                snippet = snippet.replace(val, '**' + val + '**')
            snippet = IndexedSnippet(snippet, start, start + len(val))
            snippets.setdefault(val, []).append(snippet)
        return snippets


    def keywords(self, val, text, num_chars=32, highlight=True, pages=None):
        """Finds all occurrences of a string in text"""
        delimited = r'\.? ?'.join([c for c in val])



    def parse(self, val, expand_short_ranges=True):
        """Parses catalog numbers from a string"""
        self.expand_short_ranges = expand_short_ranges
        val = val.decode('utf-8')
        orig = val
        # Split value on strings that look like museum codes. This works
        # okay for now, but would fail on something like Yale Peabody (YPM).
        # First make sure a space occurs between each museum code and the
        # rest of the string.
        for code in self.codes:
            if val.endswith('({})'.format(code)):
                val = code + ' ' + val[:-(len(code) + 2)].strip()
                logging.debug('Moved "%s" to front of string', code)
            val = val.replace(code, code + ' ').replace('  ', ' ')
        words = [w for w in re.split('([A-Z]{3,} ?)', val) if w and w not in '()']
        code = ''
        held = []
        for i, word in enumerate(words):
            if word.strip() in self.codes:
                code = word.strip()
                held.append([word])
                self.sprint('Found museum code "{}"'.format(word.strip()))
            elif word.strip().isalpha() and i and words[i - 1].strip() != code:
                code = ''
            elif code and word.strip():
                held[-1].append(word)
        vals = []
        for val in held:
            if len(val) > 1:
                val = ''.join(val)
                try:
                    parsed = self._parse(val)
                except ValueError:
                    logging.warning('Could not parse "%s" from "%s"', val, orig)
                except:
                    logging.error('Could not parse "%s" from "%s"', val, orig)
                else:
                    vals.extend(parsed)
                    logging.info('Parsed "{}" as {}'.format(val, parsed))
            else:
                logging.warning('Museum code only: %s', orig)
        return vals


    def _parse(self, val):
        """Parses catalog numbers from a string"""
        self.sprint(u'Parsing "{}"...'.format(val))
        # Clean up the string a little to simplify parsing
        val = val.replace('--', '-') \
                 .replace('^', '') \
                 .replace(' and ', ' & ') \
                 .strip('(),;& ')
        # Remove the museum code, wherever it may be
        self.code = re.findall(self.regex['code'], val)[0].strip()
        # Check for high-quality numbers and bail
        if self.simple.search(val):
            return [val]
        self.metadata = []
        nums = []
        for match in [m[0] for m in self.mask.findall(val)]:
            # The museum code interferes with parsing, so strip it here
            match = self.remove_museum_code(match)
            nums.extend(self.parse_discrete(match))
            nums.extend(self.parse_ranges(match))
        if not nums and self.is_range(val):
            self.sprint(u'Parsed as simple range:', val)
            nums = self.fill_range(val)
        if not nums:
            self.sprint(u'Parsed as catalog number:', val)
            val = self.remove_museum_code(val)
            nums = [self.parse_num(self.cluster(val))]
        # Are the lengths in the results reasonable?
        if len(nums) > 1:
            minlen = min([len(str(n.number)) for n in nums])
            if minlen < 4:
                maxlen = max([len(str(n.number)) for n in nums])
                nums = [n for n in nums if n.number > 10**(maxlen - 2)]
        nums = [self.stringify(n) for n in nums]
        nums = [n for i, n in enumerate(nums) if n not in nums[:i]]
        if 'type' in val.lower():
            nums = [n.replace(' ', ' type no. ', 1) for n in nums]
        return nums


    @staticmethod
    def stringify(spec_num):
        delim_prefix = ' '
        delim_suffix = '-'
        if not spec_num.prefix:
            delim_prefix = ''
        elif len(spec_num.prefix) > 1:
            delim_prefix = ' '
        if ((spec_num.suffix.isalpha() and len(spec_num.suffix) == 1)
            or re.match(r'[A-Za-z]-\d+', spec_num.suffix)):
            delim_suffix = ''
        return '{} {}{}{}{}{}'.format(spec_num.code,
                                      spec_num.prefix,
                                      delim_prefix,
                                      spec_num.number,
                                      delim_suffix,
                                      spec_num.suffix).rstrip(delim_suffix) \
                                                      .strip()


    def split_nums(self, val):
        # Test if string can be split into valid catalog numbers
        vals = re.split(r'(,|;|&| and ', val)


    def parse_discrete(self, val):
        """Returns a list of discrete specimen numbers"""
        self.sprint('Looking for discrete numbers in "{}"...'.format(val))
        val = re.sub(self.regex['filler'], '', val)
        prefix = re.match('(' + self.regex['prefix'] + ')', val)
        prefix = prefix.group() if prefix is not None else ''
        discrete = self.discrete.search(val)
        if discrete is None:
            val = self.cluster(self.fix_ocr_errors(val))
            discrete = self.discrete.search(val)
        nums = []
        if discrete is not None:
            val = discrete.group().strip()
            val = self.cluster(self.fix_ocr_errors(val))
            if self.is_range(val):
                nums.extend(self.get_range(val))
            elif re.match('^' + self.regex['catnum'] + '$', val):
                # Check if value is actually a single catalog number
                nums.append(self.parse_num(val.replace(' ', '')))
            else:
                # Chunk the original string into individual catalog numbers.
                # Two primary ways of doing this have been considered:
                # Splitting on (1) the catnum regex or (2) the join_discrete
                # regex. Option 1 can break up ranges. Option 2 can break up
                # prefixed catalog numbers (e.g., PAL 76012). The code below
                # uses option 2 to reconstruct ranges in option 1.
                #
                # Test if split on common delimiters yields usable numbers.
                # This helps prevent catalog numbers from grabbing an alpha
                # prefix from the succeeding catalog number.
                spec_nums = [s.strip() for s in re.split(r'(?:,|;| and | & )', val)]
                for spec_num in spec_nums:
                    if not re.match('^' + self.regex['catnum'] + '$', spec_num):
                        spec_nums = re.findall(self.regex['catnum'], val)
                        break
                # Clean up suffixes after chunking into discrete parts
                for chunk in re.split(self.regex['join_discrete'], val):
                    if self.is_range(chunk):
                        rng = [self.stringify(n) for n in self.fill_range(chunk)]
                        spec_nums.extend(rng)
                        # Ensure that this chunk is not in spec_nums
                        spec_nums = [n for n in spec_nums if n != chunk]
                spec_nums = sorted(list(set(spec_nums)))
                for spec_num in spec_nums:
                    spec_num = self.remove_museum_code(spec_num)
                    if not spec_num.startswith(prefix):
                        spec_num = prefix + spec_num
                    if self.is_range(spec_num):
                        nums.extend(self.fill_range(spec_num))
                    elif re.match(self.regex['catnum'] + '$', spec_num.strip()):
                        nums.append(self.parse_num(spec_num.replace(' ', '')))
                    else:
                        nums.append(self.parse_num(spec_num))
        if nums:
            self.sprint(u'Parsed discrete:', [self.stringify(n) for n in nums])
        else:
            self.sprint('No discrete numbers found')
        return nums


    def parse_ranges(self, val):
        """Returns a list of specimen numbers given in ranges"""
        self.sprint('Looking for ranges in "{}"...'.format(val))
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
                    self.parse_num(spec_num)
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
            self.sprint(u'Parsed range:', [self.stringify(n) for n in nums])
        else:
            self.sprint('No ranges found')
        return nums


    @staticmethod
    def validate_trailer(vals):
        val = vals[-1].strip()
        refval = vals[0].strip()
        # Always ditch the trailer after a semicolon
        if len(vals) > 1 and ';' in vals[-2] and len(vals[-1]) <= 2:
            return False
        # Keep one-letter trailers if refval also ends with a letter
        if refval[-1].isalpha() and val.isalpha() and len(val) == 1:
            return True
        # Strongly considering ditching the trailer after a comma
        if (len(vals) > 1
            and ',' in vals[-2]
            and vals[-1].startswith(' ')
            and len(vals[-1]) <= 3):
                return False
        # Discard all-letter trailers longer than one character
        if re.search(r'^[A-z]{2,4}$', val):
            return False
        # Compare numeric portions
        try:
            nval = re.search(r'[\d ]+', val).group().replace(' ', '').strip()
            nref = re.search(r'[\d ]+', refval).group().replace(' ', '').strip()
        except AttributeError:
            pass
        else:
            if ((len(nval) > 2 and len(nval) - len(nref) <= 1)
                or len(nval) + len(nref) == 6):
                return True
        return False


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
            prefix = re.match(ur'\b[A-Z ]+', val).group()
        except AttributeError:
            prefix = ''
        else:
            prefix = self.fix_ocr_errors(prefix, True)
            if prefix.isnumeric():
                prefix = ''
        # Format number
        number = val[len(prefix):].strip(' -')
        # Identify suffix
        delims = ('--', ' - ', '-', ',', '/', '.')
        suffix = ''
        for delim in delims:
            try:
                number, suffix = number.rsplit(delim, 1)
            except ValueError:
                delim = ''
            else:
                # A value after a spaced out hyphen is unlikely to be a suffix
                if delim == ' - ':
                    suffix = ''
                strip_chars = ''.join(delims) + ' '
                number = number.rstrip(strip_chars)
                suffix = suffix.strip(strip_chars)
                break
        # Clean up stray OCR errors in the number now suffix has been removed
        if (not number.isdigit()
            and not (number[:-1].isdigit() and len(number) > 6)):
                number = ''.join([self.fix_ocr_errors(c) for c in number])
        # Identify trailing letters, wacky suffixes, etc.
        if not number.isdigit():
            try:
                trailing = re.search(self.regex['suffix2'], number).group()
            except AttributeError:
                pass
            else:
                suffix = (trailing + delim + suffix).strip()
                number = number.rstrip(trailing)
        prefix = prefix.strip()
        number = self.fix_ocr_errors(number)
        if len(number) < 6:
            suffix = self.fix_ocr_errors(suffix.strip(), match=True)
        return SpecNum(self.code, prefix, int(number), suffix.upper())


    @staticmethod
    def fix_ocr_errors(val, match=False):
        pairs = {
            u'i': u'1',
            u'I': u'1',
            u'l': u'1',
            u'O': u'0',
            u'S': u'5'
        }
        if match:
            return pairs.get(val, val)
        else:
            # Filter out likely strings
            words = []
            for word in re.split(r'(\W+)', val):
                filtered = word
                for key in pairs:
                    filtered = filtered.replace(key, '')
                if not filtered[:-1].isalpha():
                    for search, repl in pairs.iteritems():
                        word = word.replace(search, repl)
                words.append(word)
            return ''.join(words)


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
        if n1 is None and n2 is None:
            return False
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
        small_diff = n2.number - n1.number < 50
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


    def short_discrete(self, n1, n2):
        """Expands shorthand numbers (e.g., 194383-85/87/92/93/99)"""
        x = 10.**(len(str(n2.number)))
        num = int(math.floor(n1.number / x) * x) + n2.number
        n2 = SpecNum(n2.code, n2.prefix, num, n2.suffix)




if __name__ == '__main__':
    # Test cluster
    if False:
        parser = Parser()
        val = 'USNM 201 1 17, 201 1 19, 201 120a, b, d-f, and 201 123a-c'
        val = 'USNM 200961, 200982a, c, e, 201182a-e, 201183a, 201184'
        clustered = parser.cluster(val)
        print 'VERBATIM:  ', val
        print 'CLUSTERED: ', clustered
    # Test filter_records
    if False:
        import pprint as pp
        catnum = u'USNM 147442'
        records = get_specimens(catnum)
        keywords = get_keywords('arthropods')
        pp.pprint(records)
        print filter_records(records, catnum, keywords=keywords)
    # Test the catalog number parser
    if False:
        parser = Parser()
        print '-' * 60
        print 'Testing parser'
        print '-' * 60
        for val in parser.regex['test']:
            matches = parser.findall(val)
            parsed = []
            for m in matches:
                parsed.extend(parser.parse(m))
            print 'VERBATIM:', val
            print 'MATCHES: ', matches
            print 'PARSED:\n ', '\n  '.join(parsed)
            print '-' * 60
            if parser.regex['troubleshoot']:
                break
    if True:
        parser = Parser()
        if parser.regex['troubleshoot']:
            parser.regex['debug'] = 1
        for key in sorted(parser.regex['testdict']):
            expected = parser.regex['testdict'][key]
            if not parser.regex['troubleshoot'] or key == parser.regex['troubleshoot']:
                matches = parser.findall(key)
                parsed = []
                for m in matches:
                    parsed.extend(parser.parse(m))
                if set(parsed) != set(expected):
                    print u'{}: Failed'.format(key)
                    print u'    Found  :', parsed
                    print u'    Missing:', sorted(list(set(expected) - set(parsed)))
                    print u'    Extra  :', sorted(list(set(parsed) - set(expected)))
                elif parser.regex['troubleshoot']:
                    print u'{}: Passed'.format(key)