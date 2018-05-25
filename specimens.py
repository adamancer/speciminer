"""Defines methods to work with USNM catalog numbers and specimen data"""

import os
import re
import time
from collections import namedtuple

import requests
import yaml


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


class Parser(object):
    regex = yaml.load(open('regex.yaml', 'rb'))

    def __init__(self):
        # Create regex masks
        self.mask = re.compile(self.regex['mask'].format(**self.regex))
        self.discrete = re.compile(self.regex['discrete_mask'].format(**self.regex))
        self.range = re.compile(self.regex['range_mask'].format(**self.regex))
        self.code = ''


    def findall(self, val):
        matches = []
        for match in set([m[0] for m in self.mask.findall(val)]):
            if re.search('\d', match):
                matches.append(match)
        return sorted(matches)


    def parse(self, val):
        """Parses catalog numbers from a string"""
        try:
            return self._parse(val)
        except ValueError:
            return []


    def _parse(self, val):
        """Parses catalog numbers from a string"""
        self.code = re.findall(self.regex['prefix'], val)[0]
        nums = []
        for match in [m[0] for m in self.mask.findall(val)]:
            nums.extend(self.parse_discrete(match))
            nums.extend(self.parse_ranges(match))
        if not nums:
            spec_num = self.split_num(val)
            nums = ['{}{}-{}'.format(spec_num.prefix,
                                     spec_num.number,
                                     spec_num.suffix).strip('-')]
        nums = [u'{} {}'.format(self.code, n) for n in nums]
        nums = sorted(list(set(nums)))
        return nums


    def parse_discrete(self, val):
        """Returns a list of discrete specimen numbers"""
        discrete = self.discrete.search(val)
        nums = []
        if discrete is not None:
            for spec_num in re.findall(self.regex['number'], discrete.group()):
                # Check if specimen number is actually a range
                if self.is_range(spec_num):
                    nums.extend(self.fill_range(spec_num))
                else:
                    nums.append(spec_num.strip())
        return nums


    def parse_ranges(self, val):
        """Returns a list of specimen numbers given in ranges"""
        ranges = self.range.search(val)
        nums = []
        if ranges is not None:
            spec_num = ranges.group()
            try:
                n1, n2 = re.findall(self.regex['number'], spec_num)
                n1, n2 = [self.split_num(n) for n in (n1, n2)]
            except ValueError:
                if self.is_range(spec_num):
                    nums.extend(self.fill_range(spec_num))
            else:
                nums.extend(self.fill_range(n1, n2))
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
        if self.code:
            return val.replace(self.code, '', 1).strip(' -')
        return re.sub(self.regex['prefix'], '', val).strip(' -')



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


    def fill_range(self, n1, n2=None):
        """Fills a catalog number range"""
        if n2 is None:
            n1, n2 = [n.strip() for n in n1.split('-')]
            n1, n2 = [self.split_num(n) for n in (n1, n2)]
        if n1.prefix and not n2.prefix:
            n2 = SpecNum(n2.code, n1.prefix, n2.number, n2.suffix)
        if self.is_range(n1, n2):
            return ['{}{}'.format(n1.prefix, n) for n in xrange(n1.number, n2.number + 1)]
        return [n1, n2]


    def is_range(self, n1, n2=None):
        """Tests if a given value is likely to be a range"""
        if n2 is None:
            try:
                n1, n2 = [n.strip() for n in n1.split('-')]
            except ValueError:
                return False
            else:
                n1, n2 = [self.split_num(n) for n in (n1, n2)]
        same_prefix = n1.prefix == n2.prefix
        big_numbers = n1.number > 100 and n2.number > 100
        big_diff = n2.number - n1.number > 100
        no_suffix = not n1.suffix and not n2.suffix
        n2_bigger = n2.number > n1.number
        return bool(same_prefix
                    and (big_numbers or big_diff)
                    and no_suffix
                    and n2_bigger)




def get_specimens(catnum, **kwargs):
    """Returns specimen metadata from the Smithsonian"""
    url = 'https://geogallery.si.edu/portal'
    headers = {'UserAgent': 'MinSciBot/0.1 (mansura@si.edu)'}
    params = {
        'dept': 'any',
        'sample_id': Parser().remove_museum_code(catnum),
        'format': 'json',
        'schema': 'simpledwr',
        'limit': 1000
    }
    params.update(**kwargs)
    response = requests.get(url, headers=headers, params=params)
    print 'Checking {}...'.format(response.url)
    if hasattr(response, 'from_cache') and not response.from_cache:
        pass # time.sleep(3)
    if response.status_code == 200:
        try:
            content = response.json().get('response', {}).get('content', {})
            records = content.get('SimpleDarwinRecordSet', [])
        except AttributeError:
            return []
        else:
            return [rec['SimpleDarwinRecord'] for rec in records]
    return []


def filter_records(records, refnum, keywords=None):
    """Returns records that match a reference catalog number"""
    parser = Parser()
    refnum = parser.split_num(refnum)
    scored = []
    for rec in records:
        score = 0
        # Check catalog number
        try:
            catnum = rec['catalogNumber'].upper().split('|')[-1].strip()
            catnum = parser.split_num(catnum)
            catnum = parser.parse_num(catnum)
        except (IndexError, KeyError, ValueError):
            pass
        else:
            # Exclude records with one-character prefixes if the refnum
            # is not prefixed. Other departments appear to have prefixes for
            # internal use (e.g., PAL) that are not (or are not always) given
            # when that specimen is cited in the literature.
            if not refnum.prefix and catnum.prefix and len(catnum.prefix) == 1:
                score -= 100
            # Exclude records that don't have the same base number
            if catnum.number != refnum.number:
                score -= 100
        # Check taxa against keywords from publication title
        if keywords:
            higher_class = rec.get('higherClassification', '').lower()
            taxa = set([t for t in higher_class.split(' | ') if t])
            matches = taxa & keywords
            score += len(matches)
        if score >= 0:
            scored.append([rec, score])
    if scored:
        max_score = max([s[1] for s in scored]) if scored else 0
        return [m[0]['occurrenceID'] for m in scored if m[1] == max_score]
    return []


if __name__ == '__main__':
    # Test the catalog number parser
    parser = Parser()
    print 'Testing parser\n-------------'
    for val in Parser.regex['test']:
        print val, '=>', parser.parse(val)
