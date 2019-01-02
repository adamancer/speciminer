"""Defines functions to cluster and expand catalog numbers found by regex"""

import logging
import os
import re

import yaml


def epen(fn, *args, **kwargs):
    fn = os.path.basename(fn)
    return open(os.path.join(os.path.dirname(__file__), fn), *args, **kwargs)


class Cluster(object):

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
        if self.regex['troubleshoot']:
            logging.basicConfig(level=logging.DEBUG)
        # A valid last number is any catalog number, suffix, or range. However,
        # the regexes defined in regex.yml are less selective than is needed
        # here. We'll limit a "good" value to a subset of high-quality matches.
        catnum = r'([A-Z]{1,3} ?\d{2,6}|\d{4,6})'
        self.p_catnum = r'^{catnum}(-{catnum})?$'.format(catnum=catnum)
        #self.p_catnum = r'^([A-Z]{1,3} ?)?\d{4,6}((-[A-Z]{1,3} ?)?\d{4,6})?$'
        self.p_suffix = r'^(\d{1,4}|[a-z](-[a-z]|[a-z]+)|[a-z]\d|\d[a-z])$'
        self.p_alpha_suffix = r'^([a-z](-[a-z]|[a-z]+)?|[a-z]\d|\d[a-z])$'


    def split_on_delim(self, val, delim=r'(,|;|\.|&| and )'):
        """Splits string on common delimiters"""
        return re.split(delim, val)


    def split_into_catnums(self, val):
        """Splits string based on catalog number regular expression"""
        pass


    def join(self, vals):
        """Joins values, adjusting spacing for punctuation"""
        delims = ';,'
        for delim in delims:
            if any([re.search('[^A-z0-9]', val) for val in vals]):
                joined = []
                for val in vals:
                    if val in delims:
                        joined[-1] += val
                    else:
                        joined.append(val)
                return ' '.join(vals).strip()
        return '; '.join(vals)


    def is_valid_catnum(self, val, minlen=4):
        """Tests if value is a valid catalog number"""
        result = bool(re.match('^' + self.regex['catnum'] + '$', val))
        if result:
            # Check if suffixed numbers are long enough
            vals = val.split('-')
            result = len(vals[0]) >= minlen
        logging.info('"%s" %slooks like a catnum', val, '' if result else 'does not ')
        return result


    def all_valid_catnums(self, vals, minlen=4, discard_delim=True):
        """Tests if all values in a list are valid catalog numbers"""
        if not isinstance(vals, list):
            vals = self.split_on_delim(val)
        if discard_delim:
            vals = [s for s in vals if re.search('[A-z0-9]', s)]
        result = all([self.is_valid_catnum(s.strip(), minlen=minlen) for s in vals])
        # Test if all values exceed the minimum length
        if result:
            result = all([len(s.strip()) > minlen for s in vals])
        # Test if any value ends with an alpha suffix range
        if result:
            result = not any([self.ends_with_range(s.strip()) for s in vals])
        return result


    def ends_with_range(self, val):
        return bool(re.search(r'[a-z]-[a-z]$', val))



    def trim_bad_values(self, val):
        """Trims unlikely catalog numbers/suffixes from a list"""
        logging.info('Trimming "%s"', val)
        orig = val
        # Strip out filler
        val = re.sub(self.regex['filler'], '', val)
        # Split on common delimiters
        vals = self.split_on_delim(val)
        # Iteratively trim last value
        while not self._validate_last(vals):
            vals = vals[:-1]
        # Go forward through the values, stopping at the first stray alpha
        for i, val in enumerate(vals):
            if val.strip().isalpha() and len(val.strip()) > 1:
                logging.debug('Trimmed "%s" (alphabetic)', val)
                break
        vals = vals[:i + 1]
        if vals != orig:
            logging.info('Trimmed to %s', vals)
        return self.join(vals).rstrip(' ,;&')


    def _validate_last(self, vals):
        """Checks if last value in list appears to be a cat number or suffix"""
        val = vals[-1].strip('# ')
        if val.replace(' ', '').isdigit() or self.ends_with_range(val):
            val = val.replace(' ', '')
        logging.debug('Checking %s...', val)
        # Always trim non-alphanumeric characters
        if re.match(r'[^A-z0-9]', val):
            logging.debug('Trimmed "%s" (not alphanumeric)', val)
            return False
        # If multiple values, consider the preceding delimiter. Semicolons
        # and commas are hard delimiters, and values after these characters
        # shoulder only be kept if they are either alpha suffixes or obvious
        # catalog numbers.
        is_valid = self.is_valid_catnum(val)
        is_digit = val.isdigit()
        is_suffix = bool(re.match(self.p_suffix, val))
        is_alpha_suffix = bool(re.match(self.p_alpha_suffix, val))
        logging.info('%s: is_valid=%s, is_digit=%s,'
                     ' is_suffix=%s, is_alpha_suffix=%s',
                     val, is_valid, is_digit, is_suffix, is_alpha_suffix)
        if len(vals) > 2:
            delim = vals[-2].strip()
            if delim in ',;':
                # Log each case separately for troubleshooting purposes
                if is_alpha_suffix and (len(val) > 1 or val not in ('lIO')):
                    logging.debug('"%s" is a valid alpha suffix', val)
                elif is_valid and not is_digit:
                    logging.debug('"%s" is an alphanumeric catnum', val)
                elif is_valid and is_digit and len(val) >= 4:
                    logging.debug('"%s" is a numeric catnum 4 digits or longer', val)
                else:
                    logging.debug('Trimmed "%s" (weak post-delim value)', val)
                    return False
            elif delim == '.' and is_alpha_suffix:
                return False
        logging.debug('Stopped trimming at "%s"', val)
        return True


    def expand_alpha_suffixes(self, val):
        # Get suffixes
        letters = 'abcdefghijklmnopqrstuvwxyz'
        # Look for suffix ranges (123456a-c)
        suf_range = self.suf_range.findall(val)
        if suf_range:
            i = letters.index(suf_range[0][1])
            j = letters.index(suf_range[0][3])
            suffixes = letters[i:j + 1]
        else:
            # Find discrete suffixes (123456a,b,d)
            suffixes = re.findall(r'(?<![A-z])([A-z])(?![A-z])', val)
        logging.debug('Suffixes in "%s": %s', val, suffixes)
        return suffixes


    def clean(self, vals):
        assert isinstance(vals, list)
        # Clean up the formatting of parts
        cleaned = []
        for val in vals:
            stripped = val.strip()
            # Remove words
            val = re.sub('[A-z]{2,}\.?', '', val)
            # Isolate ranges
            for suffix in self.suf_range.findall(val):
                cleaned.append(suffix[0])
                val = val.replace(suffix[0], '')
            if len(val) > 1 and not (stripped.isalpha() or stripped.isnumeric() or re.match(self.p_catnum, stripped)):
                cleaned.extend(list(val))
            elif val:
                cleaned.append(val)
        logging.debug('Cleaned: {}'.format(vals))
        return vals


    def combine(self, vals, minlen=None, maxlen=None, related=None):
        """Combines fragments and expands suffix ranges

        Numbers are a mix of short and long numbers. This may be a spacing
        issue, so try combining the numbers semi-intelligently.
        """
        if related is None:
            related = []
        # Are all values valid catalog numebrs?
        if self.all_valid_catnums(vals, minlen=4):
            logging.debug('Aborted: Numbers are already valid')
            return vals
        # Are all the numbers minlen digits or longer?
        nums = [p for p in vals if re.search(r'^(\d+[A-z]?|[A-z](-[A-z])?)$', p)]
        #if min([len(n) for n in nums]) >= minlen:
        #    logging.debug('Aborted: Numbers are already the right length')
        #    return ''.join(vals)
        related += nums
        if maxlen is None:
            maxlen = max([len(n) for n in related])
        # Can shorter fragments be combined into that length?
        clustered = []
        zap_frag = False
        fragment = ''
        for orig in vals:
            val = orig.rstrip(';& ')
            if re.match('[A-Z]{1,3} ?\d+', val):
                logging.info('"%s" treated as whole/partial catalog number', val)
                if fragment and not [n for n in clustered if n.startswith(fragment)]:
                    clustered.append(fragment)
                fragment = val
                zap_frag = False
            elif val.isnumeric():
                logging.info('"%s" treated as whole/partial catalog number', val)
                if len(fragment) == maxlen or zap_frag:
                    if fragment and not [n for n in clustered if n.startswith(fragment)]:
                        clustered.append(fragment)
                    fragment = ''
                    zap_frag = False
                fragment += val
                if len(fragment) > maxlen:
                    break
            elif val.isalpha() and len(val) == 1:
                logging.info('"%s" treated as one-character suffix', val)
                clustered.append((fragment + val).strip(';'))
                zap_frag = True
            elif val != ' ' and fragment:
                logging.info('"%s" treated as multi-character suffix', val)
                # Each val may contain one or more suffixes or suffix ranges
                for val in self.split_on_delim(val):
                    for suffix in self.expand_alpha_suffixes(val):
                        clustered.append(fragment + suffix)
                        zap_frag = True
            else:
                logging.warning('"%s" could not be combined', val)
        else:
            # Checks if the remaining fragment should be added to clustered
            if fragment and not [n for n in clustered if n.startswith(fragment)]:
                if len(fragment) >= minlen:
                    clustered.append(fragment)
                else:
                    logging.debug(u'Aborted: Bad fragment length')
                    return vals
            try:
                nums = [int(n) for n in nums if int(n) in clustered]
            except ValueError:
                nums = clustered
            else:
                for n in clustered:
                    mindiff = min([abs(n - m) for m in nums]) if nums else 0
                    if mindiff > 10:
                        nums = [n for n in nums if len(str(n)) > minlen]
                        break
                else:
                    nums = clustered
        logging.info('Combined: %s', nums)
        return nums


    def cluster(self, val, minlen=4, maxlen=6, related=None):
        """Clusters related digits to better resemble catalog numbers"""
        if related is None:
            related = []
        logging.debug('Clustering...')
        orig = val
        logging.debug('Orig: {}'.format(orig))
        # Format string to improve matches
        callback = lambda match: match.group(1).lower().strip(' -')
        val = re.sub('-? ?([A-z], ?[A-z](, ?[A-Z]))', callback, val)
        # Split off questionable numbers after the last delimiter. Earlier
        # versions of this part included the hyphen (-), but that doesn't work
        # well. A hyphen denotes a range, not a list, so it has a different
        # sense than the other characters included here.
        val = self.trim_bad_values(val)
        # Check for false hyphens and spacing errors
        if ' ' in val and len(val.replace(' ', '')) <= 10:
            val = val.replace(' ', '')
            logging.debug('Stripping spaces: %s', val)
        if val.count('-') == 1:
            n1, n2 = [s.strip() for s in val.split('-')]
            if len(n1) <= 3 and 2 <= len(n2) <= 4:
                val = n1 + n2
                logging.debug('Removing bad hyphen: %s', val)
        # Don't try to cluster single numbers
        if re.search(r'^\d+[a-z]?$', val):
            logging.debug('Aborted: Value appears to be a single number')
            return val
        # Don't try to cluster across different prefixes
        if re.search(r'\b\d+\b', val) and re.search(r'\b[A-Z]{1,3} ?\d+\b', val):
            logging.debug('Aborted: Value mixes prefixed and unprefixed numbers')
            return val
        # Leave values with / and a plausible suffix alone
        if val.count('/') == 1 and val.split('/')[-1].isnumeric():
            logging.debug('Aborted: Slash-delimited suffix')
            return val
        # Leave values with range keywords as-is
        if (re.search(self.regex['join_range'], val)
            and self.suf_range.search(val) is None):
            logging.debug('Aborted: Value may be a range')
            return val
        parts = [unicode(s) for s in re.split(r'([A-z]*\d+)', val) if s]
        logging.debug('Parts: {}'.format(parts))
        if parts:
            parts = self.combine(self.clean(parts), minlen=minlen,
                                 maxlen=maxlen, related=related)
        clustered = self.join(parts)
        logging.info('Clustered: %s', parts)
        return clustered
