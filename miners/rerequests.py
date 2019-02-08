from __future__ import print_function
from __future__ import unicode_literals

from builtins import object
import logging
import time

import requests


class ReRequest(object):

    def __init__(self, max_retries=8, base_wait=2, min_wait=2, stop_codes=None):
        self.stop_codes = stop_codes if stop_codes is not None else [404]
        self.max_retries = max_retries
        self.base_wait = base_wait
        self.min_wait = min_wait
        self._methods = {
            'get': requests.get,
            'post': requests.post
        }


    def retry(self, method, *args, **kwargs):
        wait = 0
        i = 0
        while i <= self.max_retries:
            if wait:
                url = response.request.url
                msg = '{}: {}. Waiting {} seconds to retry (retries={})...'.format(url, code, wait, 1)
                print(msg)
                logging.debug(msg)
                time.sleep(wait)
            response = self._methods[method](*args, **kwargs)
            code = response.status_code
            if code == 200 or code in self.stop_codes:
                return response
            # Calculate next wait
            i, wait = self.backoff(i, code)
            i += 1


    def _backoff(self, i, code):
        return i, self.min_wait + self.base_wait ** i


    def backoff(self, i, code):
        return self._backoff(i, code)


    def get(self, *args, **kwargs):
        return self.retry('get', *args, **kwargs)


    def post(self, *args, **kwargs):
        return self.retry('post', *args, **kwargs)