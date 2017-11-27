#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A base class to encapsulate the http request stuff"""

from __future__ import print_function, absolute_import

import requests

class MetricsReaderError(IOError):
    """ A error class, for all kind of exceptions for GraphiteDataReader """
    pass

class BaseReader(object):
    """
    This is a Base class for the different Datareaders
    """

    def __init__(self,
                 url=None,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30.,
                ):
        self._session = self._init_session(session)
        self._tls_verify = tls_verify
        self._timeout = timeout
        self.url = url

    def _get(self, url, params):
        """
        Makes http get request and check the status code

        Args:
            url: string
                the url for the request
            params: dict
                the query params for the request

        Returns:
            a requests object
        """
        r = self._session.get(url,
                              params=params,
                              verify=self._tls_verify,
                              timeout=self._timeout,
                             )
        if r.status_code != requests.codes['ok']:
            raise  MetricsReaderError(
                'Unable to read URL: {url} (status: {status_code})'
                .format(
                    url=r.url,
                    status_code=r.status_code,
                    )
                )
        return r

    @staticmethod
    def _init_session(session):
        """ create a default Session if no session is specified """
        if session is None:
            session = requests.Session()
        return session

if __name__ == "__main__":
    print(__doc__)
