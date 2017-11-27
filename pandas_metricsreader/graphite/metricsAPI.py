#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A class, which implements the metric API of graphite """

from __future__ import print_function, absolute_import

import os

from six.moves import urllib

from ..BaseReader import BaseReader

class GraphiteMetricsAPI(BaseReader):
    """
    Creates a GraphiteMetricAPI object, which you can use to request, which
    metrics are available in the graphite cluster

    Arguments:
        url (str):
            the base url to the Graphite host
        tls_verify (string or bool):
            enable or disable certificate validation. You can als specify the
            path to a certificate or a directory, which must have been
            processed using the c_rehash utily supplied with OppenSSL
            (default: True)
        session: a requests.Session object (default None)
        timeout (float or tuple)
    """

    def __init__(self,
                 url,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30,
                ):
        self._metrics_api = '/metrics'

        super(GraphiteMetricsAPI, self).__init__(
            url=url,
            tls_verify=tls_verify,
            session=session,
            timeout=timeout,
            )

    def find(self, target, start=None, end=None):
        """
        Finds metrics under a given path.
        """
        url_path = os.path.join(self._metrics_api, 'find')
        url = urllib.parse.urljoin(self.url, url_path)
        params = { 'query': target,
                   'formater': 'treejson',
                   'from': start,
                   'until': end,
                   'wildcards': 0,
                 }
        r = self._get(url, params=params)
        return r.json()

    def expand(self, targets, group_by_expr=False, leaves_only=False):
        """
        Expands the given query with matching paths.
        """
        if not isinstance(group_by_expr, bool):
            raise TypeError('group_by_expr has to be of type bool')
        if not isinstance(leaves_only, bool):
            raise TypeError('leaves_only has to be of type bool')

        if group_by_expr:
            group_by_expr = 1
        else:
            group_by_expr = 0

        if leaves_only:
            leaves_only = 1
        else:
            leaves_only = 0

        url_path = os.path.join(self._metrics_api, 'expand')
        url = urllib.parse.urljoin(self.url, url_path)
        params = { 'query': targets,
                   'groupByExpr':group_by_expr,
                   'leavesOnly':leaves_only,
                 }
        r = self._get(url, params=params)
        return r.json()['results']

    def index(self):
        """
        Walks the metrics tree and returns every metric found as a sorted JSON
        array.
        """
        raise NotImplementedError

if __name__ == "__main__":
    print(__doc__)
