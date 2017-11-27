#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A class to get Data from PNP4Nagios """

from __future__ import print_function, absolute_import

from six.moves import urllib
from six import string_types

#import dateutil
import requests

import numpy as np
from pandas import DataFrame, to_datetime, MultiIndex, concat

from ..BaseReader import BaseReader, MetricsReaderError

class PNP4NagiosReader(BaseReader):
    """
    Creates a PNP4NagiosReader object, which you can use to read metrics in a
    pandas DataFrame

    Arguments:
        baseurl (str): the base url to the PNP4Nagios host
        tls_verify (str or bool, optional): enable or disable certificate validation. You can als
            specify the path to a certificate or a directory, which must have
            been processed using the c_rehash utily supplied with OppenSSL.
            The default is the standard linux certificate trust store
            (/etc/ssl/certs)
        session (:py:obj:`requests.Session`): a :py:obj:`requests.Session` object
             (default None)
        timeout (float or tuple): the connect and read timeouts (see the requests documentation
            under `Timeouts`_ for details)

    .. _Timeouts: http://docs.python-requests.org/en/master/user/quickstart/#timeouts
    """

    def __init__(self,
                 baseurl,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30,
                ):

        super(PNP4NagiosReader, self).__init__(
            baseurl,
            tls_verify,
            session,
            timeout,
            )
        self._controller = 'xport'
        self._format = 'json'
        self.base_tz = 'UTC'
        self.field_sep = '_'

    def read(self,
             hosts,
             service,
             start=None,
             end=None,
             view=None,
             create_multiindex=True,
            ):
        """ read the data from PNP4Nagios

        Arguments:
            hosts (str or list): the hosts you want have metrics for
            service (str): The service metric you want to look up.
            start (str, optional): the starting date timestamp.
                All PNP4Nagios datestrings are allowed (see PNP4Nagios
                documentation under timeranges_ for details)
            end (str, optional): the ending date timestamp, same as start date
            view (Integer, optional): limits the time range to the time period specified
                in the PNP4Nagios config (for details see PNP4Nagios
                documentation under timeranges_).
            create_multiindex (bool, optional): split the metrics names and create a
                hierarchical Index.

        returns:
            a pandas DataFrame with the requested Data from PNP4Nagios

        .. _timeranges: https://docs.pnp4nagios.org/pnp-0.6/timeranges
        """
        if isinstance(hosts, string_types):
            df = self._read_single_metric(hosts, service, start, end, view)
            if create_multiindex:
                self._create_multiindex(df, self.field_sep)
        elif isinstance(hosts, list):
            dfs = {}
            for host in hosts:
                dfs[host] = self._read_single_metric(host, service, start, end, view)
                if create_multiindex:
                    self._create_multiindex(dfs[host], self.field_sep)
            df = concat(dfs, axis=1)
        else:
            raise TypeError('host has to be of type str')
        return df

    def _read_single_metric(self,
                            host,
                            service,
                            start=None,
                            end=None,
                            view=None,
                           ):
        params = {
            'host': host,
            'srv' : service,
            'start': start,
            'end': end,
            'view': view,
            }
        url = urllib.parse.urljoin(
            self.url,
            "pnp4nagios/{controller}/{format}".format(
                controller=self._controller,
                format=self._format,
                )
            )
        r = self._get(url, params=params)
        json_data = r.json()
        if not json_data:
            raise MetricsReaderError(
                'Received empty dataset for host {host} and service {service}'.format(
                    host=host,
                    service=service,
                )
            )
        index = []
        data = []
        columns = json_data['meta']['legend']['entry']
        for elem in json_data['data']['row']:
            index.append(elem['t'])
            data.append(elem['v'])
        df = DataFrame(
            data=np.array(data, dtype=np.float64),
            columns=columns,
            index=np.array(index, dtype=np.int32),
            )

        df.index = to_datetime(
            (df.index.values * 1e9).astype(int)
            ).tz_localize(self.base_tz)
        return df

    @staticmethod
    def _create_multiindex(DataFrame, sep):
        columns = [tuple(col.split(sep)) for col in DataFrame.columns.str.strip(sep)]
        DataFrame.columns = MultiIndex.from_tuples(columns)
        DataFrame.sort_index(axis=1, inplace=True)

if __name__ == "__main__":
    print(__doc__)
