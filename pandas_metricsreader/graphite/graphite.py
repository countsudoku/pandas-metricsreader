# -*- coding: utf-8 -*-

""" A class to get Data from Graphite """

from __future__ import print_function, absolute_import

import urlparse

from pandas import read_csv, MultiIndex, concat, DataFrame, to_datetime
from pandas.compat import StringIO, string_types

from pandas_metricsreader.BaseReader import BaseReader, MetricsReaderError

class GraphiteReader(BaseReader):
    """
    Creates a GraphiteDataReader object, which you can use to read different
    metrics in a pandas DataFrame

    Arguments:
        url (str): the base url to the Graphite host
        tls_verify (str or bool, optional): enable or disable certificate
            validation. You can als specify the path to a certificate or a
            directory, which must have been processed using the c_rehash utily
            supplied with OppenSSL.  The default is the standard linux
            certificate trust store (/etc/ssl/certs)
        session (:py:obj:`requests.Session`, optional):
            a :py:class:`requests.Session` object (default None)
        timeout (float or tuple, optional): the connect and read timeouts (see
            the requests documentation under `Timeouts <requests:Timeouts>`_
            for details)

    """
    def __init__(self,
                 url,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30.,
                ):

        self._format = 'json'
        self._render_api = '/render'
        self._base_tz = 'UTC'

        super(GraphiteReader, self).__init__(
            url=url,
            tls_verify=tls_verify,
            session=session,
            timeout=timeout,
        )

    def read(self,
             targets,
             start=None,
             end=None,
             create_multiindex=True,
             remove_redundant_indices=True,
            ):
        """ read the data from Graphite

        Arguments:
            targets (str or list[str] or dict): the metrics you want to look up
            start (str, optional): the starting date timestamp.
                All Graphite datestrings are allowed (see `Graphite documentation <http://graphite-api.readthedocs.io/en/latest/api.html#from-until>`_ for details)
            end (str, optional): the ending date timestamp, same as start date
            create_multiindex (bool, optional): split the metrics names and
                create a hierarchical Index.
            remove_redundant_indices (bool, optional): Remove all redundant
                rows from the hierarchical Index. This does only have an
                affect, if you have more then one metric and if
                `create_multiindex` is set to True.

        returns:
            a pandas DataFrame with the requested Data from Graphite

        """
        # sanity checks
        if not self.url:
            raise MetricsReaderError('No URL specified')
        else:
            url = urlparse.urljoin(self.url, self._render_api)

        if isinstance(targets, string_types):
            df = self._download_single_metric(url, targets, start, end)
            if create_multiindex:
                self._create_multiindex(df, remove_redundant_indices)
        elif isinstance(targets, list):
            dfs = []
            for target in targets:
                dfs.append(self._download_single_metric(url, target, start, end))
            df = concat(dfs, axis=1)
            if create_multiindex:
                self._create_multiindex(df, remove_redundant_indices)
        elif isinstance(targets, dict):
            dfs = {}
            for label, target in targets.items():
                dfs[label] = self._download_single_metric(url, target, start, end)
                if create_multiindex:
                    self._create_multiindex(dfs[label], remove_redundant_indices)
            df = concat(dfs, axis=1)
        else:
            raise TypeError('targets has to be of type str, list or dict')

        return df

    def _download_single_metric(self, url, target, start, end):
        """ downloads of the specified target

        Args:
            url: string
                The Graphite render url
            target: string
                The metric you want do download
            start: string
                The start date (see the graphite documentation for possible
                values)
            end: string
                the end date (same as start)

        returns:
            a pandas.DataFrame or Panel
        """
        params = { 'target': target,
                   'from': start,
                   'until': end,
                   'format': self._format, }
        r = self._get(url, params=params)

        if self._format == 'json':
            if not r.json:
                raise MetricsReaderError(
                    'Received empty dataset for target {target}'.format(
                        target=target,
                    )
                )
            # generator with dataframes for all returned metrics
            dfs = ( DataFrame(
                data['datapoints'],
                columns=[data['target'], 'datetime' ],
                ).set_index('datetime')
                    for data in r.json() )
            df = concat(dfs, axis=1)
            # Parse the epoch datetime index and set the _base_tz timezone
            df.index = to_datetime(
                (df.index.values*1e9).astype(int)
                ).tz_localize(self._base_tz)
            return df

        if self._format == 'csv':
            if not r.text:
                raise MetricsReaderError(
                    'Received empty dataset for target {target}'.format(
                        target=target,
                    )
                )
            df = read_csv( StringIO(r.text),
                           names=['metric', 'datetime', 'data'],
                           parse_dates=['datetime'],
                           index_col=['metric', 'datetime'],
                           squeeze=False,
                         ).unstack('metric')['data']
        return df

    @staticmethod
    def _create_multiindex(DataFrame, remove_redundant_indices=False):
        """ Tries to find the field that differs in the DataFrame and remove
        all other column levels"""

        # split the metrics on a dot
        columns = [ column.split('.') for column in DataFrame.columns.values ]
        row_idx = []

        # padding
        max_length = 0
        for column in columns:
            max_length = max(max_length, len(column))
        for column in columns:
            if len(column) < max_length:
                column.extend(['' for _ in range(max_length - len(column)) ])

        # check, which metric fields differ
        if remove_redundant_indices and (len(columns) > 1):
            for index, column in enumerate(columns[:-1]):
                for sec_column in columns[index+1:]:
                    for idx, names in enumerate(zip(column, sec_column)):
                        if names[0] != names[1] and idx not in row_idx:
                            row_idx.append(idx)
            row_idx.sort()
            new_columns = []
            for column in columns:
                new_columns.append([ column[idx] for idx in row_idx])
        else:
            new_columns = columns

        DataFrame.columns = MultiIndex.from_tuples(new_columns)
        DataFrame.sort_index(axis=1, inplace=True)

if __name__ == "__main__":
    print(__doc__)
