# -*- coding: utf-8 -*-

""" A class to get Data from Graphite """

from __future__ import print_function, absolute_import

from six.moves import urllib
from six import StringIO, string_types

from pandas import read_csv, MultiIndex, concat, DataFrame, to_datetime

from ..BaseReader import BaseReader, MetricsReaderError
from .metricsAPI import GraphiteMetricsAPI

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
            the requests documentation under `Timeouts`_
            for details)

    .. _Timeouts: http://docs.python-requests.org/en/master/user/quickstart/#timeouts

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

        self.metrics = GraphiteMetricsAPI(
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
                All Graphite datestrings are allowed (see Graphite
                documentation under `from-until
                <http://graphite-api.readthedocs.io/en/latest/api.html#from-until>`_
                for details)
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
            url = urllib.parse.urljoin(self.url, self._render_api)

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

    def walk(self, top=None, start=None, end=None):
        """ Generate the target names in the Graphite target tree by walking
        the tree down. This creates a :func:`os.walk` like generator for the
        Graphite metrics.

        Arguments:
            top (str, optional): the target, where the walk starts (without a trailing
                asterisk)
            start (str, optional): the starting date timestamp.
                All Graphite datestrings are allowed (see Graphite
                documentation under `from-until
                <http://graphite-api.readthedocs.io/en/latest/api.html#from-until>`_
                for details)
            end (str, optional): the ending date timestamp, same as start date

        Returns:
            a generator object, which yields a 3-tuple ``(targetname, non-leafs,
            leafs)`` for each metric.

            *targetname* is the current walk position in the target tree.
            *non-leafs* are all child targets of *targetname*, which do not
            contain any data. *leafs* are all child targets of *targetname*,
            which do hold data. Hence you can use the :func:`read` method to
            read data from all *leafs*.
        """
        if top is None:
            path = '*'
        else:
            path = top.rstrip('.*') + '.*'
        metrics = self.metrics.find(path, start, end)
        leafs = set()
        internal_nodes = set()
        for metric in metrics:
            try:
                if metric['allowChildren'] == 1:
                    internal_nodes.add(metric['id'])
                if metric['leaf'] == 1:
                    leafs.add(metric['id'])
            except KeyError:
                raise MetricsReaderError('Unknown metrics format')

        yield (top.rstrip('.*'), list(internal_nodes), list(leafs))
        for node in internal_nodes:
            for branch in self.walk(node, start, end):
                yield branch

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
            json_data = r.json()
            if not json_data:
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
                    for data in json_data )
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
