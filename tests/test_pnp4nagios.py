#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Unit tests for the PNP4NagiosReader """

import mock
import pytest
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal

from pandas_metricsreader import PNP4NagiosReader
from pandas_metricsreader.BaseReader import BaseReader, MetricsReaderError

test_url = 'https://example.net'


@pytest.fixture
def testDataFrame(host='host1', service='cpu'):
    columns = [ '{host}_{service}_{desc}'.format(host=host, service=service, desc=s)
                for s in ['MIN', 'MAX', 'AVR'] ]
    df = pd.DataFrame(
        np.mod(np.arange(3*8).reshape(8, 3), 7),
        columns=columns,
        )
    return df

def mock_read_single_metric():
    read_single_metric = mock.Mock(
        name='read_single_metric',
        spec=PNP4NagiosReader._read_single_metric
        )
    read_single_metric.return_value = testDataFrame()
    return read_single_metric

def mock_create_multiindex():
    create_multiindex = mock.Mock(
        name='create_multiindex',
        spec=PNP4NagiosReader._create_multiindex
        )
    return create_multiindex

class Test_PNP4NagiosReader_object_creation(object):
    def test_pnp4nagiosreader_init_minimal(self):
        pnp4nagios = PNP4NagiosReader(baseurl=test_url)
        assert pnp4nagios.url == test_url
        assert isinstance(pnp4nagios, BaseReader)

    def test_pnp4nagiosreader_init_maximal(self):
        test_session = mock.Mock(name='Session')
        pnp4nagios = PNP4NagiosReader(
            baseurl=test_url,
            tls_verify=False,
            session=test_session,
            timeout=5,
            )
        assert pnp4nagios.url == test_url

class Test_PNP4NagiosReader_read(object):
    @pytest.fixture(scope='function')
    def pnp4nagios(self):
        pnp4nagios_reader = PNP4NagiosReader(baseurl=test_url)
        # mocking
        pnp4nagios_reader._read_single_metric = mock_read_single_metric()
        pnp4nagios_reader._create_multiindex = mock_create_multiindex()
        return pnp4nagios_reader

    def test_pnp4nagiosreader_read_single_host_without_multiindex(
            self,
            pnp4nagios,
            testDataFrame,
            ):
        df = pnp4nagios.read(hosts='host1', service='cpu', create_multiindex=False)
        assert_frame_equal(df, testDataFrame)
        assert_index_equal(df.index, testDataFrame.index)
        assert_index_equal(df.columns, testDataFrame.columns)
        pnp4nagios._read_single_metric.assert_called_once_with('host1', 'cpu', None, None, None)
        pnp4nagios._create_multiindex.assert_not_called()

    def test_pnp4nagiosreader_read_single_host_with_multiindex(self, pnp4nagios):
        df = pnp4nagios.read(hosts='host1', service='cpu')

        pnp4nagios._read_single_metric.assert_called_once_with('host1', 'cpu', None, None, None)
        pnp4nagios._create_multiindex.assert_called_once()

    def test_pnp4nagiosreader_read_multiple_hosts_without_multiindex(self, pnp4nagios):
        hosts = ['host1', 'host2', 'host3']
        df = pnp4nagios.read(hosts=hosts, service='cpu', create_multiindex=False)

        calls = [((host, 'cpu', None, None, None), {}) for host in hosts]
        pnp4nagios._read_single_metric.assert_has_calls(calls, any_order=True)
        pnp4nagios._create_multiindex.assert_not_called()

    def test_pnp4nagiosreader_read_multiple_hosts_with_multiindex(self, pnp4nagios):
        hosts = ['host1', 'host2', 'host3']
        pnp4nagios.read(hosts=hosts, service='cpu', create_multiindex=True)

        calls = [((host, 'cpu', None, None, None), {}) for host in hosts]
        pnp4nagios._read_single_metric.assert_has_calls(calls, any_order=True)
        pnp4nagios._create_multiindex.assert_called()

    def test_pnp4nagiosreader_read_with_wrong_host_type(self, pnp4nagios):
        with pytest.raises(TypeError):
            pnp4nagios.read(hosts=('host1', 'host2'), service='load')

class Test_PNP4NagiosReader_create_multiindex(object):
    # XXX: we should stress the create-multiindex method more
    # * with corrupted data
    # * with different hosts
    # etc.
    def test_create_multiindex_basic_conversation(self, testDataFrame):
        df = testDataFrame.copy()
        PNP4NagiosReader._create_multiindex(df, '_')
        assert_frame_equal(
            df.sort_index(axis=1).T.reset_index(drop=True),
            testDataFrame.sort_index(axis=1).T.reset_index(drop=True),
            )
        assert_index_equal(df.index, testDataFrame.index)
        assert isinstance(df.columns, pd.MultiIndex)
        for col in testDataFrame.columns:
            assert_series_equal(
                df.loc[:, tuple(col.split('_'))],
                testDataFrame.loc[:, col],
                check_names=False,
                )

class Test_PNP4NagiosReader_read_single_metric(object):
    @pytest.fixture
    def pnp4nagios(self):
        get = mock.Mock(name='get', spec=PNP4NagiosReader._get)
        pnp4nagios_reader = PNP4NagiosReader(baseurl=test_url)
        pnp4nagios_reader._get = get
        return pnp4nagios_reader

    def test_with_empty_json(self, pnp4nagios):
        pnp4nagios._get.return_value.json = lambda: {}

        with pytest.raises(MetricsReaderError):
            pnp4nagios._read_single_metric('host1', 'load')

    def test_with_simple_json_data(self, pnp4nagios):
        json_data = {
            'meta': {'legend':{'entry':[ 'host1' ]}},
            'data': {'row': [
                {'t': 0, 'v': 1},
                {'t': 1, 'v': 4},
                {'t': 2, 'v': 0},
                {'t': 3, 'v': 7},
                ]},
            }

        pnp4nagios._get.return_value.json = lambda: json_data
        df = pnp4nagios._read_single_metric('host1', 'load')
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)
