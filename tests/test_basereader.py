#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas_metricsreader.BaseReader import BaseReader, MetricsReaderError
import requests
import requests_mock
import pytest
import mock

test_url = 'https://example.net'

@pytest.fixture
def sample_reader():
    return BaseReader(url=test_url)

def test_basereader_without_session_without_url_except_raising_exception():
    with pytest.raises(Exception):
        BaseReader()

def test_basereader_without_session_with_url(sample_reader):
    assert sample_reader.url == test_url

@requests_mock.Mocker(kw='mock')
def test_basereader_get_without_session(sample_reader, **kw):
    mock = kw['mock']
    mock.get(test_url, text='foobar')
    r = sample_reader._get(test_url, {})
    assert isinstance(r, requests.models.Response)
    assert r.text == 'foobar'
    assert mock.called
    assert mock.call_count == 1

def test_basereader_get_with_session():
    get = mock.Mock(name='get')
    get.return_value.status_code = 200
    session = type('', (), {})
    session.get = get

    reader = BaseReader(
        session=session,
        url=test_url,
        timeout=13,
        tls_verify=False)
    assert reader.url == test_url
    r = reader._get(test_url, {})
    get.assert_called_once_with(test_url, params={}, timeout=13, verify=False)

@requests_mock.Mocker(kw='mock')
def test_basereader_get_to_wrong_url(sample_reader, **kw):
    mock = kw['mock']
    mock.get(test_url, text='foobar', status_code='400')
    with pytest.raises(MetricsReaderError):
        sample_reader._get(test_url, {})