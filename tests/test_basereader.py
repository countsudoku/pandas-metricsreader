#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Unit tests for the BaseReader class """

import requests
import requests_mock
import pytest
import mock

from pandas_metricsreader.BaseReader import BaseReader, MetricsReaderError

test_url = 'https://example.net'

@pytest.fixture()
def sample_reader():
    """ Most basic BaseReader object """
    return BaseReader(url=test_url)

def test_basereader_without_session_without_url_except_raising_exception():
    """ Test if BaseReader raises exception, if created without url """
    with pytest.raises(Exception):
        BaseReader()

def test_basereader_without_session_with_url(sample_reader):
    """ Test if BaseReader has correct url attribute """
    assert sample_reader.url == test_url

def test_basereader_get_without_session(sample_reader):
    """ Test functionality of a _get request with session created per default
    """
    with requests_mock.Mocker() as rmock:
        rmock.get(test_url, text='foobar')
        r = sample_reader._get(test_url, {})
    assert isinstance(r, requests.models.Response)
    assert r.text == 'foobar'
    assert rmock.called
    assert rmock.call_count == 1

def test_basereader_get_with_session():
    """ Test functionality of a _get request, if you hand over a session
    object to the BaseReader """
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

def test_basereader_unauthorized_get(sample_reader):
    """  Tests if a unauthorized request to a url, which needs authorization,
    fails """
    with requests_mock.Mocker() as rmock:
        rmock.get(test_url, text='foobar', status_code='401')
        with pytest.raises(MetricsReaderError):
            sample_reader._get(test_url, {})

def test_basereader_get_wrong_url(sample_reader):
    """ Test if a _get request fails, when connection to a non existing url

    XXX: Maybe we should catch this requests exception inside of the
    BaseReader module and return our own Exception to the user.
    """
    with requests_mock.Mocker() as rmock:
        rmock.get(test_url, exc=requests.exceptions.ConnectionError)
        with pytest.raises(requests.exceptions.ConnectionError):
            sample_reader._get(test_url, {})
