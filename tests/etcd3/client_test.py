from __future__ import absolute_import

from time import sleep

import grpc
import os
import re
from contextlib import contextmanager
from nose.tools import eq_

from etcd3 import (ENV_ETCD3_CA, ENV_ETCD3_ENDPOINT, ENV_ETCD3_PASSWORD,
                   ENV_ETCD3_USER, _utils)
from etcd3._client import Client
from tests.toxiproxy import ToxiProxyClient

_ETCD_PROXY = 'etcd'


def setup_module():
    global _toxi_proxy_clt
    _toxi_proxy_clt = ToxiProxyClient()
    _toxi_proxy_clt.start()

    etcd_endpoint = os.getenv(ENV_ETCD3_ENDPOINT, '127.0.0.1:2379')
    user = os.getenv(ENV_ETCD3_USER, 'test')
    password = os.getenv(ENV_ETCD3_PASSWORD, 'test')
    cert_ca = os.getenv(ENV_ETCD3_CA, 'tests/fixtures/ca.pem')

    proxy_spec = _toxi_proxy_clt.add_proxy(_ETCD_PROXY, etcd_endpoint)
    proxy_endpoint = proxy_spec['listen']
    # Make sure to access proxy via 127.0.0.1 otherwise TLS verification fails.
    if proxy_endpoint.startswith('[::]:'):
        proxy_endpoint = '127.0.0.1:' + proxy_endpoint[5:]

    global _etcd3_clt
    _etcd3_clt = Client(proxy_endpoint, user, password, cert_ca=cert_ca)


def teardown_module():
    _toxi_proxy_clt.stop()


def test_get_does_not_exist():
    _etcd3_clt.delete('/test/', is_prefix=True)

    # When
    rs = _etcd3_clt.get('/test/foo')

    # Then
    _assert_get_one(None, rs)


def test_delete_one():
    _etcd3_clt.delete('/test/', is_prefix=True)

    _etcd3_clt.put('/test/foo1', 'bar1')
    _etcd3_clt.put('/test/foo2', 'bar2')
    _etcd3_clt.put('/test/foo21', 'bar3')
    _etcd3_clt.put('/test/foo212', 'bar4')
    _etcd3_clt.put('/test/foo3', 'bar5')

    # When
    rs = _etcd3_clt.delete('/test/foo2')

    # Then
    eq_(1, rs.deleted)
    _assert_get_one('bar1', _etcd3_clt.get('/test/foo1'))
    _assert_get_one(None, _etcd3_clt.get('/test/foo2'))
    _assert_get_one('bar3', _etcd3_clt.get('/test/foo21'))
    _assert_get_one('bar4', _etcd3_clt.get('/test/foo212'))
    _assert_get_one('bar5', _etcd3_clt.get('/test/foo3'))


def test_delete_range():
    _etcd3_clt.delete('/test/', is_prefix=True)

    _etcd3_clt.put('/test/foo1', 'bar1')
    _etcd3_clt.put('/test/foo2', 'bar2')
    _etcd3_clt.put('/test/foo21', 'bar3')
    _etcd3_clt.put('/test/foo212', 'bar4')
    _etcd3_clt.put('/test/foo3', 'bar5')

    # When
    rs = _etcd3_clt.delete('/test/foo2', is_prefix=True)

    # Then
    eq_(3, rs.deleted)
    _assert_get_one('bar1', _etcd3_clt.get('/test/foo1'))
    _assert_get_one(None, _etcd3_clt.get('/test/foo2'))
    _assert_get_one(None, _etcd3_clt.get('/test/foo21'))
    _assert_get_one(None, _etcd3_clt.get('/test/foo212'))
    _assert_get_one('bar5', _etcd3_clt.get('/test/foo3'))


def test_get_revisions():
    # Checks that create_revision and mod_revision are returned correctly.

    _etcd3_clt.delete('/test')

    # Given
    _etcd3_clt.put('/test/foo0', 'tox')
    create_rs = _etcd3_clt.put('/test/foo', 'bar')
    _etcd3_clt.put('/test/foo', 'bazz')
    _etcd3_clt.put('/test/foo2', 'fox')
    update_rs = _etcd3_clt.put('/test/foo', 'blah')
    _etcd3_clt.put('/test/foo3', 'socks')

    # When
    rs = _etcd3_clt.get('/test/foo')

    # Then
    eq_(create_rs.header.revision, rs.kvs[0].create_revision)
    eq_(update_rs.header.revision, rs.kvs[0].mod_revision)
    _assert_get_one('blah', rs)


def test_get_prefix():
    _etcd3_clt.delete('/test/', is_prefix=True)

    _etcd3_clt.put('/test/foo1', 'bar1')
    _etcd3_clt.put('/test/foo2', 'bar2')
    _etcd3_clt.put('/test/foo21', 'bar3')
    _etcd3_clt.put('/test/foo212', 'bar4')
    _etcd3_clt.put('/test/foo3', 'bar5')

    # When
    rs = _etcd3_clt.get('/test/foo2', is_prefix=True)

    # Then
    eq_(3, rs.count)
    eq_(b'bar2', rs.kvs[0].value)
    eq_(b'bar3', rs.kvs[1].value)
    eq_(b'bar4', rs.kvs[2].value)


def test_auto_reconnect():
    # After a server crash client restores connection with etcd automatically.

    _etcd3_clt.delete('/test')

    _etcd3_clt.put('/test/foo', 'bar')
    _assert_get_one('bar', _etcd3_clt.get('/test/foo'))

    _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=False)
    with _assert_raises_grpc_error(grpc.StatusCode.UNAVAILABLE,
                                   '(OS Error)|'
                                   '(Socket closed)|'
                                   '(Transport closed)'):
        _etcd3_clt.put('/test/foo', 'bazz1')

    # When: etcd gets back in service
    _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=True)

    # Then: client restores connection automatically
    _etcd3_clt.put('/test/foo', 'bazz2')
    _assert_get_one('bazz2', _etcd3_clt.get('/test/foo'))


def test_lease_expires():
    _etcd3_clt.delete('/test')

    rs = _etcd3_clt.lease_grant(1)
    _etcd3_clt.put('/test/foo', 'bar', rs.ID)
    _assert_get_one('bar', _etcd3_clt.get('/test/foo'))

    # When
    sleep(rs.TTL + 0.5)

    # Then
    _assert_get_one(None, _etcd3_clt.get('/test/foo'))


def test_lease_revoke():
    _etcd3_clt.delete('/test')

    rs = _etcd3_clt.lease_grant(600)
    _etcd3_clt.put('/test/foo', 'bar', rs.ID)
    _assert_get_one('bar', _etcd3_clt.get('/test/foo'))

    # When
    _etcd3_clt.lease_revoke(rs.ID)

    # Then
    _assert_get_one(None, _etcd3_clt.get('/test/foo'))


@contextmanager
def _assert_raises_grpc_error(code, pattern):
    try:
        yield
    except Exception as err:
        if not isinstance(err, grpc.RpcError):
            raise

        if err.code() != code or not re.match(pattern, err.details()):
            raise AssertionError('Unexpected RpcError: want=%s(%s), got=%s(%s)'
                                 % (code, pattern, err.code(), err.details()))


def _assert_get_one(want, range_rs):
    comment = 'Want %s, got %s' % (want, range_rs)
    if want is None:
        eq_(0, range_rs.count, comment)
        return

    eq_(1, range_rs.count, comment)
    eq_(_utils.to_bytes(want), range_rs.kvs[0].value, comment)


def _assert_event(t, k, v, got):
    eq_((t, k, v), (got.type, got.kv.key, got.kv.value))

