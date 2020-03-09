from __future__ import absolute_import

import re
from contextlib import contextmanager
from time import sleep

import grpc
from nose.tools import assert_not_equal, assert_raises_regexp, eq_, with_setup

from etcd3 import Client, _utils
from etcd3._client import SortOrder, SortTarget
from tests.etcd3 import _fixture


def test_user_password_inconsistent():
    with assert_raises_regexp(AttributeError, 'Neither or both user and password should be specified'):
        Client(user='foo')

    with assert_raises_regexp(AttributeError, 'Neither or both user and password should be specified'):
        Client(password='foo')


def test_auth_no_tls():
    with assert_raises_regexp(AttributeError, 'Authentication is only allowed via TLS'):
        Client(user='foo', password='bar')


@with_setup(_fixture.setup, _fixture.teardown)
def test_get_does_not_exist():
    proxied_clt = _fixture.proxied_clt()

    # When
    rs = proxied_clt.get('/test/foo')

    # Then
    _assert_get_one(None, rs)


@with_setup(_fixture.setup, _fixture.teardown)
def test_delete_one():
    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo1', 'bar1')
    proxied_clt.put('/test/foo2', 'bar2')
    proxied_clt.put('/test/foo21', 'bar3')
    proxied_clt.put('/test/foo212', 'bar4')
    proxied_clt.put('/test/foo3', 'bar5')

    # When
    rs = proxied_clt.delete('/test/foo2')

    # Then
    eq_(1, rs.deleted)
    _assert_get_one('bar1', proxied_clt.get('/test/foo1'))
    _assert_get_one(None, proxied_clt.get('/test/foo2'))
    _assert_get_one('bar3', proxied_clt.get('/test/foo21'))
    _assert_get_one('bar4', proxied_clt.get('/test/foo212'))
    _assert_get_one('bar5', proxied_clt.get('/test/foo3'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_delete_range():
    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo1', 'bar1')
    proxied_clt.put('/test/foo2', 'bar2')
    proxied_clt.put('/test/foo21', 'bar3')
    proxied_clt.put('/test/foo212', 'bar4')
    proxied_clt.put('/test/foo3', 'bar5')

    # When
    rs = proxied_clt.delete('/test/foo2', is_prefix=True)

    # Then
    eq_(3, rs.deleted)
    _assert_get_one('bar1', proxied_clt.get('/test/foo1'))
    _assert_get_one(None, proxied_clt.get('/test/foo2'))
    _assert_get_one(None, proxied_clt.get('/test/foo21'))
    _assert_get_one(None, proxied_clt.get('/test/foo212'))
    _assert_get_one('bar5', proxied_clt.get('/test/foo3'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_get_revisions():
    # Checks that create_revision and mod_revision are returned correctly.

    proxied_clt = _fixture.proxied_clt()

    # Given
    proxied_clt.put('/test/foo0', 'tox')
    create_rs = proxied_clt.put('/test/foo', 'bar')
    proxied_clt.put('/test/foo', 'bazz')
    proxied_clt.put('/test/foo2', 'fox')
    update_rs = proxied_clt.put('/test/foo', 'blah')
    proxied_clt.put('/test/foo3', 'socks')

    # When
    rs = proxied_clt.get('/test/foo')

    # Then
    eq_(create_rs.header.revision, rs.kvs[0].create_revision)
    eq_(update_rs.header.revision, rs.kvs[0].mod_revision)
    _assert_get_one('blah', rs)


@with_setup(_fixture.setup, _fixture.teardown)
def test_get_prefix():
    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo1', 'bar1')
    proxied_clt.put('/test/foo2', 'bar2')
    proxied_clt.put('/test/foo21', 'bar3')
    proxied_clt.put('/test/foo212', 'bar4')
    proxied_clt.put('/test/foo3', 'bar5')

    # When
    rs = proxied_clt.get('/test/foo2', is_prefix=True)

    # Then
    eq_(3, rs.count)
    eq_(3, len(rs.kvs))
    eq_(b'bar2', rs.kvs[0].value)
    eq_(b'bar3', rs.kvs[1].value)
    eq_(b'bar4', rs.kvs[2].value)


@with_setup(_fixture.setup, _fixture.teardown)
def test_get_prefix_limit():
    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo1', 'bar5')
    proxied_clt.put('/test/foo2', 'bar4')
    proxied_clt.put('/test/foo21', 'bar3')
    proxied_clt.put('/test/foo212', 'bar2')
    proxied_clt.put('/test/foo3', 'bar1')

    # When
    rs = proxied_clt.get('/test/foo', is_prefix=True, limit=2)

    # Then
    eq_(5, rs.count)
    eq_(2, len(rs.kvs))


@with_setup(_fixture.setup, _fixture.teardown)
def test_get_prefix_sort():
    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo212', 'bar4')
    proxied_clt.put('/test/foo2', 'bar2')
    proxied_clt.put('/test/foo3', 'bar1')
    proxied_clt.put('/test/foo1', 'bar5')
    proxied_clt.put('/test/foo21', 'bar3')

    for i, tc in enumerate([{
        'order': SortOrder.ASCEND,
        'target': SortTarget.KEY,
        'out': [b'bar5', b'bar2', b'bar3', b'bar4', b'bar1'],
    }, {
        'order': SortOrder.ASCEND,
        'target': SortTarget.VALUE,
        'out': [b'bar1', b'bar2', b'bar3', b'bar4', b'bar5'],
    }, {
        'order': SortOrder.ASCEND,
        'target': SortTarget.CREATE_REVISION,
        'out': [b'bar4', b'bar2', b'bar1', b'bar5', b'bar3'],
    }, {
        'order': SortOrder.DESCEND,
        'target': SortTarget.KEY,
        'out': [b'bar1', b'bar4', b'bar3', b'bar2', b'bar5'],
    }, {
        'order': SortOrder.DESCEND,
        'target': SortTarget.VALUE,
        'out': [b'bar5', b'bar4', b'bar3', b'bar2', b'bar1'],
    }, {
        'order': SortOrder.DESCEND,
        'target': SortTarget.CREATE_REVISION,
        'out': [b'bar3', b'bar5', b'bar1', b'bar2', b'bar4'],
    }]):
        print('Test case #%d: %s/%s', i, tc['order'], tc['target'])

        # When
        rs = proxied_clt.get('/test/foo', is_prefix=True,
                             sort_order=tc['order'], sort_target=tc['target'])
        # Then
        eq_(tc['out'], [kv.value for kv in rs.kvs])


@with_setup(_fixture.setup, _fixture.teardown)
def test_endpoint_discovery():
    direct_clt = _fixture.direct_clt()

    first_endpoint = direct_clt.current_endpoint
    eq_(1, len(direct_clt._endpoint_balancer._endpoints))

    # When: any operation triggers connection & discovery, might as well be get
    direct_clt.get_value('/test/foo')

    # Then: 3 endpoints are discovered.
    discovered_endpoints = direct_clt._endpoint_balancer._endpoints
    eq_(3, len(set(discovered_endpoints)))
    # The position of the current endpoint is preserved.
    eq_(first_endpoint, direct_clt.current_endpoint)
    eq_(first_endpoint, discovered_endpoints[0])


@with_setup(_fixture.setup, _fixture.teardown)
def test_auto_reconnect_node():
    # If a connection with current node is lost then the client automatically
    # connects to another.

    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo', 'bar')
    _assert_get_one('bar', proxied_clt.get('/test/foo'))
    orig_endpoint = proxied_clt.current_endpoint

    i = 0
    while True:
        val = 'bazz%d' % (i,)
        endpoint = proxied_clt.current_endpoint

        # When
        _fixture.disable_endpoint(endpoint)
        try:

            # Then
            proxied_clt.put('/test/foo', val)

            assert_not_equal(endpoint, proxied_clt.current_endpoint)
            _assert_get_one(val, proxied_clt.get('/test/foo'))

        finally:
            _fixture.enable_endpoint(endpoint)

        # Eventually we should cycle through all endpoints and get back to the
        # origin.
        if endpoint == orig_endpoint:
            break


@with_setup(_fixture.setup, _fixture.teardown)
def test_auto_reconnect_outage():
    # Connection is automatically restored even after a cluster wide outage.

    proxied_clt = _fixture.proxied_clt()

    proxied_clt.put('/test/foo', 'bar')
    _assert_get_one('bar', proxied_clt.get('/test/foo'))

    _fixture.disable_all_endpoints()
    with _assert_raises_grpc_error(grpc.StatusCode.UNAVAILABLE,
                                   '(OS Error)|'
                                   '(Socket closed)|'
                                   '(Transport closed)|'
                                   '(Connect Failed)|'
                                   '(failed to connect to all addresses)'):
        proxied_clt.put('/test/foo', 'bazz1')

    # When: etcd gets back in service
    _fixture.enabled_all_endpoints()

    # Then: client restores connection automatically
    proxied_clt.put('/test/foo', 'bazz2')
    _assert_get_one('bazz2', proxied_clt.get('/test/foo'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_lease_expires():
    proxied_clt = _fixture.proxied_clt()

    rs = proxied_clt.lease_grant(1)
    proxied_clt.put('/test/foo', 'bar', rs.ID)
    _assert_get_one('bar', proxied_clt.get('/test/foo'))

    # When
    sleep(rs.TTL + 1)

    # Then
    _assert_get_one(None, proxied_clt.get('/test/foo'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_lease_revoke():
    proxied_clt = _fixture.proxied_clt()

    rs = proxied_clt.lease_grant(600)
    proxied_clt.put('/test/foo', 'bar', rs.ID)
    _assert_get_one('bar', proxied_clt.get('/test/foo'))

    # When
    proxied_clt.lease_revoke(rs.ID)

    # Then
    _assert_get_one(None, proxied_clt.get('/test/foo'))


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

