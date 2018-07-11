from __future__ import absolute_import

from time import sleep

import os
from nose.tools import eq_

from etcd3 import (ENV_ETCD3_CA, ENV_ETCD3_ENDPOINT, ENV_ETCD3_PASSWORD,
                   ENV_ETCD3_USER)
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

    global _direct_clt
    _direct_clt = Client(etcd_endpoint, user, password, cert_ca=cert_ca)

    proxy_spec = _toxi_proxy_clt.add_proxy(_ETCD_PROXY, etcd_endpoint)
    proxy_endpoint = proxy_spec['listen']
    # Make sure to access proxy via 127.0.0.1 otherwise TLS verification fails.
    if proxy_endpoint.startswith('[::]:'):
        proxy_endpoint = '127.0.0.1:' + proxy_endpoint[5:]

    global _proxied_clt
    _proxied_clt = Client(proxy_endpoint, user, password, cert_ca=cert_ca)


def teardown_module():
    _toxi_proxy_clt.stop()


def test_normal_operation():
    # KeepAliver ensure that key exists while running and deletes it on stop.

    _proxied_clt.delete('/test')

    ttl = 2
    keep_aliver = _proxied_clt.new_keep_aliver('/test/foo', 'bar', ttl,
                                               spin_pause=0.2)
    keep_aliver.start()
    try:
        for i in range(3):
            sleep(ttl - 0.5)
            eq_(b'bar', _proxied_clt.get_value('/test/foo'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, _proxied_clt.get_value('/test/foo'))


def test_spin_pause_too_long():
    # Spin pause is adjusted if needed to be less then effective TTL.

    _proxied_clt.delete('/test')

    ttl = 3
    keep_aliver = _proxied_clt.new_keep_aliver('/test/foo', 'bar', ttl,
                                               spin_pause=10)
    keep_aliver.start()
    try:
        sleep(ttl - 0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, _proxied_clt.get_value('/test/foo'))


def test_auto_reconnect():
    # A keep alive object survives connection loses as long as they last less,
    # then the lease TTL.

    _proxied_clt.delete('/test')

    ttl = 2
    keep_aliver = _proxied_clt.new_keep_aliver('/test/foo', 'bar', ttl,
                                               spin_pause=0.2)
    keep_aliver.start()
    try:
        sleep(ttl - 0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))
        sleep(ttl - 0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))

        # If connection with Etcd is lost...
        _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=False)
        # ...the key expires
        sleep(ttl + 0.5)
        eq_(None, _direct_clt.get_value('/test/foo'))

        # When: etcd gets back in service
        _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=True)

        # Then: the key is automatically restored.
        sleep(0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))
        sleep(ttl - 0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, _proxied_clt.get_value('/test/foo'))


def test_etcd_down_on_start():
    # It is ok to create a keep_aliver when connection with Etcd is down. The
    # key will be created as soon as connection is restored.

    _proxied_clt.delete('/test')

    ttl = 2
    keep_aliver = _proxied_clt.new_keep_aliver('/test/foo', 'bar', ttl,
                                               spin_pause=0.2)
    # Simulate Etcd down on start.
    _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=False)

    keep_aliver.start()
    try:
        sleep(0.5)
        eq_(None, _direct_clt.get_value('/test/foo'))

        # When: etcd gets back in service.
        _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=True)

        # Then: the key is automatically created.
        sleep(0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))
        sleep(ttl - 0.5)
        eq_(b'bar', _proxied_clt.get_value('/test/foo'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, _proxied_clt.get_value('/test/foo'))
